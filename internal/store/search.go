package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/nethinwei/sls-local/internal/fieldcol"
	"github.com/nethinwei/sls-local/internal/query"
)

// SearchParams 是一次日志检索。时间范围为 [From, To)，精确到纳秒。
type SearchParams struct {
	Project, Logstore string
	From, To          int64
	FromNs, ToNs      int32
	Where             query.Cond
	Limit, Offset     int64
	Reverse           bool          // true：新的在前
	PackMeta          bool          // 每行带上 __pack_meta__
	Stages            []query.Stage // 逐条执行的 SPL 命令
}

// SearchResult 是检索结果。
type SearchResult struct {
	Rows     [][]byte // 每行是按 SLS 字段顺序编码好的 JSON 对象
	Keys     []string // 返回行的键并集，按出现顺序（不含 __time__、__time_ns_part__、__pack_meta__）
	LastTime int64    // 最后一行的 __time__（没有行时为 0）
}

// source 是检索范围（project、logstore、时间）在快照里的数据，作为 FROM 后面的表，带上 extra 与检索条件、
// SPL 用到的字段列。按 bloom 跳过不可能满足检索条件的分段。
func (s *Store) source(ctx context.Context, sn *snapshot, p *SearchParams, extra ...string) (string, error) {
	return sn.sourceOf(ctx, p.Project, p.Logstore, p.keys(extra...), func(g *segment) bool {
		return g.tmax >= p.From && g.tmin <= p.To
	}, p.Where.Need)
}

// keys 是检索条件、SPL 与 extra 里用到的字段（fieldcol）。
func (p *SearchParams) keys(extra ...string) []string {
	sqls := append([]string{p.Where.SQL}, extra...)
	for _, st := range p.Stages {
		sqls = append(sqls, st.SQL)
	}
	return fieldcol.Keys(sqls...)
}

// where 是 [From.FromNs, To.ToNs) 与检索条件。纳秒部分为 0 时（整秒的范围，绝大多数查询）只是 t 上的区间：
// 按纳秒比较的 OR 条件要逐行算，分段上 200 万条要多花约 6 ms。单独的区间条件还能用上按块的 min/max 剪枝。
func (p *SearchParams) where() (string, []any) {
	where := "project = ? AND logstore = ?"
	args := []any{p.Project, p.Logstore}
	if p.FromNs == 0 {
		where += " AND t >= ?"
		args = append(args, p.From)
	} else {
		where += " AND t >= ? AND (t > ? OR coalesce(ns, 0) >= ?)"
		args = append(args, p.From, p.From, p.FromNs)
	}
	if p.ToNs == 0 {
		where += " AND t < ?"
		args = append(args, p.To)
	} else {
		where += " AND t <= ? AND (t < ? OR coalesce(ns, 0) < ?)"
		args = append(args, p.To, p.To, p.ToNs)
	}
	return where + " AND " + p.Where.SQL, append(args, p.Where.Args...)
}

// Search 按条件检索日志。
//
// 结果按时间排序取一页，而排序要看完时间范围内的全部命中行。多数检索（尤其是 *）在最近的一小段时间里就能
// 凑够一页，所以按时间切成互不重叠的窗口，从排在前面的一端逐个查，凑够就停（SLS 也会提前停止扫描）。
// 窗口之间按 t 有序，各窗口的结果依次拼接就是整体排序的结果；命中很少时每行也只扫一次。
func (s *Store) Search(ctx context.Context, p SearchParams) (*SearchResult, error) {
	sn := s.acquire()
	defer s.release(sn)
	where, args := p.where()
	order := "ASC"
	if p.Reverse {
		order = "DESC"
	}
	fields := ""
	for _, k := range p.keys() { // SPL 的 where、extend 引用字段列
		fields += ", " + fieldcol.Ref(k)
	}
	from, err := s.source(ctx, sn, &p)
	if err != nil {
		return nil, err
	}
	src, ext := splSource(`SELECT seq, t, ns, topic, source, tags, contents, pack_n, pack_i, orig_keys`+fields+` FROM `+from+` WHERE `+where+
		` AND t BETWEEN ? AND ?`, p.Stages)
	extCols := ""
	for _, c := range ext {
		extCols += ", " + c
	}
	q := fmt.Sprintf(`SELECT seq, t, ns, topic, source, tags, contents, coalesce(pack_n, 1), coalesce(pack_i, 0), orig_keys%s
		FROM (%s) ORDER BY t %s, coalesce(ns, 0) %s, seq %s LIMIT ? OFFSET ?`, extCols, src, order, order, order)
	count := `SELECT count(*) FROM (` + src + `)`

	// 各窗口（以及翻页时先数后取）的查询放在同一个事务里：看到的是同一时刻的数据（DuckDB 的快照隔离），
	// 查询之间有新日志写入也不会让结果重复或错开
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() // 只读，不提交

	assemble := needsAssembly(p.Stages)
	var splRows []splRow
	res := &SearchResult{}
	keys := newKeySet()
	limit, offset := p.Limit, p.Offset
	// add 把取到的一行加进结果
	add := func(r row, origKeys sql.NullString, extVals []any) {
		res.LastTime = r.t
		if assemble {
			splRows = append(splRows, (&r).assemble(p.Stages, extVals, p.PackMeta))
			return
		}
		res.Rows = append(res.Rows, r.encode(searchLayout, p.PackMeta, 0))
		if origKeys.Valid { // 有重复字段名：SLS 的 keys 列的是原始字段名，不含改名产生的
			var names []string
			_ = json.Unmarshal([]byte(origKeys.String), &names)
			keys.add(names...)
		} else {
			keys.addObject(r.contents)
		}
		if p.PackMeta {
			keys.add("__pack_meta__")
		}
		keys.add("__topic__", "__source__")
		keys.addObject(r.tags)
	}
	// scan 取窗口 [lo, hi] 里的一页，返回取到的行数
	// 分两步取只对 Parquet 分段划算：头部是 DuckDB 的表，读 contents 很便宜，多一次查询反而慢
	late := len(ext) == 0 && sn.hasSegments(p.Project, p.Logstore, p.From, p.To)
	scan := func(lo, hi, limit, offset int64) (int64, error) {
		if late {
			return s.scanLate(ctx, tx, sn, p, src, order, append(args, lo, hi, limit, offset), add)
		}
		rows, err := tx.QueryContext(ctx, q, append(args, lo, hi, limit, offset)...)
		if err != nil {
			return 0, queryError(err)
		}
		defer rows.Close()
		var n int64
		for rows.Next() {
			n++
			var r row
			var origKeys sql.NullString
			extVals := make([]any, len(ext))
			dest := []any{&r.seq, &r.t, &r.ns, &r.topic, &r.source, &r.tags, &r.contents, &r.packN, &r.packI, &origKeys}
			for i := range extVals {
				dest = append(dest, &extVals[i])
			}
			if err := rows.Scan(dest...); err != nil {
				return 0, err
			}
			add(r, origKeys, extVals)
		}
		if err := rows.Err(); err != nil {
			return 0, queryError(err)
		}
		return n, nil
	}

	var w timeWindows
	w.init(p.From, p.To, p.Reverse)
	for limit > 0 {
		lo, hi, ok := w.next()
		if !ok {
			break
		}
		if offset > 0 { // 整个窗口都在 offset 之内就只数一下，不取行
			var n int64
			if err := tx.QueryRowContext(ctx, count, append(args, lo, hi)...).Scan(&n); err != nil {
				return nil, queryError(err)
			}
			if n <= offset {
				offset -= n
				w.found(n, offset+limit)
				continue
			}
		}
		n, err := scan(lo, hi, limit, offset)
		if err != nil {
			return nil, err
		}
		if afterWindow != nil {
			afterWindow()
		}
		w.found(n+offset, limit-n) // 这个窗口命中了 offset 跳过的与取到的
		limit -= n
		offset = 0
	}
	res.Keys = keys.list
	if assemble {
		res.Rows, res.Keys = assembleRows(splRows)
	}
	return res, nil
}

// scanLate 分两步取一页（延迟物化）：先只按排序列排出这一页的 seq，再按 seq 取这几行的内容。一步取的话，
// 排序前要把窗口里所有命中行的 contents、tags 都读出来解压，窗口越大越亏。没有 extend 新增列时才这样取。
func (s *Store) scanLate(ctx context.Context, tx *sql.Tx, sn *snapshot, p SearchParams, src, order string, args []any,
	add func(row, sql.NullString, []any)) (int64, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT seq, t FROM (%s) ORDER BY t %s, coalesce(ns, 0) %s, seq %s
		LIMIT ? OFFSET ?`, src, order, order, order), args...)
	if err != nil {
		return 0, queryError(err)
	}
	var seqs []int64
	tlo, thi := int64(math.MaxInt64), int64(math.MinInt64)
	for rows.Next() {
		var seq, t int64
		if err := rows.Scan(&seq, &t); err != nil {
			rows.Close()
			return 0, err
		}
		seqs = append(seqs, seq)
		tlo, thi = min(tlo, t), max(thi, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, queryError(err)
	}
	if len(seqs) == 0 {
		return 0, nil
	}
	list := make([]string, len(seqs))
	for i, seq := range seqs {
		list[i] = strconv.FormatInt(seq, 10)
	}
	from, err := sn.source(ctx, p.Project, p.Logstore, tlo, thi, nil)
	if err != nil {
		return 0, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT seq, t, ns, topic, source, tags, contents, coalesce(pack_n, 1), coalesce(pack_i, 0), orig_keys
		FROM `+from+`
		WHERE project = ? AND logstore = ? AND t BETWEEN ? AND ? AND seq IN (`+strings.Join(list, ", ")+`)`,
		p.Project, p.Logstore, tlo, thi)
	if err != nil {
		return 0, queryError(err)
	}
	defer rows.Close()
	type fetched struct {
		r        row
		origKeys sql.NullString
	}
	bySeq := make(map[int64]fetched, len(seqs))
	for rows.Next() {
		var f fetched
		if err := rows.Scan(&f.r.seq, &f.r.t, &f.r.ns, &f.r.topic, &f.r.source, &f.r.tags, &f.r.contents,
			&f.r.packN, &f.r.packI, &f.origKeys); err != nil {
			return 0, err
		}
		bySeq[f.r.seq] = f
	}
	if err := rows.Err(); err != nil {
		return 0, queryError(err)
	}
	for _, seq := range seqs {
		f, ok := bySeq[seq]
		if !ok { // 同一个事务里的快照，不会发生
			return 0, fmt.Errorf("log %d disappeared between the two steps of a search", seq)
		}
		add(f.r, f.origKeys, nil)
	}
	return int64(len(seqs)), nil
}

// Count 数时间范围内满足条件的日志条数。
func (s *Store) Count(ctx context.Context, p SearchParams) (int64, error) {
	sn := s.acquire()
	defer s.release(sn)
	where, args := p.where()
	var n int64
	from, err := s.source(ctx, sn, &p)
	if err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM `+from+` WHERE `+where, args...).Scan(&n); err != nil {
		return 0, queryError(err)
	}
	return n, nil
}

// afterWindow 是测试钩子：每查完一个窗口调用一次。
var afterWindow func()

// timeWindows 把 [from, to]（秒）从一端起切成互不重叠的窗口。第一个窗口取总跨度的 1/32；之后按已扫部分的
// 命中密度估算还要多大范围才能凑够（多估 1/4），一直没有命中时剩下的一次查完。
type timeWindows struct {
	from, to  int64
	reverse   bool
	scanned   int64 // 已扫过的秒数
	hits      int64 // 已扫部分的命中行数（含 offset 跳过的）
	cur, size int64
	started   bool
}

// init 把窗口限定在 __time__ 可能的取值内（uint32 秒）：客户端传来的范围可以是任意 int64，不收紧的话
// 跨度会溢出。from > to 时没有窗口。
func (w *timeWindows) init(from, to int64, reverse bool) {
	from, to = max(from, 0), min(to, math.MaxUint32)
	*w = timeWindows{from: from, to: to, reverse: reverse}
	w.size = max((to-from+1)/32, 1)
}

// found 记录上一个窗口命中 n 行、还差 missing 行，据此定下一个窗口的大小。按密度估算并多留 1/4；
// 下限是已扫跨度的一半，保证窗口至少按 1.5 倍增长，查询次数是对数级的。
func (w *timeWindows) found(n, missing int64) {
	w.hits += n
	if w.hits == 0 {
		w.size = w.to - w.from + 1 // 没有命中时无从估算，剩下的一次查完（最坏情况只多一次小窗口查询）
		return
	}
	est := float64(w.scanned) * float64(missing) / float64(w.hits) * 1.25 // 用浮点数：missing 可以很大
	w.size = max(int64(min(est, float64(w.to-w.from+1))), w.scanned/2, 1)
}

func (w *timeWindows) next() (lo, hi int64, ok bool) {
	if !w.started {
		w.started = true
		if w.reverse {
			w.cur = w.to
		} else {
			w.cur = w.from
		}
	}
	size := max(w.size, 1)
	if w.from > w.to {
		return 0, 0, false
	}
	if w.reverse {
		if w.cur < w.from {
			return 0, 0, false
		}
		hi, lo = w.cur, max(w.cur-size+1, w.from)
		w.cur = lo - 1
	} else {
		if w.cur > w.to {
			return 0, 0, false
		}
		lo, hi = w.cur, min(w.cur+size-1, w.to)
		w.cur = hi + 1
	}
	w.scanned += hi - lo + 1
	return lo, hi, true
}

// QueryError 是语句执行时 DuckDB 报的错（类型不匹配、函数不存在等），按 ParameterInvalid 返回给客户端。
type QueryError struct{ Err error }

// Error 只取 DuckDB 报错的第一行：后面的 "LINE n: ..." 是 sls-local 改写后的内部 SQL，对用户没有意义。
// 翻译器用 error('SLS_ERROR:...') 抛出的错误原样返回；能对应上的 DuckDB 错误换成 SLS 的文案。
func (e *QueryError) Error() string {
	msg, _, _ := strings.Cut(e.Err.Error(), "\n")
	msg = strings.TrimSpace(msg)
	if _, raw, ok := strings.Cut(msg, slsErrorMarker); ok {
		return raw
	}
	if m := castError.FindStringSubmatch(msg); m != nil {
		typ := map[string]string{"INT64": "BIGINT", "INT32": "INTEGER", "INT16": "SMALLINT", "INT8": "TINYINT"}[m[2]]
		if typ == "" {
			typ = m[2]
		}
		return "ErrorType:SyntaxError.ErrorMessage:Can not cast '" + m[1] + "' to " + typ
	}
	return "invalid query: " + msg
}

// slsErrorMarker 标记翻译器生成的运行时错误（见 internal/presto），其后是 SLS 的原文。
const slsErrorMarker = "SLS_ERROR:"

var castError = regexp.MustCompile(`^Conversion Error: Could not convert string '(.*)' to ([A-Z0-9]+)`)

func (e *QueryError) Unwrap() error { return e.Err }

func queryError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &QueryError{err}
}

// ---------------- 行编码 ----------------

type row struct {
	seq, t                        int64
	packN, packI                  int32 // 所在包的条数、在包里的序号
	ns                            sql.NullInt32
	topic, source, tags, contents string
}

type layout int

const (
	// searchLayout：写入字段 → (__pack_meta__) → __topic__ → __source__ → tag → __time__ → __time_ns_part__
	searchLayout layout = iota
	// contextLayout：__index_number__ → 写入字段 → __pack_meta__ → tag → __time__ → __time_ns_part__，没有 topic/source
	contextLayout
)

// encode 拼出 SLS 的一行，字段顺序与真实 SLS 一致（docs/wire-findings.md）。
func (r *row) encode(l layout, packMeta bool, indexNumber int) []byte {
	var b bytes.Buffer
	b.Grow(len(r.contents) + len(r.tags) + 160)
	b.WriteByte('{')
	first := true
	field := func(k, v string) {
		if !first {
			b.WriteByte(',')
		}
		first = false
		writeString(&b, k)
		b.WriteByte(':')
		writeString(&b, v)
	}
	raw := func(obj string) { // 嵌入一个 JSON 对象的成员
		if len(obj) > 2 {
			if !first {
				b.WriteByte(',')
			}
			first = false
			b.WriteString(obj[1 : len(obj)-1])
		}
	}
	if l == contextLayout {
		field("__index_number__", strconv.Itoa(indexNumber))
		raw(r.contents)
		field("__pack_meta__", PackMeta(r.seq, r.packN, r.packI))
		raw(r.tags)
	} else {
		raw(r.contents)
		if packMeta {
			field("__pack_meta__", PackMeta(r.seq, r.packN, r.packI))
		}
		field("__topic__", r.topic)
		field("__source__", r.source)
		raw(r.tags)
	}
	field("__time__", strconv.FormatInt(r.t, 10))
	if r.ns.Valid {
		field("__time_ns_part__", strconv.FormatInt(int64(r.ns.Int32), 10))
	}
	b.WriteByte('}')
	return b.Bytes()
}

// PackMeta 是 GetContextLogs 用来定位一条日志的标识，格式与 SLS 相同：shard|游标|包的条数|在包里的序号。
// SLS 的游标是数字串的 base64，sls-local 用写入序号。
func PackMeta(seq int64, packN, packI int32) string {
	return "0|" + EncodeCursor(seq) + "|" +
		strconv.Itoa(int(packN)) + "|" + strconv.Itoa(int(packI))
}

// EncodeCursor 把写入序号编码成 SLS 形式的游标（数字串的 base64）。
func EncodeCursor(seq int64) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(seq, 10)))
}

// Cursor 是 GetCursor：begin 是最早一条的序号，end 是最新一条之后，Unix 秒是该时间之后最早的一条。
func (s *Store) Cursor(ctx context.Context, project, logstore, from string) (int64, error) {
	sn := s.acquire()
	defer s.release(sn)
	logs, err := sn.source(ctx, project, logstore, 0, math.MaxInt64, nil)
	if err != nil {
		return 0, err
	}
	var q string
	var args []any
	switch from {
	case "begin":
		q = `SELECT coalesce(min(seq), 0) FROM ` + logs + ` WHERE project = ? AND logstore = ?`
	case "end":
		q = `SELECT coalesce(max(seq), 0) + 1 FROM ` + logs + ` WHERE project = ? AND logstore = ?`
	default:
		t, err := strconv.ParseInt(from, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("from must be begin, end or unix seconds")
		}
		q = `SELECT coalesce(min(seq), (SELECT coalesce(max(seq), 0) + 1 FROM ` + logs + ` WHERE project = ? AND logstore = ?))
			FROM ` + logs + ` WHERE project = ? AND logstore = ? AND t >= ?`
		args = []any{project, logstore}
		args = append(args, project, logstore, t)
		var seq int64
		err = s.db.QueryRowContext(ctx, q, args...).Scan(&seq)
		return seq, err
	}
	var seq int64
	err = s.db.QueryRowContext(ctx, q, project, logstore).Scan(&seq)
	return seq, err
}

// ParsePackMeta 从 PackMeta 取回写入序号（也接受 v0.1 的写法：游标是十进制数字）。
func ParsePackMeta(meta string) (int64, bool) {
	parts := strings.Split(meta, "|")
	if len(parts) != 4 {
		return 0, false
	}
	cursor := parts[1]
	if b, err := base64.StdEncoding.DecodeString(cursor); err == nil {
		cursor = string(b)
	}
	seq, err := strconv.ParseInt(cursor, 10, 64)
	return seq, err == nil
}

type keySet struct {
	seen map[string]bool
	list []string
}

func newKeySet() *keySet { return &keySet{seen: map[string]bool{}} }

func (k *keySet) add(keys ...string) {
	for _, key := range keys {
		if !k.seen[key] {
			k.seen[key] = true
			k.list = append(k.list, key)
		}
	}
}

// addObject 按顺序加入一个 JSON 对象的键。
func (k *keySet) addObject(obj string) {
	dec := json.NewDecoder(strings.NewReader(obj))
	if _, err := dec.Token(); err != nil {
		return
	}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return
		}
		if key, ok := t.(string); ok {
			k.add(key)
		}
		if _, err := dec.Token(); err != nil {
			return
		}
	}
}

// ---------------- 上下文 ----------------

// ErrNoContext 表示锚点日志不存在或没有 __pack_id__。
var ErrNoContext = errors.New("log not found or has no __tag__:__pack_id__")

// ContextResult 是 GetContextLogs 的结果，Rows 按 -back … 0 … +forward 排列。
type ContextResult struct {
	Rows          [][]byte
	Back, Forward int
}

// Context 取锚点（写入序号 seq）前后同一来源的日志：同一 pack_id 前缀，按写入顺序。
func (s *Store) Context(ctx context.Context, project, logstore string, seq int64, back, forward int) (*ContextResult, error) {
	sn := s.acquire()
	defer s.release(sn)
	var packCtx sql.NullString
	anchor, err := sn.sourceSeq(ctx, project, logstore, seq, seq)
	if err != nil {
		return nil, err
	}
	err = s.db.QueryRowContext(ctx, `SELECT pack_ctx FROM `+anchor+
		` WHERE project = ? AND logstore = ? AND seq = ?`, project, logstore, seq).Scan(&packCtx)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !packCtx.Valid) {
		return nil, ErrNoContext
	}
	if err != nil {
		return nil, err
	}
	// 同一来源的日志在写入序号上通常挨得很近：先在锚点附近的 seq 范围里找，不够再扩大，最后才扫整个 logstore。
	// 范围内按 seq 排序取最近的几条；范围外的离锚点更远，所以凑够了就是最终结果。
	near := func(lo, hi int64, order string, limit int) ([]row, error) {
		var out []row
		for _, w := range []int64{10_000, 1_000_000, math.MaxInt64} {
			l, h := max(lo, seq-w), min(hi, seq+w)
			if w == math.MaxInt64 {
				l, h = lo, hi
			}
			from, err := sn.sourceSeq(ctx, project, logstore, l, h)
			if err != nil {
				return nil, err
			}
			q := `SELECT seq, t, ns, topic, source, tags, contents, coalesce(pack_n, 1), coalesce(pack_i, 0) FROM ` +
				from + ` WHERE project = ? AND logstore = ? AND pack_ctx = ?
				AND seq BETWEEN ? AND ? ORDER BY seq ` + order + ` LIMIT ?`
			if out, err = s.scanRows(ctx, q, project, logstore, packCtx.String, l, h, limit); err != nil {
				return nil, err
			}
			if len(out) >= limit || (l == lo && h == hi) {
				break
			}
		}
		return out, nil
	}
	before, err := near(0, seq-1, "DESC", back)
	if err != nil {
		return nil, err
	}
	rest, err := near(seq, math.MaxInt64, "ASC", forward+1)
	if err != nil {
		return nil, err
	}
	res := &ContextResult{Back: len(before), Forward: len(rest) - 1}
	for i := len(before) - 1; i >= 0; i-- {
		res.Rows = append(res.Rows, before[i].encode(contextLayout, true, -(i+1)))
	}
	for i, r := range rest {
		res.Rows = append(res.Rows, r.encode(contextLayout, true, i))
	}
	return res, nil
}

func (s *Store) scanRows(ctx context.Context, q string, args ...any) ([]row, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.seq, &r.t, &r.ns, &r.topic, &r.source, &r.tags, &r.contents, &r.packN, &r.packI); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---------------- 直方图 ----------------

// Histogram 按 interval 秒分桶计数，返回 桶起点（对齐到 interval 的整数倍）→ 条数。
// 时间范围 [from, to) 只按秒计（与 SLS 的 GetHistograms 一致）。
func (s *Store) Histogram(ctx context.Context, project, logstore string, from, to, interval int64, where query.Cond) (map[int64]int64, error) {
	sn := s.acquire()
	defer s.release(sn)
	hp := SearchParams{Project: project, Logstore: logstore, From: from, To: to, Where: where}
	src, err := s.source(ctx, sn, &hp)
	if err != nil {
		return nil, err
	}
	q := `SELECT t - t % ? AS b, count(*) FROM ` + src +
		` WHERE project = ? AND logstore = ? AND t >= ? AND t < ? AND ` +
		where.SQL + ` GROUP BY b`
	args := append([]any{interval, project, logstore, from, to}, where.Args...)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, queryError(err)
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var b, n int64
		if err := rows.Scan(&b, &n); err != nil {
			return nil, err
		}
		out[b] = n
	}
	return out, rows.Err()
}

// ---------------- 字段 ----------------

// FieldNames 返回 since 之后写入的日志里出现过的字段名与 tag 名（自动索引的 GetIndex 用）。
func (s *Store) FieldNames(ctx context.Context, project, logstore string, since int64) ([]string, error) {
	sn := s.acquire()
	defer s.release(sn)
	logs, err := sn.source(ctx, project, logstore, since, math.MaxInt64, nil)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT unnest(json_keys(contents)) FROM `+logs+` WHERE project = ? AND logstore = ? AND t >= ?
		UNION SELECT DISTINCT unnest(json_keys(tags)) FROM `+logs+` WHERE project = ? AND logstore = ? AND t >= ?`,
		project, logstore, since, project, logstore, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}
