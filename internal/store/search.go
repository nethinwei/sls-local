package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

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
	Rows  [][]byte // 每行是按 SLS 字段顺序编码好的 JSON 对象
	Keys  []string // 返回行的键并集，按出现顺序（不含 __time__、__time_ns_part__、__pack_meta__）
	Count int64    // 时间范围内命中的总条数（不受 limit/offset 影响）
}

func (p *SearchParams) where() (string, []any) {
	where := `project = ? AND logstore = ?
		AND (t > ? OR (t = ? AND coalesce(ns, 0) >= ?))
		AND (t < ? OR (t = ? AND coalesce(ns, 0) < ?))
		AND ` + p.Where.SQL
	args := []any{p.Project, p.Logstore, p.From, p.From, p.FromNs, p.To, p.To, p.ToNs}
	return where, append(args, p.Where.Args...)
}

// Search 按条件检索日志。
func (s *Store) Search(ctx context.Context, p SearchParams) (*SearchResult, error) {
	where, args := p.where()
	order := "ASC"
	if p.Reverse {
		order = "DESC"
	}
	src, ext := splSource(`SELECT seq, t, ns, topic, source, tags, contents, pack_n, pack_i FROM logs WHERE `+where, p.Stages)
	extCols := ""
	for _, c := range ext {
		extCols += ", " + c
	}
	q := fmt.Sprintf(`SELECT seq, t, ns, topic, source, tags, contents, coalesce(pack_n, 1), coalesce(pack_i, 0)%s, count(*) OVER () AS total
		FROM (%s) ORDER BY t %s, coalesce(ns, 0) %s, seq %s LIMIT ? OFFSET ?`, extCols, src, order, order, order)
	assemble := needsAssembly(p.Stages)
	var splRows []splRow
	rows, err := s.db.QueryContext(ctx, q, append(args, p.Limit, p.Offset)...)
	if err != nil {
		return nil, queryError(err)
	}
	defer rows.Close()
	res := &SearchResult{}
	keys := newKeySet()
	for rows.Next() {
		var r row
		extVals := make([]any, len(ext))
		dest := []any{&r.seq, &r.t, &r.ns, &r.topic, &r.source, &r.tags, &r.contents, &r.packN, &r.packI}
		for i := range extVals {
			dest = append(dest, &extVals[i])
		}
		if err := rows.Scan(append(dest, &res.Count)...); err != nil {
			return nil, err
		}
		if assemble {
			rr := r
			splRows = append(splRows, (&rr).assemble(p.Stages, extVals, p.PackMeta))
			continue
		}
		res.Rows = append(res.Rows, r.encode(searchLayout, p.PackMeta, 0))
		keys.addObject(r.contents)
		if p.PackMeta {
			keys.add("__pack_meta__")
		}
		keys.add("__topic__", "__source__")
		keys.addObject(r.tags)
	}
	if err := rows.Err(); err != nil {
		return nil, queryError(err)
	}
	res.Keys = keys.list
	if assemble {
		res.Rows, res.Keys = assembleRows(splRows)
	}
	// 没有返回行时（offset 越界）窗口函数拿不到总数，单独数一次
	if len(res.Rows) == 0 && p.Offset > 0 {
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM logs WHERE `+where, args...).Scan(&res.Count); err != nil {
			return nil, queryError(err)
		}
	}
	return res, nil
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
	var q string
	var args []any
	switch from {
	case "begin":
		q = `SELECT coalesce(min(seq), 0) FROM logs WHERE project = ? AND logstore = ?`
	case "end":
		q = `SELECT coalesce(max(seq), 0) + 1 FROM logs WHERE project = ? AND logstore = ?`
	default:
		t, err := strconv.ParseInt(from, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("from must be begin, end or unix seconds")
		}
		q = `SELECT coalesce(min(seq), (SELECT coalesce(max(seq), 0) + 1 FROM logs WHERE project = ? AND logstore = ?))
			FROM logs WHERE project = ? AND logstore = ? AND t >= ?`
		args = []any{project, logstore}
		args = append(args, project, logstore, t)
		var seq int64
		err = s.db.QueryRowContext(ctx, q, args...).Scan(&seq)
		return seq, err
	}
	var seq int64
	err := s.db.QueryRowContext(ctx, q, project, logstore).Scan(&seq)
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
	var packCtx sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT pack_ctx FROM logs WHERE project = ? AND logstore = ? AND seq = ?`,
		project, logstore, seq).Scan(&packCtx)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !packCtx.Valid) {
		return nil, ErrNoContext
	}
	if err != nil {
		return nil, err
	}
	const cols = `SELECT seq, t, ns, topic, source, tags, contents, coalesce(pack_n, 1), coalesce(pack_i, 0) FROM logs WHERE project = ? AND logstore = ? AND pack_ctx = ?`
	before, err := s.scanRows(ctx, cols+` AND seq < ? ORDER BY seq DESC LIMIT ?`, project, logstore, packCtx.String, seq, back)
	if err != nil {
		return nil, err
	}
	rest, err := s.scanRows(ctx, cols+` AND seq >= ? ORDER BY seq ASC LIMIT ?`, project, logstore, packCtx.String, seq, forward+1)
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
	q := `SELECT t - t % ? AS b, count(*) FROM logs WHERE project = ? AND logstore = ? AND t >= ? AND t < ? AND ` +
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
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT unnest(json_keys(contents)) FROM logs WHERE project = ? AND logstore = ? AND t >= ?
		UNION SELECT DISTINCT unnest(json_keys(tags)) FROM logs WHERE project = ? AND logstore = ? AND t >= ?`,
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
