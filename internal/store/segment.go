package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math/bits"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nethinwei/sls-local/internal/fieldcol"
	"github.com/nethinwei/sls-local/internal/index"
	"github.com/nethinwei/sls-local/internal/query"
)

// 分段：logs 表只是「头部」，写入后先留在这里；刷段时按写入序号 seq 划一条边界，边界以内的日志按
// project/logstore/小时 写成不可变的 Parquet 文件（分段），再从头部删掉。查询看的是一份快照：
// 「头部里 seq > 边界的行」加上「快照里的分段」，两部分不重叠，也不会漏。
//
// 头部的删除与分段文件的删除都要等到还在用旧快照的查询结束之后才做（见 release）。
//
// 分段文件的路径：<dir>/<project>/<logstore>/<小时起点>/<seq 起>-<seq 止>_<t 起>-<t 止>_<行数>.parquet，
// 元数据都在文件名里，启动时列目录即可恢复，不需要另外的清单文件。
//
// 分段除了 logs 表的列，还把常用字段写成独立的列（fieldcol）：查询引用字段时直接读列，不再逐行解析 JSON。
// 写哪些字段由每个分段自己的数据决定（见 hotKeys），启动时从 Parquet 的 schema 读回。

// segment 是一个 Parquet 分段。
type segment struct {
	path              string
	project, logstore string
	tmin, tmax        int64 // __time__ 的范围（含两端）
	seqMin, seqMax    int64
	rows              int64
	cols              map[string]bool // 写成独立列的字段
}

// snapshot 是查询看到的数据：头部里 seq > boundary 的行加上 segs。
type snapshot struct {
	st       *Store // 读分段的旁路文件（字段列、bloom）
	gen      int64
	boundary int64
	segs     []segment
	refs     int // 正在使用它的查询数（由 Store.segMu 保护）
}

// cleanup 是要等 gen 之前的快照都不再使用后才能做的事（删头部的行、删分段文件）。
type cleanup struct {
	gen int64
	run func()
}

const (
	// DefaultFlushInterval 与 DefaultFlushRows 是刷段的默认时机：间隔到了，或头部积累的行数到了。
	DefaultFlushInterval = 5 * time.Minute
	// DefaultRefreshInterval 是查询节点重新列出对象存储上分段的间隔。
	DefaultRefreshInterval = 2 * time.Second
	DefaultFlushRows       = 1_000_000
	segmentHour            = 3600
	// segmentRowGroup 是分段的 row group 行数。DuckDB 默认 122880：翻页只取一页时也要解压一整个 row group
	// 的 JSON 列，200 万条下 * 检索 3 → 14 ms；16384 时 5.8 ms，聚合不受影响。zstd 比 lz4 慢 10%～15%，
	// 体积小 40%（对象存储上更省）。
	segmentRowGroup = 16384
)

// acquire 取当前快照，用完必须 release。
func (s *Store) acquire() *snapshot {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	s.cur.refs++
	return s.cur
}

// release 归还快照，并执行已经没有旧快照在用的清理。
func (s *Store) release(sn *snapshot) {
	s.segMu.Lock()
	sn.refs--
	runs := s.readyCleanups()
	s.segMu.Unlock()
	for _, f := range runs {
		f()
	}
}

// publish 换上新的快照；after 在所有更早的快照都不再使用后执行。
func (s *Store) publish(next *snapshot, after func()) {
	s.segMu.Lock()
	next.gen = s.cur.gen + 1
	next.st = s
	if s.cur.refs > 0 {
		s.live = append(s.live, s.cur)
	}
	s.cur = next
	if after != nil {
		s.cleanups = append(s.cleanups, cleanup{next.gen, after})
	}
	runs := s.readyCleanups()
	s.segMu.Unlock()
	for _, f := range runs {
		f()
	}
}

// readyCleanups 取出可以执行的清理（调用方持有 segMu）。
func (s *Store) readyCleanups() []func() {
	s.live = slices.DeleteFunc(s.live, func(sn *snapshot) bool { return sn.refs == 0 })
	oldest := s.cur.gen
	for _, sn := range s.live {
		oldest = min(oldest, sn.gen)
	}
	var runs []func()
	s.cleanups = slices.DeleteFunc(s.cleanups, func(c cleanup) bool {
		if c.gen <= oldest {
			runs = append(runs, c.run)
			return true
		}
		return false
	})
	return runs
}

// baseCols 是 logs 表的列，头部与分段都有。
const baseCols = "project, logstore, seq, t, ns, topic, source, tags, contents, fulltext, pack_ctx, pack_n, pack_i, orig_keys"

// source 是 project/logstore 在 [from, to]（秒）里可能有数据的部分，作为 FROM 后面的表，带上 keys 这些字段列
// （fieldcol.Ref）。不需要时间裁剪时 from = 0、to = math.MaxInt64。
//
// 头部与没写某个字段列的分段，这一列从 JSON 现取；写了的直接读。分段按写了哪些列分组，每组一个 read_parquet。
func (sn *snapshot) source(ctx context.Context, project, logstore string, from, to int64, keys []string) (string, error) {
	return sn.sourceOf(ctx, project, logstore, keys, func(g *segment) bool { return g.tmax >= from && g.tmin <= to }, nil)
}

// hasSegments 判断 project/logstore 在 [from, to] 里有没有分段。
func (sn *snapshot) hasSegments(project, logstore string, from, to int64) bool {
	for _, g := range sn.segs {
		if g.project == project && g.logstore == logstore && g.tmax >= from && g.tmin <= to {
			return true
		}
	}
	return false
}

// sourceSeq 与 source 相同，但按写入序号 [lo, hi] 选分段（上下文按 seq 找相邻的日志）。
func (sn *snapshot) sourceSeq(ctx context.Context, project, logstore string, lo, hi int64) (string, error) {
	return sn.sourceOf(ctx, project, logstore, nil, func(g *segment) bool { return g.seqMax >= lo && g.seqMin <= hi }, nil)
}

// sourceOf 是 project/logstore 里 pick 选中、按 bloom 可能满足 need 的分段加上头部。本地没有的旁路文件与
// 分段先从对象存储并行下载。
func (sn *snapshot) sourceOf(ctx context.Context, project, logstore string, keys []string, pick func(*segment) bool,
	need *query.Need) (string, error) {
	head := "logs"
	if sn.boundary > 0 || len(keys) > 0 {
		head = "SELECT " + baseCols + fieldCols(keys, nil) + " FROM logs"
		if sn.boundary > 0 {
			head += " WHERE seq > " + strconv.FormatInt(sn.boundary, 10)
		}
	}
	type group struct {
		have  map[string]bool
		paths []string
	}
	picked, err := sn.pick(ctx, project, logstore, keys, pick, need)
	if err != nil {
		return "", err
	}
	var groups []*group
	byCols := map[string]*group{}
	for _, g := range picked {
		var cols map[string]bool
		if len(keys) > 0 {
			cols = sn.st.colsOf(ctx, g)
		}
		var sig strings.Builder // 只按 keys 里哪些写成了列分组
		for _, k := range keys {
			if cols[k] {
				sig.WriteString(fieldcol.Name(k) + ",")
			}
		}
		grp, ok := byCols[sig.String()]
		if !ok {
			grp = &group{have: cols}
			byCols[sig.String()] = grp
			groups = append(groups, grp)
		}
		grp.paths = append(grp.paths, sqlQuote(g.path))
	}
	if len(groups) == 0 {
		if head == "logs" {
			return "logs", nil
		}
		return "(" + head + ") AS logs", nil
	}
	if head == "logs" {
		head = "SELECT " + baseCols + " FROM logs"
	}
	parts := []string{head}
	// 分段已经按 project/logstore 选过：这两列写成常量，过滤条件不用逐行比较（200 万条约省 6 ms）
	segCols := sqlQuote(project) + " AS project, " + sqlQuote(logstore) + " AS logstore, " +
		strings.TrimPrefix(baseCols, "project, logstore, ")
	for _, grp := range groups {
		parts = append(parts, "SELECT "+segCols+fieldCols(keys, grp.have)+" FROM read_parquet(["+
			strings.Join(grp.paths, ", ")+"])")
	}
	return "(" + strings.Join(parts, " UNION ALL ") + ") AS logs", nil
}

// pick 选出 project/logstore 里 pick 选中、按 bloom 可能满足 need 的分段，并保证它们在本地。用对象存储时
// 旁路文件（bloom 与字段列清单）、分段文件都并行下载：冷缓存上一次查询可能涉及几百个文件。
func (sn *snapshot) pick(ctx context.Context, project, logstore string, keys []string, pick func(*segment) bool,
	need *query.Need) ([]*segment, error) {
	s := sn.st
	var cands []*segment
	for i := range sn.segs {
		if g := &sn.segs[i]; g.project == project && g.logstore == logstore && pick(g) {
			cands = append(cands, g)
		}
	}
	if s.remote != nil && (need != nil || len(keys) > 0) {
		err := parallel(ctx, len(cands), func(i int) error {
			g := cands[i]
			if need == nil && g.cols != nil || s.blooms.cached(g.path) {
				return nil
			}
			if err := s.ensureLocal(ctx, g.path+".bloom"); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	picked := cands[:0:0]
	for _, g := range cands {
		if s.mayContain(ctx, g, need) {
			picked = append(picked, g)
		}
	}
	if err := parallel(ctx, len(picked), func(i int) error { return s.ensureLocal(ctx, picked[i].path) }); err != nil {
		return nil, err
	}
	return picked, nil
}

// fieldCols 是 SELECT 里的字段列：have 里有的直接读，其余从 contents 现取。
func fieldCols(keys []string, have map[string]bool) string {
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(", ")
		if have[k] {
			b.WriteString(fieldcol.Ref(k))
		} else {
			b.WriteString(fieldcol.Extract("contents", k) + " AS " + fieldcol.Ref(k))
		}
	}
	return b.String()
}

// sqlQuote 把字符串写成 SQL 字面量。
func sqlQuote(v string) string { return fieldcol.Quote(v) }

// ---------------- 刷段 ----------------

// Flush 把头部当前的全部日志写成分段（没有配置分段目录时什么也不做）。返回写出的行数。
func (s *Store) Flush(ctx context.Context) (int64, error) {
	if s.segDir == "" {
		return 0, nil
	}
	if s.fenced.Load() {
		return 0, ErrFenced
	}
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.segMu.Lock()
	cur := s.cur
	s.segMu.Unlock()
	s.mu.Lock() // seq 以内的行都已经由 Append 刷进表里（Append 持锁直到 Flush 完成）
	upper := s.seq
	s.mu.Unlock()
	lower := cur.boundary
	if upper <= lower {
		return 0, nil
	}

	type part struct {
		project, logstore string
		hour, tmin, tmax  int64
		seqMin, seqMax, n int64
	}
	// 每个 project/logstore/小时 按写入顺序切成不超过 segmentFileRows 行的文件：bloom 按文件跳过，文件太大就
	// 跳不掉什么。一块是 seq 上连续的一段（在这个分区里），所以 seq 范围加分区条件就能选出它。
	rows, err := s.db.QueryContext(ctx, `SELECT project, logstore, h, min(t), max(t), min(seq), max(seq), count(*)
		FROM (SELECT project, logstore, t, seq, t - t % ? AS h,
			(row_number() OVER (PARTITION BY project, logstore, t - t % ? ORDER BY seq) - 1) // ? AS chunk
			FROM logs WHERE seq > ? AND seq <= ?)
		GROUP BY project, logstore, h, chunk ORDER BY min(seq)`, segmentHour, segmentHour, segmentFileRows, lower, upper)
	if err != nil {
		return 0, err
	}
	var parts []part
	for rows.Next() {
		var p part
		if err := rows.Scan(&p.project, &p.logstore, &p.hour, &p.tmin, &p.tmax, &p.seqMin, &p.seqMax, &p.n); err != nil {
			rows.Close()
			return 0, err
		}
		parts = append(parts, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	var added []segment
	var total int64
	fail := func(err error) (int64, error) {
		for _, g := range added {
			os.Remove(g.path)
			os.Remove(g.path + ".bloom")
		}
		return 0, err
	}
	for _, p := range parts {
		g := segment{project: p.project, logstore: p.logstore, tmin: p.tmin, tmax: p.tmax,
			seqMin: p.seqMin, seqMax: p.seqMax, rows: p.n}
		// COPY 不支持参数，值都内联：project/logstore 经 sqlQuote，其余是整数
		from := fmt.Sprintf(`logs WHERE seq BETWEEN %d AND %d AND project = %s AND logstore = %s AND t >= %d AND t < %d`,
			p.seqMin, p.seqMax, sqlQuote(p.project), sqlQuote(p.logstore), p.hour, p.hour+segmentHour)
		if err := s.writeSegment(ctx, &g, p.hour, from); err != nil {
			return fail(err)
		}
		added = append(added, g)
		total += p.n
	}
	// 边界写进库里才算刷完：之前崩溃的话，启动时 seq 超过边界的分段当作没写完删掉（数据还在头部）
	if _, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO sls_meta VALUES ('flushed_seq', ?)`,
		strconv.FormatInt(upper, 10)); err != nil {
		return fail(err)
	}
	next := &snapshot{boundary: upper, segs: append(slices.Clone(cur.segs), added...)}
	s.publish(next, func() { s.deleteHead(upper) })
	for _, g := range added {
		s.lastAdded[partitionOf(g)] = time.Now()
	}
	s.stats.flushes.Add(1)
	s.stats.flushedRows.Add(total)
	s.markPending(added) // 提交之后才上传；失败的下次再传
	if err := s.upload(ctx); err != nil && s.onError != nil {
		s.onError(err)
	}
	return total, nil
}

// Compact 合并小分段：同一个 project/logstore/小时里，seq 上相邻、各自不到 segmentFileRows/2 行的文件，
// 合成不超过 segmentFileRows 行的一个（刷段频繁、迟到的日志都会留下小文件，文件多了查询要打开的也多）。
// 返回合并掉的文件数。
//
// 还在写入的分区（compactQuiet 内刷出过新分段）分层合并：大小同一层（行数差不到 compactFanIn 倍）的相邻文件
// 攒够 compactFanIn 个才合成上一层的一个，每行日志只被重写约 log(segmentFileRows/每次刷段行数)/log(compactFanIn)
// 次；不然每次刷段都会把越来越大的合并结果再重写一遍。安静下来的分区把剩下的相邻小文件一次合并完。
//
// 合并后的文件的 seq 范围包含被合并的各个文件：写完改名之后、删掉旧文件之前崩溃的话，启动时按包含关系
// 删掉旧文件（loadSegments）。所以只合并 seq 上相邻的一串，中间不能隔着不参与合并的文件。
func (s *Store) Compact(ctx context.Context) (int, error) {
	if s.segDir == "" || s.fenced.Load() {
		return 0, nil
	}
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.segMu.Lock()
	cur := s.cur
	s.segMu.Unlock()

	now := time.Now()
	for k, t := range s.lastAdded {
		if now.Sub(t) >= s.compactQuiet {
			delete(s.lastAdded, k)
		}
	}
	segs := slices.Clone(cur.segs)
	slices.SortFunc(segs, compareSegments)
	var groups [][]segment
	var run []segment
	var runRows int64
	closeRun := func() {
		if len(run) >= 2 {
			groups = append(groups, run)
		}
		run, runRows = nil, 0
	}
	for i, g := range segs {
		if i > 0 && !samePartition(segs[i-1], g) {
			run, runRows = nil, 0 // 分层时没攒够的不合并
		}
		_, busy := s.lastAdded[partitionOf(g)]
		if g.rows >= segmentFileRows/2 || runRows+g.rows > segmentFileRows ||
			busy && len(run) > 0 && sizeTier(run[0].rows) != sizeTier(g.rows) {
			if busy {
				run, runRows = nil, 0
			} else {
				closeRun()
			}
			if g.rows >= segmentFileRows/2 {
				continue
			}
		}
		run = append(run, g)
		runRows += g.rows
		if busy && len(run) == compactFanIn {
			closeRun()
		}
		if !busy && (i+1 == len(segs) || !samePartition(g, segs[i+1])) {
			closeRun()
		}
	}

	merged := 0
	for _, grp := range groups {
		m := segment{project: grp[0].project, logstore: grp[0].logstore, tmin: grp[0].tmin, tmax: grp[0].tmax,
			seqMin: grp[0].seqMin, seqMax: grp[len(grp)-1].seqMax}
		paths := make([]string, len(grp))
		for i, g := range grp {
			m.tmin, m.tmax, m.rows = min(m.tmin, g.tmin), max(m.tmax, g.tmax), m.rows+g.rows
			paths[i] = sqlQuote(g.path)
			if err := s.ensureLocal(ctx, g.path); err != nil { // 本地缓存可能已经淘汰
				return merged, fmt.Errorf("compact segments: %w", err)
			}
		}
		from := "read_parquet([" + strings.Join(paths, ", ") + "], union_by_name = true)"
		if err := s.writeSegment(ctx, &m, hourOf(m), from); err != nil {
			return merged, fmt.Errorf("compact segments: %w", err)
		}
		s.segMu.Lock()
		cur = s.cur
		s.segMu.Unlock()
		next := &snapshot{boundary: cur.boundary}
		for _, g := range cur.segs {
			if !slices.ContainsFunc(grp, func(x segment) bool { return x.path == g.path }) {
				next.segs = append(next.segs, g)
			}
		}
		next.segs = append(next.segs, m)
		s.publish(next, func() { s.removeSegments(grp) })
		merged += len(grp)
		s.stats.compactedFiles.Add(int64(len(grp)))
		if s.remote != nil {
			s.markPending([]segment{m})
			if err := s.upload(ctx); err != nil { // 合并后的没传上去之前，对象存储上的旧分段不删
				return merged, err
			}
			s.trashRemote(grp)
		}
	}
	return merged, nil
}

func hourOf(g segment) int64 { return g.tmin - g.tmin%segmentHour }

// partition 是合并的范围：写入节点的一个 project/logstore/小时。
type partition struct {
	project, logstore string
	hour, writer      int64
}

func partitionOf(g segment) partition {
	return partition{g.project, g.logstore, hourOf(g), writerOf(g.seqMin)}
}

const (
	// compactFanIn 是分层合并时一次合并的文件数。
	compactFanIn = 8
	// DefaultCompactQuiet：分区这么久没有刷出新分段，就把剩下的小文件一次合并完。
	DefaultCompactQuiet = 10 * time.Minute
)

// sizeTier 是文件大小的层级：每层行数相差 compactFanIn 倍。
func sizeTier(rows int64) int { return bits.Len64(uint64(rows)) / 3 }

// samePartition：同一个写入节点的同一个 project/logstore/小时（合并、包含关系只在这个范围里）。
func samePartition(a, b segment) bool {
	return a.project == b.project && a.logstore == b.logstore && hourOf(a) == hourOf(b) &&
		writerOf(a.seqMin) == writerOf(b.seqMin)
}

// writerShift：写入节点的编号在 seq 的高位，各节点的 seq（与分段文件名）不会重复。每个编号 2^48 条（每秒
// 10 万条约 89 年），用完时拒绝写入（ErrSeqExhausted）而不是编进下一个编号。
const writerShift = 48

// MaxWriterID 是写入节点编号的上限（不含）。
const MaxWriterID = 1 << (63 - writerShift)

func writerOf(seq int64) int64 { return seq >> writerShift }

// compareSegments 按分区、seq 起点排序；起点相同时范围大的在前（包含别人的在前）。
func compareSegments(a, b segment) int {
	return cmp.Or(cmp.Compare(a.project, b.project), cmp.Compare(a.logstore, b.logstore),
		cmp.Compare(hourOf(a), hourOf(b)), cmp.Compare(a.seqMin, b.seqMin), cmp.Compare(b.seqMax, a.seqMax))
}

// writeSegment 把 from（FROM 子句：表与 WHERE 条件）选中的日志写成分段 g（g 的元数据由调用方填好），
// 同时写好它的 bloom、定下 g.path 与 g.cols。字段列一律从 JSON 现取：来源可能是各自写了不同字段列的分段。
func (s *Store) writeSegment(ctx context.Context, g *segment, hour int64, from string) error {
	dir := filepath.Join(s.segDir, url.PathEscape(g.project), url.PathEscape(g.logstore), strconv.FormatInt(hour, 10))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	g.path = filepath.Join(dir, fmt.Sprintf("%d-%d_%d-%d_%d.parquet", g.seqMin, g.seqMax, g.tmin, g.tmax, g.rows))
	tmp := g.path + ".tmp"
	keys, err := s.hotKeys(ctx, from, g.rows)
	if err != nil {
		return err
	}
	g.cols = map[string]bool{}
	for _, k := range keys {
		g.cols[k] = true
	}
	q := fmt.Sprintf(`COPY (SELECT %s%s FROM %s ORDER BY t, coalesce(ns, 0), seq)
		TO %s (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE %d)`,
		baseCols, fieldCols(keys, nil), from, sqlQuote(tmp), segmentRowGroup)
	if _, err := s.db.ExecContext(ctx, q); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write segment: %w", err)
	}
	if err := syncFile(tmp); err != nil {
		os.Remove(tmp)
		return err
	}
	// bloom 先于分段文件就位：分段文件在，它的 bloom 就是完整的（没有 bloom 的分段只是不跳过）
	if err := s.writeBloom(ctx, g.path+".bloom", from, g.project, g.logstore, g.cols); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, g.path); err != nil {
		os.Remove(g.path + ".bloom")
		os.Remove(tmp)
		return err
	}
	syncFile(dir)
	return nil
}

// segmentFileRows 是一个分段文件最多的行数（测试里调小）。
var segmentFileRows int64 = 1_000_000

// writeBloom 为 from（FROM 子句）选中的日志建 bloom：logstore 用到的每组分词符一个。
func (s *Store) writeBloom(ctx context.Context, path, from, project, logstore string, cols map[string]bool) error {
	f := &bloomFile{cols: cols, blooms: map[string]*bloom{}}
	for _, sig := range s.tokenSigs(project, logstore) {
		// 全文列（含 __source__、字段名、非数值字段的值）加上全部字段的值，按分词符切成整词，小写
		rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT w FROM (SELECT unnest(string_split_regex(lower(
			fulltext || chr(31) || coalesce(array_to_string(json_extract_string(contents, '$.*'), chr(31)), '')), ?)) AS w
			FROM `+from+`) WHERE w <> ''`, tokenClass(sig))
		if err != nil {
			return fmt.Errorf("collect words: %w", err)
		}
		var words []string
		for rows.Next() {
			var w string
			if err := rows.Scan(&w); err != nil {
				rows.Close()
				return err
			}
			words = append(words, w)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		b := newBloom(len(words))
		for _, w := range words {
			b.add(w)
		}
		f.blooms[sig] = b
	}
	return writeBloomFile(path, f)
}

// tokenClass 是按分词符切词的 RE2：分词符组成的字符类，连续多个算一处。
func tokenClass(sig string) string {
	var b strings.Builder
	b.WriteString("[")
	for _, r := range sig {
		switch r {
		case '\\', ']', '[', '^', '-':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteString("]+")
	return b.String()
}

// tokenSigs 是 logstore 的全文索引与 text 字段用到的分词符组（query.TokenSig）。
func (s *Store) tokenSigs(project, logstore string) []string {
	if fn := s.sigs.Load(); fn != nil {
		return (*fn)(project, logstore)
	}
	return []string{query.TokenSig(strings.Join(index.DefaultTokens, ""))}
}

// SetTokenSigs 设置每个 logstore 用到的分词符组（由索引配置决定，见 api）。没有设置时按默认分词符。
func (s *Store) SetTokenSigs(fn func(project, logstore string) []string) { s.sigs.Store(&fn) }

// 每个分段最多把这么多字段写成独立的列；字段至少出现在 1/hotKeyRatio 的日志里才写。
const (
	maxHotKeys  = 64
	hotKeyRatio = 100
)

// hotKeys 是 from（FROM 子句）选中的 n 条日志里常见的字段，按出现次数从多到少。不常见的字段写成列也基本是 NULL，
// 查询时现取 JSON 的代价很小。
func (s *Store) hotKeys(ctx context.Context, from string, n int64) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT k FROM (SELECT unnest(json_keys(contents)) AS k FROM `+from+`)
		GROUP BY k HAVING count(*) >= ? ORDER BY count(*) DESC, k LIMIT ?`, max(n/hotKeyRatio, 1), maxHotKeys)
	if err != nil {
		return nil, fmt.Errorf("find hot keys: %w", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// deleteHead 删除头部里已经写进分段的行。失败只影响空间，不影响结果（查询只看边界以后的行）。
func (s *Store) deleteHead(upper int64) {
	if _, err := s.db.Exec(`DELETE FROM logs WHERE seq <= ?`, upper); err != nil && s.onError != nil {
		s.onError(fmt.Errorf("delete flushed rows: %w", err))
	}
}

func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// flusher 在后台按间隔或头部行数刷段，直到 ctx 结束。
func (s *Store) flusher(ctx context.Context, interval time.Duration) {
	defer close(s.flusherDone)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-s.flushNow:
		}
		if _, err := s.Flush(ctx); err != nil && ctx.Err() == nil && s.onError != nil {
			s.onError(fmt.Errorf("flush segments: %w", err))
		}
		if _, err := s.Compact(ctx); err != nil && ctx.Err() == nil && s.onError != nil {
			s.onError(err)
		}
		if err := s.maintainRemote(ctx); err != nil && ctx.Err() == nil && s.onError != nil {
			s.onError(err)
		}
	}
}

// pendingRows 是头部里还没刷段的行数（seq 是最后写入的一行）。
func (s *Store) pendingRows(seq int64) int64 {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	// 写入节点的 seq 从 writerID<<writerShift 起编：还没刷过段时边界是 0，不能按它算
	return seq - max(s.cur.boundary, s.writerID<<writerShift)
}

// maybeFlush 在头部积累的行数到了阈值时触发刷段（Append 之后调用）。
func (s *Store) maybeFlush(ctx context.Context, seq int64) {
	if s.segDir == "" || s.flushRows <= 0 {
		return
	}
	if s.pendingRows(seq) < s.flushRows {
		return
	}
	if s.syncFlush {
		if _, err := s.Flush(ctx); err != nil && s.onError != nil {
			s.onError(err)
		}
		if _, err := s.Compact(ctx); err != nil && s.onError != nil {
			s.onError(err)
		}
		if err := s.maintainRemote(ctx); err != nil && s.onError != nil {
			s.onError(err)
		}
		return
	}
	select {
	case s.flushNow <- struct{}{}:
	default:
	}
}

// ---------------- 启动恢复 ----------------

// loadSegments 读出已刷的边界，列出分段目录：删掉没写完的（.tmp，或 seq 超过边界的），
// 删掉头部里已经在分段里的行，返回当前快照。
func (s *Store) loadSegments(ctx context.Context) (*snapshot, error) {
	var v sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT value FROM sls_meta WHERE key = 'flushed_seq'`).Scan(&v)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var boundary int64
	if v.Valid {
		if boundary, err = strconv.ParseInt(v.String, 10, 64); err != nil {
			return nil, fmt.Errorf("flushed_seq %q: %w", v.String, err)
		}
	}
	// 对象存储上的分段都是提交过的（先提交后上传）：本地库是新的（换了磁盘）时，边界与 seq 接着它们往下
	var remote []segment
	inRemote := map[string]bool{}
	if s.remote != nil {
		if s.readOnly {
			remote, err = s.manifestSegments(ctx)
		} else {
			remote, err = s.remoteSegments(ctx)
		}
		if err != nil {
			return nil, err
		}
		if !s.readOnly { // 写入节点只管自己写的分段
			remote = slices.DeleteFunc(remote, func(g segment) bool { return !s.owns(g) })
		}
		for _, g := range remote {
			if !s.readOnly {
				boundary = max(boundary, g.seqMax)
			}
			inRemote[g.path] = true
		}
		if err := s.registerLogstores(ctx, remote); err != nil {
			return nil, err
		}
	}
	sn := &snapshot{st: s, boundary: boundary}
	if s.segDir == "" {
		if boundary > 0 {
			return nil, fmt.Errorf("the data file has logs in segments but no segment directory is configured")
		}
		return sn, nil
	}
	err = filepath.WalkDir(s.segDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if strings.HasSuffix(path, ".tmp") {
			return removeIfExists(path)
		}
		if seg, ok := strings.CutSuffix(path, ".bloom"); ok { // 分段文件没了（或没写完）的 bloom
			if _, err := os.Stat(seg); os.IsNotExist(err) {
				return removeIfExists(path)
			}
			return nil
		}
		g, ok := parseSegment(s.segDir, path)
		if !ok {
			return nil // 不认识的文件不动
		}
		if g.seqMax > boundary { // 遍历时可能已经（或稍后）遇到它的 .bloom：两边都容许文件已不存在
			if err := removeIfExists(path + ".bloom"); err != nil {
				return err
			}
			return removeIfExists(path)
		}
		if !inRemote[g.path] {
			sn.segs = append(sn.segs, g)
			s.markPending([]segment{g}) // 提交了还没上传
		}
		return nil
	})
	sn.segs = append(sn.segs, remote...)
	if err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}
	// 合并写完、旧文件还没删时崩溃：同一分区里 seq 范围被别的文件包含的，是已经合并进去的旧文件
	slices.SortFunc(sn.segs, compareSegments)
	kept := sn.segs[:0]
	for _, g := range sn.segs {
		if n := len(kept); n > 0 && samePartition(kept[n-1], g) && g.seqMax <= kept[n-1].seqMax {
			if err := removeIfExists(g.path + ".bloom"); err != nil {
				return nil, err
			}
			if err := removeIfExists(g.path); err != nil {
				return nil, err
			}
			if inRemote[g.path] && s.owns(g) {
				s.trashRemote([]segment{g})
			}
			continue
		}
		kept = append(kept, g)
	}
	sn.segs = kept
	slices.SortFunc(sn.segs, func(a, b segment) int { return cmp.Compare(a.seqMin, b.seqMin) })
	if boundary > 0 {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM logs WHERE seq <= ?`, boundary); err != nil {
			return nil, err
		}
	}
	return sn, nil
}

// removeIfExists 删除文件，文件已经不存在不算错。
func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// parseSegment 从路径解析分段的元数据。
func parseSegment(root, path string) (segment, bool) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return segment{}, false
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 4 || !strings.HasSuffix(parts[3], ".parquet") {
		return segment{}, false
	}
	g := segment{path: path}
	if g.project, err = url.PathUnescape(parts[0]); err != nil {
		return segment{}, false
	}
	if g.logstore, err = url.PathUnescape(parts[1]); err != nil {
		return segment{}, false
	}
	name := strings.TrimSuffix(parts[3], ".parquet")
	var n int
	n, err = fmt.Sscanf(name, "%d-%d_%d-%d_%d", &g.seqMin, &g.seqMax, &g.tmin, &g.tmax, &g.rows)
	if err != nil || n != 5 {
		return segment{}, false
	}
	return g, true
}

// ---------------- 保留与重建 ----------------

// dropSegmentsBefore 去掉整段都早于 cutoff 的分段，返回去掉的行数；文件在没有查询使用后删除。
func (s *Store) dropSegmentsBefore(ctx context.Context, cutoff int64) (int64, error) {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.segMu.Lock()
	cur := s.cur
	s.segMu.Unlock()
	var keep, drop []segment
	var n int64
	for _, g := range cur.segs {
		if g.tmax < cutoff {
			drop = append(drop, g)
			n += g.rows
		} else {
			keep = append(keep, g)
		}
	}
	if len(drop) == 0 {
		return 0, nil
	}
	s.trashRemote(drop)
	s.publish(&snapshot{boundary: cur.boundary, segs: keep}, func() { s.removeSegments(drop) })
	return n, s.syncManifests(ctx)
}

// removeSegments 删除分段文件与它们的 bloom（本地），连同本地缓存的使用记录。
func (s *Store) removeSegments(segs []segment) {
	for _, g := range segs {
		s.blooms.forget(g.path)
		if s.remote != nil {
			s.remote.mu.Lock()
			delete(s.remote.lastUse, g.path)
			delete(s.remote.lastUse, g.path+".bloom")
			s.remote.mu.Unlock()
		}
		for _, p := range []string{g.path, g.path + ".bloom"} {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) && s.onError != nil {
				s.onError(err)
			}
		}
	}
}

// unflush 把全部分段读回头部（-reindex 等需要改写全部日志时用），调用方持有 flushMu。
// 读回与把边界清零在同一个事务里；之后崩溃的话，启动时这些分段的 seq 都超过边界，会被当作没写完删掉。
func (s *Store) unflush(ctx context.Context) error {
	s.segMu.Lock()
	cur, busy := s.cur, len(s.cleanups) > 0
	s.segMu.Unlock()
	if len(cur.segs) == 0 {
		return nil
	}
	if busy { // 还有没做完的头部删除：读回的行可能被它删掉
		return errors.New("segments are in use, try again when no queries are running")
	}
	if s.remote != nil {
		// 全部下载到本地，再从对象存储删掉（在提交之前删：之后崩溃的话，本地的分段仍是提交过的，会重新上传；
		// 反过来的话，对象存储上的分段会被当成提交过的，与读回头部的日志重复）
		for _, g := range cur.segs {
			if err := s.ensureLocal(ctx, g.path); err != nil {
				return err
			}
		}
		for _, g := range cur.segs {
			for _, k := range []string{s.objectKey(g.path), s.objectKey(g.path + ".bloom")} {
				if err := s.remote.obj.Delete(ctx, k); err != nil {
					return err
				}
			}
		}
		s.markPending(nil)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	paths := make([]string, len(cur.segs))
	for i, g := range cur.segs {
		paths[i] = sqlQuote(g.path)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO logs BY NAME SELECT `+baseCols+` FROM read_parquet([`+
		strings.Join(paths, ", ")+`])`); err != nil {
		return fmt.Errorf("read segments back: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR REPLACE INTO sls_meta VALUES ('flushed_seq', '0')`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.publish(&snapshot{}, func() { s.removeSegments(cur.segs) })
	return s.syncManifests(ctx)
}
