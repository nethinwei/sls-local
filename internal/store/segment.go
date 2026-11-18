package store

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nethinwei/sls-local/internal/fieldcol"
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
	DefaultFlushRows     = 1_000_000
	segmentHour          = 3600
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
func (sn *snapshot) source(project, logstore string, from, to int64, keys []string) string {
	return sn.sourceOf(project, logstore, keys, func(g *segment) bool { return g.tmax >= from && g.tmin <= to })
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
func (sn *snapshot) sourceSeq(project, logstore string, lo, hi int64) string {
	return sn.sourceOf(project, logstore, nil, func(g *segment) bool { return g.seqMax >= lo && g.seqMin <= hi })
}

// sourceOf 是 project/logstore 里 pick 选中的分段加上头部。
func (sn *snapshot) sourceOf(project, logstore string, keys []string, pick func(*segment) bool) string {
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
	var groups []*group
	byCols := map[string]*group{}
	for i := range sn.segs {
		g := &sn.segs[i]
		if g.project != project || g.logstore != logstore || !pick(g) {
			continue
		}
		var sig strings.Builder // 只按 keys 里哪些写成了列分组
		for _, k := range keys {
			if g.cols[k] {
				sig.WriteString(fieldcol.Name(k) + ",")
			}
		}
		grp, ok := byCols[sig.String()]
		if !ok {
			grp = &group{have: g.cols}
			byCols[sig.String()] = grp
			groups = append(groups, grp)
		}
		grp.paths = append(grp.paths, sqlQuote(g.path))
	}
	if len(groups) == 0 {
		if head == "logs" {
			return "logs"
		}
		return "(" + head + ") AS logs"
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
	return "(" + strings.Join(parts, " UNION ALL ") + ") AS logs"
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
	rows, err := s.db.QueryContext(ctx, `SELECT project, logstore, t - t % ?, min(t), max(t), min(seq), max(seq), count(*)
		FROM logs WHERE seq > ? AND seq <= ? GROUP BY ALL`, segmentHour, lower, upper)
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
		}
		return 0, err
	}
	for _, p := range parts {
		dir := filepath.Join(s.segDir, url.PathEscape(p.project), url.PathEscape(p.logstore), strconv.FormatInt(p.hour, 10))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fail(err)
		}
		g := segment{project: p.project, logstore: p.logstore, tmin: p.tmin, tmax: p.tmax,
			seqMin: p.seqMin, seqMax: p.seqMax, rows: p.n}
		g.path = filepath.Join(dir, fmt.Sprintf("%d-%d_%d-%d_%d.parquet", g.seqMin, g.seqMax, g.tmin, g.tmax, g.rows))
		tmp := g.path + ".tmp"
		// COPY 不支持参数，值都内联：project/logstore 经 sqlQuote，其余是整数
		filter := fmt.Sprintf(`seq > %d AND seq <= %d AND project = %s AND logstore = %s AND t >= %d AND t < %d`,
			lower, upper, sqlQuote(p.project), sqlQuote(p.logstore), p.hour, p.hour+segmentHour)
		keys, err := s.hotKeys(ctx, filter, p.n)
		if err != nil {
			return fail(err)
		}
		g.cols = map[string]bool{}
		for _, k := range keys {
			g.cols[k] = true
		}
		q := fmt.Sprintf(`COPY (SELECT %s%s FROM logs WHERE %s ORDER BY t, coalesce(ns, 0), seq)
			TO %s (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE %d)`,
			baseCols, fieldCols(keys, nil), filter, sqlQuote(tmp), segmentRowGroup)
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			os.Remove(tmp)
			return fail(fmt.Errorf("write segment: %w", err))
		}
		if err := syncFile(tmp); err != nil {
			os.Remove(tmp)
			return fail(err)
		}
		if err := os.Rename(tmp, g.path); err != nil {
			os.Remove(tmp)
			return fail(err)
		}
		syncFile(dir)
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
	return total, nil
}

// 每个分段最多把这么多字段写成独立的列；字段至少出现在 1/hotKeyRatio 的日志里才写。
const (
	maxHotKeys  = 64
	hotKeyRatio = 100
)

// hotKeys 是 filter 选中的 n 条日志里常见的字段，按出现次数从多到少。不常见的字段写成列也基本是 NULL，
// 查询时现取 JSON 的代价很小。
func (s *Store) hotKeys(ctx context.Context, filter string, n int64) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT k FROM (SELECT unnest(json_keys(contents)) AS k FROM logs WHERE `+filter+`)
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
	}
}

// maybeFlush 在头部积累的行数到了阈值时触发刷段（Append 之后调用）。
func (s *Store) maybeFlush(ctx context.Context, seq int64) {
	if s.segDir == "" || s.flushRows <= 0 {
		return
	}
	s.segMu.Lock()
	pending := seq - s.cur.boundary
	s.segMu.Unlock()
	if pending < s.flushRows {
		return
	}
	if s.syncFlush {
		if _, err := s.Flush(ctx); err != nil && s.onError != nil {
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
	sn := &snapshot{boundary: boundary}
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
			return os.Remove(path)
		}
		g, ok := parseSegment(s.segDir, path)
		if !ok {
			return nil // 不认识的文件不动
		}
		if g.seqMax > boundary {
			return os.Remove(path)
		}
		sn.segs = append(sn.segs, g)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load segments: %w", err)
	}
	slices.SortFunc(sn.segs, func(a, b segment) int { return cmp.Compare(a.seqMin, b.seqMin) })
	if err := s.loadColumns(ctx, sn.segs); err != nil {
		return nil, err
	}
	if boundary > 0 {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM logs WHERE seq <= ?`, boundary); err != nil {
			return nil, err
		}
	}
	return sn, nil
}

// loadColumns 从 Parquet 的 schema 读出各分段写成独立列的字段。
func (s *Store) loadColumns(ctx context.Context, segs []segment) error {
	if len(segs) == 0 {
		return nil
	}
	idx := map[string]int{}
	paths := make([]string, len(segs))
	for i := range segs {
		segs[i].cols = map[string]bool{}
		idx[segs[i].path] = i
		paths[i] = sqlQuote(segs[i].path)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT file_name, name FROM parquet_schema([`+strings.Join(paths, ", ")+`])`)
	if err != nil {
		return fmt.Errorf("read segment schema: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var file, name string
		if err := rows.Scan(&file, &name); err != nil {
			return err
		}
		if k, ok := fieldcol.Key(name); ok {
			if i, ok := idx[file]; ok {
				segs[i].cols[k] = true
			}
		}
	}
	return rows.Err()
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
func (s *Store) dropSegmentsBefore(cutoff int64) int64 {
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
		return 0
	}
	s.publish(&snapshot{boundary: cur.boundary, segs: keep}, func() {
		for _, g := range drop {
			if err := os.Remove(g.path); err != nil && !os.IsNotExist(err) && s.onError != nil {
				s.onError(err)
			}
		}
	})
	return n
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
	s.publish(&snapshot{}, func() {
		for _, g := range cur.segs {
			os.Remove(g.path)
		}
	})
	return nil
}
