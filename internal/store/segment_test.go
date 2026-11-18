package store

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nethinwei/sls-local/internal/fieldcol"
	"github.com/nethinwei/sls-local/internal/index"
	"github.com/nethinwei/sls-local/internal/query"
)

const segBase = 1_800_000_000

func openSeg(t *testing.T, path, dir string) *Store {
	t.Helper()
	s, err := Open(context.Background(), path, Options{SQLOffset: DefaultSQLOffset, SegmentDir: dir, FlushRows: 1 << 40,
		FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	s.compactQuiet = 0 // 合并测试默认按安静的分区（一次合并完）
	return s
}

// putSeq 写入 n 条日志，第 i 条在 segBase+offset+i 秒，字段 i 是全局编号。
func putSeq(t *testing.T, s *Store, offset, n int) {
	t.Helper()
	g := &Group{Source: "src"}
	for i := range n {
		g.Logs = append(g.Logs, Log{Time: uint32(segBase + offset + i), Contents: []KV{{"i", strconv.Itoa(offset + i)}}})
	}
	if err := s.Append(context.Background(), "p", "l", g); err != nil {
		t.Fatal(err)
	}
}

func countAll(t *testing.T, s *Store) int64 {
	t.Helper()
	n, err := s.Count(context.Background(), SearchParams{Project: "p", Logstore: "l", From: 0, To: segBase * 2,
		Where: query.Cond{SQL: "TRUE"}})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// segFiles 列出分段文件（不含旁边的 .bloom）。
func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && filepath.Ext(path) == ".parquet" {
			out = append(out, path)
		}
		return nil
	})
	return out
}

// 刷段跨小时分区；数据一部分在头部、一部分在分段时，检索、SQL、直方图都看到全部数据各一次。
func TestSegmentFlush(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openSeg(t, "", dir)
	defer s.Close()
	putSeq(t, s, 3500, 200) // 跨过一个整点
	n, err := s.Flush(ctx)
	if err != nil || n != 200 {
		t.Fatalf("flush = %d, %v", n, err)
	}
	if files := segFiles(t, dir); len(files) != 2 {
		t.Fatalf("segments = %v, want 2 (one per hour)", files)
	}
	putSeq(t, s, 3700, 50) // 留在头部
	if got := countAll(t, s); got != 250 {
		t.Fatalf("count = %d, want 250", got)
	}
	res, err := s.Search(ctx, SearchParams{Project: "p", Logstore: "l", From: 0, To: segBase * 2,
		Where: query.Cond{SQL: "TRUE"}, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 250 {
		t.Fatalf("search rows = %d, want 250", len(res.Rows))
	}
	h, err := s.Histogram(ctx, "p", "l", segBase, segBase+7200, 3600, query.Cond{SQL: "TRUE"})
	if err != nil {
		t.Fatal(err)
	}
	if h[segBase] != 100 || h[segBase+3600] != 150 {
		t.Fatalf("histogram = %v", h)
	}
	// 头部里已刷的行删掉了
	var head int64
	s.db.QueryRow(`SELECT count(*) FROM logs`).Scan(&head)
	if head != 50 {
		t.Fatalf("head rows = %d, want 50", head)
	}
}

// 查询拿着旧快照时刷段：头部的行要等它用完才删，查询结果不重复、不缺。
func TestSegmentSnapshotDuringFlush(t *testing.T) {
	ctx := context.Background()
	s := openSeg(t, "", t.TempDir())
	defer s.Close()
	putSeq(t, s, 0, 100)
	old := s.acquire()
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var n int64
	src, err := old.source(ctx, "p", "l", 0, segBase*2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM ` + src).Scan(&n); err != nil || n != 100 {
		t.Fatalf("old snapshot sees %d (%v), want 100", n, err)
	}
	s.release(old)
	var head int64
	s.db.QueryRow(`SELECT count(*) FROM logs`).Scan(&head)
	if head != 0 {
		t.Fatalf("head rows after release = %d, want 0", head)
	}

	// 并发：一边写一边刷一边查，每次查到的条数只增不减、不超过已写入的
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			putSeq(t, s, 100*i, 100)
			if i%3 == 0 {
				if _, err := s.Flush(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()
	last := int64(0)
	for range 200 {
		got := countAll(t, s)
		if got < last || got%100 != 0 {
			t.Fatalf("count = %d after %d", got, last)
		}
		last = got
	}
	close(stop)
	wg.Wait()
}

// 重启：已刷的分段与头部都恢复；没写完的分段（seq 超过边界、.tmp）删掉，数据以头部为准。
func TestSegmentRecovery(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	path, dir := filepath.Join(tmp, "sls.duckdb"), filepath.Join(tmp, "segments")
	s := openSeg(t, path, dir)
	putSeq(t, s, 0, 100)
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	putSeq(t, s, 100, 30)
	s.Close()

	// 模拟刷段写完文件、还没记边界就崩溃
	part := filepath.Join(dir, "p", "l", strconv.Itoa(segBase-segBase%3600))
	orphan := filepath.Join(part, "101-130_1800000100-1800000129_30.parquet")
	if err := os.WriteFile(orphan, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(part, "x.parquet.tmp"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan+".bloom", []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	s = openSeg(t, path, dir)
	defer s.Close()
	if got := countAll(t, s); got != 130 {
		t.Fatalf("count after restart = %d, want 130", got)
	}
	filepath.WalkDir(dir, func(f string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && (f == orphan || f == orphan+".bloom" || filepath.Ext(f) == ".tmp") {
			t.Errorf("%s should have been removed", f)
		}
		return nil
	})
	putSeq(t, s, 130, 10) // seq 接着已有的往下编
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := countAll(t, s); got != 140 {
		t.Fatalf("count = %d, want 140", got)
	}
}

// 保留期按整个分段删除：分段里最晚的日志也过期了才删。
func TestSegmentRetention(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openSeg(t, "", dir)
	defer s.Close()
	putSeq(t, s, 0, 100)      // 第一个小时
	putSeq(t, s, 3600*2, 100) // 第三个小时
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	putSeq(t, s, 3600*3, 10) // 头部
	cutoff := time.Unix(segBase+3600*2+50, 0)
	n, err := s.DeleteBefore(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatalf("deleted %d, want 100 (only the whole expired segment)", n)
	}
	if got := countAll(t, s); got != 110 {
		t.Fatalf("count = %d, want 110", got)
	}
	if files := segFiles(t, dir); len(files) != 1 {
		t.Fatalf("segments left = %v", files)
	}
}

// 带分段时重建全文列：分段读回头部一起重建，数据不重复、不丢。
func TestSegmentRebuild(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openSeg(t, "", dir)
	defer s.Close()
	putSeq(t, s, 0, 100)
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	putSeq(t, s, 100, 20)
	n, err := s.RebuildFulltext(ctx, func(string, string) map[string]bool { return nil }, true)
	if err != nil || n != 120 {
		t.Fatalf("rebuild = %d, %v", n, err)
	}
	if got := countAll(t, s); got != 120 {
		t.Fatalf("count = %d, want 120", got)
	}
	if files := segFiles(t, dir); len(files) != 0 {
		t.Fatalf("segments left after rebuild = %v", files)
	}
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if got := countAll(t, s); got != 120 {
		t.Fatalf("count after flush = %d, want 120", got)
	}
}

// 放行分段目录后，用户的表达式仍然读不到目录以外的文件。
func TestSegmentNoExternalAccess(t *testing.T) {
	dir := t.TempDir()
	s := openSeg(t, "", dir)
	defer s.Close()
	outside := filepath.Join(t.TempDir(), "x.csv")
	os.WriteFile(outside, []byte("a\n1\n"), 0o644)
	for _, q := range []string{
		`SELECT * FROM read_csv('/etc/hosts')`,
		`SELECT * FROM read_csv('` + outside + `')`,
		`SELECT * FROM read_csv('` + s.segDir + `/../` + filepath.Base(filepath.Dir(outside)) + `/x.csv')`,
		`SET allowed_directories = ['/']`,
		`SET enable_external_access = true`,
	} {
		if _, err := s.db.Exec(q); err == nil {
			t.Errorf("%s: want error", q)
		}
	}
}

// 字段列：一部分分段写了某个字段的列、一部分没写（那时它不常见），再加上头部，查询结果与逐行解析 JSON 相同；
// 只差大小写的字段名是两个字段。
func TestSegmentFieldColumns(t *testing.T) {
	ctx := context.Background()
	s := openSeg(t, "", t.TempDir())
	defer s.Close()
	put := func(offset int, kvs func(i int) []KV, n int) {
		g := &Group{}
		for i := range n {
			g.Logs = append(g.Logs, Log{Time: uint32(segBase + offset + i), Contents: kvs(offset + i)})
		}
		if err := s.Append(ctx, "p", "l", g); err != nil {
			t.Fatal(err)
		}
	}
	// 第一个小时：lvl 只出现在 1 条里（不到 1%），不写成列
	put(0, func(i int) []KV {
		kv := []KV{{"n", strconv.Itoa(i)}, {"Level", "upper"}}
		if i == 0 {
			kv = append(kv, KV{"lvl", "rare"})
		}
		return kv
	}, 200)
	// 第二个小时：lvl 很常见，写成列
	put(3600, func(i int) []KV {
		return []KV{{"n", strconv.Itoa(i)}, {"lvl", []string{"a", "b"}[i%2]}, {"level", "lower"}}
	}, 200)
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	put(7200, func(i int) []KV { return []KV{{"lvl", "a"}, {"n", strconv.Itoa(i)}} }, 10) // 头部

	sn := s.acquire()
	defer s.release(sn)
	var withCol, without int
	for _, g := range sn.segs {
		if g.cols["lvl"] {
			withCol++
		} else {
			without++
		}
		if !g.cols["n"] {
			t.Errorf("%s: n should be a column", g.path)
		}
	}
	if withCol != 1 || without != 1 {
		t.Fatalf("segments with lvl column = %d, without = %d", withCol, without)
	}
	src, err := sn.source(ctx, "p", "l", 0, segBase*2, []string{"lvl", "Level", "level", "n"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		where string
		want  int
	}{
		{fieldcol.Ref("lvl") + " = 'a'", 110},
		{fieldcol.Ref("lvl") + " = 'rare'", 1},
		{fieldcol.Ref("lvl") + " IS NULL", 199},
		{fieldcol.Ref("Level") + " = 'upper'", 200},
		{fieldcol.Ref("level") + " = 'lower'", 200},
		{"TRY_CAST(" + fieldcol.Ref("n") + " AS BIGINT) >= 3700", 110},
	} {
		var n, viaJSON int
		if err := s.db.QueryRow(`SELECT count(*) FROM ` + src + ` WHERE ` + c.where).Scan(&n); err != nil {
			t.Fatal(err)
		}
		// 同一条件改成从 JSON 取值
		j := c.where
		for _, k := range []string{"lvl", "Level", "level", "n"} {
			j = strings.ReplaceAll(j, fieldcol.Ref(k), fieldcol.Extract("contents", k))
		}
		if err := s.db.QueryRow(`SELECT count(*) FROM ` + src + ` WHERE ` + j).Scan(&viaJSON); err != nil {
			t.Fatal(err)
		}
		if n != c.want || viaJSON != c.want {
			t.Errorf("%s: %d rows (JSON: %d), want %d", c.where, n, viaJSON, c.want)
		}
	}
}

// 上下文：同一来源的日志之间隔着超过一个查找范围（1 万条）的其他日志，跨分段与头部，仍按写入顺序找到。
func TestSegmentContextFar(t *testing.T) {
	ctx := context.Background()
	s := openSeg(t, "", t.TempDir())
	defer s.Close()
	put := func(pack string, n int) {
		g := &Group{Tags: []KV{{"__pack_id__", pack}}}
		for i := range n {
			g.Logs = append(g.Logs, Log{Time: segBase, Contents: []KV{{"pack", pack}, {"i", strconv.Itoa(i)}}})
		}
		if err := s.Append(ctx, "p", "l", g); err != nil {
			t.Fatal(err)
		}
	}
	put("AAA-1", 3) // seq 1..3
	for i := range 15 {
		put("BBB-"+strconv.Itoa(i), 1000) // seq 4..15003
	}
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	put("AAA-2", 3) // seq 15004..15006，在头部
	res, err := s.Context(ctx, "p", "l", 2, 5, 10)
	if err != nil {
		t.Fatal(err)
	}
	if res.Back != 1 || res.Forward != 4 {
		t.Fatalf("back = %d, forward = %d, want 1, 4", res.Back, res.Forward)
	}
	var packs []string
	for _, r := range res.Rows {
		var m map[string]string
		if err := json.Unmarshal(r, &m); err != nil {
			t.Fatal(err)
		}
		packs = append(packs, m["pack"]+"/"+m["i"])
	}
	want := []string{"AAA-1/0", "AAA-1/1", "AAA-1/2", "AAA-2/0", "AAA-2/1", "AAA-2/2"}
	if !slices.Equal(packs, want) {
		t.Fatalf("context = %v, want %v", packs, want)
	}
	// 反方向：从头部的锚点往回找到分段里的
	res, err = s.Context(ctx, "p", "l", 15005, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Back != 4 || res.Forward != 0 {
		t.Fatalf("back = %d, forward = %d, want 4, 0", res.Back, res.Forward)
	}
}

// 合并小分段：文件变少、数据不变、合并后的文件有字段列与 bloom；大文件两边的小文件不跨过它合并。
func TestSegmentCompact(t *testing.T) {
	defer func(n int64) { segmentFileRows = n }(segmentFileRows)
	segmentFileRows = 100
	ctx := context.Background()
	dir := t.TempDir()
	s := openSeg(t, "", dir)
	defer s.Close()
	flush := func(offset, n int) {
		putSeq(t, s, offset, n)
		if _, err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 6 {
		flush(i*10, 10) // 6 个 10 行的小文件
	}
	flush(60, 80) // 一个大文件（≥ 50）
	for i := range 3 {
		flush(140+i*10, 10)
	}
	if n := len(segFiles(t, dir)); n != 10 {
		t.Fatalf("%d segments before compaction", n)
	}
	merged, err := s.Compact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if merged != 9 {
		t.Errorf("merged %d files, want 9", merged)
	}
	sn := s.acquire()
	var rows []int64
	for _, g := range sn.segs {
		rows = append(rows, g.rows)
		if !g.cols["i"] {
			t.Errorf("%s: no field column i", g.path)
		}
		if _, err := os.Stat(g.path + ".bloom"); err != nil {
			t.Errorf("%s: %v", g.path, err)
		}
	}
	s.release(sn)
	slices.Sort(rows)
	if !slices.Equal(rows, []int64{30, 60, 80}) {
		t.Errorf("segment rows = %v, want [30 60 80]", rows)
	}
	if n := len(segFiles(t, dir)); n != 3 {
		t.Errorf("%d segment files after compaction", n)
	}
	if got := countAll(t, s); got != 170 {
		t.Errorf("count = %d, want 170", got)
	}
	cond, _ := query.Search("i: 45", index.Auto())
	if n, err := s.Count(ctx, SearchParams{Project: "p", Logstore: "l", From: 0, To: segBase * 2, Where: cond}); err != nil || n != 1 {
		t.Errorf("i: 45 = %d, %v", n, err)
	}
	if merged, err := s.Compact(ctx); err != nil || merged != 0 {
		t.Errorf("second compaction merged %d, %v", merged, err)
	}
}

// 合并写完、旧文件还没删时崩溃：重启后旧文件删掉，数据不重复。
func TestSegmentCompactRecovery(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	path, dir := filepath.Join(tmp, "nested", "data", "sls.duckdb"), filepath.Join(tmp, "segments") // 数据目录不存在
	s := openSeg(t, path, dir)
	for i := range 3 {
		putSeq(t, s, i*10, 10)
		if _, err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	old := segFiles(t, dir)
	// 留一份旧文件的拷贝，合并后放回去，模拟没来得及删
	saved := map[string][]byte{}
	for _, f := range old {
		for _, p := range []string{f, f + ".bloom"} {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			saved[p] = b
		}
	}
	if n, err := s.Compact(ctx); err != nil || n != 3 {
		t.Fatalf("compact = %d, %v", n, err)
	}
	s.Close()
	for p, b := range saved {
		os.WriteFile(p, b, 0o644)
	}
	if n := len(segFiles(t, dir)); n != 4 {
		t.Fatalf("%d files before restart", n)
	}
	s = openSeg(t, path, dir)
	defer s.Close()
	if got := countAll(t, s); got != 30 {
		t.Errorf("count after restart = %d, want 30", got)
	}
	if n := len(segFiles(t, dir)); n != 1 {
		t.Errorf("%d files after restart, want 1", n)
	}
	for p := range saved {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s should have been removed", p)
		}
	}
}

// 还在写入的分区分层合并：同一层的攒够 compactFanIn 个才合并，合并结果不会每次刷段都被重写。
func TestSegmentCompactTiered(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openSeg(t, "", dir)
	defer s.Close()
	s.compactQuiet = time.Hour
	compact := func() {
		for { // 一次合并出的文件可能又凑够了上一层
			n, err := s.Compact(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				return
			}
		}
	}
	for i := range 8 * 8 {
		putSeq(t, s, i, 1) // 每次刷段 1 行
		if _, err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		compact()
	}
	sn := s.acquire()
	var rows []int64
	for _, g := range sn.segs {
		rows = append(rows, g.rows)
	}
	s.release(sn)
	// 64 个 1 行的文件：每 8 个合成一个 8 行的，8 个 8 行的再合成一个 64 行的
	if !slices.Equal(rows, []int64{64}) {
		t.Errorf("segment rows = %v, want [64]", rows)
	}
	// 每行只被重写两次：64 + 8×8 个文件合并
	if n := s.stats.compactedFiles.Load(); n != 64+8 {
		t.Errorf("compacted files = %d, want %d", n, 64+8)
	}

	// 安静下来后，剩下的小文件一次合并完
	for i := range 3 {
		putSeq(t, s, 100+i, 1)
		if _, err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.Compact(ctx); err != nil || n != 0 {
		t.Fatalf("busy partition merged %d, %v", n, err)
	}
	s.compactQuiet = 0
	if n, err := s.Compact(ctx); err != nil || n != 4 {
		t.Fatalf("quiet partition merged %d, %v", n, err)
	}
	if got := countAll(t, s); got != 67 {
		t.Errorf("count = %d", got)
	}
}
