package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/nethinwei/sls-local/internal/query"
)

func openNode(t *testing.T, opts Options) *Store {
	t.Helper()
	opts.SQLOffset, opts.FlushRows, opts.FlushInterval, opts.RefreshInterval = DefaultSQLOffset, 1<<40, time.Hour, time.Hour
	opts.OnError = func(err error) { t.Error(err) }
	s, err := Open(context.Background(), "", opts)
	if err != nil {
		t.Fatal(err)
	}
	s.compactQuiet = 0 // 合并测试默认按安静的分区（一次合并完）
	return s
}

// 两个写入节点、一个查询节点共用一个对象存储。
func TestCluster(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	storage := "file://" + filepath.Join(tmp, "remote")
	w1 := openNode(t, Options{WriterID: 1, SegmentDir: filepath.Join(tmp, "w1"), Storage: storage})
	defer w1.Close()
	w2 := openNode(t, Options{WriterID: 2, SegmentDir: filepath.Join(tmp, "w2"), Storage: storage})
	defer w2.Close()
	r := openNode(t, Options{ReadOnly: true, SegmentDir: filepath.Join(tmp, "r"), Storage: storage})
	defer r.Close()

	for i := range 3 { // w1 刷出几个小分段，之后合并
		putSeq(t, w1, i*20, 20)
		if _, err := w1.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	putSeq(t, w2, 1000, 50)
	if _, err := w2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	check := func(when string) {
		t.Helper()
		if n := countAll(t, r); n != 110 {
			t.Errorf("%s: reader count = %d, want 110", when, n)
		}
		for q, want := range map[string]int64{"i: 1042": 1, "i: 42": 1, "i: 5": 1} {
			if n := countWhere(t, r, q); n != want {
				t.Errorf("%s: %s = %d", when, q, n)
			}
		}
		// 两步取页：seq 在各写入节点间不重复，按 seq 取回的就是排出来的那些行
		cond, _ := query.Search("*", nil)
		res, err := r.Search(ctx, SearchParams{Project: "p", Logstore: "l", From: 0, To: segBase * 2, Where: cond, Limit: 200})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Rows) != 110 {
			t.Errorf("%s: search rows = %d", when, len(res.Rows))
		}
	}
	check("after flush")

	if err := r.Append(ctx, "p", "l", &Group{Logs: []Log{{Time: segBase}}}); !errors.Is(err, ErrReadOnly) {
		t.Errorf("append to reader: %v", err)
	}

	// w1 合并：旧分段在对象存储上过一会儿才删，查询节点按包含关系只读合并后的
	if n, err := w1.Compact(ctx); err != nil || n != 3 {
		t.Fatalf("compact = %d, %v", n, err)
	}
	if err := r.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	check("after compaction")
	if keys := remoteKeys(t, filepath.Join(tmp, "remote")); len(keys) != 10 { // 旧分段 3 + 合并后 1 + w2 1，各带旁路文件
		t.Errorf("remote = %v", keys)
	}

	// w1 换了磁盘：只接管自己的分段，seq 接着自己的往下编
	w1.Close()
	w1 = openNode(t, Options{WriterID: 1, SegmentDir: filepath.Join(tmp, "w1-new"), Storage: storage})
	if n := countAll(t, w1); n != 60 {
		t.Errorf("w1 after new disk sees %d, want its own 60", n)
	}
	if got := w1.seq >> writerShift; got != 1 || w1.seq != 1<<writerShift+60 {
		t.Errorf("w1 seq = %d", w1.seq)
	}
	putSeq(t, w1, 100, 10)
	if _, err := w1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countAll(t, r); n != 120 {
		t.Errorf("reader count = %d, want 120", n)
	}
	if keys := remoteKeys(t, filepath.Join(tmp, "remote")); len(keys) != 6 { // w1 启动时删掉了自己被包含的旧分段
		t.Errorf("remote after w1 restart = %v", keys)
	}
}

// 写入节点（编号不为 0）还没刷过段时，不能因为 seq 从高位起编就以为积累了很多行。
func TestWriterFlushThreshold(t *testing.T) {
	s, err := Open(context.Background(), "", Options{SQLOffset: DefaultSQLOffset, WriterID: 3, SegmentDir: t.TempDir(),
		FlushRows: 1000, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	putSeq(t, s, 0, 10)
	s.mu.Lock()
	seq := s.seq
	s.mu.Unlock()
	if n := s.pendingRows(seq); n != 10 {
		t.Fatalf("pending rows = %d, want 10", n)
	}
}
