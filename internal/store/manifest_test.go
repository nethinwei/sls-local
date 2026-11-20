package store

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nethinwei/sls-local/internal/query"
)

// countingObjects 数一数读了几次小对象（清单）。
type countingObjects struct {
	objects
	reads atomic.Int64
}

func (o *countingObjects) ReadSmall(ctx context.Context, key string) ([]byte, error) {
	o.reads.Add(1)
	return o.objects.ReadSmall(ctx, key)
}

// 查询节点只按清单看分段：清单没变时不重新下载；上传了却没写进清单的分段（写清单前崩溃）在写入节点重启后、
// 或者节点下线后由其他写入节点补上。
func TestManifests(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	remote := filepath.Join(tmp, "remote")
	storage := "file://" + remote
	w := openNode(t, Options{WriterID: 1, SegmentDir: filepath.Join(tmp, "w1"), Storage: storage})
	r := openNode(t, Options{ReadOnly: true, SegmentDir: filepath.Join(tmp, "r"), Storage: storage})
	defer r.Close()
	co := &countingObjects{objects: r.remote.obj}
	r.remote.obj = co

	putSeq(t, w, 0, 10)
	putSeq(t, w, 7200, 10) // 另一个小时
	if _, err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	refresh := func(want int64) {
		t.Helper()
		if err := r.refresh(ctx); err != nil {
			t.Fatal(err)
		}
		if n := countAll(t, r); n != want {
			t.Errorf("reader count = %d, want %d", n, want)
		}
	}
	refresh(20)
	if n := co.reads.Load(); n != 2 {
		t.Errorf("first refresh read %d manifests, want 2", n)
	}
	putSeq(t, w, 10, 5) // 只改了一个小时
	if _, err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	refresh(25)
	if n := co.reads.Load(); n != 3 {
		t.Errorf("second refresh read %d manifests in all, want 3", n)
	}
	refresh(25)
	if n := co.reads.Load(); n != 3 {
		t.Errorf("unchanged manifests were read again (%d)", n)
	}

	// 写清单之前崩溃：把清单删掉模拟，重启后补上
	if err := w.remote.obj.Delete(ctx, manifestKey(1, segBase)); err != nil {
		t.Fatal(err)
	}
	refresh(10)
	w.Close()
	w = openNode(t, Options{WriterID: 1, SegmentDir: filepath.Join(tmp, "w1"), Storage: storage})
	refresh(25)

	// 节点下线后：由其他写入节点按列举结果重写它的清单
	w.Close()
	if err := r.remote.obj.Delete(ctx, manifestKey(1, segBase)); err != nil {
		t.Fatal(err)
	}
	refresh(10)
	other := openNode(t, Options{WriterID: 2, SegmentDir: filepath.Join(tmp, "w2"), Storage: storage})
	defer other.Close()
	if _, err := other.DeleteBefore(ctx, time.Unix(segBase-3600, 0)); err != nil {
		t.Fatal(err)
	}
	refresh(25)
}

// 写入节点滚动重启时，另一个写入节点可能把它当成已下线（正常退出删掉了租约），用重启前的列举结果覆盖它的清单。
// 写入节点记着自己写过的清单内容，内容没变就不再写：保留期检查时要按当前快照重写全部清单，被覆盖的才能恢复。
func TestManifestsHealAfterOverwrite(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	storage := "file://" + filepath.Join(tmp, "remote")
	w := openNode(t, Options{WriterID: 1, SegmentDir: filepath.Join(tmp, "w1"), Storage: storage})
	defer w.Close()
	r := openNode(t, Options{ReadOnly: true, SegmentDir: filepath.Join(tmp, "r"), Storage: storage})
	defer r.Close()

	putSeq(t, w, 0, 10)
	if _, err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// 另一个节点用旧的列举结果覆盖了这个小时的清单（这里是一个空清单）
	if err := w.remote.obj.WriteSmall(ctx, manifestKey(1, segBase), []byte(`{"segments":[]}`)); err != nil {
		t.Fatal(err)
	}
	if err := r.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countAll(t, r); n != 0 {
		t.Fatalf("reader count after overwrite = %d, want 0", n)
	}
	if _, err := w.DeleteBefore(ctx, time.Unix(segBase-3600, 0)); err != nil {
		t.Fatal(err)
	}
	if err := r.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countAll(t, r); n != 10 {
		t.Errorf("reader count after retention check = %d, want 10", n)
	}
}

// slowObjects 让下载慢一点，记下同时在下载的最大个数。
type slowObjects struct {
	objects
	cur, peak atomic.Int64
}

func (o *slowObjects) Get(ctx context.Context, key, local string) error {
	n := o.cur.Add(1)
	defer o.cur.Add(-1)
	for p := o.peak.Load(); n > p && !o.peak.CompareAndSwap(p, n); p = o.peak.Load() {
	}
	select {
	case <-time.After(20 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	return o.objects.Get(ctx, key, local)
}

// 查询节点：刷新时预先下载新分段的旁路文件；查询时并行下载分段；查询取消时下载也停。
func TestReaderDownloads(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	storage := "file://" + filepath.Join(tmp, "remote")
	w := openNode(t, Options{WriterID: 1, SegmentDir: filepath.Join(tmp, "w"), Storage: storage})
	defer w.Close()
	r := openNode(t, Options{ReadOnly: true, SegmentDir: filepath.Join(tmp, "r"), Storage: storage})
	defer r.Close()
	so := &slowObjects{objects: r.remote.obj}
	r.remote.obj = so
	for i := range 20 { // 20 个小时，20 个分段
		putSeq(t, w, i*3600, 1)
	}
	if _, err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	r.segMu.Lock()
	segs := slices.Clone(r.cur.segs)
	r.segMu.Unlock()
	if len(segs) != 20 {
		t.Fatalf("%d segments", len(segs))
	}
	for _, g := range segs {
		if _, err := os.Stat(g.path + ".bloom"); err != nil {
			t.Errorf("sidecar not prefetched: %v", err)
		}
		if _, err := os.Stat(g.path); err == nil {
			t.Errorf("segment %s downloaded before any query", g.path)
		}
	}
	if p := so.peak.Load(); p < 2 {
		t.Errorf("sidecars downloaded one at a time (peak %d)", p)
	}

	so.peak.Store(0)
	cctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := r.Count(cctx, SearchParams{Project: "p", Logstore: "l", From: 0, To: segBase * 2,
		Where: query.Cond{SQL: "TRUE"}}); err == nil {
		t.Error("canceled query succeeded")
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Errorf("canceled query took %v", d)
	}
	if n := countAll(t, r); n != 20 {
		t.Errorf("count = %d", n)
	}
	if p := so.peak.Load(); p < 2 {
		t.Errorf("segments downloaded one at a time (peak %d)", p)
	}
}
