package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 编号相同的第二个写入节点启动不了；同一块盘上重启（包括没来得及释放租约的崩溃）可以直接启动。
func TestWriterLease(t *testing.T) {
	tmp := t.TempDir()
	storage := "file://" + filepath.Join(tmp, "remote")
	open := func(data, segs string) (*Store, error) {
		return Open(context.Background(), data, Options{SQLOffset: DefaultSQLOffset, WriterID: 1, Storage: storage,
			SegmentDir: segs, FlushRows: 1 << 40, FlushInterval: time.Hour})
	}
	a, err := open(filepath.Join(tmp, "a.duckdb"), filepath.Join(tmp, "a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := open(filepath.Join(tmp, "b.duckdb"), filepath.Join(tmp, "b")); err == nil ||
		!strings.Contains(err.Error(), "in use") {
		t.Fatalf("second writer with the same id: %v", err)
	}

	// 崩溃：不释放租约就退出，同一块盘上重启
	a.stopLease()
	<-a.leaseDone
	a.stopLease = nil
	a.Close()
	if a, err = open(filepath.Join(tmp, "a.duckdb"), filepath.Join(tmp, "a")); err != nil {
		t.Fatalf("restart on the same disk: %v", err)
	}
	if _, err := open(filepath.Join(tmp, "b.duckdb"), filepath.Join(tmp, "b")); err == nil {
		t.Fatal("second writer started after the restart")
	}

	// 正常退出释放租约，换盘也能启动
	a.Close()
	b, err := open(filepath.Join(tmp, "b.duckdb"), filepath.Join(tmp, "b"))
	if err != nil {
		t.Fatalf("after release: %v", err)
	}
	b.Close()
}

// 租约被别人拿走后停止写入、刷段，就绪检查失败。
func TestWriterFenced(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	storage := "file://" + filepath.Join(tmp, "remote")
	s, err := Open(ctx, "", Options{SQLOffset: DefaultSQLOffset, WriterID: 2, Storage: storage,
		SegmentDir: filepath.Join(tmp, "s"), FlushRows: 1 << 40, FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	putSeq(t, s, 0, 5)
	if err := s.renewLease(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.remote.obj.WriteSmall(ctx, leaseKey(2), []byte(`{"token":"other","renewed":1,"host":"x"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.renewLease(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("renew = %v", err)
	}
	if err := s.Append(ctx, "p", "l", &Group{Logs: []Log{{Time: segBase}}}); !errors.Is(err, ErrFenced) {
		t.Errorf("append = %v", err)
	}
	if _, err := s.Flush(ctx); !errors.Is(err, ErrFenced) {
		t.Errorf("flush = %v", err)
	}
	if _, err := s.DeleteBefore(ctx, time.Unix(segBase*2, 0)); !errors.Is(err, ErrFenced) {
		t.Errorf("delete = %v", err)
	}
	if err := s.Ping(ctx); !errors.Is(err, ErrFenced) {
		t.Errorf("ping = %v", err)
	}
	if keys := remoteKeys(t, filepath.Join(tmp, "remote")); len(keys) != 0 {
		t.Errorf("fenced writer uploaded %v", keys)
	}
	// 退出时不删别人的租约
	s.Close()
	if r, err := s.readLease(ctx, 2); err != nil || r == nil || r.Token != "other" {
		t.Errorf("lease after close = %+v, %v", r, err)
	}
}

// 编号下的 seq 用完时拒绝写入，不编进下一个编号。
func TestSeqExhausted(t *testing.T) {
	for _, id := range []int64{0, 5, MaxWriterID - 1} {
		s, err := Open(context.Background(), "", Options{SQLOffset: DefaultSQLOffset, WriterID: id})
		if err != nil {
			t.Fatal(err)
		}
		s.seq = (id+1)<<writerShift - 3 // 还剩 2 个
		if id == MaxWriterID-1 {
			s.seq = 1<<63 - 1 - 2
		}
		putSeq(t, s, 0, 2)
		if err := s.Append(context.Background(), "p", "l", &Group{Logs: []Log{{Time: segBase}}}); !errors.Is(err, ErrSeqExhausted) {
			t.Errorf("writer %d: append past the end = %v", id, err)
		}
		if n := countAll(t, s); n != 2 {
			t.Errorf("writer %d: count = %d", id, n)
		}
		s.Close()
	}
}

// 下线的写入节点（租约过期或已释放）留下的过期分段由其他写入节点删除；还在运行的节点的不动。
func TestDropAbandoned(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	remote := filepath.Join(tmp, "remote")
	storage := "file://" + remote
	gone := openNode(t, Options{WriterID: 1, SegmentDir: filepath.Join(tmp, "w1"), Storage: storage})
	putSeq(t, gone, 0, 10) // 过期
	if _, err := gone.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	gone.Close()
	alive := openNode(t, Options{WriterID: 2, SegmentDir: filepath.Join(tmp, "w2"), Storage: storage})
	defer alive.Close()
	putSeq(t, alive, 0, 10) // 过期，但节点在运行：由它自己删
	if _, err := alive.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	w := openNode(t, Options{WriterID: 3, SegmentDir: filepath.Join(tmp, "w3"), Storage: storage})
	defer w.Close()
	putSeq(t, w, 7200, 10) // 未过期
	if _, err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if keys := remoteKeys(t, remote); len(keys) != 6 {
		t.Fatalf("remote = %v", keys)
	}
	w.remote.grace = 0
	n, err := w.DeleteBefore(ctx, time.Unix(segBase+3600, 0))
	if err != nil || n != 10 {
		t.Fatalf("delete = %d, %v", n, err)
	}
	if err := w.emptyTrash(ctx); err != nil {
		t.Fatal(err)
	}
	keys := remoteKeys(t, remote)
	if len(keys) != 4 {
		t.Fatalf("remote after = %v", keys)
	}
	for _, k := range keys {
		g, ok := parseSegment(remote, filepath.Join(remote, strings.TrimSuffix(k, ".bloom")))
		if !ok || writerOf(g.seqMin) == 1 {
			t.Errorf("abandoned segment %s kept (parsed %v)", k, ok)
		}
	}
}
