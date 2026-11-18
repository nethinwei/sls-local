package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nethinwei/sls-local/internal/index"
	"github.com/nethinwei/sls-local/internal/query"
)

func openRemote(t *testing.T, path, cache, storage string, cacheSize int64) *Store {
	t.Helper()
	s, err := Open(context.Background(), path, Options{SQLOffset: DefaultSQLOffset, SegmentDir: cache, FlushRows: 1 << 40,
		FlushInterval: time.Hour, Storage: storage, CacheSize: cacheSize, OnError: func(err error) { t.Error(err) }})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func remoteKeys(t *testing.T, dir string) []string {
	t.Helper()
	keys, err := fsObjects{root: dir}.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	return keys
}

func countWhere(t *testing.T, s *Store, q string) int64 {
	t.Helper()
	cond, err := query.Search(q, index.Auto())
	if err != nil {
		t.Fatal(err)
	}
	n, err := s.Count(context.Background(), SearchParams{Project: "p", Logstore: "l", From: 0, To: segBase * 2, Where: cond})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// 本地的库与缓存都丢了（换了磁盘），只剩对象存储：数据都在，字段列与 bloom 从旁路文件读，seq 接着往下编。
func TestRemoteNewDisk(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	remote := filepath.Join(tmp, "remote")
	s := openRemote(t, filepath.Join(tmp, "a", "sls.duckdb"), filepath.Join(tmp, "a", "segments"), "file://"+remote, 0)
	putSeq(t, s, 0, 100)
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if keys := remoteKeys(t, remote); len(keys) != 2 || !strings.HasSuffix(keys[0], ".parquet") || !strings.HasSuffix(keys[1], ".bloom") {
		t.Fatalf("remote = %v", keys)
	}

	s = openRemote(t, filepath.Join(tmp, "b", "sls.duckdb"), filepath.Join(tmp, "b", "segments"), "file://"+remote, 0)
	defer s.Close()
	// 与 main 一样，启动后做升级检查：新库没有版本记录，不能因此把对象存储上的分段读回、删掉
	if _, err := s.RebuildFulltext(ctx, func(string, string) map[string]bool { return nil }, false); err != nil {
		t.Fatal(err)
	}
	if keys := remoteKeys(t, remote); len(keys) != 2 {
		t.Fatalf("remote after upgrade check = %v", keys)
	}
	if info, ok, err := s.Logstore(ctx, "p", "l"); err != nil || !ok || info.Created != segBase {
		t.Fatalf("logstore after new disk = %+v, %v, %v", info, ok, err)
	}
	if n := countAll(t, s); n != 100 {
		t.Fatalf("count = %d", n)
	}
	if n := countWhere(t, s, "i: 42"); n != 1 { // 字段列（旁路文件里的列清单）
		t.Errorf("i: 42 = %d", n)
	}
	if n := countWhere(t, s, "nosuchword"); n != 0 {
		t.Errorf("nosuchword = %d", n)
	}
	putSeq(t, s, 100, 10)
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countAll(t, s); n != 110 {
		t.Errorf("count after more writes = %d", n)
	}
	if keys := remoteKeys(t, remote); len(keys) != 4 {
		t.Errorf("remote = %v", keys)
	}
}

// failingObjects 让上传失败，其余照常。
type failingObjects struct{ objects }

func (failingObjects) Put(context.Context, string, string) error { return errors.New("upload failed") }

// 上传失败：分段留在本地，重启后补传。
func TestRemoteRetryUpload(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	remote, path, cache := filepath.Join(tmp, "remote"), filepath.Join(tmp, "sls.duckdb"), filepath.Join(tmp, "segments")
	s := openRemote(t, path, cache, "file://"+remote, 0)
	s.onError = nil
	s.remote.obj = failingObjects{s.remote.obj}
	putSeq(t, s, 0, 50)
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if keys := remoteKeys(t, remote); len(keys) != 0 {
		t.Fatalf("remote = %v", keys)
	}
	s.Close()
	s = openRemote(t, path, cache, "file://"+remote, 0)
	defer s.Close()
	if err := s.maintainRemote(ctx); err != nil {
		t.Fatal(err)
	}
	if keys := remoteKeys(t, remote); len(keys) != 2 {
		t.Errorf("remote after restart = %v", keys)
	}
	if n := countAll(t, s); n != 50 {
		t.Errorf("count = %d", n)
	}
}

// 本地缓存超过上限时淘汰已上传的分段；之后的查询重新下载。合并与过期的分段在对象存储上也删掉。
func TestRemoteEvictCompactRetention(t *testing.T) {
	defer func(g time.Duration, n int64) { remoteGrace, segmentFileRows = g, n }(remoteGrace, segmentFileRows)
	remoteGrace, segmentFileRows = 0, 100
	ctx := context.Background()
	tmp := t.TempDir()
	remote, cache := filepath.Join(tmp, "remote"), filepath.Join(tmp, "segments")
	s := openRemote(t, "", cache, "file://"+remote, 1)
	defer s.Close()
	for i := range 3 {
		putSeq(t, s, i*10, 10)
		if _, err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	putSeq(t, s, 7200, 10) // 另一个小时
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Compact(ctx); err != nil || n != 3 {
		t.Fatalf("compact = %d, %v", n, err)
	}
	if err := s.maintainRemote(ctx); err != nil {
		t.Fatal(err)
	}
	if keys := remoteKeys(t, remote); len(keys) != 4 { // 合并后的 + 另一个小时的，各带旁路文件
		t.Fatalf("remote after compaction = %v", keys)
	}
	if files := segFiles(t, cache); len(files) != 0 {
		t.Fatalf("local cache not evicted: %v", files)
	}
	if n := countAll(t, s); n != 40 {
		t.Fatalf("count after eviction = %d", n)
	}
	if n := countWhere(t, s, "i: 25"); n != 1 {
		t.Errorf("i: 25 = %d", n)
	}
	if n, err := s.DeleteBefore(ctx, time.Unix(segBase+3600, 0)); err != nil || n != 30 {
		t.Fatalf("retention = %d, %v", n, err)
	}
	if err := s.maintainRemote(ctx); err != nil {
		t.Fatal(err)
	}
	if keys := remoteKeys(t, remote); len(keys) != 2 {
		t.Errorf("remote after retention = %v", keys)
	}
	if n := countAll(t, s); n != 10 {
		t.Errorf("count after retention = %d", n)
	}
}

// 带对象存储重建全文列：分段读回头部、从对象存储删掉，再刷出去，数据不丢不重。
func TestRemoteRebuild(t *testing.T) {
	ctx := context.Background()
	tmp := t.TempDir()
	remote, path, cache := filepath.Join(tmp, "remote"), filepath.Join(tmp, "sls.duckdb"), filepath.Join(tmp, "segments")
	s := openRemote(t, path, cache, "file://"+remote, 0)
	putSeq(t, s, 0, 100)
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(cache) // 本地缓存没了，重建要先下载
	s.Close()
	s = openRemote(t, path, cache, "file://"+remote, 0)
	defer s.Close()
	if n, err := s.RebuildFulltext(ctx, func(string, string) map[string]bool { return nil }, true); err != nil || n != 100 {
		t.Fatalf("rebuild = %d, %v", n, err)
	}
	if keys := remoteKeys(t, remote); len(keys) != 0 {
		t.Fatalf("remote after read back = %v", keys)
	}
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countAll(t, s); n != 100 {
		t.Errorf("count = %d", n)
	}
	if keys := remoteKeys(t, remote); len(keys) != 2 {
		t.Errorf("remote = %v", keys)
	}
}
