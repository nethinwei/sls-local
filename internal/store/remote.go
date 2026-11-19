package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

// 分段放在对象存储上时，分段目录是本地缓存：
//
//   - 分段在本地写好、边界提交之后才上传（先旁路文件后分段），所以对象存储上的分段都是完整、已提交的；
//     上传失败的留在本地，下次刷段时重试。
//   - 查询用到本地没有的分段时下载（同一个文件同时只下载一次）。
//   - 已上传、一段时间没用过的本地文件按最近使用时间淘汰，缓存不超过 cacheLimit。
//   - 合并掉的旧分段、过期的分段在对象存储上过一段时间再删（正在下载它们的查询、其他读节点还要用）；
//     进程退出时没来得及删的，启动时按包含关系、保留期清理。

// remoteGrace 是对象存储上删除分段前的等待时间，也是本地文件最近用过多久内不淘汰。
var remoteGrace = 10 * time.Minute

type remoteState struct {
	obj   objects
	limit int64         // 本地缓存上限（字节），0 表示不限
	grace time.Duration // 见 remoteGrace

	mu       sync.Mutex
	pending  map[string]bool       // 还没上传的分段（本地路径）
	lastUse  map[string]time.Time  // 本地文件最近一次被查询用到的时间
	trash    []trashItem           // 等着从对象存储删除的键
	fetching map[string]*fetchCall // 正在下载的文件
	fetchWG  sync.WaitGroup        // 后台的下载（关闭时等它们结束）
	closed   bool                  // 已关闭：不再开始新的下载
	evicted  time.Time             // 上次淘汰本地缓存的时间

	manifests map[int64]string        // 写入节点：各小时清单在对象存储上的内容（见 manifest.go，flushMu 保护）
	read      map[string]readManifest // 查询节点：读到的清单（按键）
}

type trashItem struct {
	key string
	at  time.Time
}

type fetchCall struct {
	done chan struct{}
	err  error
}

// ensureLocal 保证分段（或旁路文件）在本地：本地没有就从对象存储下载。同一个文件同时只下载一次。
//
// 下载不随调用方的 ctx 取消（另一个查询可能在等同一个文件，下好了以后的查询也用得上），调用方的 ctx 结束时
// 只是不再等。
func (s *Store) ensureLocal(ctx context.Context, local string) error {
	r := s.remote
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.lastUse[local] = time.Now()
	c, ok := r.fetching[local]
	if !ok {
		if _, err := os.Stat(local); err == nil {
			r.mu.Unlock()
			return nil
		}
		if r.closed {
			r.mu.Unlock()
			return errStoreClosed
		}
		c = &fetchCall{done: make(chan struct{})}
		r.fetching[local] = c
		r.fetchWG.Add(1)
		go s.fetch(local, c)
	}
	r.mu.Unlock()
	select {
	case <-c.done:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

var errStoreClosed = errors.New("store is closed")

// closeDownloads 不再开始新的下载，并等正在进行的结束（关闭存储时）。
func (r *remoteState) closeDownloads() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.fetchWG.Wait()
}

// downloadTimeout 是下载一个文件的最长时间。
const downloadTimeout = 10 * time.Minute

func (s *Store) fetch(local string, c *fetchCall) {
	r := s.remote
	defer r.fetchWG.Done()
	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()
	err := r.obj.Get(ctx, s.objectKey(local), local)
	if err != nil {
		s.stats.downloadFailures.Add(1)
		err = fmt.Errorf("download segment: %w", err)
	} else {
		s.stats.downloads.Add(1)
	}
	r.mu.Lock()
	c.err = err
	delete(r.fetching, local)
	r.mu.Unlock()
	close(c.done)
}

// markPending 记下新写的分段，等着上传。
func (s *Store) markPending(segs []segment) {
	if s.remote == nil {
		return
	}
	s.remote.mu.Lock()
	for _, g := range segs {
		s.remote.pending[g.path] = true
	}
	s.remote.mu.Unlock()
}

// upload 上传还没上传的分段（先旁路文件后分段）。已经不在当前快照里的（合并掉了、过期了）不再上传。
// 调用方持有 flushMu。
func (s *Store) upload(ctx context.Context) error {
	r := s.remote
	if r == nil {
		return nil
	}
	s.segMu.Lock()
	live := map[string]bool{}
	for _, g := range s.cur.segs {
		live[g.path] = true
	}
	s.segMu.Unlock()
	r.mu.Lock()
	var todo []string
	for p := range r.pending {
		if live[p] {
			todo = append(todo, p)
		} else {
			delete(r.pending, p)
		}
	}
	r.mu.Unlock()
	slices.Sort(todo)
	for _, p := range todo {
		for _, f := range []string{p + ".bloom", p} {
			if err := r.obj.Put(ctx, s.objectKey(f), f); err != nil && !errors.Is(err, fs.ErrNotExist) {
				s.stats.uploadFailures.Add(1)
				return fmt.Errorf("upload %s: %w", s.objectKey(f), err)
			}
			s.stats.uploads.Add(1)
		}
		r.mu.Lock()
		delete(r.pending, p)
		r.mu.Unlock()
	}
	return s.syncManifests(ctx)
}

// trashRemote 安排在 remoteGrace 之后从对象存储删除分段与旁路文件。
func (s *Store) trashRemote(segs []segment) {
	if s.remote == nil {
		return
	}
	at := time.Now().Add(s.remote.grace)
	s.remote.mu.Lock()
	for _, g := range segs {
		delete(s.remote.pending, g.path)
		// 先删分段再删旁路文件：分段在，旁路文件就在
		s.remote.trash = append(s.remote.trash, trashItem{s.objectKey(g.path), at}, trashItem{s.objectKey(g.path + ".bloom"), at})
	}
	s.remote.mu.Unlock()
}

// emptyTrash 删除到期的对象。
func (s *Store) emptyTrash(ctx context.Context) error {
	r := s.remote
	if r == nil {
		return nil
	}
	now := time.Now()
	r.mu.Lock()
	var due []trashItem
	keep := r.trash[:0]
	for _, t := range r.trash {
		if t.at.After(now) {
			keep = append(keep, t)
		} else {
			due = append(due, t)
		}
	}
	r.trash = keep
	r.mu.Unlock()
	for i, t := range due {
		if err := r.obj.Delete(ctx, t.key); err != nil {
			r.mu.Lock()
			r.trash = append(r.trash, due[i:]...)
			r.mu.Unlock()
			return fmt.Errorf("delete %s: %w", t.key, err)
		}
	}
	return nil
}

// evictEvery 是淘汰本地缓存的最短间隔：要遍历整个缓存目录，不必每次刷段、刷新都做。
var evictEvery = 30 * time.Second

// evict 删除已上传、remoteGrace 内没用过的本地分段，直到本地缓存不超过上限。最久没用的先删。
func (s *Store) evict() error {
	r := s.remote
	if r == nil || r.limit <= 0 {
		return nil
	}
	r.mu.Lock()
	if since := time.Since(r.evicted); since < evictEvery && !s.testRemote {
		r.mu.Unlock()
		return nil
	}
	r.evicted = time.Now()
	r.mu.Unlock()
	type file struct {
		path string
		size int64
		used time.Time
	}
	var files []file
	var total int64
	err := filepath.WalkDir(s.segDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return nil // 刚被删
		}
		total += info.Size()
		files = append(files, file{p, info.Size(), info.ModTime()})
		return nil
	})
	if err != nil || total <= r.limit {
		return err
	}
	cutoff := time.Now().Add(-r.grace)
	r.mu.Lock()
	var cand []file
	for _, f := range files {
		seg := strings.TrimSuffix(f.path, ".bloom")
		if r.pending[seg] || strings.HasSuffix(f.path, ".tmp") {
			continue
		}
		if u, ok := r.lastUse[f.path]; ok {
			f.used = u
		}
		if f.used.Before(cutoff) {
			cand = append(cand, f)
		}
	}
	r.mu.Unlock()
	slices.SortFunc(cand, func(a, b file) int { return a.used.Compare(b.used) })
	for _, f := range cand {
		if total <= r.limit {
			break
		}
		if err := removeIfExists(f.path); err != nil {
			return err
		}
		r.mu.Lock()
		delete(r.lastUse, f.path)
		r.mu.Unlock()
		if strings.HasSuffix(f.path, ".bloom") {
			s.blooms.forget(strings.TrimSuffix(f.path, ".bloom"))
		}
		total -= f.size
	}
	return nil
}

// owns 判断分段是不是这个节点写的（只有写入节点删、合并自己的分段；查询节点什么也不删）。
func (s *Store) owns(g segment) bool { return !s.readOnly && writerOf(g.seqMin) == s.writerID }

// registerLogstores 按分段补登记 logstore（登记在本地库里，换了磁盘、查询节点都要从分段得知），
// 创建时间取最早的日志。
func (s *Store) registerLogstores(ctx context.Context, segs []segment) error {
	created := map[[2]string]int64{}
	for _, g := range segs {
		k := [2]string{g.project, g.logstore}
		if c, ok := created[k]; !ok || g.tmin < c {
			created[k] = g.tmin
		}
	}
	for k, c := range created {
		if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO logstores VALUES (?, ?, ?)`, k[0], k[1], c); err != nil {
			return err
		}
	}
	return nil
}

// refresh 是查询节点定期做的：重新列出对象存储上的分段，换上新的快照，淘汰本地缓存。
func (s *Store) refresh(ctx context.Context) error {
	segs, err := s.manifestSegments(ctx)
	if err != nil {
		return err
	}
	if err := s.registerLogstores(ctx, segs); err != nil {
		return err
	}
	s.segMu.Lock()
	old := map[string]segment{}
	for _, g := range s.cur.segs {
		old[g.path] = g
	}
	s.segMu.Unlock()
	var added []string
	for _, g := range segs {
		if _, ok := old[g.path]; ok {
			delete(old, g.path)
		} else {
			added = append(added, g.path+".bloom")
		}
	}
	// 不在新快照里的分段（合并掉了、过期了）不会再被查询用到：旧快照都不用了以后删掉本地的文件。
	// 不删的话本地会积下大量刷段产生的小文件，要等缓存总量超过上限才淘汰
	var removed []segment
	for _, g := range old {
		removed = append(removed, g)
	}
	s.publish(&snapshot{segs: segs}, func() { s.removeSegments(removed) })
	// 新分段的旁路文件（bloom、字段列清单）先下载好：查询多半看最近的数据，它们小，查询时就不用等
	err = parallel(ctx, len(added), func(i int) error {
		if err := s.ensureLocal(ctx, added[i]); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		return nil
	})
	return errors.Join(err, s.evict())
}

// refresher 在后台定期 refresh，直到 ctx 结束。
func (s *Store) refresher(ctx context.Context, interval time.Duration) {
	defer close(s.flusherDone)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if err := s.refresh(ctx); err != nil && ctx.Err() == nil && s.onError != nil {
			s.onError(fmt.Errorf("refresh segments: %w", err))
		}
	}
}

// maintainRemote 重试上传、删除到期的对象、淘汰本地缓存（刷段线程定期调用）。
func (s *Store) maintainRemote(ctx context.Context) error {
	if s.remote == nil || s.fenced.Load() {
		return nil
	}
	s.flushMu.Lock()
	err := s.upload(ctx)
	s.flushMu.Unlock()
	return errors.Join(err, s.emptyTrash(ctx), s.evict())
}

// remoteSegments 列出对象存储上的分段（本地路径形式），去掉被别的分段包含的（合并后没来得及删的旧分段；
// 是自己写的就顺带从对象存储删掉）。
func (s *Store) remoteSegments(ctx context.Context) ([]segment, error) {
	keys, err := s.remote.obj.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", s.remote.obj, err)
	}
	var segs []segment
	for _, k := range keys {
		local := filepath.Join(s.segDir, filepath.FromSlash(k))
		if g, ok := parseSegment(s.segDir, local); ok {
			segs = append(segs, g)
		}
	}
	kept, dropped := dropContained(segs)
	for _, g := range dropped {
		if s.owns(g) {
			for _, k := range []string{s.objectKey(g.path), s.objectKey(g.path + ".bloom")} {
				if err := s.remote.obj.Delete(ctx, k); err != nil {
					return nil, err
				}
			}
		}
	}
	return kept, nil
}
