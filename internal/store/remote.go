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
}

type trashItem struct {
	key string
	at  time.Time
}

type fetchCall struct {
	done chan struct{}
	err  error
}

// ensureLocal 保证分段（或旁路文件）在本地：本地没有就从对象存储下载。
func (s *Store) ensureLocal(ctx context.Context, local string) error {
	r := s.remote
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.lastUse[local] = time.Now()
	if c, ok := r.fetching[local]; ok {
		r.mu.Unlock()
		select {
		case <-c.done:
			return c.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if _, err := os.Stat(local); err == nil {
		r.mu.Unlock()
		return nil
	}
	c := &fetchCall{done: make(chan struct{})}
	r.fetching[local] = c
	r.mu.Unlock()

	c.err = r.obj.Get(ctx, s.objectKey(local), local)
	r.mu.Lock()
	delete(r.fetching, local)
	r.mu.Unlock()
	close(c.done)
	if c.err != nil {
		s.stats.downloadFailures.Add(1)
		return fmt.Errorf("download segment: %w", c.err)
	}
	s.stats.downloads.Add(1)
	return nil
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
	return nil
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

// evict 删除已上传、remoteGrace 内没用过的本地分段，直到本地缓存不超过上限。最久没用的先删。
func (s *Store) evict() error {
	r := s.remote
	if r == nil || r.limit <= 0 {
		return nil
	}
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
	segs, err := s.remoteSegments(ctx)
	if err != nil {
		return err
	}
	if err := s.registerLogstores(ctx, segs); err != nil {
		return err
	}
	s.publish(&snapshot{segs: segs}, nil)
	return s.evict()
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
	slices.SortFunc(segs, compareSegments)
	kept := segs[:0]
	for _, g := range segs {
		if n := len(kept); n > 0 && samePartition(kept[n-1], g) && g.seqMax <= kept[n-1].seqMax {
			if s.owns(g) {
				for _, k := range []string{s.objectKey(g.path), s.objectKey(g.path + ".bloom")} {
					if err := s.remote.obj.Delete(ctx, k); err != nil {
						return nil, err
					}
				}
			}
			continue
		}
		kept = append(kept, g)
	}
	return kept, nil
}
