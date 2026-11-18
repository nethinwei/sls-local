package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// 分段清单：查询节点每隔几秒要知道对象存储上有哪些分段。每次都列出全部分段的话，保留 30 天、几十个 logstore
// 时是十几万个键、上百次 LIST 请求。所以每个写入节点按小时维护自己的清单 _manifests/<编号>/<小时>.json
// （这个小时里它的全部分段），查询节点只列出清单（带版本），下载变了的。
//
//   - 写入节点在分段上传之后才把它写进清单，从清单去掉之后过 remoteGrace 才删分段：清单里的分段一定在。
//   - 合并只在一个小时里，新旧分段的替换是同一个清单的一次覆盖写。
//   - 某个小时还有没上传成功的分段时，这个小时的清单先不动（不然合并掉的旧分段会先从清单里消失）。
//   - 写入节点启动时按对象存储上自己的分段重写全部清单；上传之后、写清单之前崩溃的，重启后补上。
//     下线了的写入节点由其他写入节点按列举结果重写清单（dropAbandonedBefore，每小时一次）。

const manifestPrefix = "_manifests/"

type manifest struct {
	Segments []string `json:"segments"` // 分段的键（分段目录下的相对路径）
}

func manifestKey(writer, hour int64) string {
	return manifestPrefix + strconv.FormatInt(writer, 10) + "/" + strconv.FormatInt(hour, 10) + ".json"
}

// encodeManifest 按小时把分段写成清单内容（键排序，内容不变时字节不变）。
func (s *Store) encodeManifest(segs []segment) string {
	keys := make([]string, len(segs))
	for i, g := range segs {
		keys[i] = s.objectKey(g.path)
	}
	slices.Sort(keys)
	b, _ := json.Marshal(manifest{Segments: keys})
	return string(b)
}

// syncManifests 让对象存储上本节点的清单与当前快照里已上传的分段一致。调用方持有 flushMu。
func (s *Store) syncManifests(ctx context.Context) error {
	r := s.remote
	if r == nil || s.readOnly || s.fenced.Load() {
		return nil
	}
	if r.manifests == nil { // 第一次：先认领对象存储上已有的清单，下面没有分段的小时会被删掉
		r.manifests = map[int64]string{}
		vers, err := r.obj.ListVersions(ctx, manifestPrefix+strconv.FormatInt(s.writerID, 10)+"/")
		if err != nil {
			r.manifests = nil
			return fmt.Errorf("list manifests: %w", err)
		}
		for k := range vers {
			if h, err := strconv.ParseInt(strings.TrimSuffix(k[strings.LastIndexByte(k, '/')+1:], ".json"), 10, 64); err == nil {
				r.manifests[h] = "?"
			}
		}
	}
	s.segMu.Lock()
	segs := slices.Clone(s.cur.segs)
	s.segMu.Unlock()
	byHour := map[int64][]segment{}
	busy := map[int64]bool{}
	r.mu.Lock()
	for _, g := range segs {
		h := hourOf(g)
		if r.pending[g.path] {
			busy[h] = true
		}
		byHour[h] = append(byHour[h], g)
	}
	r.mu.Unlock()
	for h, gs := range byHour {
		if busy[h] {
			continue
		}
		body := s.encodeManifest(gs)
		if r.manifests[h] == body {
			continue
		}
		if err := r.obj.WriteSmall(ctx, manifestKey(s.writerID, h), []byte(body)); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
		r.manifests[h] = body
	}
	for h := range r.manifests {
		if _, ok := byHour[h]; ok || busy[h] {
			continue
		}
		if err := r.obj.Delete(ctx, manifestKey(s.writerID, h)); err != nil {
			return fmt.Errorf("delete manifest: %w", err)
		}
		delete(r.manifests, h)
	}
	return nil
}

// manifestSegments 是查询节点看到的分段：全部清单里的分段的并集。只下载版本变了的清单。
func (s *Store) manifestSegments(ctx context.Context) ([]segment, error) {
	r := s.remote
	vers, err := r.obj.ListVersions(ctx, manifestPrefix)
	if err != nil {
		return nil, fmt.Errorf("list manifests: %w", err)
	}
	r.mu.Lock()
	cache := r.read
	r.mu.Unlock()
	next := map[string]readManifest{}
	var todo []string
	for k, v := range vers {
		if !strings.HasSuffix(k, ".json") {
			continue
		}
		if c, ok := cache[k]; ok && c.version == v {
			next[k] = c
		} else {
			todo = append(todo, k)
		}
	}
	var mu sync.Mutex
	err = parallel(ctx, len(todo), func(i int) error {
		k := todo[i]
		data, err := r.obj.ReadSmall(ctx, k)
		if errors.Is(err, fs.ErrNotExist) {
			return nil // 列出之后刚被删
		}
		if err != nil {
			return fmt.Errorf("read manifest %s: %w", k, err)
		}
		var m manifest
		if err := json.Unmarshal(data, &m); err != nil {
			return fmt.Errorf("read manifest %s: %w", k, err)
		}
		mu.Lock()
		next[k] = readManifest{version: vers[k], segments: m.Segments}
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.read = next
	r.mu.Unlock()
	var segs []segment
	for _, m := range next {
		for _, k := range m.segments {
			if g, ok := parseSegment(s.segDir, filepath.Join(s.segDir, filepath.FromSlash(k))); ok {
				segs = append(segs, g)
			}
		}
	}
	segs, _ = dropContained(segs)
	return segs, nil
}

type readManifest struct {
	version  string
	segments []string
}

// dropContained 去掉 seq 范围被同一分区里别的分段包含的分段（合并后还没来得及删的旧分段），返回留下的与去掉的。
func dropContained(segs []segment) (kept, dropped []segment) {
	slices.SortFunc(segs, compareSegments)
	for _, g := range segs {
		if n := len(kept); n > 0 && samePartition(kept[n-1], g) && g.seqMax <= kept[n-1].seqMax {
			dropped = append(dropped, g)
			continue
		}
		kept = append(kept, g)
	}
	return kept, dropped
}

// rewriteManifests 按 segs（某个下线写入节点在对象存储上的、还要保留的分段）重写它的清单。
func (s *Store) rewriteManifests(ctx context.Context, writer int64, segs []segment) error {
	byHour := map[int64][]segment{}
	for _, g := range segs {
		byHour[hourOf(g)] = append(byHour[hourOf(g)], g)
	}
	vers, err := s.remote.obj.ListVersions(ctx, manifestPrefix+strconv.FormatInt(writer, 10)+"/")
	if err != nil {
		return fmt.Errorf("list manifests: %w", err)
	}
	for h, gs := range byHour {
		key, body := manifestKey(writer, h), s.encodeManifest(gs)
		if _, ok := vers[key]; ok {
			if old, err := s.remote.obj.ReadSmall(ctx, key); err == nil && string(old) == body {
				continue
			}
		}
		if err := s.remote.obj.WriteSmall(ctx, key, []byte(body)); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	}
	for key := range vers {
		h, err := strconv.ParseInt(strings.TrimSuffix(key[strings.LastIndexByte(key, '/')+1:], ".json"), 10, 64)
		if _, ok := byHour[h]; err == nil && !ok {
			if err := s.remote.obj.Delete(ctx, key); err != nil {
				return fmt.Errorf("delete manifest: %w", err)
			}
		}
	}
	return nil
}

// parallel 以有限的并发执行 fn(0) … fn(n-1)，返回第一个错误（出错后不再开始新的）。
func parallel(ctx context.Context, n int, fn func(i int) error) error {
	const workers = 16
	if n == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
		next  = make(chan int)
	)
	for range min(workers, n) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				if err := fn(i); err != nil {
					once.Do(func() { first = err; cancel() })
				}
			}
		}()
	}
	for i := 0; i < n && ctx.Err() == nil; i++ {
		select {
		case next <- i:
		case <-ctx.Done():
		}
	}
	close(next)
	wg.Wait()
	if first == nil {
		first = ctx.Err() // 调用方的 ctx 结束了
	}
	return first
}
