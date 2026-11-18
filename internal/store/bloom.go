package store

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"sync"

	"github.com/nethinwei/sls-local/internal/query"
)

// 分段的 bloom 过滤：刷段时把每个分段文件里出现过的词（按分词符切出的整词，小写）记进 bloom，存成旁边的
// <分段>.bloom 文件；检索时 query.Need 要求的词在 bloom 里都不可能存在的分段直接跳过。
//
// 一个分段可以有多个 bloom，每个对应一组分词符（query.TokenSig）：词只能在同一组分词符切出的 bloom 里查，
// 分词符改过之后旧分段查不到对应的 bloom，就不跳过（结果仍然正确，只是慢）。
//
// 词的来源是全文列加上全部字段的值（数值字段的值不在全文列里，索引改过之后仍可能被字段检索匹配），所以字段
// 检索与全文检索的词都一定在里面。bloom 只会误判「可能有」，不会误判「没有」。

// 旁路文件（<分段>.bloom）还记着分段写成独立列的字段：分段只在对象存储上、本地没有时也知道该读哪些列。
const bloomMagic = "SLSBLM2\n"

// bloomFalsePositive 是 bloom 误判「可能有」的概率：每个词约 9.6 bit。
const bloomFalsePositive = 0.01

type bloom struct {
	k    uint32
	bits []byte
}

func newBloom(n int) *bloom {
	n = max(n, 1)
	m := int(math.Ceil(-float64(n) * math.Log(bloomFalsePositive) / (math.Ln2 * math.Ln2)))
	k := uint32(math.Max(1, math.Round(float64(m)/float64(n)*math.Ln2)))
	return &bloom{k: k, bits: make([]byte, (m+7)/8)}
}

// hashes 是双重哈希（Kirsch–Mitzenmacher）的两个基：FNV-64a 的高低两半。哈希必须跨进程稳定（存进文件）。
func hashes(word string) (uint64, uint64) {
	h := fnv.New64a()
	h.Write([]byte(word))
	v := h.Sum64()
	return v & 0xffffffff, v>>32 | 1
}

func (b *bloom) add(word string) {
	h1, h2 := hashes(word)
	m := uint64(len(b.bits)) * 8
	for i := range uint64(b.k) {
		x := (h1 + i*h2) % m
		b.bits[x/8] |= 1 << (x % 8)
	}
}

func (b *bloom) has(word string) bool {
	h1, h2 := hashes(word)
	m := uint64(len(b.bits)) * 8
	for i := range uint64(b.k) {
		x := (h1 + i*h2) % m
		if b.bits[x/8]&(1<<(x%8)) == 0 {
			return false
		}
	}
	return true
}

// bloomFile 是一个分段的旁路文件：写成独立列的字段，与每组分词符（签名）的 bloom。
type bloomFile struct {
	cols   map[string]bool
	blooms map[string]*bloom
}

func (f *bloomFile) size() int {
	if f == nil {
		return 0
	}
	n := 0
	for k := range f.cols {
		n += len(k)
	}
	for sig, b := range f.blooms {
		n += len(sig) + len(b.bits)
	}
	return n
}

func writeBloomFile(path string, f *bloomFile) error {
	var buf bytes.Buffer
	buf.WriteString(bloomMagic)
	buf.Write(binary.AppendUvarint(nil, uint64(len(f.cols))))
	for k := range f.cols {
		buf.Write(binary.AppendUvarint(nil, uint64(len(k))))
		buf.WriteString(k)
	}
	buf.Write(binary.AppendUvarint(nil, uint64(len(f.blooms))))
	for sig, b := range f.blooms {
		buf.Write(binary.AppendUvarint(nil, uint64(len(sig))))
		buf.WriteString(sig)
		buf.Write(binary.AppendUvarint(nil, uint64(b.k)))
		buf.Write(binary.AppendUvarint(nil, uint64(len(b.bits))))
		buf.Write(b.bits)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	if err := syncFile(tmp); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func readBloomFile(path string) (*bloomFile, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	r := bufio.NewReader(fh)
	magic := make([]byte, len(bloomMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != bloomMagic {
		return nil, fmt.Errorf("%s: not a bloom file", path)
	}
	f := &bloomFile{cols: map[string]bool{}, blooms: map[string]*bloom{}}
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	for range n {
		k, err := readBytes(r)
		if err != nil {
			return nil, err
		}
		f.cols[string(k)] = true
	}
	if n, err = binary.ReadUvarint(r); err != nil {
		return nil, err
	}
	for range n {
		sig, err := readBytes(r)
		if err != nil {
			return nil, err
		}
		k, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		bits, err := readBytes(r)
		if err != nil {
			return nil, err
		}
		if k == 0 || k > 64 || len(bits) == 0 {
			return nil, fmt.Errorf("%s: corrupt bloom", path)
		}
		f.blooms[string(sig)] = &bloom{k: uint32(k), bits: bits}
	}
	return f, nil
}

func readBytes(r *bufio.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if n > 1<<31 {
		return nil, errors.New("corrupt bloom: length too large")
	}
	b := make([]byte, n)
	_, err = io.ReadFull(r, b)
	return b, err
}

// bloomCache 按分段路径缓存读进来的 bloom，总大小超过 limit 时淘汰最久没用的。
type bloomCache struct {
	fetch func(ctx context.Context, local string) error // 本地没有时从对象存储下载（可为空）
	mu    sync.Mutex
	limit int
	size  int
	lru   *list.List // 元素是 *bloomEntry，最近用过的在前
	byKey map[string]*list.Element
}

type bloomEntry struct {
	path string
	f    *bloomFile // nil 表示没有旁路文件
}

// DefaultBloomCache 是 bloom 缓存的默认大小。
const DefaultBloomCache = 256 << 20

func newBloomCache(limit int) *bloomCache {
	return &bloomCache{limit: limit, lru: list.New(), byKey: map[string]*list.Element{}}
}

// get 返回分段的 bloom；没有 bloom 文件时为 nil。
func (c *bloomCache) get(segPath string) (*bloomFile, error) {
	c.mu.Lock()
	if e, ok := c.byKey[segPath]; ok {
		c.lru.MoveToFront(e)
		f := e.Value.(*bloomEntry).f
		c.mu.Unlock()
		return f, nil
	}
	c.mu.Unlock()
	if c.fetch != nil {
		if err := c.fetch(context.Background(), segPath+".bloom"); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	f, err := readBloomFile(segPath + ".bloom")
	if errors.Is(err, os.ErrNotExist) {
		f, err = nil, nil
	}
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.byKey[segPath]; !ok {
		c.byKey[segPath] = c.lru.PushFront(&bloomEntry{segPath, f})
		c.size += f.size()
		for c.size > c.limit && c.lru.Len() > 1 {
			old := c.lru.Back()
			e := old.Value.(*bloomEntry)
			c.lru.Remove(old)
			delete(c.byKey, e.path)
			c.size -= e.f.size()
		}
	}
	return f, nil
}

// forget 在分段删除后丢掉它的缓存。
func (c *bloomCache) forget(segPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.byKey[segPath]; ok {
		c.lru.Remove(e)
		delete(c.byKey, segPath)
		c.size -= e.Value.(*bloomEntry).f.size()
	}
}

// mayContain 判断分段是否可能满足 need；读不到 bloom 时不跳过。
func (s *Store) mayContain(g *segment, need *query.Need) bool {
	if need == nil || s.blooms == nil {
		return true
	}
	f, err := s.blooms.get(g.path)
	if err != nil {
		if s.onError != nil {
			s.onError(fmt.Errorf("read bloom: %w", err))
		}
		return true
	}
	if f == nil {
		return true
	}
	return need.Eval(func(sig, word string) bool {
		b, ok := f.blooms[sig]
		return !ok || b.has(word)
	})
}

// colsOf 是分段写成独立列的字段：刚写的分段带着，其余从旁路文件读（读不到时为空，字段全从 JSON 取）。
func (s *Store) colsOf(g *segment) map[string]bool {
	if g.cols != nil {
		return g.cols
	}
	f, err := s.blooms.get(g.path)
	if err != nil || f == nil {
		if err != nil && s.onError != nil {
			s.onError(fmt.Errorf("read segment sidecar: %w", err))
		}
		return nil
	}
	return f.cols
}
