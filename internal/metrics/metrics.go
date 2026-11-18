// Package metrics 是一个很小的 Prometheus 指标注册表：计数器、直方图、按需读取的量值，输出文本格式
// （https://prometheus.io/docs/instrumenting/exposition_formats/）。只实现 sls-local 用到的部分，不引入依赖。
package metrics

import (
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// DefaultBuckets 是耗时直方图的桶（秒）。
var DefaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// Registry 保存全部指标。零值不可用，用 New。
type Registry struct {
	mu     sync.Mutex
	family map[string]*family
}

type family struct {
	help, typ string
	series    map[string]*series // 标签（已编码）→ 值
	gauge     func() float64     // 用 Func 登记的：输出时读取
}

type series struct {
	value   float64
	buckets []uint64 // 直方图：各桶（累计前）的计数
	sum     float64
	count   uint64
}

// New 创建注册表。
func New() *Registry { return &Registry{family: map[string]*family{}} }

func (r *Registry) get(name, help, typ string) *family {
	f, ok := r.family[name]
	if !ok {
		f = &family{help: help, typ: typ, series: map[string]*series{}}
		r.family[name] = f
	}
	return f
}

// labels 把 k1, v1, k2, v2 … 编码成 {k1="v1",k2="v2"}。
func labels(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("{")
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(kv[i] + `="` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(kv[i+1]) + `"`)
	}
	b.WriteString("}")
	return b.String()
}

// Add 给计数器加 v。labels 是成对的标签名与值。
func (r *Registry) Add(name, help string, v float64, kv ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.get(name, help, "counter")
	l := labels(kv)
	s, ok := f.series[l]
	if !ok {
		s = &series{}
		f.series[l] = s
	}
	s.value += v
}

// Inc 给计数器加 1。
func (r *Registry) Inc(name, help string, kv ...string) { r.Add(name, help, 1, kv...) }

// Observe 记录一次直方图观测（桶为 DefaultBuckets）。
func (r *Registry) Observe(name, help string, v float64, kv ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.get(name, help, "histogram")
	l := labels(kv)
	s, ok := f.series[l]
	if !ok {
		s = &series{buckets: make([]uint64, len(DefaultBuckets))}
		f.series[l] = s
	}
	if i, _ := slices.BinarySearch(DefaultBuckets, v); i < len(DefaultBuckets) {
		s.buckets[i]++
	}
	s.sum += v
	s.count++
}

// Func 登记一个输出时才读取的指标（typ 为 gauge 或 counter）：值来自别处（如存储的累计计数）。
func (r *Registry) Func(name, help, typ string, fn func() float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.get(name, help, typ)
	f.gauge = fn
}

// WriteTo 按 Prometheus 文本格式输出全部指标（按名字、标签排序）。
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	names := make([]string, 0, len(r.family))
	for n := range r.family {
		names = append(names, n)
	}
	slices.Sort(names)
	var b strings.Builder
	var gauges []func() (string, float64)
	for _, n := range names {
		f := r.family[n]
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", n, f.help, n, f.typ)
		if f.gauge != nil {
			// 量值在锁外读取（可能要访问存储），先占位
			fn := f.gauge
			name := n
			gauges = append(gauges, func() (string, float64) { return name, fn() })
			b.WriteString("\x00" + n + "\n")
			continue
		}
		ls := make([]string, 0, len(f.series))
		for l := range f.series {
			ls = append(ls, l)
		}
		slices.Sort(ls)
		for _, l := range ls {
			s := f.series[l]
			if f.typ != "histogram" {
				fmt.Fprintf(&b, "%s%s %s\n", n, l, num(s.value))
				continue
			}
			var cum uint64
			for i, ub := range DefaultBuckets {
				cum += s.buckets[i]
				fmt.Fprintf(&b, "%s_bucket%s %d\n", n, withLE(l, num(ub)), cum)
			}
			fmt.Fprintf(&b, "%s_bucket%s %d\n%s_sum%s %s\n%s_count%s %d\n", n, withLE(l, "+Inf"), s.count, n, l, num(s.sum), n, l, s.count)
		}
	}
	r.mu.Unlock()
	out := b.String()
	for _, g := range gauges {
		name, v := g()
		out = strings.Replace(out, "\x00"+name+"\n", name+" "+num(v)+"\n", 1)
	}
	n, err := io.WriteString(w, out)
	return int64(n), err
}

func withLE(l, le string) string {
	if l == "" {
		return `{le="` + le + `"}`
	}
	return l[:len(l)-1] + `,le="` + le + `"}`
}

func num(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
