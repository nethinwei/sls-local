package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/nethinwei/sls-local/internal/query"
)

// TestSearchWindows 对比分窗检索与「全部排序后取一页」：数据有大段空窗、同一秒多条、纳秒落在边界上。
func TestSearchWindows(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, "", Options{SQLOffset: DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := rand.New(rand.NewPCG(7, 8))
	type entry struct {
		seq, t int64
		ns     int32
		k      string
	}
	var all []entry
	const base = 1_800_000_000
	seq := int64(0)
	for g := 0; g < 60; g++ {
		grp := &Group{Source: "s"}
		// 一半的组挤在最后 100 秒，其余分散在 10000 秒里（中间有空窗）
		start := int64(base + r.IntN(10000))
		if g%2 == 0 {
			start = base + 9900 + int64(r.IntN(100))
		}
		for i := 0; i < 1+r.IntN(40); i++ {
			tt := start + int64(r.IntN(3))
			ns := uint32(r.IntN(3)) * 500_000_000
			k := []string{"a", "b", "c"}[r.IntN(3)]
			if r.IntN(50) == 0 {
				k = "rare"
			}
			grp.Logs = append(grp.Logs, Log{Time: uint32(tt), TimeNs: &ns, Contents: []KV{{"k", k}}})
			seq++
			all = append(all, entry{seq, tt, int32(ns), k})
		}
		if err := s.Append(ctx, "p", "l", grp); err != nil {
			t.Fatal(err)
		}
	}
	conds := map[string]func(string) bool{
		"TRUE": func(string) bool { return true },
		`json_extract_string(contents, '$.k') = 'a'`:    func(k string) bool { return k == "a" },
		`json_extract_string(contents, '$.k') = 'rare'`: func(k string) bool { return k == "rare" },
		"FALSE": func(string) bool { return false },
	}
	for c, pred := range conds {
		for _, reverse := range []bool{true, false} {
			for range 40 {
				from := int64(base + r.IntN(10100) - 50)
				to := from + int64(r.IntN(10200))
				fromNs, toNs := int32(r.IntN(3))*500_000_000, int32(r.IntN(3))*500_000_000
				limit, offset := int64(1+r.IntN(120)), int64(0)
				if r.IntN(2) == 0 {
					offset = int64(r.IntN(200))
				}
				var want []int64
				var sel []entry
				for _, e := range all {
					if (e.t > from || (e.t == from && e.ns >= fromNs)) && (e.t < to || (e.t == to && e.ns < toNs)) && pred(e.k) {
						sel = append(sel, e)
					}
				}
				slices.SortFunc(sel, func(a, b entry) int {
					if a.t != b.t {
						return int(a.t - b.t)
					}
					if a.ns != b.ns {
						return int(a.ns - b.ns)
					}
					return int(a.seq - b.seq)
				})
				if reverse {
					slices.Reverse(sel)
				}
				for i := offset; i < offset+limit && i < int64(len(sel)); i++ {
					want = append(want, sel[i].t*10+int64(sel[i].ns/500_000_000))
				}
				res, err := s.Search(ctx, SearchParams{Project: "p", Logstore: "l", From: from, To: to, FromNs: fromNs, ToNs: toNs,
					Where: query.Cond{SQL: c}, Limit: limit, Offset: offset, Reverse: reverse})
				if err != nil {
					t.Fatal(err)
				}
				var got []int64
				for _, row := range res.Rows {
					var m map[string]any
					if err := json.Unmarshal(row, &m); err != nil {
						t.Fatal(err)
					}
					var tt, ns int64
					fmt.Sscan(m["__time__"].(string), &tt)
					fmt.Sscan(m["__time_ns_part__"].(string), &ns)
					got = append(got, tt*10+ns/500_000_000)
				}
				if !slices.Equal(got, want) {
					t.Fatalf("%s reverse=%v [%d.%d, %d.%d) limit=%d offset=%d\n got %v\nwant %v", c, reverse, from, fromNs, to, toNs, limit, offset, got, want)
				}
			}
		}
	}
}

// 客户端传来的时间范围可以是任意 int64：窗口不能因溢出而死循环，结果与正常范围相同。
func TestSearchWindowsExtremeRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := Open(ctx, "", Options{SQLOffset: DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ns := uint32(0)
	if err := s.Append(ctx, "p", "l", &Group{Logs: []Log{{Time: 1_800_000_000, TimeNs: &ns, Contents: []KV{{"k", "v"}}}}}); err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]int64{{math.MinInt64, math.MaxInt64}, {0, math.MaxInt64}, {math.MinInt64, 1_800_000_001}, {-5, 1 << 40}} {
		for _, reverse := range []bool{true, false} {
			for _, cond := range []string{"TRUE", "FALSE"} {
				res, err := s.Search(ctx, SearchParams{Project: "p", Logstore: "l", From: r[0], To: r[1], Where: query.Cond{SQL: cond},
					Limit: 10, Offset: math.MaxInt64 / 2 * map[bool]int64{true: 0, false: 1}[cond == "TRUE"], Reverse: reverse})
				if err != nil {
					t.Fatal(r, err)
				}
				if want := map[string]int{"TRUE": 1, "FALSE": 0}[cond]; len(res.Rows) != want {
					t.Errorf("%v reverse=%v %s: %d rows, want %d", r, reverse, cond, len(res.Rows), want)
				}
			}
		}
	}
}

func TestTimeWindowsBounded(t *testing.T) {
	for _, r := range [][2]int64{{math.MinInt64, math.MaxInt64}, {0, 0}, {5, 4}} {
		for _, reverse := range []bool{true, false} {
			var w timeWindows
			w.init(r[0], r[1], reverse)
			steps := 0
			for {
				if _, _, ok := w.next(); !ok {
					break
				}
				w.found(1, math.MaxInt64) // 每窗只命中 1 行、还差很多：估算值会很大
				if steps++; steps > 200 {
					t.Fatalf("%v reverse=%v: too many windows", r, reverse)
				}
			}
		}
	}
}

// 翻页跨多个窗口时，期间写入的日志不影响这一页：各窗口的查询看到同一时刻的数据。
func TestSearchSnapshot(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, "", Options{SQLOffset: DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const base = 1_800_000_000
	put := func(sec uint32, v string) {
		ns := uint32(0)
		if err := s.Append(ctx, "p", "l", &Group{Logs: []Log{{Time: base + sec, TimeNs: &ns, Contents: []KV{{"v", v}}}}}); err != nil {
			t.Fatal(err)
		}
	}
	// 最新的 1/32 时段只有 1 条，凑不够一页，要继续查更早的窗口
	put(3199, "newest")
	for i := range 50 {
		put(uint32(i), "old")
	}
	p := SearchParams{Project: "p", Logstore: "l", From: base, To: base + 3200, Where: query.Cond{SQL: "TRUE"}, Limit: 20, Reverse: true}
	want, err := s.Search(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	afterWindow = func() { put(45, "late") } // 第一个窗口查完后，往还没查到、且在这一页范围内的时段写一条
	defer func() { afterWindow = nil }()
	got, err := s.Search(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Rows) != len(want.Rows) {
		t.Fatalf("%d rows, want %d", len(got.Rows), len(want.Rows))
	}
	for i := range want.Rows {
		if string(got.Rows[i]) != string(want.Rows[i]) {
			t.Fatalf("row %d = %s, want %s", i, got.Rows[i], want.Rows[i])
		}
	}
}
