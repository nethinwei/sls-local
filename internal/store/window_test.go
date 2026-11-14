package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

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
