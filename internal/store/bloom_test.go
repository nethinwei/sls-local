package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"testing"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/index"
	"github.com/nethinwei/sls-local/internal/query"
)

func TestBloom(t *testing.T) {
	b := newBloom(10000)
	for i := range 10000 {
		b.add(fmt.Sprint("w", i))
	}
	for i := range 10000 {
		if !b.has(fmt.Sprint("w", i)) {
			t.Fatalf("w%d added but not found", i)
		}
	}
	fp := 0
	for i := range 10000 {
		if b.has(fmt.Sprint("x", i)) {
			fp++
		}
	}
	if fp > 300 { // 期望约 1%
		t.Errorf("false positives = %d / 10000", fp)
	}
	path := filepath.Join(t.TempDir(), "x.bloom")
	cols := map[string]bool{"level": true, "Level": true}
	if err := writeBloomFile(path, &bloomFile{cols: cols, blooms: map[string]*bloom{"sig": b}}); err != nil {
		t.Fatal(err)
	}
	f, err := readBloomFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := f.blooms["sig"]
	if !got.has("w42") || got.k != b.k || len(got.bits) != len(b.bits) || len(f.cols) != 2 || !f.cols["Level"] {
		t.Fatal("bloom file round trip")
	}
}

// 随机数据切成很多小分段，随机检索在有 bloom 跳过分段与不跳过时结果相同；罕见词确实跳过了分段。
func TestBloomPruning(t *testing.T) {
	defer func(n int64) { segmentFileRows = n }(segmentFileRows)
	segmentFileRows = 50
	ctx := context.Background()
	s := openSeg(t, "", t.TempDir())
	defer s.Close()
	r := rand.New(rand.NewPCG(1, 2))
	words := []string{"alpha", "beta", "gamma", "delta", "Epsilon", "zeta", "eta", "theta"}
	pick := func() string { return words[r.IntN(len(words))] }
	const n = 2000
	for i := 0; i < n; i += 100 {
		g := &Group{Source: "10.0.0.1"}
		for j := range 100 {
			kv := []KV{
				{"level", []string{"INFO", "WARN", "ERROR"}[r.IntN(3)]},
				{"msg", pick() + " " + pick() + "/" + pick() + "-x"},
				{"id", fmt.Sprintf("ID%05d", i+j)},
				{"cost", fmt.Sprint(r.IntN(1000))},
			}
			if r.IntN(200) == 0 {
				kv = append(kv, KV{"rare", "needle"})
			}
			g.Logs = append(g.Logs, Log{Time: uint32(segBase + (i+j)/5), Contents: kv})
		}
		if err := s.Append(ctx, "p", "l", g); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	queries := []string{
		"needle", "rare: needle", "ID00042", "id: ID00042", "id: id00042", "ID99999", "nosuchword",
		"alpha", "alpha and beta", "alpha or nosuchword", "nosuchword or ID00042", "not alpha", "alpha not ID00042",
		"msg: epsilon", "EPSILON", "msg: alpha/beta", `msg: "alpha beta"`, `#"alpha beta"`, "msg: alph*", "ID000*",
		"level: ERROR and ID01999", "(ID00001 or ID00002) and level: WARN", "x", "msg: x", "cost > 500 and needle",
		"10.0.0.1", "__source__: 10.0.0.1", "level", "rare", "*",
	}
	for range 100 { // 随机组合
		a, b := []string{"ID0" + fmt.Sprint(r.IntN(2100)), pick(), "nosuch", "needle"}[r.IntN(4)], pick()
		op := []string{" and ", " or ", " not "}[r.IntN(3)]
		queries = append(queries, a+op+b, "msg: "+strings.ToUpper(b)+op+a)
	}
	ix := index.Auto()
	for _, q := range queries {
		cond, err := query.Search(q, ix)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		p := SearchParams{Project: "p", Logstore: "l", From: 0, To: segBase * 2, Where: cond}
		got, err := s.Count(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		p.Where.Need = nil
		want, err := s.Count(ctx, p)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%q: %d rows with bloom pruning, %d without", q, got, want)
		}
	}

	// 跳过：罕见词只读含它的分段
	sn := s.acquire()
	defer s.release(sn)
	if len(sn.segs) < 20 {
		t.Fatalf("only %d segments", len(sn.segs))
	}
	kept := func(q string) int {
		cond, _ := query.Search(q, ix)
		n := 0
		for i := range sn.segs {
			if s.mayContain(&sn.segs[i], cond.Need) {
				n++
			}
		}
		return n
	}
	if k := kept("ID00042"); k > 2 {
		t.Errorf("ID00042 keeps %d of %d segments", k, len(sn.segs))
	}
	if k := kept("nosuchword"); k > 2 {
		t.Errorf("nosuchword keeps %d of %d segments", k, len(sn.segs))
	}
	if k := kept("alpha or ID00042"); k != len(sn.segs) {
		t.Errorf("alpha or ID00042 keeps %d of %d segments", k, len(sn.segs))
	}
	if k := kept("not ID00042"); k != len(sn.segs) {
		t.Errorf("not ID00042 keeps %d of %d segments", k, len(sn.segs))
	}
}

// 分词符不同（索引改过）：bloom 对不上就不跳过，结果仍然正确。
func TestBloomTokenMismatch(t *testing.T) {
	ctx := context.Background()
	s := openSeg(t, "", t.TempDir())
	defer s.Close()
	g := &Group{}
	for i := range 10 {
		g.Logs = append(g.Logs, Log{Time: segBase + uint32(i), Contents: []KV{{"msg", "a-b c"}}})
	}
	if err := s.Append(ctx, "p", "l", g); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Flush(ctx); err != nil { // bloom 按默认分词符（没有 -）：只有 a-b、c
		t.Fatal(err)
	}
	raw := `{"line": {"token": [" ", "-"]}, "keys": {"msg": {"type": "text", "token": [" ", "-"]}}}`
	ix := mustIndex(t, raw)
	for _, q := range []string{"a", "msg: b", "a and c"} {
		cond, err := query.Search(q, ix)
		if err != nil {
			t.Fatal(err)
		}
		n, err := s.Count(ctx, SearchParams{Project: "p", Logstore: "l", From: 0, To: segBase * 2, Where: cond})
		if err != nil || n != 10 {
			t.Errorf("%q: %d, %v; want 10", q, n, err)
		}
	}
}

func mustIndex(t *testing.T, raw string) *index.Index {
	t.Helper()
	var ix sls.Index
	if err := json.Unmarshal([]byte(raw), &ix); err != nil {
		t.Fatal(err)
	}
	return index.FromConfig(&ix)
}
