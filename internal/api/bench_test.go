package api

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/query"
	"github.com/nethinwei/sls-local/internal/store"
)

// 基准测试：go test ./internal/api -run '^$' -bench . -benchtime 20x
// 数据量用 SLS_BENCH_N 调整（默认 20 万条，分布在 2 小时内）。

var (
	benchOnce   sync.Once
	benchClient *sls.Client
	benchStore  *store.Store
	benchFrom   int64
	benchTo     int64
)

func benchIndex() *sls.Index {
	text := func() sls.IndexKey {
		return sls.IndexKey{Type: "text", Token: []string{" ", ",", ":", "/", "=", "&", "?"}, DocValue: true}
	}
	return &sls.Index{
		Line: &sls.IndexLine{Token: []string{" ", ",", ":", "/", "=", "&", "?"}},
		Keys: map[string]sls.IndexKey{
			"level": text(), "msg": text(), "service": text(), "trace_id": text(), "order_id": text(), "url": text(),
			"latency_ms": {Type: "long", DocValue: true}, "ratio": {Type: "double", DocValue: true},
		},
	}
}

func benchGroup(r *rand.Rand, base int64, from, n int, pack int) *sls.LogGroup {
	ptr := func(s string) *string { return &s }
	g := &sls.LogGroup{Topic: ptr(""), Source: ptr("10.0.0.1"), LogTags: []*sls.LogTag{
		{Key: ptr("__hostname__"), Value: ptr("host-a")},
		{Key: ptr("__pack_id__"), Value: ptr(fmt.Sprintf("BENCH0000000000%02X-%X", pack%4, pack))},
	}}
	levels := []string{"INFO", "INFO", "INFO", "INFO", "WARN", "ERROR"}
	msgs := []string{"request completed", "order created", "payment authorized", "upstream timeout", "cache miss for key sku"}
	services := []string{"checkout", "inventory", "gateway", "auth"}
	for i := from; i < from+n; i++ {
		t := uint32(base + int64(i)*7200/int64(benchN()))
		ns := uint32(r.IntN(1e9))
		kv := func(k, v string) *sls.LogContent { return &sls.LogContent{Key: ptr(k), Value: ptr(v)} }
		g.Logs = append(g.Logs, &sls.Log{Time: &t, TimeNs: &ns, Contents: []*sls.LogContent{
			kv("level", levels[r.IntN(len(levels))]), kv("msg", msgs[r.IntN(len(msgs))]), kv("service", services[r.IntN(len(services))]),
			kv("order_id", fmt.Sprintf("ORD%08d", r.IntN(1e8))), kv("url", fmt.Sprintf("/api/v1/orders/%d?page=%d", r.IntN(1000), r.IntN(9))),
			kv("latency_ms", strconv.Itoa(r.IntN(2000))), kv("ratio", strconv.FormatFloat(r.Float64(), 'f', 3, 64)),
			kv("trace_id", fmt.Sprintf("%032x", r.Uint64())),
		}})
	}
	return g
}

func benchN() int {
	if n, err := strconv.Atoi(os.Getenv("SLS_BENCH_N")); err == nil && n > 0 {
		return n
	}
	return 200000
}

func benchEnv(b *testing.B) *sls.Client {
	benchOnce.Do(func() {
		st, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset})
		if err != nil {
			b.Fatal(err)
		}
		h := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), map[string]*sls.Index{"bench/app": benchIndex()})
		srv := httptest.NewServer(h.Handler())
		addr := srv.Listener.Addr().String()
		c := &sls.Client{Endpoint: "sls-local:80", AccessKeyID: "local", AccessKeySecret: "local"}
		c.SetHTTPClient(&http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		}})
		benchFrom = time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC).Unix()
		benchTo = benchFrom + 7200
		r := rand.New(rand.NewPCG(1, 2))
		start := time.Now()
		for i, p := 0, 0; i < benchN(); i, p = i+1000, p+1 {
			if err := c.PutLogs("bench", "app", benchGroup(r, benchFrom, i, min(1000, benchN()-i), p)); err != nil {
				b.Fatal(err)
			}
		}
		b.Logf("loaded %d logs in %v", benchN(), time.Since(start))
		benchClient, benchStore = c, st
	})
	return benchClient
}

func benchLogs(b *testing.B, q string, lines int64, reverse bool) {
	c := benchEnv(b)
	b.ResetTimer()
	for range b.N {
		if _, err := c.GetLogsV3("bench", "app", &sls.GetLogRequest{From: benchFrom, To: benchTo, Query: q, Lines: lines, Reverse: reverse}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutLogs(b *testing.B) {
	c := benchEnv(b)
	r := rand.New(rand.NewPCG(3, 4))
	b.ResetTimer()
	for i := range b.N {
		if err := c.PutLogs("bench", "write", benchGroup(r, benchFrom, 0, 1000, i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "logs/s")
}

func BenchmarkSearchStar(b *testing.B)     { benchLogs(b, "*", 100, true) }
func BenchmarkSearchKV(b *testing.B)       { benchLogs(b, "level: ERROR", 100, true) }
func BenchmarkSearchKVRare(b *testing.B)   { benchLogs(b, "order_id: ORD00000042", 100, true) }
func BenchmarkSearchFulltext(b *testing.B) { benchLogs(b, "timeout", 100, true) }
func BenchmarkSearchNumeric(b *testing.B)  { benchLogs(b, "latency_ms > 1900", 100, true) }
func BenchmarkSearchAndOr(b *testing.B) {
	benchLogs(b, "(level: ERROR or level: WARN) and service: checkout", 100, true)
}
func BenchmarkSearchPage5(b *testing.B) { benchLogs(b, "*", 5, true) }
func BenchmarkSPLWhere(b *testing.B)    { benchLogs(b, "* | where latency_ms > '1900'", 100, true) }
func BenchmarkSPLExtendProject(b *testing.B) {
	benchLogs(b, "* | extend x = cast(latency_ms as bigint) * 2 | project level, x", 100, true)
}
func BenchmarkSQLCount(b *testing.B) { benchLogs(b, "* | select count(*) as c", 0, false) }
func BenchmarkSQLGroupBy(b *testing.B) {
	benchLogs(b, "* | select service, level, count(*) as c, avg(latency_ms) as a group by service, level", 0, false)
}
func BenchmarkSQLTimeSeries(b *testing.B) {
	benchLogs(b, "* | select __time__ - __time__ % 60 as t, count(*) as c group by t order by t limit 1000", 0, false)
}
func BenchmarkSQLSearchThenAgg(b *testing.B) {
	benchLogs(b, "level: ERROR | select service, count(*) as c group by service", 0, false)
}
func BenchmarkSQLPercentile(b *testing.B) {
	benchLogs(b, "* | select approx_percentile(latency_ms, 0.99) as p", 0, false)
}

func BenchmarkHistogram(b *testing.B) {
	c := benchEnv(b)
	b.ResetTimer()
	for range b.N {
		if _, err := c.GetHistograms("bench", "app", "", benchFrom, benchTo, "level: ERROR"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkContext(b *testing.B) {
	c := benchEnv(b)
	resp, err := c.GetLogsV3("bench", "app", &sls.GetLogRequest{From: benchFrom, To: benchTo, Query: "* | with_pack_meta", Lines: 1, Offset: int64(benchN() / 2)})
	if err != nil || len(resp.Logs) == 0 {
		b.Fatal(err)
	}
	ls, _ := c.GetLogStore("bench", "app")
	a := resp.Logs[0]
	b.ResetTimer()
	for range b.N {
		if _, err := ls.GetContextLogs(50, 50, a["__tag__:__pack_id__"], a["__pack_meta__"]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStoreSearchStar 绕过 HTTP 与 SDK，只看存储层：与 BenchmarkSearchStar 的差是协议开销。
func BenchmarkStoreSearchStar(b *testing.B) {
	benchEnv(b)
	b.ResetTimer()
	for range b.N {
		if _, err := benchStore.Search(context.Background(), store.SearchParams{Project: "bench", Logstore: "app",
			From: benchFrom, To: benchTo, Where: query.Cond{SQL: "TRUE"}, Limit: 100, Reverse: true}); err != nil {
			b.Fatal(err)
		}
	}
}
