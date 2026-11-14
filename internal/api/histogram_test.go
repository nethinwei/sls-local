package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/store"
)

// 时间范围与间隔来自客户端：再极端的参数也不能生成海量的桶。
func TestHistogramBucketLimit(t *testing.T) {
	st, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.Append(context.Background(), "p", "ls", &store.Group{Logs: []store.Log{{Time: 1_800_000_000}}}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), map[string]*sls.Index{}).Handler())
	t.Cleanup(srv.Close)
	for _, q := range []string{
		fmt.Sprintf("from=0&to=%d", int64(math.MaxInt64)),
		fmt.Sprintf("from=%d&to=%d", int64(math.MinInt64), int64(math.MaxInt64)),
		"from=0&to=4000000000&interval=1",
		"from=1700000000&to=1800000000&interval=1",
		fmt.Sprintf("from=0&to=%d&interval=%d", int64(math.MaxInt64), int64(math.MaxInt64)),
	} {
		start := time.Now()
		req, _ := http.NewRequest("GET", srv.URL+"/logstores/ls?type=histogram&query=*&"+q, nil)
		req.Host = "p.sls-local"
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var buckets []json.RawMessage
		if err := json.Unmarshal(b, &buckets); err != nil {
			t.Fatalf("%s: %s", q, b)
		}
		if len(buckets) > 10000 || time.Since(start) > 5*time.Second {
			t.Errorf("%s: %d buckets in %v", q, len(buckets), time.Since(start))
		}
	}
}
