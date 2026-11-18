package api

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/store"
)

func opsEnv(t *testing.T, limits Limits, keys map[string]string) (*Server, string, *sls.Client) {
	t.Helper()
	st, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	s := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	s.SetLimits(limits)
	s.SetAccessKeys(keys)
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	c := &sls.Client{Endpoint: "sls-local:80", AccessKeyID: "ak", AccessKeySecret: "sk"}
	c.SetHTTPClient(&http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}})
	return s, addr, c
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func putN(c *sls.Client, n int) error {
	now := uint32(time.Now().Unix())
	g := &sls.LogGroup{Topic: ptr(""), Source: ptr("s")}
	for range n {
		g.Logs = append(g.Logs, &sls.Log{Time: &now, Contents: []*sls.LogContent{{Key: ptr("k"), Value: ptr("v")}}})
	}
	return c.PutLogs("proj", "app", g)
}

func slsCode(err error) string {
	if se, ok := err.(*sls.Error); ok {
		return se.Code
	}
	return ""
}

// 健康检查与指标不要签名；指标里有请求、写入与存储的状态。
func TestHealthAndMetrics(t *testing.T) {
	_, addr, c := opsEnv(t, Limits{}, map[string]string{"ak": "sk"})
	if err := putN(c, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetLogsV3("proj", "app", &sls.GetLogRequest{From: 0, To: 2000000000, Query: "*", Lines: 10}); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"/health", "/ready"} {
		if code, body := get(t, "http://"+addr+p); code != 200 || body != "ok\n" {
			t.Errorf("%s = %d %q", p, code, body)
		}
	}
	code, body := get(t, "http://"+addr+"/metrics")
	if code != 200 {
		t.Fatalf("/metrics = %d", code)
	}
	for _, want := range []string{
		`sls_local_requests_total{api="PutLogs",status="200"} 1`,
		`sls_local_requests_total{api="GetLogs",status="200"} 1`,
		`sls_local_request_duration_seconds_bucket{api="GetLogs",le="+Inf"} 1`,
		`sls_local_request_duration_seconds_count{api="PutLogs"} 1`,
		"sls_local_ingested_logs_total 3",
		"# TYPE sls_local_flushes_total counter",
	} {
		if !strings.Contains(body, want+"\n") {
			t.Errorf("/metrics lacks %q", want)
		}
	}
	// 写入的 3 条要么还在头部，要么已经刷进分段（测试的刷段模式）
	value := func(name string) float64 {
		for _, line := range strings.Split(body, "\n") {
			if v, ok := strings.CutPrefix(line, name+" "); ok {
				f, _ := strconv.ParseFloat(v, 64)
				return f
			}
		}
		t.Errorf("/metrics lacks %s", name)
		return 0
	}
	if n := value("sls_local_pending_rows") + value("sls_local_segment_rows"); n != 3 {
		t.Errorf("pending + segment rows = %v, want 3", n)
	}
	if code, _ := get(t, "http://"+addr+"/logstores"); code != 401 { // SLS 的接口仍要签名
		t.Errorf("unsigned /logstores = %d", code)
	}
}

// 同时执行的查询满了：排队等不到返回 ReadQuotaExceed。查询超时返回 RequestTimeout。
func TestQueryLimits(t *testing.T) {
	s, _, c := opsEnv(t, Limits{MaxQueries: 1, QueueWait: 50 * time.Millisecond}, nil)
	if err := putN(c, 10); err != nil {
		t.Fatal(err)
	}
	s.querySlots <- struct{}{} // 占住唯一的名额
	_, err := c.GetLogsV3("proj", "app", &sls.GetLogRequest{From: 0, To: 2000000000, Query: "*", Lines: 10})
	if slsCode(err) != "ReadQuotaExceed" {
		t.Errorf("busy: %v", err)
	}
	<-s.querySlots
	if _, err := c.GetLogsV3("proj", "app", &sls.GetLogRequest{From: 0, To: 2000000000, Query: "*", Lines: 10}); err != nil {
		t.Errorf("after release: %v", err)
	}

	s2, _, c2 := opsEnv(t, Limits{QueryTimeout: time.Nanosecond}, nil)
	_ = s2
	if err := putN(c2, 10); err != nil {
		t.Fatal(err)
	}
	_, err = c2.GetLogsV3("proj", "app", &sls.GetLogRequest{From: 0, To: 2000000000, Query: "* | select count(*) as c"})
	if slsCode(err) != "RequestTimeout" {
		t.Errorf("timeout: %v", err)
	}
}

// 写入超过每秒条数（允许 2 秒的突发）返回 WriteQuotaExceed。
func TestWriteRate(t *testing.T) {
	_, _, c := opsEnv(t, Limits{WriteRate: 10}, nil)
	if err := putN(c, 15); err != nil {
		t.Fatalf("within burst: %v", err)
	}
	if err := putN(c, 15); slsCode(err) != "WriteQuotaExceed" {
		t.Errorf("over rate: %v", err)
	}
	if err := putN(c, 100); err == nil { // 桶没满时，超过突发量的一包也不放行
		t.Errorf("huge batch with a partly used bucket was accepted")
	}
}

// DuckDB 的内存上限生效；写错时启动就报错。
func TestMemoryLimit(t *testing.T) {
	st, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset, MemoryLimit: "1GB"})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	if _, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset, MemoryLimit: "lots"}); err == nil {
		t.Error("invalid memory limit accepted")
	}
}
