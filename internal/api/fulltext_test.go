package api

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/store"
)

// 全文索引的分词符里没有换行时，字段之间仍是词边界（SLS 按字段分别分词）。
func TestFulltextFieldBoundary(t *testing.T) {
	st, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	ix := &sls.Index{Line: &sls.IndexLine{Token: []string{" ", ","}}}
	srv := httptest.NewServer(New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), map[string]*sls.Index{"p/ls": ix}).Handler())
	t.Cleanup(srv.Close)
	c := &sls.Client{Endpoint: "sls-local:80", AccessKeyID: "local", AccessKeySecret: "local"}
	c.SetHTTPClient(&http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
	}}})
	g := &sls.LogGroup{Topic: ptr(""), Source: ptr("10.0.0.1"), Logs: []*sls.Log{{Time: ptr(uint32(base.Unix())), Contents: []*sls.LogContent{
		{Key: ptr("msg"), Value: ptr("upstream timeout")}, {Key: ptr("level"), Value: ptr("WARN")},
	}}}}
	if err := c.PutLogs("p", "ls", g); err != nil {
		t.Fatal(err)
	}
	for q, want := range map[string]int{"timeout": 1, "upstream": 1, "msg": 1, "level": 1, "warn": 1, "10.0.0.1": 1, "time*": 1, "timeout*level": 0} {
		r, err := c.GetLogsV3("p", "ls", &sls.GetLogRequest{From: base.Unix() - 10, To: base.Unix() + 10, Query: q, Lines: 10})
		if err != nil {
			t.Fatal(q, err)
		}
		if len(r.Logs) != want {
			t.Errorf("%q: %d logs, want %d", q, len(r.Logs), want)
		}
	}
}
