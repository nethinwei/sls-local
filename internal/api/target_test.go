package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nethinwei/sls-local/internal/store"
)

// 写入节点只接受写入，查询节点只做查询：发错了地方返回 NotSupported 并说明该发到哪里。
func TestTarget(t *testing.T) {
	st, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for _, c := range []struct {
		target, method, path, wantCode string
	}{
		{"read", "POST", "/logstores/app", "NotSupported"},
		{"read", "POST", "/logstores/app/shards/lb", "NotSupported"},
		{"write", "POST", "/logstores/app/logs", "NotSupported"},
		{"write", "GET", "/logstores", "NotSupported"},
		{"write", "POST", "/logstores/app", ""}, // 空包体：协议错误，但不是 NotSupported
		{"all", "GET", "/logstores", ""},
	} {
		srv := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
		srv.Target = c.target
		req := httptest.NewRequest(c.method, c.path, strings.NewReader(""))
		req.Host = "proj.sls-local"
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		got := rec.Header().Get("x-log-errorcode")
		body := rec.Body.String()
		if c.wantCode != "" && (rec.Code != http.StatusNotImplemented || !strings.Contains(body, c.wantCode)) {
			t.Errorf("%s %s %s: %d %s", c.target, c.method, c.path, rec.Code, body)
		}
		if c.wantCode == "" && strings.Contains(body, "NotSupported") {
			t.Errorf("%s %s %s: unexpected %s %s", c.target, c.method, c.path, got, body)
		}
	}
}
