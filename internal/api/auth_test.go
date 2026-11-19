package api

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/store"
)

func authEnv(t *testing.T) (string, func(id, secret string) *sls.Client) {
	t.Helper()
	st, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	h := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	h.SetAccessKeys(map[string]string{"ak1": "sk1", "ak2": "sk2"})
	h.SetKeyProjects(map[string][]string{"ak2": {"proj"}}) // ak2 只能访问 proj
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	return addr, func(id, secret string) *sls.Client {
		c := &sls.Client{Endpoint: "sls-local:80", AccessKeyID: id, AccessKeySecret: secret}
		c.SetHTTPClient(&http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		}})
		return c
	}
}

// 配置了 AccessKey：SDK 用对的密钥签名的请求都能通过（含带查询参数的 GetHistograms），其余拒绝。
func TestAuth(t *testing.T) {
	addr, client := authEnv(t)
	now := uint32(time.Now().Unix())
	group := &sls.LogGroup{Topic: ptr("t"), Source: ptr("s"), Logs: []*sls.Log{{Time: &now, Contents: []*sls.LogContent{
		{Key: ptr("level"), Value: ptr("ERROR")}, {Key: ptr("msg"), Value: ptr("a b/c?d=e&f")}}}}}

	ok := client("ak2", "sk2")
	if err := ok.PutLogs("proj", "app", group); err != nil {
		t.Fatalf("PutLogs: %v", err)
	}
	from, to := int64(now)-60, int64(now)+60
	if _, err := ok.GetLogsV3("proj", "app", &sls.GetLogRequest{From: from, To: to, Query: "level: ERROR | select count(*) as c"}); err != nil {
		t.Errorf("GetLogsV3: %v", err)
	}
	if _, err := ok.GetHistograms("proj", "app", "", from, to, "msg: \"a b\" and level: ERROR"); err != nil {
		t.Errorf("GetHistograms: %v", err)
	}
	if _, err := ok.GetIndex("proj", "app"); err != nil {
		t.Errorf("GetIndex: %v", err)
	}
	if _, err := ok.ListLogStore("proj"); err != nil {
		t.Errorf("ListLogStore: %v", err)
	}

	for _, c := range []struct {
		name string
		c    *sls.Client
		code string
	}{
		{"wrong secret", client("ak1", "nope"), "SignatureNotMatch"},
		{"unknown key", client("ak9", "sk1"), "Unauthorized"},
		{"v4", func() *sls.Client {
			c := client("ak1", "sk1")
			c.AuthVersion, c.Region = sls.AuthV4, "cn-hangzhou"
			return c
		}(), "Unauthorized"},
	} {
		err := c.c.PutLogs("proj", "app", group)
		if se, isSLS := err.(*sls.Error); !isSLS || se.Code != c.code {
			t.Errorf("%s: %v, want %s", c.name, err, c.code)
		}
	}

	// 限定了 project 的密钥访问别的 project；没有限定的不受影响
	if err := ok.PutLogs("other", "app", group); err == nil || err.(*sls.Error).Code != "Unauthorized" {
		t.Errorf("ak2 writing to another project: %v", err)
	}
	if _, err := ok.ListLogStore("other"); err == nil {
		t.Error("ak2 listed another project")
	}
	if err := client("ak1", "sk1").PutLogs("other", "app", group); err != nil {
		t.Errorf("ak1 writing to another project: %v", err)
	}

	// 用合法签名换掉请求体；Date 偏得太多
	raw := func(body []byte, md5hex, date string) string {
		h := http.Header{"Content-Type": {"application/json"}, "X-Log-Apiversion": {"0.6.0"}, "X-Log-Signaturemethod": {"hmac-sha1"}}
		sig := signV1("sk1", "POST", md5hex, "application/json", date, h, "/logstores/app/logs", nil)
		req, _ := http.NewRequest("POST", "http://"+addr+"/logstores/app/logs", bytes.NewReader(body))
		req.Host = "proj.sls-local"
		req.Header = h
		req.Header.Set("Content-MD5", md5hex)
		req.Header.Set("Date", date)
		req.Header.Set("Authorization", "LOG ak1:"+sig)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return fmt.Sprint(resp.StatusCode, " ", string(b))
	}
	body := []byte(`{"from":0,"to":2000000000,"query":"*"}`)
	sum := fmt.Sprintf("%X", md5.Sum(body))
	date := time.Now().UTC().Format(http.TimeFormat)
	if got := raw(body, sum, date); !strings.HasPrefix(got, "200") {
		t.Errorf("signed raw request: %s", got)
	}
	if got := raw([]byte(`{"from":0,"to":2000000000,"query":"level: ERROR"}`), sum, date); !strings.Contains(got, "InvalidContentMD5") {
		t.Errorf("tampered body: %s", got)
	}
	if got := raw(body, sum, time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)); !strings.Contains(got, "RequestTimeTooSkewed") {
		t.Errorf("old date: %s", got)
	}
}

// 签名不对时不读请求体：只知道 AccessKeyId 的请求不能让服务端读入整个请求体。
func TestAuthRejectsBeforeReadingBody(t *testing.T) {
	s := &Server{accessKeys: map[string]string{"ak1": "sk1"}, now: time.Now}
	body := &countingReader{r: strings.NewReader(strings.Repeat("x", 1<<20))}
	req := httptest.NewRequest("POST", "/logstores/app", body)
	req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("Content-MD5", "00000000000000000000000000000000")
	req.Header.Set("Authorization", "LOG ak1:bad")
	if _, code, _ := s.verify(req); code != "SignatureNotMatch" {
		t.Fatalf("code = %q", code)
	}
	if body.n != 0 {
		t.Errorf("read %d bytes of the body before checking the signature", body.n)
	}
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}
