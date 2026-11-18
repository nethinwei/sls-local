// authcheck 对照鉴权失败时的返回：同一组构造的请求（错的密钥、不存在的 AccessKeyId、过期的 Date、对不上的
// Content-MD5、没有签名……）分别发给真实 SLS 与 sls-local，并排打印状态码、errorCode、errorMessage，用来补
// docs/compatibility.md「鉴权、限流与多节点」里还没对照过的报错。
//
// 请求都是 GetLogsV3（只读），不会写入任何数据。
//
//	SLS_ENDPOINT=cn-hangzhou.log.aliyuncs.com SLS_PROJECT=... SLS_LOGSTORE=... \
//	SLS_ACCESS_KEY_ID=... SLS_ACCESS_KEY_SECRET=... \
//	go run ./tools/authcheck -local http://127.0.0.1:8080 -local-project demo -local-logstore app \
//	  -local-key demo-grafana:change-me-grafana-secret
//
// -denied-key id:secret（可选）是一把签名正确、但没有这个 project 权限的密钥（RAM 用户），对照无权访问时的报错。
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"
)

type target struct {
	name              string
	base              *url.URL // scheme://endpoint
	project, logstore string
	id, secret        string
	denied            string // id:secret，可为空
}

type result struct {
	status        int
	code, message string
}

func main() {
	local := flag.String("local", "http://127.0.0.1:8080", "sls-local 的查询地址")
	localProject := flag.String("local-project", "demo", "")
	localLogstore := flag.String("local-logstore", "app", "")
	localKey := flag.String("local-key", "demo-grafana:change-me-grafana-secret", "sls-local 配置里的一把密钥 id:secret")
	localDenied := flag.String("local-denied-key", "", "sls-local 上限定了 project、不含 -local-project 的密钥 id:secret（可选）")
	denied := flag.String("denied-key", "", "SLS 上没有这个 project 权限的密钥 id:secret（可选）")
	flag.Parse()

	var targets []target
	if ep := os.Getenv("SLS_ENDPOINT"); ep != "" {
		if !strings.Contains(ep, "://") {
			ep = "https://" + ep
		}
		u, err := url.Parse(ep)
		if err != nil {
			fail(err)
		}
		targets = append(targets, target{name: "SLS", base: u, project: os.Getenv("SLS_PROJECT"), logstore: os.Getenv("SLS_LOGSTORE"),
			id: os.Getenv("SLS_ACCESS_KEY_ID"), secret: os.Getenv("SLS_ACCESS_KEY_SECRET"), denied: *denied})
	}
	u, err := url.Parse(*local)
	if err != nil {
		fail(err)
	}
	id, secret, _ := strings.Cut(*localKey, ":")
	targets = append(targets, target{name: "sls-local", base: u, project: *localProject, logstore: *localLogstore,
		id: id, secret: secret, denied: *localDenied})

	cases := []struct {
		name string
		do   func(t target) result
	}{
		{"正确签名", func(t target) result { return send(t, t.id, t.secret, req{}) }},
		{"错误的 secret", func(t target) result { return send(t, t.id, t.secret+"x", req{}) }},
		{"不存在的 AccessKeyId", func(t target) result { return send(t, "LTAInotexist0000", t.secret, req{}) }},
		{"没有 Authorization", func(t target) result { return send(t, t.id, t.secret, req{noAuth: true}) }},
		{"Authorization 格式错", func(t target) result { return send(t, t.id, t.secret, req{rawAuth: "LOG nocolon"}) }},
		{"Date 早 1 小时", func(t target) result { return send(t, t.id, t.secret, req{skew: -time.Hour}) }},
		{"Date 晚 1 小时", func(t target) result { return send(t, t.id, t.secret, req{skew: time.Hour}) }},
		{"Date 格式错", func(t target) result { return send(t, t.id, t.secret, req{date: "yesterday"}) }},
		{"Content-MD5 对不上", func(t target) result { return send(t, t.id, t.secret, req{tamper: true}) }},
		{"无权访问的 project", func(t target) result {
			if t.denied == "" {
				return result{code: "（未提供密钥，跳过）"}
			}
			id, secret, _ := strings.Cut(t.denied, ":")
			return send(t, id, secret, req{})
		}},
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprint(w, "情形")
	for _, t := range targets {
		fmt.Fprintf(w, "\t%s", t.name)
	}
	fmt.Fprintln(w)
	for _, c := range cases {
		fmt.Fprint(w, c.name)
		for _, t := range targets {
			r := c.do(t)
			fmt.Fprintf(w, "\t%d %s: %s", r.status, r.code, truncate(r.message, 90))
		}
		fmt.Fprintln(w)
	}
	w.Flush()
}

type req struct {
	noAuth  bool
	rawAuth string
	skew    time.Duration
	date    string
	tamper  bool // 签名后换掉请求体
}

func send(t target, id, secret string, o req) result {
	now := time.Now().Unix()
	body, _ := json.Marshal(map[string]any{"from": now - 60, "to": now, "query": "*", "line": 1})
	sum := fmt.Sprintf("%X", md5.Sum(body))
	path := "/logstores/" + t.logstore + "/logs"
	date := time.Now().Add(o.skew).UTC().Format(http.TimeFormat)
	if o.date != "" {
		date = o.date
	}
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Content-MD5", sum)
	h.Set("Date", date)
	h.Set("x-log-apiversion", "0.6.0")
	h.Set("x-log-signaturemethod", "hmac-sha1")
	h.Set("x-log-bodyrawsize", fmt.Sprint(len(body)))
	sig := signV1(secret, "POST", sum, "application/json", date, h, path)
	switch {
	case o.noAuth:
	case o.rawAuth != "":
		h.Set("Authorization", o.rawAuth)
	default:
		h.Set("Authorization", "LOG "+id+":"+sig)
	}
	if o.tamper { // 换成同样长度的请求体：只有 Content-MD5 对不上
		body, _ = json.Marshal(map[string]any{"from": now - 60, "to": now, "query": "a", "line": 1})
	}
	r, err := http.NewRequest("POST", t.base.Scheme+"://"+t.base.Host+path, bytes.NewReader(body))
	if err != nil {
		fail(err)
	}
	r.Header = h
	r.Host = t.project + "." + t.base.Hostname()
	if p := t.base.Port(); p != "" {
		r.Host += ":" + p
	}
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return result{code: "请求失败", message: err.Error()}
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var e struct {
		Code    string `json:"errorCode"`
		Message string `json:"errorMessage"`
	}
	json.Unmarshal(b, &e)
	if resp.StatusCode == 200 {
		return result{status: 200, code: "OK"}
	}
	return result{status: resp.StatusCode, code: e.Code, message: e.Message}
}

// signV1 与 internal/api 里的相同（SLS 签名 v1）。
func signV1(secret, method, contentMD5, contentType, date string, header http.Header, path string) string {
	var names []string
	vals := map[string]string{}
	for k, v := range header {
		l := strings.ToLower(k)
		if strings.HasPrefix(l, "x-log-") || strings.HasPrefix(l, "x-acs-") {
			names = append(names, l)
			vals[l] = v[0]
		}
	}
	slices.Sort(names)
	var b strings.Builder
	b.WriteString(method + "\n" + contentMD5 + "\n" + contentType + "\n" + date + "\n")
	for _, k := range names {
		b.WriteString(k + ":" + vals[k] + "\n")
	}
	b.WriteString(path)
	mac := hmac.New(sha1.New, []byte(secret))
	mac.Write([]byte(b.String()))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if r := []rune(s); len(r) > n {
		return string(r[:n]) + "…"
	}
	return s
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "authcheck:", err)
	os.Exit(1)
}
