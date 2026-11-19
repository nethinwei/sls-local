package api

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

// 请求签名（SLS 的 v1：Authorization: LOG <AccessKeyId>:<签名>，HMAC-SHA1）。配置了 AccessKey 时校验，
// 没配置时与 v0.2 一样只解析不校验（本地开发）。待签名的字符串与 SLS SDK 的 SignerV1 相同：
//
//	方法 \n Content-MD5 \n Content-Type \n Date \n x-log-*、x-acs-* 头（小写、排序、k:v，以 \n 连接）\n 路径?排序后的查询参数
//
// 报错码取自 SLS 文档，文案没有与真实服务逐条对照（docs/compatibility.md）。

// maxSkew 是请求的 Date 与本机时间允许的差。
const maxSkew = 15 * time.Minute

// maxBody 是校验签名时读入的请求体上限（PutLogs 的包远小于它）。
const maxBody = 64 << 20

// SetAccessKeys 设置允许的 AccessKey（id → secret）；为空时不校验签名。
func (s *Server) SetAccessKeys(keys map[string]string) { s.accessKeys = keys }

// SetKeyProjects 限定 AccessKey 能访问的 project（id → project 列表）；不在里面的密钥能访问全部 project。
func (s *Server) SetKeyProjects(scopes map[string][]string) { s.keyScopes = scopes }

// authenticate 校验请求签名，通过后交给 h。
func (s *Server) authenticate(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(s.accessKeys) == 0 {
			h.ServeHTTP(w, r)
			return
		}
		status, code, msg := s.verify(r)
		if code == "" {
			status, code, msg = s.authorize(r)
		}
		if code != "" {
			// 签名错误时采集器会不停重试（实测 LoongCollector 每秒几千次）：日志限频，响应照常
			if s.authLog.allow() {
				s.fail(w, r, status, code, msg)
			} else {
				writeJSON(w, status, map[string]string{"errorCode": code, "errorMessage": msg})
			}
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) verify(r *http.Request) (int, string, string) {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "SLS4-") {
		return http.StatusUnauthorized, "Unauthorized", "signature version 4 is not supported by sls-local, use signature version 1"
	}
	cred, ok := strings.CutPrefix(auth, "LOG ")
	id, sig, ok2 := strings.Cut(cred, ":")
	if !ok || !ok2 || id == "" {
		return http.StatusUnauthorized, "Unauthorized", "missing or malformed Authorization header"
	}
	secret, ok := s.accessKeys[id]
	if !ok {
		return http.StatusUnauthorized, "Unauthorized", "AccessKeyId not found: " + id
	}
	date := r.Header.Get("Date")
	t, err := http.ParseTime(date)
	if err != nil {
		return http.StatusBadRequest, "RequestTimeTooSkewed", "invalid Date header: " + date
	}
	if d := s.now().Sub(t); d > maxSkew || d < -maxSkew {
		return http.StatusBadRequest, "RequestTimeTooSkewed", fmt.Sprintf("request time %s differs from server time by more than %s", date, maxSkew)
	}
	contentMD5 := r.Header.Get("Content-MD5")
	// 先核对签名再读请求体：不然只知道 AccessKeyId、没有密钥的请求也能让服务端读入整个请求体
	want := signV1(secret, r.Method, contentMD5, r.Header.Get("Content-Type"), date, r.Header, r.URL.EscapedPath(), r.URL.Query())
	if !hmac.Equal([]byte(sig), []byte(want)) {
		return http.StatusUnauthorized, "SignatureNotMatch", "signature " + sig + " not match"
	}
	if r.Body != nil && r.Body != http.NoBody {
		body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxBody))
		r.Body.Close()
		if err != nil {
			return http.StatusBadRequest, "PostBodyInvalid", "read body: " + err.Error()
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		// 签名只覆盖 Content-MD5 这个头：不核对的话，截获的请求可以换掉请求体
		if len(body) > 0 && !strings.EqualFold(contentMD5, fmt.Sprintf("%X", md5.Sum(body))) {
			return http.StatusBadRequest, "InvalidContentMD5", "Content-MD5 does not match the body"
		}
	}
	return 0, "", ""
}

// authorize 检查签名通过的密钥能否访问请求的 project（Host 的第一段）。
func (s *Server) authorize(r *http.Request) (int, string, string) {
	id, _, _ := strings.Cut(strings.TrimPrefix(r.Header.Get("Authorization"), "LOG "), ":")
	scope, ok := s.keyScopes[id]
	if !ok {
		return 0, "", ""
	}
	project := projectFromHost(r.Host)
	if slices.Contains(scope, project) {
		return 0, "", ""
	}
	return http.StatusUnauthorized, "Unauthorized", fmt.Sprintf("AccessKeyId %s is not allowed to access project %q", id, project)
}

// logLimiter 让一类日志每 every 最多打一条，并带上期间省略的条数。
type logLimiter struct {
	mu         sync.Mutex
	every      time.Duration
	last       time.Time
	suppressed int
	log        func(suppressed int)
}

// allow 判断这次能不能打日志；不能时记一次省略。能打时先报告之前省略的条数。
func (l *logLimiter) allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if now.Sub(l.last) < l.every {
		l.suppressed++
		return false
	}
	if l.suppressed > 0 && l.log != nil {
		l.log(l.suppressed)
	}
	l.last, l.suppressed = now, 0
	return true
}

// signV1 计算 v1 签名（与 aliyun-log-go-sdk 的 SignerV1 一致）。
func signV1(secret, method, contentMD5, contentType, date string, header http.Header, path string, query map[string][]string) string {
	var names []string
	vals := map[string]string{}
	for k, v := range header {
		l := strings.TrimSpace(strings.ToLower(k))
		if strings.HasPrefix(l, "x-log-") || strings.HasPrefix(l, "x-acs-") {
			names = append(names, l)
			vals[l] = strings.TrimSpace(v[0])
		}
	}
	slices.Sort(names)
	var b strings.Builder
	b.WriteString(method + "\n" + contentMD5 + "\n" + contentType + "\n" + date + "\n")
	for i, k := range names {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(k + ":" + vals[k])
	}
	b.WriteString("\n" + path)
	if len(query) > 0 {
		keys := make([]string, 0, len(query))
		for k := range query {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		b.WriteString("?")
		for i, k := range keys {
			if i > 0 {
				b.WriteString("&")
			}
			for _, v := range query[k] { // 与 SDK 相同：同名参数的多个值直接相连
				b.WriteString(k + "=" + v)
			}
		}
	}
	mac := hmac.New(sha1.New, []byte(secret))
	mac.Write([]byte(b.String()))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
