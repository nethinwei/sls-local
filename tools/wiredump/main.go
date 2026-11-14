// wiredump 记录收到的每个 HTTP 请求（方法、路径、头、body 长度），并把 body 存到 -out 目录，用于对齐 SLS 线上协议。
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/nethinwei/sls-local/internal/tlsutil"
)

func main() {
	listen := flag.String("listen", ":8080", "HTTP 监听地址")
	listenTLS := flag.String("listen-tls", ":443", "HTTPS 监听地址（自签证书；LoongCollector 社区版只走 443 且不校验证书）")
	out := flag.String("out", "wire", "报文输出目录")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	var n atomic.Int64
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := n.Add(1)
		body, _ := io.ReadAll(r.Body)
		keys := make([]string, 0, len(r.Header))
		for k := range r.Header {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		msg := fmt.Sprintf("#%d %s %s host=%s body=%d\n", id, r.Method, r.URL.RequestURI(), r.Host, len(body))
		for _, k := range keys {
			msg += fmt.Sprintf("  %s: %s\n", k, r.Header.Get(k))
		}
		log.Print(msg)
		_ = os.WriteFile(filepath.Join(*out, fmt.Sprintf("%04d.bin", id)), body, 0o644)
		_ = os.WriteFile(filepath.Join(*out, fmt.Sprintf("%04d.txt", id)), []byte(msg), 0o644)
		w.Header().Set("x-log-requestid", fmt.Sprintf("wiredump-%d", id))
		w.WriteHeader(http.StatusOK)
	})
	go func() { log.Fatal(serveTLS(*listenTLS, h)) }()
	log.Fatal(http.ListenAndServe(*listen, h))
}

// serveTLS 用启动时生成的自签证书提供 HTTPS。
func serveTLS(addr string, h http.Handler) error {
	cfg, err := tlsutil.SelfSigned()
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: addr, Handler: h, TLSConfig: cfg}
	return srv.ListenAndServeTLS("", "")
}
