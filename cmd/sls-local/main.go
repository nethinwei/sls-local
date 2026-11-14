// sls-local 是说阿里云日志服务（SLS）协议的本地服务：LoongCollector / SLS SDK 用 PutLogs 写入，
// Grafana 的 SLS 数据源插件用 GetLogs 查询，数据存在本地 DuckDB 里。
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/api"
	"github.com/nethinwei/sls-local/internal/config"
	"github.com/nethinwei/sls-local/internal/store"
	"github.com/nethinwei/sls-local/internal/tlsutil"
)

// version 由构建时的 -ldflags "-X main.version=..." 写入。
var version = "dev"

type logstores []string

func (l *logstores) String() string     { return strings.Join(*l, ",") }
func (l *logstores) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	var (
		httpAddr  = flag.String("http", ":8080", "HTTP 监听地址（查询端：Grafana 插件、SDK）")
		httpsAddr = flag.String("https", ":443", "HTTPS 监听地址，自签证书（写入端：LoongCollector 只走 443）；为空时不监听")
		dataPath  = flag.String("data", "/data/sls.duckdb", "DuckDB 数据文件；为空时使用内存库")
		ttl       = flag.Duration("ttl", 72*time.Hour, "日志保留时长，按 __time__ 删除；0 表示不删除")
		cfgPath   = flag.String("config", "", "配置文件（YAML）：预建 logstore 并声明索引，格式见 internal/config")
		sqlOffset = flag.Duration("sql-utc-offset", store.DefaultSQLOffset, "SQL 时间函数（from_unixtime、date_format、now）使用的时区偏移，SLS 默认东八区")
		showVer   = flag.Bool("version", false, "打印版本后退出")
		stores    logstores
	)
	flag.Var(&stores, "logstore", "启动时预建的 logstore，格式 project/logstore，可重复（写入时也会自动创建）")
	flag.Parse()
	if *showVer {
		fmt.Println("sls-local", version)
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log, *httpAddr, *httpsAddr, *dataPath, *cfgPath, *ttl, *sqlOffset, stores); err != nil {
		log.Error("sls-local exited", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, httpAddr, httpsAddr, dataPath, cfgPath string, ttl, sqlOffset time.Duration, stores []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, dataPath, store.Options{SQLOffset: sqlOffset})
	if err != nil {
		return err
	}
	defer st.Close()
	indexes := map[string]*sls.Index{}
	if cfgPath != "" {
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		for _, l := range cfg.Logstores {
			stores = append(stores, l.Project+"/"+l.Logstore)
			if l.Index != nil {
				indexes[l.Project+"/"+l.Logstore] = l.Index
			}
		}
	}
	for _, s := range stores {
		project, logstore, ok := strings.Cut(s, "/")
		if !ok || project == "" || logstore == "" {
			return fmt.Errorf("-logstore %q: want project/logstore", s)
		}
		if err := st.EnsureLogstore(ctx, project, logstore); err != nil {
			return err
		}
	}

	srv := api.New(st, log, indexes)
	if ttl > 0 { // GetLogStore、GetIndex 展示的保留天数
		srv.TTLDays = int((ttl + 24*time.Hour - 1) / (24 * time.Hour))
	} else {
		srv.TTLDays = 3650
	}
	h := srv.Handler()
	servers := []*http.Server{{Addr: httpAddr, Handler: h, ReadHeaderTimeout: 10 * time.Second}}
	if httpsAddr != "" {
		cfg, err := tlsutil.SelfSigned()
		if err != nil {
			return err
		}
		servers = append(servers, &http.Server{Addr: httpsAddr, Handler: h, TLSConfig: cfg,
			ReadHeaderTimeout: 10 * time.Second,
			// LoongCollector 的 libcurl 不需要 HTTP/2，关掉以免 h2 的连接复用影响排查
			TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){}})
	}
	errc := make(chan error, len(servers))
	for _, srv := range servers {
		go func() {
			var err error
			if srv.TLSConfig != nil {
				err = srv.ListenAndServeTLS("", "")
			} else {
				err = srv.ListenAndServe()
			}
			if !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("listen %s: %w", srv.Addr, err)
			}
		}()
	}
	log.Info("sls-local started", "version", version, "http", httpAddr, "https", httpsAddr, "data", dataPath, "ttl", ttl.String(),
		"config", cfgPath, "indexed_logstores", len(indexes))

	if ttl > 0 {
		go retain(ctx, log, st, ttl)
	}

	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, srv := range servers {
		_ = srv.Shutdown(shutdown)
	}
	return err
}

// retain 每小时删除一次超过保留期的日志。
func retain(ctx context.Context, log *slog.Logger, st *store.Store, ttl time.Duration) {
	tick := time.NewTicker(time.Hour)
	defer tick.Stop()
	for {
		n, err := st.DeleteBefore(ctx, time.Now().Add(-ttl))
		if err != nil {
			log.Warn("retention failed", "error", err)
		} else if n > 0 {
			log.Info("retention deleted logs", "count", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
