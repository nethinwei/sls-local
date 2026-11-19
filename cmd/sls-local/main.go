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
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/api"
	"github.com/nethinwei/sls-local/internal/config"
	"github.com/nethinwei/sls-local/internal/index"
	"github.com/nethinwei/sls-local/internal/store"
	"github.com/nethinwei/sls-local/internal/tlsutil"
)

// version 由构建时的 -ldflags "-X main.version=..." 写入。
var version = "dev"

// backgroundErrors 数后台工作（刷段、合并、上传、刷新）的错误。
var backgroundErrors atomic.Int64

type logstores []string

func (l *logstores) String() string     { return strings.Join(*l, ",") }
func (l *logstores) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	var (
		httpAddr  = flag.String("http", ":8080", "HTTP 监听地址（查询端：Grafana 插件、SDK）")
		httpsAddr = flag.String("https", ":443", "HTTPS 监听地址，自签证书（写入端：LoongCollector 只走 443）；为空时不监听")
		dataPath  = flag.String("data", "/data/sls.duckdb", "DuckDB 数据文件；为空时使用内存库")
		segDir    = flag.String("segments", "", "Parquet 分段目录，默认是数据文件旁的 segments/；内存库时不刷段")
		flushInt  = flag.Duration("flush-interval", store.DefaultFlushInterval, "刷段间隔：把新写入的日志写成 Parquet 分段")
		flushRows = flag.Int64("flush-rows", store.DefaultFlushRows, "新写入的日志积累到这么多条时提前刷段")
		storage   = flag.String("storage", "", "放分段的对象存储：file:///path 或 s3://bucket/prefix（S3 端点与凭证见 SLS_S3_ENDPOINT、"+
			"AWS_ACCESS_KEY_ID、AWS_SECRET_ACCESS_KEY）。设置时 -segments 是本地缓存")
		cacheSize = flag.Int64("cache-size", 50<<30, "使用对象存储时本地缓存的上限（字节）；0 表示不限")
		target    = flag.String("target", "all", "节点角色：all 写入与查询都做（单机）；write 只接受写入，把分段写进对象存储；"+
			"read 只做查询，读对象存储上全部写入节点的分段（看到的数据比写入晚一个刷段间隔）。write、read 需要 -storage")
		writerID = flag.String("writer-id", "0", "写入节点的编号：共用一个对象存储的写入节点各不相同。auto 取主机名末尾的 -<序号>"+
			"（Kubernetes StatefulSet 的 Pod 名，如 sls-local-write-2 → 2）")
		refresh   = flag.Duration("refresh-interval", store.DefaultRefreshInterval, "查询节点重新列出对象存储上分段的间隔")
		memLimit  = flag.String("memory-limit", "", "DuckDB 的内存上限，如 8GB；默认物理内存的 80%（容器里应设成低于容器内存上限）")
		qTimeout  = flag.Duration("query-timeout", time.Minute, "一次查询（检索、SQL、直方图、上下文）的最长执行时间；0 表示不限")
		maxQ      = flag.Int("max-queries", 0, "同时执行的查询数上限，超过的排队 5 秒，仍拿不到返回 ReadQuotaExceed；0 表示 CPU 数的 2 倍")
		writeRate = flag.Float64("write-rate", 0, "每个 logstore 每秒最多写入的日志条数（允许 2 秒的突发），超过返回 WriteQuotaExceed；0 表示不限")
		ttl       = flag.Duration("ttl", 72*time.Hour, "日志保留时长，按 __time__ 删除；0 表示不删除")
		cfgPath   = flag.String("config", "", "配置文件（YAML）：预建 logstore 并声明索引，格式见 internal/config")
		sqlOffset = flag.Duration("sql-utc-offset", store.DefaultSQLOffset, "SQL 时间函数（from_unixtime、date_format、now）使用的时区偏移，SLS 默认东八区")
		showVer   = flag.Bool("version", false, "打印版本后退出")
		reindex   = flag.Bool("reindex", false, "按 -config 里当前的索引配置重建全部日志的全文列后退出（需先停掉正在运行的 sls-local）。"+
			"与 SLS 一样，修改索引配置只影响之后写入的日志，旧日志要用它重建")
		stores logstores
	)
	flag.Var(&stores, "logstore", "启动时预建的 logstore，格式 project/logstore，可重复（写入时也会自动创建）")
	flag.Parse()
	if *showVer {
		fmt.Println("sls-local", version)
		return
	}

	id, err := parseWriterID(*writerID, os.Hostname)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sls-local:", err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	opts := store.Options{SQLOffset: *sqlOffset, SegmentDir: *segDir, FlushInterval: *flushInt, FlushRows: *flushRows,
		Storage: *storage, CacheSize: *cacheSize, WriterID: id, RefreshInterval: *refresh, MemoryLimit: *memLimit,
		OnError: func(err error) { backgroundErrors.Add(1); log.Warn("segments", "error", err) }}
	if opts.SegmentDir == "" && *dataPath != "" {
		opts.SegmentDir = filepath.Join(filepath.Dir(*dataPath), "segments")
	}
	if err := applyTarget(*target, &opts, dataPath, httpsAddr, *reindex); err != nil {
		fmt.Fprintln(os.Stderr, "sls-local:", err)
		os.Exit(2)
	}
	if *reindex {
		if err := runReindex(log, *dataPath, *cfgPath, opts); err != nil {
			log.Error("reindex failed", "error", err)
			os.Exit(1)
		}
		return
	}
	limits := api.Limits{QueryTimeout: *qTimeout, MaxQueries: *maxQ, WriteRate: *writeRate}
	if limits.MaxQueries == 0 {
		limits.MaxQueries = 2 * runtime.NumCPU()
	}
	if err := run(log, *httpAddr, *httpsAddr, *dataPath, *cfgPath, *target, *ttl, opts, limits, stores); err != nil {
		log.Error("sls-local exited", "error", err)
		os.Exit(1)
	}
}

// parseWriterID 解析 -writer-id：数字，或 auto（主机名末尾的 -<序号>）。
func parseWriterID(v string, hostname func() (string, error)) (int64, error) {
	if v != "auto" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("-writer-id %q: want a number or auto", v)
		}
		return id, nil
	}
	host, err := hostname()
	if err != nil {
		return 0, fmt.Errorf("-writer-id=auto: %w", err)
	}
	i := strings.LastIndexByte(host, '-')
	id, err := strconv.ParseInt(host[i+1:], 10, 64)
	if i < 0 || err != nil || id < 0 {
		return 0, fmt.Errorf("-writer-id=auto: hostname %q does not end with -<number> (a StatefulSet pod name)", host)
	}
	return id, nil
}

// flagSet 判断命令行上是否显式给了某个参数。
func flagSet(name string) bool {
	set := false
	flag.Visit(func(f *flag.Flag) { set = set || f.Name == name })
	return set
}

// singleBackupFlush 是单机（-target=all）开了 -storage 时默认的刷段间隔。
const singleBackupFlush = 30 * time.Second

// applyTarget 按节点角色检查、调整参数。
func applyTarget(target string, opts *store.Options, dataPath, httpsAddr *string, reindex bool) error {
	switch target {
	case "all":
		if opts.Storage != "" && !flagSet("flush-interval") {
			// 单机带对象存储时，对象存储是盘坏后的备份：没刷段的日志只在本地盘上，刷段间隔就是最多丢多少
			opts.FlushInterval = singleBackupFlush
		}
		return nil
	case "write", "read":
	default:
		return fmt.Errorf("-target %q: want all, write or read", target)
	}
	if opts.Storage == "" {
		return fmt.Errorf("-target=%s needs -storage (object storage shared by all nodes)", target)
	}
	if opts.SegmentDir == "" {
		return fmt.Errorf("-target=%s needs -segments (local cache) or -data", target)
	}
	if target == "write" {
		if !flagSet("flush-interval") { // 查询节点看到的数据比写入晚一个刷段间隔
			opts.FlushInterval = 2 * time.Second
		}
		return nil
	}
	if reindex {
		return errors.New("-reindex runs on a write node")
	}
	opts.ReadOnly = true
	*dataPath = "" // 查询节点没有自己的数据：内存库，-data 只用来定缓存目录
	if !flagSet("https") {
		// 查询节点不接受写入：不开采集器用的 443。开着的话，采集器缓存的旧地址指到查询节点时，
		// 它会对着 NotSupported 不停重试（实测 LoongCollector 每秒上万次）；连不上时它会退避
		*httpsAddr = ""
	}
	return nil
}

func run(log *slog.Logger, httpAddr, httpsAddr, dataPath, cfgPath, target string, ttl time.Duration, opts store.Options,
	limits api.Limits, stores []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, dataPath, opts)
	if err != nil {
		return err
	}
	defer st.Close()
	indexes, configured, err := loadIndexes(cfgPath)
	if err != nil {
		return err
	}
	stores = append(stores, configured...)
	for _, s := range stores {
		project, logstore, ok := strings.Cut(s, "/")
		if !ok || project == "" || logstore == "" {
			return fmt.Errorf("-logstore %q: want project/logstore", s)
		}
		if err := st.EnsureLogstore(ctx, project, logstore); err != nil {
			return err
		}
	}

	// 升级后第一次启动：按当前规则重建旧数据的全文列
	if n, err := st.RebuildFulltext(ctx, numericKeys(indexes), false); err != nil {
		return fmt.Errorf("rebuild full-text column: %w", err)
	} else if n > 0 {
		log.Info("rebuilt full-text column for logs written by an older version", "rows", n)
	}

	srv := api.New(st, log, indexes)
	srv.Target = target
	keys, projects, err := accessKeys(cfgPath)
	if err != nil {
		return err
	}
	if len(keys) == 0 && target != "all" {
		log.Warn("no access_keys in -config: requests are not authenticated, anyone who can reach this node can write and query")
	}
	srv.SetAccessKeys(keys)
	srv.SetKeyProjects(projects)
	srv.SetLimits(limits)
	srv.Metrics.Func("sls_local_background_errors_total", "Errors of background work (flush, compaction, upload, refresh).",
		"counter", func() float64 { return float64(backgroundErrors.Load()) })
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
	log.Info("sls-local started", "version", version, "http", httpAddr, "https", httpsAddr, "data", dataPath,
		"segments", opts.SegmentDir, "storage", opts.Storage, "target", target, "writer_id", opts.WriterID, "ttl", ttl.String(),
		"config", cfgPath, "indexed_logstores", len(indexes), "access_keys", len(keys))

	if ttl > 0 {
		if !opts.ReadOnly { // 过期的分段由写入它的节点删
			go retain(ctx, log, st, ttl)
		}
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
	// 用对象存储时，退出前把头部刷出去：不然查询节点要等这个节点重启后才看得到这些日志
	if opts.Storage != "" && !opts.ReadOnly {
		flush, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if n, ferr := st.Flush(flush); ferr != nil {
			log.Warn("flush before exit failed", "error", ferr)
		} else if n > 0 {
			log.Info("flushed before exit", "rows", n)
		}
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

// loadIndexes 读配置文件里的索引声明，返回 project/logstore → 索引，以及配置里的全部 logstore。
func loadIndexes(cfgPath string) (map[string]*sls.Index, []string, error) {
	if cfgPath == "" {
		return map[string]*sls.Index{}, nil, nil
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, err
	}
	return indexesOf(cfg)
}

// accessKeys 读配置文件里的 AccessKey（id → secret）与限定了 project 的密钥（id → project 列表）；
// 没有配置时为空（不校验签名）。
func accessKeys(cfgPath string) (map[string]string, map[string][]string, error) {
	keys, projects := map[string]string{}, map[string][]string{}
	if cfgPath == "" {
		return keys, projects, nil
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, err
	}
	for _, k := range cfg.AccessKeys {
		keys[k.ID] = k.Secret
		if len(k.Projects) > 0 {
			projects[k.ID] = k.Projects
		}
	}
	return keys, projects, nil
}

func indexesOf(cfg *config.Config) (map[string]*sls.Index, []string, error) {
	indexes := map[string]*sls.Index{}
	var stores []string
	for _, l := range cfg.Logstores {
		stores = append(stores, l.Project+"/"+l.Logstore)
		if l.Index != nil {
			indexes[l.Project+"/"+l.Logstore] = l.Index
		}
	}
	return indexes, stores, nil
}

// numericKeys 返回 logstore 里按 long/double 建了索引的字段：它们的值不进全文列。
func numericKeys(indexes map[string]*sls.Index) func(project, logstore string) map[string]bool {
	return func(project, logstore string) map[string]bool {
		if raw, ok := indexes[project+"/"+logstore]; ok {
			return index.FromConfig(raw).NumericKeys()
		}
		return nil
	}
}

// runReindex 按当前索引配置重建全文列。
func runReindex(log *slog.Logger, dataPath, cfgPath string, opts store.Options) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	indexes, _, err := loadIndexes(cfgPath)
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, dataPath, opts)
	if err != nil {
		return err
	}
	defer st.Close()
	start := time.Now()
	n, err := st.RebuildFulltext(ctx, numericKeys(indexes), true)
	if err != nil {
		return err
	}
	if _, err := st.Flush(ctx); err != nil { // 重建时分段读回了头部，写回分段
		return err
	}
	log.Info("rebuilt full-text column", "rows", n, "elapsed", time.Since(start).String())
	return nil
}
