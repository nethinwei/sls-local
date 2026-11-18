// Package api 实现 SLS 的 HTTP 接口。project 取自 Host 的第一段（{project}.{endpoint}），
// 与 SLS SDK、LoongCollector 的请求方式一致。签名只解析不校验。
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/index"
	"github.com/nethinwei/sls-local/internal/metrics"
	"github.com/nethinwei/sls-local/internal/query"
	"github.com/nethinwei/sls-local/internal/store"
)

// Server 是 sls-local 的 HTTP 处理器。
type Server struct {
	store   *store.Store
	log     *slog.Logger
	reqID   atomic.Int64
	now     func() time.Time
	indexes map[string]*index.Index // project/logstore → 配置的索引；没有配置的用自动索引
	// TTLDays 是 GetLogStore、GetIndex 返回的保留天数（与 -ttl 一致；只用于展示）
	TTLDays int
	// Target 是节点的角色：""（或 all）什么都做；write 只接受写入；read 只做查询。
	Target string

	accessKeys map[string]string // 见 SetAccessKeys
	authLog    logLimiter        // 签名错误的日志限频

	// Metrics 是 /metrics 输出的指标。
	Metrics    *metrics.Registry
	limits     Limits
	querySlots chan struct{} // 查询的执行名额（MaxQueries）
	bucketMu   sync.Mutex
	buckets    map[string]*tokenBucket // 写入限流
}

// New 创建处理器。indexes 是配置文件里声明了索引的 logstore（键为 project/logstore）。
func New(st *store.Store, log *slog.Logger, indexes map[string]*sls.Index) *Server {
	s := &Server{store: st, log: log, now: time.Now, indexes: map[string]*index.Index{}, TTLDays: 3, Metrics: metrics.New()}
	s.registerStoreMetrics()
	s.authLog = logLimiter{every: 10 * time.Second, log: func(n int) {
		log.Warn("authentication failures not logged", "count", n)
	}}
	sigs := map[string][]string{} // project/logstore → 分段 bloom 用的分词符组
	for k, raw := range indexes {
		s.indexes[k] = index.FromConfig(raw)
		sigs[k] = tokenSigs(s.indexes[k])
	}
	auto := tokenSigs(index.Auto())
	st.SetTokenSigs(func(project, logstore string) []string {
		if v, ok := sigs[project+"/"+logstore]; ok {
			return v
		}
		return auto
	})
	return s
}

func tokenSigs(ix *index.Index) []string {
	var out []string
	for _, t := range ix.TokenSets() {
		if sig := query.TokenSig(t); !slices.Contains(out, sig) {
			out = append(out, sig)
		}
	}
	return out
}

// only 让 kind（read 或 write）类的请求只在对应角色的节点上执行，其余节点说明该发到哪里。
func (s *Server) only(kind string, h http.HandlerFunc) http.HandlerFunc {
	if s.Target == "" || s.Target == "all" || s.Target == kind {
		return h
	}
	return func(w http.ResponseWriter, r *http.Request) {
		msg := "this sls-local node only accepts writes (-target=write); send queries to a read node"
		if s.Target == "read" {
			msg = "this sls-local node only serves queries (-target=read); send writes to a write node"
		}
		s.fail(w, r, http.StatusNotImplemented, "NotSupported", msg)
	}
}

// index 返回 logstore 的索引。
func (s *Server) index(project, logstore string) *index.Index {
	if ix, ok := s.indexes[project+"/"+logstore]; ok {
		return ix
	}
	return index.Auto()
}

// Handler 返回路由好的 http.Handler。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	read := func(h http.HandlerFunc) http.HandlerFunc { return s.instrument(s.only("read", h)) }
	write := func(h http.HandlerFunc) http.HandlerFunc { return s.instrument(s.only("write", h)) }
	mux.HandleFunc("GET /{$}", read(s.withProject(s.getProject)))
	mux.HandleFunc("GET /logstores", read(s.withProject(s.listLogstores)))
	mux.HandleFunc("GET /logstores/{logstore}", read(s.withProject(s.getLogstore)))
	// PutLogs：SDK 用 /logstores/{logstore}，LoongCollector 用 /shards/lb，按 hash 写入时是 /shards/route
	mux.HandleFunc("POST /logstores/{logstore}", write(s.withProject(s.putLogs)))
	mux.HandleFunc("POST /logstores/{logstore}/shards/{shard}", write(s.withProject(s.putLogs)))
	mux.HandleFunc("POST /logstores/{logstore}/logs", read(s.withProject(s.getLogs)))
	mux.HandleFunc("GET /logstores/{logstore}/index", read(s.withProject(s.getIndex)))
	mux.HandleFunc("GET /logstores/{logstore}/shards", read(s.withProject(s.listShards)))
	mux.HandleFunc("GET /logstores/{logstore}/shards/{shard}", read(s.withProject(s.getShard)))
	mux.HandleFunc("/", s.instrument(func(w http.ResponseWriter, r *http.Request) {
		s.fail(w, r, http.StatusNotFound, "NotSupported", fmt.Sprintf("%s %s is not supported by sls-local", r.Method, r.URL.Path))
	}))
	// 健康检查与指标不要签名，也不按 project 路由（探针的 Host 是 Pod 的 IP）
	outer := http.NewServeMux()
	outer.HandleFunc("GET /health", s.health)
	outer.HandleFunc("GET /ready", s.ready)
	outer.HandleFunc("GET /metrics", s.metricsHandler)
	outer.Handle("/", s.authenticate(mux))
	return outer
}

type projectHandler func(w http.ResponseWriter, r *http.Request, project string)

func (s *Server) withProject(h projectHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := fmt.Sprintf("sls-local-%d", s.reqID.Add(1))
		w.Header().Set("x-log-requestid", id)
		project := projectFromHost(r.Host)
		if project == "" {
			s.fail(w, r, http.StatusBadRequest, "ProjectNotExist",
				"no project in Host "+r.Host+": send requests to {project}.{endpoint}")
			return
		}
		h(w, r, project)
	}
}

// projectFromHost 取 Host 的第一段作为 project；Host 是 IP 或单个标签时没有 project。
func projectFromHost(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if net.ParseIP(host) != nil {
		return ""
	}
	i := strings.IndexByte(host, '.')
	if i <= 0 {
		return ""
	}
	return host[:i]
}

// fail 按 SLS 的格式返回错误：{"errorCode": ..., "errorMessage": ...}。
func (s *Server) fail(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	s.log.Warn("request failed", "method", r.Method, "path", r.URL.Path, "host", r.Host,
		"status", status, "code", code, "error", msg)
	writeJSON(w, status, map[string]string{"errorCode": code, "errorMessage": msg})
}

// failQuery 按错误类型返回：语句错误与执行错误是 400 ParameterInvalid，暂不支持的是 400 NotSupported。
func (s *Server) failQuery(w http.ResponseWriter, r *http.Request, err error) {
	var qe *query.Error
	var se *store.QueryError
	var ne *query.LogstoreNotExistError
	switch {
	case errors.As(err, &ne):
		s.fail(w, r, http.StatusNotFound, "LogStoreNotExist", err.Error())
	case errors.As(err, &qe), errors.As(err, &se):
		s.fail(w, r, http.StatusBadRequest, "ParameterInvalid", err.Error())
	case errors.Is(err, query.ErrUnsupported):
		s.fail(w, r, http.StatusBadRequest, "NotSupported", err.Error())
	default:
		s.failInternal(w, r, err)
	}
}

// failInternal 返回 500；查询超时引起的返回 RequestTimeout（4xx：SDK 对 5xx 自动重试约 90 秒，同样的查询
// 再跑还会超时，只会加重负载）。
func (s *Server) failInternal(w http.ResponseWriter, r *http.Request, err error) {
	if s.timedOut(r, err) {
		s.Metrics.Inc("sls_local_rejected_total", "Requests rejected by limits.", "reason", "query_timeout")
		s.fail(w, r, http.StatusBadRequest, "RequestTimeout", fmt.Sprintf("query timed out after %s", s.limits.QueryTimeout))
		return
	}
	s.fail(w, r, http.StatusInternalServerError, "InternalServerError", err.Error())
}

// requireLogstore 检查 logstore 存在，不存在时返回 LogStoreNotExist 并返回 false。
func (s *Server) requireLogstore(w http.ResponseWriter, r *http.Request, project, logstore string) bool {
	_, ok, err := s.store.Logstore(r.Context(), project, logstore)
	switch {
	case err != nil:
		s.failInternal(w, r, err)
	case !ok:
		s.fail(w, r, http.StatusNotFound, "LogStoreNotExist", "logstore "+logstore+" does not exist")
	}
	return err == nil && ok
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
