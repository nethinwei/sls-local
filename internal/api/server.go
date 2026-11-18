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
	"sync/atomic"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/index"
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
}

// New 创建处理器。indexes 是配置文件里声明了索引的 logstore（键为 project/logstore）。
func New(st *store.Store, log *slog.Logger, indexes map[string]*sls.Index) *Server {
	s := &Server{store: st, log: log, now: time.Now, indexes: map[string]*index.Index{}, TTLDays: 3}
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
	mux.HandleFunc("GET /{$}", s.withProject(s.getProject))
	mux.HandleFunc("GET /logstores", s.withProject(s.listLogstores))
	mux.HandleFunc("GET /logstores/{logstore}", s.withProject(s.getLogstore))
	// PutLogs：SDK 用 /logstores/{logstore}，LoongCollector 用 /shards/lb，按 hash 写入时是 /shards/route
	mux.HandleFunc("POST /logstores/{logstore}", s.withProject(s.putLogs))
	mux.HandleFunc("POST /logstores/{logstore}/shards/{shard}", s.withProject(s.putLogs))
	mux.HandleFunc("POST /logstores/{logstore}/logs", s.withProject(s.getLogs))
	mux.HandleFunc("GET /logstores/{logstore}/index", s.withProject(s.getIndex))
	mux.HandleFunc("GET /logstores/{logstore}/shards", s.withProject(s.listShards))
	mux.HandleFunc("GET /logstores/{logstore}/shards/{shard}", s.withProject(s.getShard))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.fail(w, r, http.StatusNotFound, "NotSupported", fmt.Sprintf("%s %s is not supported by sls-local", r.Method, r.URL.Path))
	})
	return mux
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
		s.fail(w, r, http.StatusInternalServerError, "InternalServerError", err.Error())
	}
}

// requireLogstore 检查 logstore 存在，不存在时返回 LogStoreNotExist 并返回 false。
func (s *Server) requireLogstore(w http.ResponseWriter, r *http.Request, project, logstore string) bool {
	_, ok, err := s.store.Logstore(r.Context(), project, logstore)
	switch {
	case err != nil:
		s.fail(w, r, http.StatusInternalServerError, "InternalServerError", err.Error())
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
