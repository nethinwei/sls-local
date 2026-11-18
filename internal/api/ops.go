package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/nethinwei/sls-local/internal/store"
)

// 运维：限流、超时、健康检查与指标。报错码取自 SLS 文档（ReadQuotaExceed、WriteQuotaExceed），
// 文案没有与真实服务对照。

// Limits 是查询与写入的限制，零值表示不限。
type Limits struct {
	QueryTimeout time.Duration // 一次查询（检索、SQL、直方图、上下文）最长执行时间
	MaxQueries   int           // 同时执行的查询数，超过的排队
	QueueWait    time.Duration // 排队最长等待，超过返回 ReadQuotaExceed；为 0 时 5 秒
	WriteRate    float64       // 每个 logstore 每秒最多写入的日志条数（允许 2 秒的突发），超过返回 WriteQuotaExceed
}

// SetLimits 设置限制（在 Handler 之前调用）。
func (s *Server) SetLimits(l Limits) {
	if l.QueueWait == 0 {
		l.QueueWait = 5 * time.Second
	}
	s.limits = l
	if l.MaxQueries > 0 {
		s.querySlots = make(chan struct{}, l.MaxQueries)
	}
}

// apiName 是请求对应的 SLS 接口名（指标的标签）。r.Pattern 是路由匹配到的模式。
func apiName(r *http.Request) string {
	switch r.Pattern {
	case "GET /{$}":
		return "GetProject"
	case "GET /logstores":
		return "ListLogStores"
	case "GET /logstores/{logstore}":
		switch r.URL.Query().Get("type") {
		case "histogram":
			return "GetHistograms"
		case "context_log":
			return "GetContextLogs"
		}
		return "GetLogStore"
	case "POST /logstores/{logstore}", "POST /logstores/{logstore}/shards/{shard}":
		return "PutLogs"
	case "POST /logstores/{logstore}/logs":
		return "GetLogs"
	case "GET /logstores/{logstore}/index":
		return "GetIndex"
	case "GET /logstores/{logstore}/shards":
		return "ListShards"
	case "GET /logstores/{logstore}/shards/{shard}":
		return "GetCursor"
	}
	return "Other"
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// instrument 记录请求的指标；查询类的请求先排队拿执行名额、加上超时。
func (s *Server) instrument(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := apiName(r)
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w}
		defer func() {
			s.Metrics.Inc("sls_local_requests_total", "Requests by SLS API and HTTP status.", "api", name, "status", fmt.Sprint(sw.status))
			s.Metrics.Observe("sls_local_request_duration_seconds", "Request duration by SLS API.", time.Since(start).Seconds(), "api", name)
		}()
		if name == "GetLogs" || name == "GetHistograms" || name == "GetContextLogs" {
			release, ok := s.acquireQuery(r.Context())
			if !ok {
				s.Metrics.Inc("sls_local_rejected_total", "Requests rejected by limits.", "reason", "max_queries")
				s.fail(sw, r, http.StatusForbidden, "ReadQuotaExceed", fmt.Sprintf("too many concurrent queries (limit %d)", s.limits.MaxQueries))
				return
			}
			defer release()
			if s.limits.QueryTimeout > 0 {
				ctx, cancel := context.WithTimeout(r.Context(), s.limits.QueryTimeout)
				defer cancel()
				r = r.WithContext(ctx)
			}
		}
		h(sw, r)
	}
}

// acquireQuery 拿一个查询名额，最多等 QueueWait。
func (s *Server) acquireQuery(ctx context.Context) (func(), bool) {
	if s.querySlots == nil {
		return func() {}, true
	}
	t := time.NewTimer(s.limits.QueueWait)
	defer t.Stop()
	select {
	case s.querySlots <- struct{}{}:
		return func() { <-s.querySlots }, true
	case <-t.C:
	case <-ctx.Done():
	}
	return nil, false
}

// timedOut 判断查询是不是因为超时失败的。
func (s *Server) timedOut(r *http.Request, err error) bool {
	return s.limits.QueryTimeout > 0 && (errors.Is(err, context.DeadlineExceeded) || errors.Is(r.Context().Err(), context.DeadlineExceeded))
}

// tokenBucket 是一个 logstore 的写入令牌桶。
type tokenBucket struct {
	tokens float64
	last   time.Time
}

// allowWrite 判断 project/logstore 还能不能写入 n 条。
func (s *Server) allowWrite(project, logstore string, n int) bool {
	rate := s.limits.WriteRate
	if rate <= 0 {
		return true
	}
	burst := 2 * rate
	s.bucketMu.Lock()
	defer s.bucketMu.Unlock()
	if s.buckets == nil {
		s.buckets = map[string]*tokenBucket{}
	}
	key := project + "/" + logstore
	now := time.Now()
	b, ok := s.buckets[key]
	if !ok {
		b = &tokenBucket{tokens: burst, last: now}
		s.buckets[key] = b
	}
	b.tokens = min(burst, b.tokens+now.Sub(b.last).Seconds()*rate)
	b.last = now
	if b.tokens < float64(n) && b.tokens < burst { // 一包超过突发量时，桶满就放行，免得永远写不进去
		return false
	}
	b.tokens -= float64(n)
	return true
}

// health 是存活检查：进程在就是 200。
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	w.Write([]byte("ok\n"))
}

// ready 是就绪检查：数据库能用才是 200。
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ok\n"))
}

// metricsHandler 输出 Prometheus 文本格式的指标。
func (s *Server) metricsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	s.Metrics.WriteTo(w)
}

// registerStoreMetrics 把存储的状态登记成指标（输出时读取）。
func (s *Server) registerStoreMetrics() {
	var mu sync.Mutex
	var cached store.Stats
	var at time.Time
	stat := func(f func(store.Stats) int64) func() float64 {
		return func() float64 {
			mu.Lock()
			defer mu.Unlock()
			if time.Since(at) > time.Second { // 一次输出只读一次
				cached, at = s.store.Stats(), time.Now()
			}
			return float64(f(cached))
		}
	}
	m := s.Metrics
	m.Func("sls_local_segments", "Segments in the current snapshot.", "gauge", stat(func(t store.Stats) int64 { return t.Segments }))
	m.Func("sls_local_segment_rows", "Logs in segments.", "gauge", stat(func(t store.Stats) int64 { return t.SegmentRows }))
	m.Func("sls_local_pending_rows", "Logs written but not flushed to segments yet.", "gauge", stat(func(t store.Stats) int64 { return t.PendingRows }))
	m.Func("sls_local_flushes_total", "Flushes of the head into segments.", "counter", stat(func(t store.Stats) int64 { return t.Flushes }))
	m.Func("sls_local_flushed_rows_total", "Logs flushed into segments.", "counter", stat(func(t store.Stats) int64 { return t.FlushedRows }))
	m.Func("sls_local_compacted_files_total", "Segment files merged by compaction.", "counter", stat(func(t store.Stats) int64 { return t.CompactedFiles }))
	m.Func("sls_local_uploads_total", "Files uploaded to object storage.", "counter", stat(func(t store.Stats) int64 { return t.Uploads }))
	m.Func("sls_local_upload_failures_total", "Failed uploads to object storage.", "counter", stat(func(t store.Stats) int64 { return t.UploadFailures }))
	m.Func("sls_local_downloads_total", "Files downloaded from object storage.", "counter", stat(func(t store.Stats) int64 { return t.Downloads }))
	m.Func("sls_local_download_failures_total", "Failed downloads from object storage.", "counter", stat(func(t store.Stats) int64 { return t.DownloadFailures }))
	m.Func("sls_local_bloom_checked_segments_total", "Segments checked against a search's bloom filters.", "counter", stat(func(t store.Stats) int64 { return t.BloomChecked }))
	m.Func("sls_local_bloom_skipped_segments_total", "Segments skipped because their bloom filters rule the search out.", "counter", stat(func(t store.Stats) int64 { return t.BloomSkipped }))
}
