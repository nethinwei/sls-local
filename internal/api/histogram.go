package api

import (
	"net/http"
	"strconv"

	"github.com/nethinwei/sls-local/internal/query"
)

// histogramIntervals 是自动选择的分桶间隔（秒）：取使桶数不超过 60 的最小值。
// 前 9 个与真实 SLS 实测一致（docs/wire-findings.md），更大的是外推。
var histogramIntervals = []int64{1, 10, 30, 60, 300, 600, 1800, 3600, 14400, 86400, 604800}

const maxBuckets = 60

// maxRequestedBuckets 是客户端指定 interval 时允许的最多桶数。
const maxRequestedBuckets = 10000

func histogramInterval(span int64) int64 {
	for _, iv := range histogramIntervals {
		if span <= iv*maxBuckets {
			return iv
		}
	}
	return histogramIntervals[len(histogramIntervals)-1]
}

// getHistograms 实现 GetHistograms：GET /logstores/{logstore}?type=histogram&from&to&query&topic&interval。
// body 是桶数组，总数与进度放在 x-log-count、x-log-progress 头里（SDK 从头里取）。
func (s *Server) getHistograms(w http.ResponseWriter, r *http.Request, project string) {
	logstore := r.PathValue("logstore")
	if !s.requireLogstore(w, r, project, logstore) {
		return
	}
	q := r.URL.Query()
	from, err1 := strconv.ParseInt(q.Get("from"), 10, 64)
	to, err2 := strconv.ParseInt(q.Get("to"), 10, 64)
	if err1 != nil || err2 != nil || to < from {
		s.fail(w, r, http.StatusBadRequest, "ParameterInvalid", "from and to must be unix seconds with from <= to")
		return
	}
	// 带 SQL 时 SLS 忽略 SQL 部分，只按检索条件统计
	plan, err := query.ParseFor(q.Get("query"), s.index(project, logstore), logstore)
	if err != nil {
		s.failQuery(w, r, err)
		return
	}
	where := plan.Where
	if topic := q.Get("topic"); topic != "" {
		where.SQL = "(" + where.SQL + " AND topic = ?)"
		where.Args = append(where.Args, topic)
	}
	// 桶在下面逐个生成，参数又来自客户端：时间收紧到 __time__ 可能的取值（uint32 秒），
	// 指定的间隔会产生过多的桶时改用自动间隔（自动间隔在这个范围内最多约 7000 个桶）
	from, to = min(max(from, 0), 1<<32), min(max(to, 0), 1<<32)
	interval, _ := strconv.ParseInt(q.Get("interval"), 10, 64)
	if interval <= 0 || (to-from)/interval > maxRequestedBuckets {
		interval = histogramInterval(to - from)
	}
	counts, err := s.store.Histogram(r.Context(), project, logstore, from, to, interval, where)
	if err != nil {
		s.failQuery(w, r, err)
		return
	}
	buckets := []histogramBucket{}
	var total int64
	for start := from; start < to; {
		end := min((start/interval+1)*interval, to) // 边界对齐到 interval 的整数倍
		n := counts[start-start%interval]
		total += n
		buckets = append(buckets, histogramBucket{From: start, Count: n, To: end, Progress: "Complete"})
		start = end
	}
	w.Header().Set("x-log-progress", "Complete")
	w.Header().Set("x-log-count", strconv.FormatInt(total, 10))
	writeJSON(w, http.StatusOK, buckets)
}

// histogramBucket 是一个桶，字段顺序与 SLS 一致。
type histogramBucket struct {
	From     int64  `json:"from"`
	Count    int64  `json:"count"`
	To       int64  `json:"to"`
	Progress string `json:"progress"`
}
