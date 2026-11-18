package api

import (
	"bytes"
	"errors"
	"net/http"
	"strconv"

	"github.com/nethinwei/sls-local/internal/store"
)

// maxContextLines 是 GetContextLogs 每个方向的上限，与 SLS 一致。
const maxContextLines = 100

// getContextLogs 实现 GetContextLogs：GET /logstores/{logstore}?type=context_log&back_lines&forward_lines&pack_id&pack_meta。
// 返回锚点前后同一来源的日志，每行带 __index_number__（锚点 0，之前为负、之后为正）。
func (s *Server) getContextLogs(w http.ResponseWriter, r *http.Request, project string) {
	logstore := r.PathValue("logstore")
	if !s.requireLogstore(w, r, project, logstore) {
		return
	}
	q := r.URL.Query()
	back, err1 := strconv.Atoi(q.Get("back_lines"))
	forward, err2 := strconv.Atoi(q.Get("forward_lines"))
	if err1 != nil || err2 != nil || back < 0 || forward < 0 || back+forward == 0 {
		// SLS 的原文（含重复的 "back_lines"）
		s.fail(w, r, http.StatusBadRequest, "ParameterInvalid", "back/forward_linesback_lines or forward_lines must be positive")
		return
	}
	// 超过上限时 SLS 不报错，按上限返回
	back, forward = min(back, maxContextLines), min(forward, maxContextLines)
	seq, ok := store.ParsePackMeta(q.Get("pack_meta"))
	if !ok || q.Get("pack_id") == "" {
		s.fail(w, r, http.StatusBadRequest, "ParameterInvalid", "invalid pack_id or pack_meta")
		return
	}
	res, err := s.store.Context(r.Context(), project, logstore, seq, back, forward)
	if errors.Is(err, store.ErrNoContext) {
		s.fail(w, r, http.StatusBadRequest, "ParameterInvalid", err.Error())
		return
	}
	if err != nil {
		s.failInternal(w, r, err)
		return
	}
	var b bytes.Buffer
	b.WriteString(`{"progress":"Complete","back_lines":` + strconv.Itoa(res.Back) +
		`,"forward_lines":` + strconv.Itoa(res.Forward) + `,"logs":[`)
	for i, row := range res.Rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(row)
	}
	b.WriteString(`],"total_lines":` + strconv.Itoa(len(res.Rows)) + "}")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b.Bytes())
}
