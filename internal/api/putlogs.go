package api

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"

	"github.com/nethinwei/sls-local/internal/store"
	"github.com/nethinwei/sls-local/internal/wire"
)

// putLogs 实现 PutLogs，body 是 protobuf LogGroup。
// logstore 不存在时自动创建（SLS 会报 LogStoreNotExist；本地开发省去建库步骤）。
func (s *Server) putLogs(w http.ResponseWriter, r *http.Request, project string) {
	logstore := r.PathValue("logstore")
	body, err := io.ReadAll(io.LimitReader(r.Body, wire.MaxRawSize+1))
	if err != nil {
		s.fail(w, r, http.StatusBadRequest, "PostBodyInvalid", err.Error())
		return
	}
	if len(body) > wire.MaxRawSize {
		s.fail(w, r, http.StatusRequestEntityTooLarge, "PostBodyTooLarge", "body too large")
		return
	}
	rawSize, _ := strconv.Atoi(r.Header.Get("x-log-bodyrawsize"))
	g, err := wire.DecodeLogGroup(body, r.Header.Get("x-log-compresstype"), rawSize)
	if err != nil {
		s.fail(w, r, http.StatusBadRequest, "PostBodyInvalid", err.Error())
		return
	}
	group := &store.Group{Topic: g.GetTopic(), Source: g.GetSource()}
	if group.Source == "" {
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			group.Source = host
		}
	}
	for _, t := range g.LogTags {
		group.Tags = append(group.Tags, store.KV{Key: t.GetKey(), Value: t.GetValue()})
	}
	group.Logs = make([]store.Log, 0, len(g.Logs))
	for _, l := range g.Logs {
		log := store.Log{Time: l.GetTime(), TimeNs: l.TimeNs, Contents: make([]store.KV, 0, len(l.Contents))}
		for _, c := range l.Contents {
			log.Contents = append(log.Contents, store.KV{Key: c.GetKey(), Value: c.GetValue()})
		}
		group.Logs = append(group.Logs, log)
	}
	if !s.allowWrite(project, logstore, len(group.Logs)) {
		s.Metrics.Inc("sls_local_rejected_total", "Requests rejected by limits.", "reason", "write_rate")
		s.fail(w, r, http.StatusForbidden, "WriteQuotaExceed", fmt.Sprintf("write quota exceeded: logstore %s accepts %g logs/s", logstore, s.limits.WriteRate))
		return
	}
	group.NumericKeys = s.index(project, logstore).NumericKeys()
	if err := s.store.Append(r.Context(), project, logstore, group); err != nil {
		s.fail(w, r, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}
	s.Metrics.Add("sls_local_ingested_logs_total", "Logs written.", float64(len(group.Logs)))
	s.Metrics.Add("sls_local_ingested_bytes_total", "Bytes of PutLogs bodies as received (compressed).", float64(len(body)))
	w.WriteHeader(http.StatusOK)
}
