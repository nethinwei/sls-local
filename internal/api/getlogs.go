package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/query"
	"github.com/nethinwei/sls-local/internal/store"
)

// maxLines 是检索单次返回的上限，与 SLS 一致；更多的行由调用方用 offset 翻页。
const maxLines = 100

// getLogs 实现 GetLogsV3：POST /logstores/{logstore}/logs，JSON 请求体，返回 {"meta": ..., "data": [...]}。
func (s *Server) getLogs(w http.ResponseWriter, r *http.Request, project string) {
	start := s.now()
	logstore := r.PathValue("logstore")
	var req sls.GetLogRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err == nil {
		err = json.Unmarshal(body, &req)
	}
	if err != nil {
		s.fail(w, r, http.StatusBadRequest, "ParameterInvalid", "invalid request body: "+err.Error())
		return
	}
	if !s.requireLogstore(w, r, project, logstore) {
		return
	}
	plan, err := query.Parse(req.Query, s.index(project, logstore))
	if err != nil {
		s.failQuery(w, r, err)
		return
	}
	if plan.SQL != nil {
		s.runSQL(w, r, project, logstore, &req, plan, start)
		return
	}
	lines := req.Lines
	if lines <= 0 || lines > maxLines {
		lines = maxLines
	}
	res, err := s.store.Search(r.Context(), store.SearchParams{
		Project: project, Logstore: logstore,
		From: req.From, FromNs: req.FromNsPart, To: req.To, ToNs: req.ToNsPart,
		Where: plan.Where, Limit: lines, Offset: max(req.Offset, 0), Reverse: req.Reverse,
		PackMeta: plan.PackMeta,
	})
	if err != nil {
		s.failQuery(w, r, err)
		return
	}

	whereQuery := plan.Search
	if plan.SPL {
		whereQuery += " " // 与 SLS 一致：带 SPL 时 whereQuery 是检索部分加一个空格
	}
	meta := sls.GetLogsV3ResponseMeta{
		Progress:           "Complete",
		WhereQuery:         whereQuery,
		Keys:               res.Keys,
		ProcessedRows:      res.Count,
		ElapsedMillisecond: time.Since(start).Milliseconds(),
		Count:              int64(len(res.Rows)),
		TelemetryType:      "logging",
	}
	writeRows(w, meta, res.Rows)
}

// runSQL 执行 检索 | SQL：lines、offset、reverse 不生效（与 SLS 一致），行数由 LIMIT 决定（默认 100）。
func (s *Server) runSQL(w http.ResponseWriter, r *http.Request, project, logstore string, req *sls.GetLogRequest,
	plan *query.Plan, start time.Time) {
	res, err := s.store.RunSQL(r.Context(), store.SearchParams{
		Project: project, Logstore: logstore,
		From: req.From, FromNs: req.FromNsPart, To: req.To, ToNs: req.ToNsPart, Where: plan.Where,
	}, plan.SQL)
	if err != nil {
		s.failQuery(w, r, err)
		return
	}
	meta := sls.GetLogsV3ResponseMeta{
		Progress:           "Complete",
		WhereQuery:         plan.Search + " ",
		HasSQL:             true,
		ProcessedRows:      res.Processed,
		ElapsedMillisecond: time.Since(start).Milliseconds(),
		Count:              int64(len(res.Rows)),
		TelemetryType:      "logging",
		PowerSql:           req.PowerSQL,
		Keys:               res.Keys,
		ColumnTypes:        res.Types,
	}
	writeRows(w, meta, res.Rows)
}

// writeRows 写出 GetLogsV3 的响应：{"meta": ..., "data": [行...]}，行是编码好的 JSON 对象。
func writeRows(w http.ResponseWriter, meta sls.GetLogsV3ResponseMeta, rows [][]byte) {
	var b bytes.Buffer
	b.WriteString(`{"meta":`)
	metaJSON, _ := json.Marshal(meta)
	b.Write(metaJSON)
	b.WriteString(`,"data":[`)
	for i, row := range rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(row)
	}
	b.WriteString("]}")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-log-progress", "Complete")
	w.Header().Set("x-log-count", strconv.Itoa(len(rows)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b.Bytes())
}
