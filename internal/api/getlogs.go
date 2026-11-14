package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
	if req.From > req.To {
		s.fail(w, r, http.StatusBadRequest, "InvalidTimeRange", fmt.Sprintf("Time Range %d to %d is invalid", req.From, req.To))
		return
	}
	plan, err := query.ParseFor(req.Query, s.index(project, logstore), logstore)
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
		PackMeta: plan.PackMeta, Stages: plan.Stages,
	})
	if err != nil {
		s.failQuery(w, r, err)
		return
	}

	count, offset := int64(len(res.Rows)), max(req.Offset, 0)
	processed := count
	full := count == lines // 凑满了一页
	if plan.Where.Phrase && !full {
		// 短语查询没凑满一页时扫完了全部候选行（各词都出现的），processedRows 是候选行数（实测）
		if processed, err = s.store.Count(r.Context(), store.SearchParams{Project: project, Logstore: logstore,
			From: req.From, FromNs: req.FromNsPart, To: req.To, ToNs: req.ToNsPart, Where: *plan.Where.Relaxed}); err != nil {
			s.failQuery(w, r, err)
			return
		}
	} else if plan.Where.Phrase {
		processed = offset + count
	}
	m := baseMeta(plan, count, processed, start, false)
	if len(res.Keys) > 0 { // 没有结果时 SLS 不返回 keys
		m.add("keys", capKeys(res.Keys))
	}
	m.add("terms", terms(plan))
	if plan.SPL {
		m.add("new_mode", 4)
		m.add("scanBytes", 0)
	}
	if plan.Where.Phrase { // 短语查询走另一种执行方式，多出这几项（实测，见 docs/wire-findings.md）
		if count > 0 {
			m.add("new_mode", 1)
			m.add("mode", 1)
		}
		info := &meta{}
		info.add("scanAll", !full)
		info.add("beginOffset", max(offset-lines, 0))
		info.add("endOffset", offset+count)
		info.add("endTime", res.LastTime)
		m.add("phraseQueryInfo", info)
	}
	m.add("isAccurate", true)
	writeRows(w, m, res.Rows)
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
	m := baseMeta(plan, int64(len(res.Rows)), res.Processed, start, true)
	m.add("cpuSec", 0)
	m.add("cpuCores", 0)
	m.add("powerSql", req.PowerSQL)
	if st := plan.SQL; st.InsertAt >= 0 && st.Text != "" {
		before := st.Text[:min(st.InsertAt, len(st.Text))]
		line := 1 + strings.Count(before, "\n")
		col := len(before) - (strings.LastIndex(before, "\n") + 1)
		m.add("insertedSQL", fmt.Sprintf("%d,%d, from %q ", line, col, logstore))
	}
	m.add("keys", res.Keys)
	m.add("terms", terms(plan))
	if plan.SQL.DefaultLimit {
		m.add("limited", 100)
	}
	m.add("isAccurate", true)
	m.add("columnTypes", res.Types)
	writeRows(w, m, res.Rows)
}

// meta 是按 SLS 字段顺序输出的 meta 对象。
type meta struct {
	keys []string
	vals []any
}

func (m *meta) add(k string, v any) { m.keys = append(m.keys, k); m.vals = append(m.vals, v) }

func (m *meta) json() []byte {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range m.keys {
		if i > 0 {
			b.WriteByte(',')
		}
		writeJSONValue(&b, k)
		b.WriteByte(':')
		writeJSONValue(&b, m.vals[i])
	}
	b.WriteByte('}')
	return b.Bytes()
}

func writeJSONValue(b *bytes.Buffer, v any) {
	if m, ok := v.(*meta); ok {
		b.Write(m.json())
		return
	}
	enc := json.NewEncoder(b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	b.Truncate(b.Len() - 1)
}

// baseMeta 是各种语句共有的前半部分。whereQuery、aggQuery 的写法与 SLS 一致：
//   - 只有检索：whereQuery 是原文，aggQuery 为空；
//   - 带 SQL：whereQuery 是 "|" 之前的原文（含末尾空格），aggQuery 是 "|" 之后的原文；
//   - 带 SPL：whereQuery 同上，aggQuery 是 "*" 加上 "|" 及之后的原文；
//   - 只有 | with_pack_meta：whereQuery 是整条语句。
func baseMeta(plan *query.Plan, count, processed int64, start time.Time, hasSQL bool) *meta {
	where, agg := plan.Query, ""
	if plan.Pipes != "" && (hasSQL || plan.SPL) {
		where = plan.Query[:len(plan.Query)-len(plan.Pipes)]
		if hasSQL {
			agg = plan.Pipes[1:]
		} else {
			agg = "*" + plan.Pipes
		}
	}
	m := &meta{}
	m.add("count", count)
	m.add("progress", "Complete")
	m.add("processedRows", processed)
	m.add("processedBytes", 0)
	m.add("elapsedMillisecond", time.Since(start).Milliseconds())
	m.add("hasSQL", hasSQL)
	m.add("telementryType", "logging") // SLS 同时返回拼错的与正确的两个字段
	m.add("telemetryType", "logging")
	m.add("whereQuery", where)
	m.add("aggQuery", agg)
	return m
}

// capKeys：SLS 的 meta.keys 最多 20 个。
func capKeys(keys []string) []string {
	if len(keys) > 20 {
		return keys[:20]
	}
	return keys
}

func terms(plan *query.Plan) []query.Term {
	if plan.Terms == nil {
		return []query.Term{}
	}
	return plan.Terms
}

// writeRows 写出 GetLogsV3 的响应：{"meta": ..., "data": [行...]}，行是编码好的 JSON 对象。
// 与 SLS 一致，不带 x-log-count、x-log-progress 头（只有 GetHistograms 带）。
func writeRows(w http.ResponseWriter, m *meta, rows [][]byte) {
	var b bytes.Buffer
	b.WriteString(`{"meta":`)
	b.Write(m.json())
	b.WriteString(`,"data":[`)
	for i, row := range rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(row)
	}
	b.WriteString("]}")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b.Bytes())
}
