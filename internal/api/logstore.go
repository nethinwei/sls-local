package api

import (
	"net/http"
	"strconv"

	"github.com/nethinwei/sls-local/internal/store"
)

func (s *Server) getProject(w http.ResponseWriter, r *http.Request, project string) {
	writeJSON(w, http.StatusOK, map[string]any{
		"projectName": project,
		"status":      "Normal",
		"region":      "local",
		"description": "sls-local",
	})
}

// listLogstores 实现 ListLogStores：GET /logstores?offset=&size=。
func (s *Server) listLogstores(w http.ResponseWriter, r *http.Request, project string) {
	all, err := s.store.Logstores(r.Context(), project)
	if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	size, _ := strconv.Atoi(r.URL.Query().Get("size"))
	if size <= 0 {
		size = 500
	}
	names := []string{}
	for i := max(offset, 0); i < len(all) && len(names) < size; i++ {
		names = append(names, all[i].Name)
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(names), "total": len(all), "logstores": names})
}

// getLogstore 实现 GetLogStore：GET /logstores/{logstore}。
func (s *Server) getLogstore(w http.ResponseWriter, r *http.Request, project string) {
	name := r.PathValue("logstore")
	switch t := r.URL.Query().Get("type"); t {
	case "":
	case "histogram":
		s.getHistograms(w, r, project)
		return
	case "context_log":
		s.getContextLogs(w, r, project)
		return
	default:
		s.fail(w, r, http.StatusBadRequest, "NotSupported", "type="+t+" is not supported by sls-local yet")
		return
	}
	info, ok, err := s.store.Logstore(r.Context(), project, name)
	if err != nil {
		s.fail(w, r, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}
	if !ok {
		s.fail(w, r, http.StatusNotFound, "LogStoreNotExist", "logstore "+name+" does not exist")
		return
	}
	m := &meta{} // 字段与顺序与 SLS 一致
	m.add("logstoreName", info.Name)
	m.add("ttl", s.TTLDays)
	m.add("shardCount", 1)
	m.add("enable_tracking", false)
	m.add("autoSplit", false)
	m.add("maxSplitShard", 0)
	m.add("createTime", info.Created)
	m.add("lastModifyTime", info.Created)
	m.add("archiveSeconds", 0)
	m.add("appendMeta", false)
	m.add("telemetryType", "")
	m.add("mode", "standard")
	m.add("resourceGroupId", "rg-sls-local")
	m.add("enableModify", false)
	m.add("resourceQuota", map[string]any{"storage": map[string]any{"preserved": -1, "used": 0}})
	writeObject(w, m)
}

// listShards 实现 ListShards：sls-local 只有一个 shard，覆盖整个哈希范围。
func (s *Server) listShards(w http.ResponseWriter, r *http.Request, project string) {
	logstore := r.PathValue("logstore")
	if !s.requireLogstore(w, r, project, logstore) {
		return
	}
	info, _, _ := s.store.Logstore(r.Context(), project, logstore)
	m := &meta{}
	m.add("shardID", 0)
	m.add("status", "readwrite")
	m.add("inclusiveBeginKey", "00000000000000000000000000000000")
	m.add("exclusiveEndKey", "ffffffffffffffffffffffffffffffff")
	m.add("createTime", info.Created)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(append(append([]byte("["), m.json()...), ']'))
}

// getShard 处理 GET /logstores/{logstore}/shards/{shard}：目前只有 type=cursor（GetCursor）。
// 游标与 __pack_meta__ 里的一样，是写入序号的 base64；begin 是最早的一条，end 是最新一条之后。
func (s *Server) getShard(w http.ResponseWriter, r *http.Request, project string) {
	logstore := r.PathValue("logstore")
	if !s.requireLogstore(w, r, project, logstore) {
		return
	}
	if r.PathValue("shard") != "0" {
		s.fail(w, r, http.StatusNotFound, "ShardNotExist", "shard "+r.PathValue("shard")+" does not exist")
		return
	}
	q := r.URL.Query()
	if q.Get("type") != "cursor" {
		s.fail(w, r, http.StatusBadRequest, "NotSupported", "type="+q.Get("type")+" is not supported by sls-local yet")
		return
	}
	seq, err := s.store.Cursor(r.Context(), project, logstore, q.Get("from"))
	if err != nil {
		s.fail(w, r, http.StatusBadRequest, "ParameterInvalid", err.Error())
		return
	}
	m := &meta{}
	m.add("cursor", store.EncodeCursor(seq))
	writeObject(w, m)
}

func writeObject(w http.ResponseWriter, m *meta) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(m.json())
}
