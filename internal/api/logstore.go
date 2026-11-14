package api

import (
	"net/http"
	"strconv"
)

// defaultTTL 是 GetLogStore 返回的保留天数，只用于展示；实际保留期由 -ttl 决定。
const defaultTTL = 3

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
	writeJSON(w, http.StatusOK, map[string]any{
		"logstoreName":   info.Name,
		"ttl":            defaultTTL,
		"shardCount":     1,
		"autoSplit":      false,
		"appendMeta":     true,
		"telemetryType":  "None",
		"mode":           "standard",
		"createTime":     info.Created,
		"lastModifyTime": info.Created,
	})
}
