package api

import (
	"net/http"
	"sort"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/index"
)

// autoIndexWindow 是自动索引列出字段时回看的时间：最近一天写入过的字段。
const autoIndexWindow = 24 * time.Hour

// getIndex 实现 GetIndex：GET /logstores/{logstore}/index。配置了索引时原样返回配置；
// 自动索引时列出最近写入过的字段（全部 text、开启统计），插件的字段补全与字段浏览靠它。
func (s *Server) getIndex(w http.ResponseWriter, r *http.Request, project string) {
	logstore := r.PathValue("logstore")
	if !s.requireLogstore(w, r, project, logstore) {
		return
	}
	info, _, _ := s.store.Logstore(r.Context(), project, logstore)
	ix := s.index(project, logstore)
	raw := ix.Raw
	if ix.Auto {
		names, err := s.store.FieldNames(r.Context(), project, logstore, s.now().Add(-autoIndexWindow).Unix())
		if err != nil {
			s.fail(w, r, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}
		sort.Strings(names)
		raw = &sls.Index{
			Line: &sls.IndexLine{Token: index.DefaultTokens},
			Keys: map[string]sls.IndexKey{},
		}
		for _, n := range names {
			raw.Keys[n] = sls.IndexKey{Type: "text", Token: index.DefaultTokens, DocValue: true}
		}
	}
	writeJSON(w, http.StatusOK, struct {
		*sls.Index
		IndexMode          string `json:"index_mode"`
		Storage            string `json:"storage"`
		LastModifyTime     int64  `json:"lastModifyTime"`
		LastUserModifyTime int64  `json:"lastUserModifyTime"`
	}{raw, "v2", "pg", info.Created, info.Created})
}
