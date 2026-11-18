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
			s.failInternal(w, r, err)
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
	writeObject(w, indexObject(raw, info.Created, s.TTLDays))
}

// indexObject 按 SLS 的写法输出索引：顶层字段按字母序；每个字段先是按字母序的属性，再是 alias、
// caseSensitive、chn；long/double 没有分词相关的属性，JSON 子键只有 doc_value、type、alias。
func indexObject(raw *sls.Index, modified int64, ttl int) *meta {
	keys := &meta{}
	names := make([]string, 0, len(raw.Keys))
	for n := range raw.Keys {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		keys.add(n, keyObject(raw.Keys[n]))
	}
	m := &meta{}
	m.add("index_mode", "v2")
	m.add("keys", keys)
	m.add("lastModifyTime", modified)
	m.add("lastUserModifyTime", modified)
	if raw.Line != nil {
		line := &meta{}
		line.add("caseSensitive", raw.Line.CaseSensitive)
		line.add("chn", raw.Line.Chn)
		line.add("token", raw.Line.Token)
		m.add("line", line)
	}
	m.add("storage", "pg")
	m.add("ttl", ttl)
	return m
}

// keyObject 输出一个字段的索引配置。SLS 的顺序：取了非默认值的属性按字母序在前，取默认值的
// alias、caseSensitive、chn 按这个顺序在后（实测：caseSensitive 为 true 时排在最前）。
func keyObject(k sls.IndexKey) *meta {
	typ := k.Type
	if typ == "" {
		typ = "text"
	}
	numeric := typ == "long" || typ == "double"
	type prop struct {
		name string
		val  any
	}
	set := []prop{{"doc_value", k.DocValue}, {"type", typ}}
	var defaults []prop
	if typ == "json" {
		sub := &meta{}
		subNames := make([]string, 0, len(k.JsonKeys))
		for n := range k.JsonKeys {
			subNames = append(subNames, n)
		}
		sort.Strings(subNames)
		for _, n := range subNames {
			jk := k.JsonKeys[n]
			so := &meta{}
			so.add("doc_value", jk.DocValue)
			so.add("type", jk.Type)
			so.add("alias", jk.Alias)
			sub.add(n, so)
		}
		// SDK 的结构体没有 index_all、max_depth：按 SLS 控制台的默认值输出
		set = append(set, prop{"index_all", true}, prop{"json_keys", sub}, prop{"max_depth", 3})
	}
	if !numeric {
		set = append(set, prop{"token", k.Token})
	}
	optional := []prop{{"alias", k.Alias}}
	if !numeric {
		optional = append(optional, prop{"caseSensitive", k.CaseSensitive}, prop{"chn", k.Chn})
	}
	for _, p := range optional {
		if p.val == "" || p.val == false {
			defaults = append(defaults, p)
		} else {
			set = append(set, p)
		}
	}
	sort.Slice(set, func(i, j int) bool { return set[i].name < set[j].name })
	o := &meta{}
	for _, p := range append(set, defaults...) {
		o.add(p.name, p.val)
	}
	return o
}
