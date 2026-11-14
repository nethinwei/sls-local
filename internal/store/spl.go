package store

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/nethinwei/sls-local/internal/query"
)

// splSource 把 SPL 的 where、extend 按顺序套在检索结果外面：每条命令一层子查询，extend 多出一列。
func splSource(base string, stages []query.Stage) (string, []string) {
	src := base
	var ext []string
	for _, st := range stages {
		switch st.Kind {
		case "where":
			src = "SELECT * FROM (" + src + ") WHERE " + st.SQL
		case "extend":
			src = "SELECT *, (" + st.SQL + ") AS " + st.Col + " FROM (" + src + ")"
			ext = append(ext, st.Col)
		}
	}
	return src, ext
}

// needsAssembly 判断输出行要不要按字段重新组装（有 extend、project、parse-json 等）。
func needsAssembly(stages []query.Stage) bool {
	for _, st := range stages {
		if st.Kind != "where" {
			return true
		}
	}
	return false
}

// splRow 是组装好的一行：fields 是输入的字段，added 是 extend、parse-json 新增的字段，
// projected 表示经过 project（字段顺序已经确定）。
type splRow struct {
	r             *row
	fields, added []KV
	projected     bool
}

// assembleRows 输出一页结果。SLS 的 SPL 引擎先按各行字段首次出现的顺序排好列（输入的字段在前，新增的在后），
// 每行按这个列序输出（实测：某行独有的字段排在 tag 之后）。
func assembleRows(rows []splRow) ([][]byte, []string) {
	var order, addedOrder []string
	seen := map[string]bool{}
	for _, sr := range rows {
		for _, kv := range sr.fields {
			if !seen[kv.Key] {
				seen[kv.Key] = true
				order = append(order, kv.Key)
			}
		}
	}
	for _, sr := range rows {
		for _, kv := range sr.added {
			if !seen[kv.Key] {
				seen[kv.Key] = true
				addedOrder = append(addedOrder, kv.Key)
			}
		}
	}
	order = append(order, addedOrder...)
	keys := newKeySet()
	var out [][]byte
	for _, sr := range rows {
		vals := map[string]string{}
		var present []KV
		if sr.projected {
			present = append(sr.fields, sr.added...)
		} else {
			for _, kv := range append(append([]KV{}, sr.fields...), sr.added...) {
				vals[kv.Key] = kv.Value
			}
			for _, k := range order {
				if v, ok := vals[k]; ok {
					present = append(present, KV{k, v})
				}
			}
		}
		var b bytes.Buffer
		b.WriteByte('{')
		for _, kv := range present {
			writeString(&b, kv.Key)
			b.WriteByte(':')
			writeString(&b, kv.Value)
			b.WriteByte(',')
			keys.add(kv.Key)
		}
		b.WriteString(`"__time__":"` + strconv.FormatInt(sr.r.t, 10) + `"`)
		if sr.r.ns.Valid {
			b.WriteString(`,"__time_ns_part__":"` + strconv.FormatInt(int64(sr.r.ns.Int32), 10) + `"`)
		}
		b.WriteByte('}')
		out = append(out, b.Bytes())
	}
	return out, keys.list
}

// assemble 按 SPL 命令得到一行的字段：写入字段 → (__pack_meta__) → __topic__ → __source__ → tag，
// extend、parse-json 新增的字段另列，再按 project、project-away 取舍。
func (r *row) assemble(stages []query.Stage, ext []any, packMeta bool) splRow {
	fields := orderedObject(r.contents)
	if packMeta {
		fields = append(fields, KV{"__pack_meta__", PackMeta(r.seq, r.packN, r.packI)})
	}
	fields = append(fields, KV{"__topic__", r.topic}, KV{"__source__", r.source})
	fields = append(fields, orderedObject(r.tags)...)
	var added []KV
	set := func(k, v string) {
		for _, list := range []*[]KV{&fields, &added} {
			for i := range *list {
				if (*list)[i].Key == k {
					(*list)[i].Value = v
					return
				}
			}
		}
		added = append(added, KV{k, v})
	}
	projected := false
	e := 0
	for _, st := range stages {
		switch st.Kind {
		case "extend":
			set(st.Name, formatValue(ext[e], ""))
			e++
		case "parse-json":
			for _, kv := range append(append([]KV{}, fields...), added...) {
				if kv.Key == st.Name {
					for _, sub := range jsonMembers(kv.Value) {
						set(sub.Key, sub.Value)
					}
					break
				}
			}
		case "parse-regexp":
			if v, ok := lookup(fields, added, st.Name); ok {
				if m := compileRegexp(st.Regex).FindStringSubmatch(v); m != nil {
					for i, n := range st.Fields {
						if i+1 < len(m) {
							set(n, m[i+1])
						}
					}
				}
			}
		case "parse-kv":
			if v, ok := lookup(fields, added, st.Name); ok {
				if st.Regex != "" {
					for _, m := range compileRegexp(st.Regex).FindAllStringSubmatch(v, -1) {
						if len(m) >= 3 {
							set(m[1], m[2])
						}
					}
				} else {
					for _, pair := range strings.Split(v, st.Delim) {
						if k, val, ok := strings.Cut(pair, st.Sep); ok {
							set(k, val)
						}
					}
				}
			}
		case "project":
			// SLS 保留 __tag__:__pack_id__（放在最前），其余只留列出的字段，按列出的顺序
			all := append(append([]KV{}, fields...), added...)
			var out []KV
			for _, kv := range all {
				if kv.Key == "__tag__:__pack_id__" {
					out = append(out, kv)
				}
			}
			for _, f := range st.Fields {
				for _, kv := range all {
					if kv.Key == f {
						out = append(out, kv)
						break
					}
				}
			}
			fields, added, projected = out, nil, true
		case "project-away":
			drop := map[string]bool{}
			for _, f := range st.Fields {
				drop[f] = true
			}
			keep := func(list []KV) []KV {
				out := list[:0:0]
				for _, kv := range list {
					if !drop[kv.Key] {
						out = append(out, kv)
					}
				}
				return out
			}
			fields, added = keep(fields), keep(added)
		}
	}
	return splRow{r: r, fields: fields, added: added, projected: projected}
}

// orderedObject 按出现顺序取出 JSON 对象（值都是字符串）的成员。
func orderedObject(obj string) []KV {
	dec := json.NewDecoder(strings.NewReader(obj))
	var out []KV
	if _, err := dec.Token(); err != nil {
		return nil
	}
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return out
		}
		var v string
		if err := dec.Decode(&v); err != nil {
			return out
		}
		out = append(out, KV{k.(string), v})
	}
	return out
}

// jsonMembers 是 parse-json 展开的字段：顶层成员，字符串取原值，其他取紧凑的 JSON 文本。
func jsonMembers(text string) []KV {
	dec := json.NewDecoder(strings.NewReader(text))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil
	}
	var out []KV
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			return out
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return out
		}
		v := string(raw)
		var s string
		if json.Unmarshal(raw, &s) == nil {
			v = s
		} else {
			var buf bytes.Buffer
			if json.Compact(&buf, raw) == nil {
				v = buf.String()
			}
		}
		out = append(out, KV{k.(string), v})
	}
	return out
}

func lookup(fields, added []KV, name string) (string, bool) {
	for _, list := range [][]KV{added, fields} {
		for _, kv := range list {
			if kv.Key == name {
				return kv.Value, true
			}
		}
	}
	return "", false
}

var regexpCache sync.Map

// compileRegexp 编译 SPL 的正则（query 包已经校验过能编译）。
func compileRegexp(re string) *regexp.Regexp {
	if v, ok := regexpCache.Load(re); ok {
		return v.(*regexp.Regexp)
	}
	r := regexp.MustCompile(re)
	regexpCache.Store(re, r)
	return r
}
