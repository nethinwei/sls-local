// Package index 模拟 SLS 的索引：决定哪些字段能检索、按什么分词、能否用于 SQL。
// 配置格式与 SLS GetIndex 返回的 JSON 相同，可以直接从生产导出。
package index

import (
	"strings"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

// DefaultTokens 是 SLS 控制台的默认分词符。
var DefaultTokens = []string{",", " ", "'", "\"", ";", "=", "(", ")", "[", "]", "{", "}", "?", "@", "&", "<", ">", "/", ":", "\n", "\t", "\r"}

// Field 是一个可检索的字段（或 JSON 字段的子键）。
type Field struct {
	Name          string // 索引里的字段名（取值时用它）
	Type          string // text、long、double、json
	Tokens        string // 分词符
	CaseSensitive bool
	DocValue      bool // 开启统计，可用于 SQL
	sub           map[string]*Field
}

// Index 是一个 logstore 的索引。Auto 为 true 时没有配置：全部字段按默认分词建 text 索引并开启统计，
// 全文索引开启（本地开箱即用；需要与生产一致时在配置里写明索引）。
type Index struct {
	Auto bool
	Raw  *sls.Index // 配置原文，GetIndex 原样返回；Auto 时为 nil

	Line *Field // 全文索引，nil 表示未开启
	keys map[string]*Field
}

// Auto 返回自动索引。
func Auto() *Index {
	return &Index{Auto: true, Line: defaultText("")}
}

func defaultText(name string) *Field {
	return &Field{Name: name, Type: "text", Tokens: strings.Join(DefaultTokens, ""), DocValue: true}
}

// FromConfig 由 SLS 格式的索引配置构建索引。
func FromConfig(raw *sls.Index) *Index {
	ix := &Index{Raw: raw, keys: map[string]*Field{}}
	if raw.Line != nil {
		ix.Line = &Field{Type: "text", Tokens: tokens(raw.Line.Token), CaseSensitive: raw.Line.CaseSensitive}
	}
	for name, k := range raw.Keys {
		f := &Field{Name: name, Type: k.Type, Tokens: tokens(k.Token), CaseSensitive: k.CaseSensitive, DocValue: k.DocValue}
		if f.Type == "" {
			f.Type = "text"
		}
		if len(k.JsonKeys) > 0 {
			f.sub = map[string]*Field{}
			for sub, jk := range k.JsonKeys {
				// JSON 子键的分词与大小写沿用父字段（与 SLS 一致）
				sf := &Field{Name: sub, Type: jk.Type, Tokens: f.Tokens, CaseSensitive: f.CaseSensitive, DocValue: jk.DocValue}
				if sf.Type == "" {
					sf.Type = "text"
				}
				f.sub[strings.ToLower(sub)] = sf
			}
		}
		ix.keys[strings.ToLower(name)] = f
	}
	return ix
}

func tokens(ts []string) string {
	if len(ts) == 0 {
		return strings.Join(DefaultTokens, "")
	}
	return strings.Join(ts, "")
}

// Lookup 按检索语句里的字段名（不区分大小写）找到索引字段。JSON 字段的子键写作 父字段.子键，
// 返回的 path 是从父字段取子键的路径。系统字段 __topic__、__source__ 总是可检索。
func (ix *Index) Lookup(key string) (f *Field, path []string, ok bool) {
	lower := strings.ToLower(key)
	switch lower {
	case "__topic__", "__source__":
		return defaultText(lower), nil, true
	}
	if ix.Auto {
		return defaultText(key), nil, true
	}
	if f, ok := ix.keys[lower]; ok {
		return f, nil, true
	}
	// 父字段.子键：父字段名本身可能带点号，从最长的前缀开始找
	for i := len(lower) - 1; i > 0; i-- {
		if lower[i] != '.' {
			continue
		}
		parent, ok := ix.keys[lower[:i]]
		if !ok || parent.Type != "json" {
			continue
		}
		if sf, ok := parent.sub[lower[i+1:]]; ok {
			return &Field{Name: parent.Name, Type: sf.Type, Tokens: sf.Tokens, CaseSensitive: sf.CaseSensitive,
				DocValue: sf.DocValue}, []string{sf.Name}, true
		}
	}
	return nil, nil, false
}
