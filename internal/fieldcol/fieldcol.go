// Package fieldcol 把日志字段映射成独立的列。
//
// 检索与 SQL 翻译出的表达式不再逐行解析 contents 的 JSON，而是引用 Ref(key) 这个列；存储层扫描最终的 SQL
// 找出用到的字段（Keys），在数据源里提供这些列：Parquet 分段里写好了的直接读，头部表与没写这一列的分段用
// Extract 从 JSON 现取。两种来源的值完全相同（分段写入时也是用 Extract 取的）。
//
// 列名是 __f_ 加字段名的十六进制：DuckDB 的列名不区分大小写，SLS 的字段名区分。
package fieldcol

import (
	"encoding/hex"
	"regexp"
	"slices"
	"strings"
)

const prefix = "__f_"

// Name 是字段 key 的列名。
func Name(key string) string { return prefix + hex.EncodeToString([]byte(key)) }

// Ref 是在 SQL 里引用字段 key 的列（带引号的标识符）。
func Ref(key string) string { return `"` + Name(key) + `"` }

// Key 从列名取回字段名；不是字段列时 ok 为 false。
func Key(name string) (string, bool) {
	h, ok := strings.CutPrefix(name, prefix)
	if !ok {
		return "", false
	}
	b, err := hex.DecodeString(h)
	if err != nil {
		return "", false
	}
	return string(b), true
}

var refRE = regexp.MustCompile(`"` + prefix + `([0-9a-f]*)"`)

// Keys 返回 SQL 里引用到的字段，去重并排序。字符串字面量里碰巧写了同样的内容时会多算一个，只是多取一列。
func Keys(sqls ...string) []string {
	var keys []string
	for _, s := range sqls {
		for _, m := range refRE.FindAllStringSubmatch(s, -1) {
			if b, err := hex.DecodeString(m[1]); err == nil {
				keys = append(keys, string(b))
			}
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// Extract 是从 JSON 列 col 里取字段 key 的表达式（字段不存在时为 NULL），列的值与它完全相同。
func Extract(col, key string) string {
	return "json_extract_string(" + col + ", " + Quote(JSONPath(key)) + ")"
}

// JSONPath 生成 DuckDB 的 JSON 路径 $."key"，字段名原样保留（含点号、冒号）。
func JSONPath(key string) string {
	return `$."` + strings.ReplaceAll(strings.ReplaceAll(key, `\`, `\\`), `"`, `\"`) + `"`
}

// Quote 把字符串写成 SQL 字面量。
func Quote(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }
