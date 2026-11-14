// Package query 把 SLS 查询语句（检索 | SPL | SQL）翻译成 DuckDB 上的查询。
package query

import (
	"regexp"
	"strings"
)

// Split 把语句拆成检索部分与其后的管道部分（以 "|" 开头）。只有双引号算引号，
// 与 Grafana 插件的 splitStatement 规则一致（testdata/plugin/filter-syntax.json 两边共用）。
func Split(q string) (search, rest string) {
	var quote bool
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch {
		case quote:
			if c == '\\' {
				i++
			} else if c == '"' {
				quote = false
			}
		case c == '"':
			quote = true
		case c == '|':
			return strings.TrimSpace(q[:i]), q[i:]
		}
	}
	return strings.TrimSpace(q), ""
}

var sqlStart = regexp.MustCompile(`(?i)^\|\s*(select\b|with\b|set\s+session\b)`)

// sqlPart 是 SQL 管道的文本：从第一个 "|" 之后到下一个引号外的 "|" 为止。SLS 也是先按 "|" 切分再解析 SQL，
// 所以 SQL 里写 a || b 时只有 "|" 之前的部分生效（实测 select level || '/' || service 返回的是 level）。
func sqlPart(rest string) string {
	s := rest[1:]
	var quote bool
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case quote:
			if c == '\\' {
				i++
			} else if c == '"' {
				quote = false
			}
		case c == '"':
			quote = true
		case c == '|':
			return s[:i]
		}
	}
	return s
}

// IsSQL 判断语句是否带 SQL 部分：第一个引号外的管道以 select（或 set session ...; select）开头。
func IsSQL(q string) bool {
	_, rest := Split(q)
	return sqlStart.MatchString(rest)
}

// pipes 把管道部分按引号外的 "|" 切成各个命令（去掉首尾空白，不含 "|"）。
func pipes(rest string) []string {
	var out []string
	var quote bool
	start := -1
	for i := 0; i < len(rest); i++ {
		c := rest[i]
		switch {
		case quote:
			if c == '\\' {
				i++
			} else if c == '"' {
				quote = false
			}
		case c == '"':
			quote = true
		case c == '|':
			if start >= 0 {
				out = append(out, strings.TrimSpace(rest[start:i]))
			}
			start = i + 1
		}
	}
	if start >= 0 {
		out = append(out, strings.TrimSpace(rest[start:]))
	}
	return out
}
