package query

import (
	"fmt"
	"strings"
	"unicode"
)

// exprKeywords 是表达式里原样保留的关键字；其余裸标识符是字段引用（后面跟 "(" 的是函数名）。
var exprKeywords = map[string]bool{
	"and": true, "or": true, "not": true, "is": true, "null": true, "like": true, "ilike": true, "in": true,
	"between": true, "true": true, "false": true, "case": true, "when": true, "then": true, "else": true,
	"end": true, "as": true, "distinct": true, "escape": true, "from": true,
}

// forbidden 是不允许出现在表达式里的关键字：表达式只能读当前这一行。
var forbidden = map[string]bool{"select": true, "with": true, "union": true, "exists": true}

// funcAlias 把 SLS（Presto）函数名换成 DuckDB 的同义函数。
var funcAlias = map[string]string{
	"regexp_like": "regexp_matches",
}

// fieldRef 返回字段引用在 logs 表上的表达式。SPL 里字段都是 varchar，只有 __time__、__time_ns_part__ 是数值。
func fieldRef(name string) (string, []any) {
	switch strings.ToLower(name) {
	case "__time__":
		return "t", nil
	case "__time_ns_part__":
		return "coalesce(ns, 0)", nil
	}
	return valueExpr(name, nil)
}

// Expr 把一个 SQL 标量表达式（SPL where 的条件）翻译成 DuckDB 表达式：
// 字段引用（裸标识符或双引号标识符）换成取值表达式，字符串、数字、运算符与函数原样保留。
func Expr(src string) (Cond, error) {
	var b strings.Builder
	var args []any
	afterAs := false // cast(x as TYPE) 里 as 之后的标识符是类型名
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ';':
			return Cond{}, &Error{"invalid query: ';' is not allowed"}
		case c == '\'':
			j := i + 1
			for ; j < len(src); j++ {
				if src[j] == '\'' {
					if j+1 < len(src) && src[j+1] == '\'' {
						j++
						continue
					}
					break
				}
			}
			if j >= len(src) {
				return Cond{}, &Error{"invalid query: unterminated string literal"}
			}
			b.WriteString(src[i : j+1])
			i = j + 1
		case c == '"':
			var name strings.Builder
			j := i + 1
			for ; j < len(src); j++ {
				if src[j] == '"' {
					if j+1 < len(src) && src[j+1] == '"' {
						name.WriteByte('"')
						j++
						continue
					}
					break
				}
				name.WriteByte(src[j])
			}
			if j >= len(src) {
				return Cond{}, &Error{"invalid query: unterminated quoted identifier"}
			}
			expr, a := fieldRef(name.String())
			b.WriteString(expr)
			args = append(args, a...)
			i = j + 1
		case isIdentStart(c):
			j := i
			for j < len(src) && isIdentPart(src[j]) {
				j++
			}
			word := src[i:j]
			lower := strings.ToLower(word)
			k := j
			for k < len(src) && unicode.IsSpace(rune(src[k])) {
				k++
			}
			isCall := k < len(src) && src[k] == '('
			switch {
			case forbidden[lower]:
				return Cond{}, &Error{fmt.Sprintf("invalid query: %q is not allowed in an expression", word)}
			case afterAs:
				b.WriteString(word)
				afterAs = false
			case isCall:
				if alias, ok := funcAlias[lower]; ok {
					word = alias
				}
				b.WriteString(word)
			case exprKeywords[lower]:
				b.WriteString(word)
				afterAs = lower == "as"
			default:
				expr, a := fieldRef(word)
				b.WriteString(expr)
				args = append(args, a...)
			}
			i = j
		default:
			b.WriteByte(c)
			i++
		}
	}
	if strings.TrimSpace(b.String()) == "" {
		return Cond{}, &Error{"invalid query: empty expression"}
	}
	return Cond{SQL: "(" + b.String() + ")", Args: args}, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentPart(c byte) bool { return isIdentStart(c) || (c >= '0' && c <= '9') }
