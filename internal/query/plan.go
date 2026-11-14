package query

import (
	"errors"
	"fmt"
	"github.com/nethinwei/sls-local/internal/presto"
	"regexp"
	"strconv"
	"strings"

	"github.com/nethinwei/sls-local/internal/index"
)

// ErrUnsupported 表示语句合法但 sls-local 还不支持（例如 SPL 中除 where 之外的指令）。
var ErrUnsupported = errors.New("not supported by sls-local yet")

// Plan 是一条检索/SPL 语句的执行计划。
type Plan struct {
	Query    string // 语句原文
	Pipes    string // 第一个 "|" 及之后的原文（没有管道时为空）
	Terms    []Term // meta.terms
	Search   string // 检索部分原文，回填 meta.whereQuery
	Where    Cond   // 检索条件与各个 | where 的合取
	PackMeta bool   // 带 | with_pack_meta：每行返回 __pack_meta__
	SPL      bool   // 带 SPL 指令（with_pack_meta 不算）
	SQL      *SQLStmt
	Stages   []Stage // 需要逐条执行的 SPL 命令（只有 where 时已并进 Where，这里为空）
}

// Parse 按索引 ix 解析一条语句：检索、检索 | SPL，或检索 | SQL。
func Parse(q string, ix *index.Index) (*Plan, error) { return ParseFor(q, ix, "log") }

// ParseFor 与 Parse 相同，logstore 是查询的 logstore 名（SQL 报错的列号与它的长度有关）。
func ParseFor(q string, ix *index.Index, logstore string) (*Plan, error) {
	search, rest := Split(q)
	where, err := Search(search, ix)
	if err != nil {
		return nil, err
	}
	p := &Plan{Query: q, Pipes: rest, Terms: where.Terms, Search: search, Where: where}
	if sqlStart.MatchString(rest) {
		if sqlEngine == "antlr" {
			p.SQL, err = translatePresto(sqlPart(rest), ix, logstore)
		} else {
			p.SQL, err = TranslateSQL(rest[1:], ix)
		}
		if err != nil {
			return nil, err
		}
		return p, nil
	}
	return p, p.parseSPL(rest, ix)
}

// Stage 是 SPL 的一条命令。where、extend 的表达式是 logs 表上的 SQL（字段按 varchar 取值），
// 可以引用之前 extend 出来的列（Col）。
type Stage struct {
	Kind   string   // where、extend、project、project-away、parse-json、parse-regexp、parse-kv
	Name   string   // extend 的字段名；parse-* 的源字段
	SQL    string   // where 的条件、extend 的值
	Col    string   // extend 的结果列名
	Fields []string // project、project-away 的字段；parse-regexp 的捕获组字段名
	Regex  string   // parse-regexp；parse-kv -regexp（两个捕获组：键、值）
	Delim  string   // parse-kv：键值对之间的分隔符（整个字符串）
	Sep    string   // parse-kv：键与值之间的分隔符
}

// msgSPLNotTranslatable 是 SLS 对 sort、limit 等的报错原文。
const msgSPLNotTranslatable = "invalid query: this spl is not supported translate to sql, column cannot be predicted"

var (
	extendAssign = regexp.MustCompile(`^\s*("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_.]*)\s*=\s*`)
	statsCall    = regexp.MustCompile(`^stats\s+[A-Za-z_][A-Za-z0-9_]*\s*\(`)
)

// parseSPL 解析检索之后的 SPL 命令。报错的列号按 SLS 的 aggQuery 文本（"*" 加上 "|" 及之后的原文）计算。
// 没有 extend 时，where 直接并进检索条件（直方图也能用）；有 extend 时按顺序执行全部命令。
func (p *Plan) parseSPL(rest string, ix *index.Index) error {
	extended := map[string]string{}        // extend、parse-regexp 产生的字段（小写）→ SQL
	var dynamic []func(name string) string // parse-json、parse-kv：键不固定，按名字现查（后面的优先）
	resolve := func(name string) (string, string) {
		if col, ok := extended[strings.ToLower(name)]; ok {
			return col, ""
		}
		switch strings.ToLower(name) {
		case "__time__":
			return "t", "bigint"
		case "__time_ns_part__":
			return "coalesce(ns, 0)", "bigint"
		}
		expr, _ := valueExpr(name, nil)
		expr = strings.Replace(expr, "?", sqlString(jsonPath(name)), 1)
		if len(dynamic) > 0 {
			parts := make([]string, 0, len(dynamic)+1)
			for i := len(dynamic) - 1; i >= 0; i-- {
				parts = append(parts, dynamic[i](name))
			}
			expr = "coalesce(" + strings.Join(append(parts, expr), ", ") + ")"
		}
		return expr, "varchar"
	}
	var stages []Stage
	hasExtend := false
	for _, cmd := range pipeOffsets(rest) {
		off := 1 + cmd.off // aggQuery 前面多一个 "*"
		name, arg, _ := strings.Cut(cmd.text, " ")
		argOff := off + len(name) + (len(cmd.text) - len(name) - len(strings.TrimLeft(cmd.text[len(name):], " ")))
		arg = strings.TrimLeft(arg, " ")
		expr := func(src string, at int) (string, error) {
			sql, err := presto.TranslateExpr(src, resolve)
			var se *presto.ExprSyntaxError
			if errors.As(err, &se) {
				return "", &Error{fmt.Sprintf("invalid query: line 1:%d %s", at+se.Col, se.Msg)}
			}
			var pe *presto.Error
			if errors.As(err, &pe) {
				return "", &Error{pe.Msg}
			}
			return sql, err
		}
		switch strings.ToLower(name) {
		case "with_pack_meta":
			p.PackMeta = true
			continue
		case "where":
			sql, err := expr(arg, argOff)
			if err != nil {
				return err
			}
			stages = append(stages, Stage{Kind: "where", SQL: sql})
		case "extend":
			m := extendAssign.FindStringSubmatch(arg)
			if m == nil {
				return &Error{fmt.Sprintf("invalid query: line 1:%d missing EQ at '%s'", argOff, firstWord(arg))}
			}
			field := unquoteIdent(m[1])
			sql, err := expr(arg[len(m[0]):], argOff+len(m[0]))
			if err != nil {
				return err
			}
			col := fmt.Sprintf(`"__spl_e%d"`, len(stages))
			stages = append(stages, Stage{Kind: "extend", Name: field, SQL: sql, Col: col})
			extended[strings.ToLower(field)] = col
			hasExtend = true
		case "project", "project-away":
			var fields []string
			for _, f := range strings.Split(arg, ",") {
				if f = strings.TrimSpace(f); f != "" {
					fields = append(fields, unquoteIdent(f))
				}
			}
			stages = append(stages, Stage{Kind: strings.ToLower(name), Fields: fields})
		case "parse-json":
			field := unquoteIdent(strings.TrimSpace(arg))
			src, _ := resolve(field)
			dynamic = append(dynamic, func(k string) string {
				return "(CASE WHEN json_valid(" + src + ") THEN json_extract_string(" + src + ", " + sqlString(jsonPath(k)) + ") END)"
			})
			stages = append(stages, Stage{Kind: "parse-json", Name: field})
			hasExtend = true
		case "parse-regexp":
			// parse-regexp 字段, '正则' as a, b：捕获组依次成为字段，没匹配上的行不增加字段
			field, re, names, ok := splitParseArgs(arg)
			if !ok || len(names) == 0 {
				return &Error{fmt.Sprintf("invalid query: line 1:%d no viable alternative at input '%s'", off+len(cmd.text), cmd.text)}
			}
			if _, err := regexp.Compile(re); err != nil {
				return &Error{"invalid query: invalid regular expression: " + err.Error()}
			}
			src, _ := resolve(field)
			for i, n := range names {
				extended[strings.ToLower(n)] = fmt.Sprintf("(CASE WHEN regexp_matches(%s, %s) THEN regexp_extract(%s, %s, %d) END)", src, sqlString(re), src, sqlString(re), i+1)
			}
			stages = append(stages, Stage{Kind: "parse-regexp", Name: field, Regex: re, Fields: names})
			hasExtend = true
		case "parse-kv":
			// parse-kv 字段, '分隔符', '键值分隔符'，或 parse-kv -regexp 字段, '正则'（捕获组：键、值）
			st := Stage{Kind: "parse-kv"}
			body := arg
			isRegexp := strings.HasPrefix(body, "-regexp ")
			if isRegexp {
				body = strings.TrimSpace(strings.TrimPrefix(body, "-regexp "))
			}
			field, lits, ok := splitArgs(body)
			if !ok || (isRegexp && len(lits) != 1) || (!isRegexp && len(lits) != 2) {
				return &Error{fmt.Sprintf("invalid query: line 1:%d no viable alternative at input '%s'", off+len(cmd.text), cmd.text)}
			}
			st.Name = field
			src, _ := resolve(field)
			if isRegexp {
				if _, err := regexp.Compile(lits[0]); err != nil {
					return &Error{"invalid query: invalid regular expression: " + err.Error()}
				}
				st.Regex = lits[0]
				re := sqlString(lits[0])
				dynamic = append(dynamic, func(k string) string {
					return "regexp_extract_all(" + src + ", " + re + ", 2)[list_position(regexp_extract_all(" + src + ", " + re + ", 1), " + sqlString(k) + ")]"
				})
			} else {
				st.Delim, st.Sep = lits[0], lits[1]
				d, sep := sqlString(lits[0]), sqlString(lits[1])
				dynamic = append(dynamic, func(k string) string {
					prefix := sqlString(k + lits[1])
					_ = sep
					return "substr(list_filter(string_split(" + src + ", " + d + "), x -> starts_with(x, " + prefix + "))[1], " +
						strconv.Itoa(len(k+lits[1])+1) + ")"
				})
			}
			stages = append(stages, st)
			hasExtend = true
		case "stats":
			if m := statsCall.FindStringIndex(cmd.text); m != nil {
				return &Error{fmt.Sprintf("invalid query: line 1:%d missing EQ at '('", off+m[1]-1)}
			}
			return &Error{msgSPLNotTranslatable}
		case "sort", "limit":
			return &Error{msgSPLNotTranslatable}
		case "":
			return &Error{"invalid query: empty pipe"}
		default:
			return fmt.Errorf("SPL %q: %w", name, ErrUnsupported)
		}
		p.SPL = true
	}
	if !hasExtend {
		var rest []Stage
		for _, st := range stages {
			if st.Kind == "where" {
				p.Where.SQL = "(" + p.Where.SQL + " AND " + st.SQL + ")"
				continue
			}
			rest = append(rest, st)
		}
		stages = rest
	}
	p.Stages = stages
	return nil
}

func firstWord(s string) string {
	if f := strings.Fields(s); len(f) > 0 {
		return f[0]
	}
	return "<EOF>"
}

func unquoteIdent(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return s
}

// pipeOffsets 与 pipes 相同，但保留每条命令在 rest 里的位置（命令文本已去掉首尾空白）。
type pipeCmd struct {
	text string
	off  int
}

func pipeOffsets(rest string) []pipeCmd {
	var out []pipeCmd
	var quote bool
	start := -1
	flush := func(end int) {
		seg := rest[start:end]
		trimmed := strings.TrimLeft(seg, " \t\n\r")
		out = append(out, pipeCmd{strings.TrimRight(trimmed, " \t\n\r"), start + len(seg) - len(trimmed)})
	}
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
				flush(i)
			}
			start = i + 1
		}
	}
	if start >= 0 {
		flush(len(rest))
	}
	return out
}

// splitArgs 解析 字段, '字面量', '字面量' …（字段可以加双引号）。
func splitArgs(s string) (field string, lits []string, ok bool) {
	i := strings.IndexByte(s, ',')
	if i < 0 {
		return "", nil, false
	}
	field = unquoteIdent(strings.TrimSpace(s[:i]))
	rest := strings.TrimSpace(s[i+1:])
	for rest != "" {
		lit, n, ok := stringLiteral(rest)
		if !ok {
			return field, lits, len(lits) > 0 && strings.TrimSpace(rest) == ""
		}
		lits = append(lits, lit)
		rest = strings.TrimSpace(rest[n:])
		if strings.HasPrefix(rest, ",") {
			rest = strings.TrimSpace(rest[1:])
		} else {
			break
		}
	}
	return field, lits, rest == "" || strings.HasPrefix(strings.ToLower(rest), "as ")
}

// splitParseArgs 解析 parse-regexp 的参数：字段, '正则' as a, b。
func splitParseArgs(s string) (field, re string, names []string, ok bool) {
	field, lits, ok := splitArgs(s)
	if !ok || len(lits) != 1 {
		return "", "", nil, false
	}
	i := strings.Index(strings.ToLower(s), " as ")
	if i < 0 {
		return field, lits[0], nil, true
	}
	for _, n := range strings.Split(s[i+4:], ",") {
		if n = strings.TrimSpace(n); n != "" {
			names = append(names, unquoteIdent(n))
		}
	}
	return field, lits[0], names, true
}

// stringLiteral 读出开头的单引号字符串（” 表示一个单引号），返回内容与长度。
func stringLiteral(s string) (string, int, bool) {
	if !strings.HasPrefix(s, "'") {
		return "", 0, false
	}
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] == '\'' {
			if i+1 < len(s) && s[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			return b.String(), i + 1, true
		}
		b.WriteByte(s[i])
	}
	return "", 0, false
}
