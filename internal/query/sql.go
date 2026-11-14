package query

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/nethinwei/sls-local/internal/index"
)

// SLS SQL（Presto 方言）到 DuckDB 的改写。不做完整的语法分析，而是逐个记号改写：
//
//   - 字段名（裸标识符或双引号标识符）换成取值表达式，按索引类型转换：long → BIGINT、double → DOUBLE、
//     其余 → VARCHAR；没有开启统计（doc_value）的字段报 SLS 同款错误。
//   - FROM log 指向一个 CTE：只含本 project/logstore、时间范围内且满足检索条件的日志，列名加了
//     __sls_ 前缀，不会与字段或别名冲突。FROM 其他任何表都拒绝（不同 project 共用一张表）。
//   - 别名、函数名、关键字原样保留；Presto 特有的函数换成 DuckDB 的等价写法（funcMap）。
//   - 顶层 select 列表里没有别名的项补上列名（字段名或 _colN），没有 LIMIT 时补 LIMIT 100（SLS 的默认值）。
//
// 语句里的数据库对象只能通过这些改写访问到，DuckDB 本身也关掉了读文件与改配置（store.dsnOptions）。

// SQLStmt 是改写好的 SQL，用在 WITH log AS (...) 之后。
type SQLStmt struct {
	SQL  string
	Args []any
	// InsertAt 是没写 FROM 时 SLS 插入 " from <logstore>" 的位置（只有 antlr 翻译器提供，否则为 0）
	InsertAt int
	// Types 是翻译器推断出的列类型（SLS 的写法），为空的列用 DuckDB 的类型
	Types []string
	// DefaultLimit 表示用了默认的 LIMIT 100
	DefaultLimit bool
	// Shifts 是 compare 用到的时间偏移（秒），见 presto.Stmt.Shifts
	Shifts []int64
	// Processed 是 meta.processedRows 的计算式（为空时取检索条件命中的条数）
	Processed string
	// Text 是 SQL 部分的原文（"|" 之后到下一个 "|"），InsertAt 相对于它
	Text string
	// funcPos 记录每个函数名第一次出现的列号（从 1 起，相对于 "|" 之后的文本），DuckDB 报函数不存在时用它
	// 拼出 SLS 格式的报错。
	funcPos map[string]int
}

// LogstoreNotExistError 是 FROM 引用了 log 之外的表：SLS 把它当成另一个 logstore。
type LogstoreNotExistError struct{ Name string }

func (e *LogstoreNotExistError) Error() string { return "logstore (" + e.Name + ") is not found" }

// SQL 列与 CTE 列的对应。
const (
	colTime     = "__sls_t"
	colNs       = "__sls_ns"
	colTopic    = "__sls_topic"
	colSource   = "__sls_source"
	colTags     = "__sls_tags"
	colContents = "__sls_contents"
)

// CTE 返回 WITH log AS (...) 的定义：base 是 logs 表上的过滤条件（project、时间范围、检索）。
func CTE(base string) string { return "WITH " + CTEDef("log", base) + " " }

// CTEDef 是一个 log 形式的表定义：name AS (...)。compare 的 log__shift_<n> 也用它。
func CTEDef(name, base string) string {
	return name + " AS (SELECT t AS " + colTime + ", ns AS " + colNs + ", topic AS " + colTopic +
		", source AS " + colSource + ", tags AS " + colTags + ", contents AS " + colContents +
		" FROM logs WHERE " + base + ")"
}

// sqlKeywords 是原样保留的 SQL 关键字（不当作字段名）。
var sqlKeywords = toSet(`select from where group by having order limit offset asc desc nulls first last and or not
	is null like ilike in between case when then else end distinct all as on join left right inner outer full cross
	union except intersect true false interval exists over partition rows range unbounded preceding following
	current row filter within escape using natural lateral window qualify fetch next only ties for array
	year month week day hour minute second millisecond quarter`)

// fromFuncs 是参数里可以出现 from 关键字的函数（extract(hour from ts) 等）；其他括号里出现 from
// 只能是 select 子查询，否则拒绝（DuckDB 的 FROM-first 写法 (from logs) 能绕过表名检查）。
var fromFuncs = toSet(`extract substring substr trim overlay position`)

// clauseKeywords 切换当前所在的子句。
var clauseKeywords = toSet(`select from where group having order limit offset window qualify on join union except intersect`)

// funcMap 把 SLS（Presto）函数换成 DuckDB 的写法；sls_ 开头的是 store 在每个连接上建的临时宏。
var funcMap = map[string]string{
	"from_unixtime":       "sls_from_unixtime",
	"to_unixtime":         "sls_to_unixtime",
	"now":                 "sls_now",
	"approx_distinct":     "approx_count_distinct",
	"approx_percentile":   "approx_quantile",
	"regexp_like":         "regexp_matches",
	"json_extract_scalar": "json_extract_string",
	"cardinality":         "len",
	"date_format":         "strftime",
	"date_parse":          "strptime",
}

// constMap 是不带括号的时间常量。
var constMap = map[string]string{
	"current_timestamp": "sls_now()",
	"localtimestamp":    "sls_now()",
	"current_date":      "CAST(sls_now() AS DATE)",
}

func toSet(s string) map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.Fields(s) {
		m[w] = true
	}
	return m
}

var setSession = regexp.MustCompile(`(?is)^\s*set\s+session\s+[^;]*;`)

// TranslateSQL 改写 "|" 之后的 SQL 部分。
func TranslateSQL(src string, ix *index.Index) (*SQLStmt, error) {
	// set session mode=scan; 之类的会话设置在本地没有意义，去掉（列号仍按原文计算）
	offset := 0
	for {
		m := setSession.FindStringIndex(src[offset:])
		if m == nil {
			break
		}
		offset += m[1]
	}
	toks, err := sqlLex(src, offset)
	if err != nil {
		return nil, err
	}
	r := &sqlRewriter{ix: ix, toks: toks, funcPos: map[string]int{}}
	return r.rewrite()
}

// ---------------- 词法 ----------------

type sqlKind int

const (
	sWS     sqlKind = iota
	sIdent          // 裸标识符
	sQIdent         // 双引号标识符（text 为去掉引号后的名字）
	sString         // 单引号字符串（text 含引号）
	sNumber
	sPunct
)

type sqlTok struct {
	kind sqlKind
	text string
	pos  int // 在 "|" 之后文本中的字节位置
}

func (t sqlTok) lower() string { return strings.ToLower(t.text) }

func sqlLex(src string, from int) ([]sqlTok, error) {
	var out []sqlTok
	for i := from; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			j := i
			for j < len(src) && strings.IndexByte(" \t\n\r", src[j]) >= 0 {
				j++
			}
			out = append(out, sqlTok{sWS, src[i:j], i})
			i = j
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
				return nil, sqlError(i, "unterminated string literal")
			}
			out = append(out, sqlTok{sString, src[i : j+1], i})
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
				return nil, sqlError(i, "unterminated quoted identifier")
			}
			out = append(out, sqlTok{sQIdent, name.String(), i})
			i = j + 1
		case isIdentStart(c):
			j := i
			for j < len(src) && isIdentPart(src[j]) {
				j++
			}
			out = append(out, sqlTok{sIdent, src[i:j], i})
			i = j
		case c >= '0' && c <= '9' || (c == '.' && i+1 < len(src) && src[i+1] >= '0' && src[i+1] <= '9'):
			j := i
			for j < len(src) && (src[j] >= '0' && src[j] <= '9' || src[j] == '.' || src[j] == 'e' || src[j] == 'E' ||
				((src[j] == '+' || src[j] == '-') && (src[j-1] == 'e' || src[j-1] == 'E'))) {
				j++
			}
			out = append(out, sqlTok{sNumber, src[i:j], i})
			i = j
		case c == ';':
			return nil, sqlError(i, "multiple statements are not allowed")
		default:
			n := 1
			if i+1 < len(src) {
				switch src[i : i+2] {
				case "<=", ">=", "<>", "!=", "||", "->", "::":
					n = 2
				}
			}
			out = append(out, sqlTok{sPunct, src[i : i+n], i})
			i += n
		}
	}
	return out, nil
}

// sqlError 按 SLS 的格式报 SQL 错误：line 1:列号: 说明。
func sqlError(pos int, format string, a ...any) error {
	return &Error{fmt.Sprintf("line 1:%d: ", pos+1) + fmt.Sprintf(format, a...)}
}

// ---------------- 改写 ----------------

type frame struct {
	clause   string // 当前子句：select、from、where、group、having、order、limit……
	castType bool   // 在 cast(... as TYPE) 的括号里
	fn       string // 函数调用的括号：函数名
	isSelect bool   // 这一层括号里是一个 select（顶层或子查询）
	items    []item // select 列表的各项（下标区间），isSelect 时记录
	itemFrom int
	hasFrom  bool
	hasLimit bool
	selEnd   int // select 列表结束处（下一个子句关键字）的记号下标
}

type item struct{ from, to int } // 记号下标 [from, to)

type sqlRewriter struct {
	ix      *index.Index
	toks    []sqlTok
	out     []string // 与 toks 一一对应的改写结果
	args    [][]any  // 与 toks 一一对应的参数
	aliases map[string]bool
	funcPos map[string]int
}

func (r *sqlRewriter) next(i int) int {
	for j := i + 1; j < len(r.toks); j++ {
		if r.toks[j].kind != sWS {
			return j
		}
	}
	return -1
}

func (r *sqlRewriter) prev(i int) int {
	for j := i - 1; j >= 0; j-- {
		if r.toks[j].kind != sWS {
			return j
		}
	}
	return -1
}

func (r *sqlRewriter) is(i int, kind sqlKind, text string) bool {
	return i >= 0 && r.toks[i].kind == kind && (text == "" || strings.EqualFold(r.toks[i].text, text))
}

func (r *sqlRewriter) rewrite() (*SQLStmt, error) {
	n := len(r.toks)
	r.out = make([]string, n)
	r.args = make([][]any, n)
	// 预扫一遍别名：as 之后的标识符（cast 里的类型名除外，在改写时区分）
	r.aliases = map[string]bool{}
	for i, t := range r.toks {
		if t.kind == sIdent && t.lower() == "as" {
			if j := r.next(i); j >= 0 && (r.toks[j].kind == sIdent || r.toks[j].kind == sQIdent) {
				r.aliases[strings.ToLower(r.toks[j].text)] = true
			}
		}
	}
	// 只能是一条 select（分号在词法阶段就拒绝了）
	if first := r.next(-1); first < 0 || !r.is(first, sIdent, "select") {
		pos := 0
		if first >= 0 {
			pos = r.toks[first].pos
		}
		return nil, sqlError(pos, "only select statements are allowed after |")
	}

	stack := []*frame{{}}
	var formatArg []int // strftime/strptime 的括号深度：其中第二个参数是格式串
	for i := 0; i < n; i++ {
		t := r.toks[i]
		top := stack[len(stack)-1]
		r.out[i] = t.text
		switch t.kind {
		case sWS, sNumber:
		case sString:
			if len(formatArg) > 0 && formatArg[len(formatArg)-1] == len(stack) {
				if p := r.prev(i); r.is(p, sPunct, ",") {
					r.out[i] = translateDateFormat(t.text)
				}
			}
		case sPunct:
			switch t.text {
			case "(":
				f := &frame{clause: top.clause}
				if p := r.prev(i); p >= 0 && r.toks[p].kind == sIdent {
					f.fn = r.toks[p].lower()
					f.castType = f.fn == "cast" || f.fn == "try_cast"
				}
				stack = append(stack, f)
			case ")":
				if len(stack) == 1 {
					return nil, sqlError(t.pos, "mismatched input ')'")
				}
				r.finishSelect(top, i)
				if len(formatArg) > 0 && formatArg[len(formatArg)-1] == len(stack) {
					formatArg = formatArg[:len(formatArg)-1]
				}
				stack = stack[:len(stack)-1]
			case ",":
				if top.isSelect && top.clause == "select" {
					top.items = append(top.items, item{top.itemFrom, i})
					top.itemFrom = i + 1
				}
			case "*":
				// select * 会带出内部列：只允许出现在函数参数里（count(*)）
				if top.clause == "select" && top.isSelect {
					return nil, sqlError(t.pos, "select * is not supported by sls-local, list the columns")
				}
			case ".":
				return nil, sqlError(t.pos, "qualified names are not supported by sls-local")
			}
		case sIdent, sQIdent:
			lower := t.lower()
			if t.kind == sIdent && clauseKeywords[lower] {
				if lower == "from" && !top.isSelect {
					if !fromFuncs[top.fn] {
						return nil, sqlError(t.pos, "mismatched input 'from'")
					}
					continue // extract(hour from ts)
				}
				if lower != "select" && !top.isSelect {
					continue // 函数括号里的 order by（窗口、聚合）等，不是 select 的子句
				}
				r.enterClause(top, lower, i)
				continue
			}
			// FROM/JOIN/逗号之后是表名：只能是 log。表函数（query()、range()……）与恰好是关键字的名字
			// 也在这里拒绝，所以这个检查要在关键字判断之前
			if top.clause == "from" {
				if p := r.prev(i); r.is(p, sIdent, "from") || r.is(p, sIdent, "join") || r.is(p, sPunct, ",") {
					if !strings.EqualFold(t.text, "log") || r.is(r.next(i), sPunct, "(") {
						return nil, &LogstoreNotExistError{t.text}
					}
					r.out[i] = "log"
					continue
				}
			}
			if t.kind == sIdent && (sqlKeywords[lower] || lower == "cast" || lower == "try_cast") {
				continue
			}
			if t.kind == sIdent {
				if c, ok := constMap[lower]; ok {
					r.out[i] = c
					continue
				}
				if j := r.next(i); r.is(j, sPunct, "(") { // 函数调用
					if _, seen := r.funcPos[lower]; !seen {
						r.funcPos[lower] = t.pos + 1
					}
					if m, ok := funcMap[lower]; ok {
						r.out[i] = m
						if m == "strftime" || m == "strptime" {
							formatArg = append(formatArg, len(stack)+1)
						}
					}
					continue
				}
			}
			p := r.prev(i)
			switch {
			case r.is(p, sIdent, "as") && top.castType: // cast(x as TYPE) 的类型名
				continue
			case r.is(p, sIdent, "as"): // 别名定义
				r.out[i] = quoteIdent(t.text)
				continue
			case top.clause == "from":
				// 表名之后的表别名：原样保留（表名本身在 checkTable 里检查过）
				r.out[i] = quoteIdent(t.text)
				continue
			case r.aliases[lower] && (top.clause == "group" || top.clause == "order" || top.clause == "having"):
				r.out[i] = quoteIdent(t.text)
				continue
			}
			expr, args, err := r.field(t)
			if err != nil {
				if r.aliases[lower] {
					r.out[i] = quoteIdent(t.text)
					continue
				}
				return nil, err
			}
			r.out[i], r.args[i] = expr, args
		}
	}
	if len(stack) != 1 {
		return nil, sqlError(r.toks[n-1].pos, "missing ')'")
	}
	r.finishSelect(stack[0], n)

	var b strings.Builder
	var args []any
	for i := range r.toks {
		b.WriteString(r.out[i])
		args = append(args, r.args[i]...)
	}
	if !stack[0].hasLimit {
		b.WriteString(" LIMIT 100") // SLS 的 SQL 不写 LIMIT 时返回 100 行
	}
	return &SQLStmt{SQL: b.String(), Args: args, funcPos: r.funcPos}, nil
}

// enterClause 切换子句；select 开始时记录 select 列表。
func (r *sqlRewriter) enterClause(f *frame, kw string, i int) {
	switch kw {
	case "select":
		f.isSelect, f.items, f.itemFrom, f.selEnd = true, nil, i+1, -1
	case "from":
		f.hasFrom = true
	case "limit":
		f.hasLimit = true
	case "join":
		kw = "from"
	}
	if f.clause == "select" && kw != "select" && f.selEnd < 0 {
		f.items = append(f.items, item{f.itemFrom, i})
		f.selEnd = i
	}
	f.clause = kw
}

// finishSelect 在一层 select 结束时（")" 或语句末尾）补列名与 FROM log。end 是结束处的记号下标。
func (r *sqlRewriter) finishSelect(f *frame, end int) {
	if !f.isSelect {
		return
	}
	if f.selEnd < 0 {
		f.items = append(f.items, item{f.itemFrom, end})
		f.selEnd = end
	}
	if len(f.items) == 0 {
		return
	}
	// 只有顶层 select 的列名会返回给客户端；子查询里也补上，方便外层按名字引用
	for k, it := range f.items {
		last := -1
		var names []int
		for j := it.from; j < it.to; j++ {
			if r.toks[j].kind != sWS {
				names = append(names, j)
				last = j
			}
		}
		if last < 0 {
			continue
		}
		// 已有别名：最后一个记号之前是 as
		if len(names) >= 2 && r.is(names[len(names)-2], sIdent, "as") {
			continue
		}
		name := fmt.Sprintf("_col%d", k)
		if len(names) == 1 && (r.toks[last].kind == sIdent || r.toks[last].kind == sQIdent) {
			name = r.toks[last].text
		}
		r.out[last] += " AS " + quoteIdent(name)
	}
	if !f.hasFrom {
		last := f.items[len(f.items)-1]
		k := last.to - 1
		for k > last.from && r.toks[k].kind == sWS {
			k--
		}
		r.out[k] += " FROM log"
		f.hasFrom = true
	}
}

// field 把字段引用改写成 CTE 上的取值表达式，按索引类型转换。
func (r *sqlRewriter) field(t sqlTok) (string, []any, error) {
	name := t.text
	switch strings.ToLower(name) {
	case "__time__":
		return colTime, nil, nil
	case "__time_ns_part__":
		return "coalesce(" + colNs + ", 0)", nil, nil
	case "__topic__":
		return "NULLIF(" + colTopic + ", '')", nil, nil // SLS 的 SQL 里空 topic 是 null
	case "__source__":
		return colSource, nil, nil
	}
	f, path, ok := r.ix.Lookup(name)
	if !ok || !f.DocValue {
		if t.kind == sQIdent && strings.HasPrefix(name, "__tag__:") {
			return "", nil, sqlError(t.pos, "Column '%s' cannot be resolved; it seems %s is wrapped by \"; if %s is a string rather than a column, please use '%s'", name, name, name, name)
		}
		return "", nil, sqlError(t.pos, "Column '%s' cannot be resolved; please add the column in the index attribute", name)
	}
	col := colContents
	if strings.HasPrefix(f.Name, "__tag__:") {
		col = colTags
	}
	// JSON 路径内联成字符串字面量而不是参数：同一字段在 select 与 group by 里必须写成同一个表达式，
	// DuckDB 会把参数不同（$1、$3）的两处当成不同的表达式
	expr := "json_extract_string(" + col + ", " + sqlString(jsonPath(f.Name)) + ")"
	for _, p := range path {
		expr = "json_extract_string(" + expr + ", " + sqlString(jsonPath(p)) + ")"
	}
	var args []any
	switch f.Type {
	case "long":
		expr = "TRY_CAST(" + expr + " AS BIGINT)"
	case "double":
		expr = "TRY_CAST(" + expr + " AS DOUBLE)"
	}
	return expr, args, nil
}

// sqlString 把 s 写成 SQL 字符串字面量。
func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

var duckFuncMissing = regexp.MustCompile(`Function with name "?([A-Za-z0-9_]+)"? does not exist`)

// FuncNotRegistered 把 DuckDB 的「函数不存在」改写成 SLS 的报错；不是这类错误时返回 nil。
func (s *SQLStmt) FuncNotRegistered(duckErr error) error {
	m := duckFuncMissing.FindStringSubmatch(duckErr.Error())
	if m == nil {
		return nil
	}
	name := strings.ToLower(m[1])
	col, ok := s.funcPos[name]
	if !ok { // 改写后的名字（例如 sls_now），按原名找
		for orig, mapped := range funcMap {
			if mapped == name {
				col, ok = s.funcPos[orig]
				name = orig
				break
			}
		}
	}
	if !ok {
		col = 1
	}
	return &Error{fmt.Sprintf("ErrorType:SyntaxError.ErrorPosition,line:1,column:%d.ErrorMessage:line 1:%d: Function %s not registered", col, col, name)}
}

// translateDateFormat 把 MySQL/Presto 风格的格式串（'%Y-%m-%d %H:%i:%s'）换成 DuckDB strftime 的写法。
func translateDateFormat(lit string) string {
	repl := map[byte]string{
		'Y': "%Y", 'y': "%y", 'm': "%m", 'c': "%-m", 'd': "%d", 'e': "%-d", 'H': "%H", 'k': "%-H", 'h': "%I",
		'I': "%I", 'l': "%-I", 'i': "%M", 's': "%S", 'S': "%S", 'f': "%f", 'p': "%p", 'T': "%H:%M:%S",
		'r': "%I:%M:%S %p", 'j': "%j", 'a': "%a", 'W': "%A", 'b': "%b", 'M': "%B", 'v': "%V", 'x': "%G", '%': "%%",
	}
	var b strings.Builder
	for i := 0; i < len(lit); i++ {
		if lit[i] == '%' && i+1 < len(lit) {
			if s, ok := repl[lit[i+1]]; ok {
				b.WriteString(s)
				i++
				continue
			}
		}
		b.WriteByte(lit[i])
	}
	return b.String()
}
