package presto

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nethinwei/sls-local/internal/presto/parser"
)

// fcall 是一次函数调用的参数（已翻译）。
type fcall struct {
	name     string
	args     []val
	star     bool   // count(*)
	distinct bool   // f(DISTINCT x)
	order    string // f(x ORDER BY ...) 里 ORDER BY 之后的部分
}

// fn 生成一个函数调用。返回的类型为空时不确定。
type fn func(c *fcall) (val, error)

// argsSQL 拼接参数列表（含 DISTINCT 与 ORDER BY）。
func (c *fcall) argsSQL() string {
	var s []string
	for _, a := range c.args {
		s = append(s, a.sql)
	}
	out := strings.Join(s, ", ")
	if c.star {
		out = "*"
	}
	if c.distinct {
		out = "DISTINCT " + out
	}
	if c.order != "" {
		out += " ORDER BY " + c.order
	}
	return out
}

func (c *fcall) argType(i int) string {
	if i < len(c.args) {
		return c.args[i].typ
	}
	return ""
}

// rename 换一个 DuckDB 函数名；typ 为 "$0" 时返回第一个参数的类型。
func rename(duck, typ string) fn {
	return func(c *fcall) (val, error) {
		t := typ
		if t == "$0" {
			t = c.argType(0)
		}
		return val{duck + "(" + c.argsSQL() + ")", t}, nil
	}
}

// tmpl 用模板生成：$1、$2… 是参数。
func tmpl(t, typ string) fn {
	return func(c *fcall) (val, error) {
		s := t
		for i := len(c.args); i >= 1; i-- {
			s = strings.ReplaceAll(s, "$"+string(rune('0'+i)), c.args[i-1].sql)
		}
		if typ == "$0" {
			return val{s, c.argType(0)}, nil
		}
		return val{s, typ}, nil
	}
}

// timePart 是 year(ts) 之类：参数可以是时间戳，也可以是 Unix 秒。
func timePart(part string) fn {
	return func(c *fcall) (val, error) {
		return val{"extract(" + part + " FROM " + tsArg(c.args[0]) + ")", "bigint"}, nil
	}
}

// aggregates 是聚合函数：可以带 DISTINCT、FILTER、ORDER BY，嵌套时 SLS 报错。
var aggregates = map[string]bool{}

func agg(f fn, names ...string) {
	for _, n := range names {
		aggregates[n] = true
		funcs[n] = f
	}
}

var funcs = map[string]fn{
	// 数学
	"abs": rename("abs", "$0"), "ceil": rename("ceil", "$0"), "ceiling": rename("ceil", "$0"),
	"floor": rename("floor", "$0"), "round": rename("round", "$0"), "truncate": rename("trunc", "$0"),
	"sqrt": rename("sqrt", "double"), "cbrt": rename("cbrt", "double"), "exp": rename("exp", "double"),
	"ln": rename("ln", "double"), "log2": rename("log2", "double"), "log10": rename("log10", "double"),
	"power": rename("power", "double"), "pow": rename("power", "double"), "sign": rename("sign", "$0"),
	"mod": tmpl("($1 % $2)", "$0"), "pi": rename("pi", "double"), "e": tmpl("exp(1)", "double"),
	"random": rename("random", "double"), "rand": rename("random", "double"),
	"degrees": rename("degrees", "double"), "radians": rename("radians", "double"),
	"sin": rename("sin", "double"), "cos": rename("cos", "double"), "tan": rename("tan", "double"),
	"asin": rename("asin", "double"), "acos": rename("acos", "double"), "atan": rename("atan", "double"),
	"atan2": rename("atan2", "double"), "greatest": rename("greatest", "$0"), "least": rename("least", "$0"),
	"is_nan": rename("isnan", "boolean"), "is_finite": rename("isfinite", "boolean"), "is_infinite": rename("isinf", "boolean"),
	"nan": tmpl("CAST('nan' AS DOUBLE)", "double"), "infinity": tmpl("CAST('inf' AS DOUBLE)", "double"),

	// 字符串
	"lower": rename("lower", "varchar"), "upper": rename("upper", "varchar"), "length": rename("length", "bigint"),
	"substr": rename("substring", "varchar"), "substring": rename("substring", "varchar"), "concat": rename("concat", "varchar"),
	"reverse": rename("reverse", "varchar"), "trim": rename("trim", "varchar"), "ltrim": rename("ltrim", "varchar"),
	"rtrim": rename("rtrim", "varchar"), "lpad": rename("lpad", "varchar"), "rpad": rename("rpad", "varchar"),
	"strpos": rename("strpos", "bigint"), "split_part": rename("split_part", "varchar"), "split": rename("string_split", "array"),
	"starts_with": rename("starts_with", "boolean"), "chr": rename("chr", "varchar"), "codepoint": rename("unicode", "integer"),
	"levenshtein_distance": rename("levenshtein", "bigint"), "hamming_distance": rename("hamming", "bigint"),
	"regexp_like": rename("regexp_matches", "boolean"), "regexp_extract_all": rename("regexp_extract_all", "array"),
	"regexp_split": rename("regexp_split_to_array", "array"),
	"to_utf8":      rename("encode", "varbinary"), "from_utf8": rename("decode", "varchar"),
	"to_base64": rename("to_base64", "varchar"), "from_base64": rename("from_base64", "varbinary"),
	"to_hex": rename("hex", "varchar"), "md5": tmpl("unhex(md5($1))", "varbinary"),
	"url_encode": tmpl("replace(replace(url_encode($1), '%20', '+'), '~', '%7E')", "varchar"), "url_decode": rename("url_decode", "varchar"),

	// JSON
	"json_extract_scalar": jsonFn("json_extract_string", "varchar"), "json_extract": jsonFn("json_extract", "json"),
	"json_array_length": jsonFn("json_array_length", "bigint"), "json_size": jsonFn("json_array_length", "bigint"),
	"json_parse": tmpl("CAST($1 AS JSON)", "json"), "json_format": tmpl("CAST($1 AS VARCHAR)", "varchar"),

	// 数组与 map
	"cardinality": rename("len", "bigint"), "contains": rename("list_contains", "boolean"),
	"array_join": rename("array_to_string", "varchar"), "array_distinct": rename("list_distinct", "array"),
	"array_max": rename("list_max", ""), "array_min": rename("list_min", ""), "array_sort": rename("list_sort", "array"),
	"array_position": rename("list_position", "bigint"), "slice": rename("list_slice", "array"),
	"sequence": rename("generate_series", "array"), "filter": rename("list_filter", "array"),
	"transform": rename("list_transform", "array"), "map_keys": rename("map_keys", "array"), "map_values": rename("map_values", "array"),
	"map": rename("map", "map"),

	// 条件
	"coalesce": rename("coalesce", "$0"), "nullif": rename("nullif", "$0"),

	// 时间（时间戳是 -sql-utc-offset 时区的本地时间，不带时区；store 在每个连接上建了 sls_ 开头的宏）
	"from_unixtime": tmpl("sls_from_unixtime($1)", "timestamp"), "to_unixtime": rename("sls_to_unixtime", "double"),
	"now":  tmpl("sls_now()", "timestamp"),
	"year": timePart("year"), "month": timePart("month"), "day": timePart("day"), "day_of_month": timePart("day"),
	"hour": timePart("hour"), "minute": timePart("minute"), "second": timePart("second"), "millisecond": timePart("millisecond"),
	"day_of_week": timePart("isodow"), "dow": timePart("isodow"), "day_of_year": timePart("doy"), "doy": timePart("doy"),
	"week": timePart("week"), "week_of_year": timePart("week"), "quarter": timePart("quarter"), "year_of_week": timePart("isoyear"),
	"yow": timePart("isoyear"),
}

// lenRule 按 Presto 函数签名里的长度公式推出结果类型 varchar(n)（参数不是 varchar(n) 时就是 varchar）。
func lenRule(f fn, rule func(x, y, z int) int) fn {
	return func(c *fcall) (val, error) {
		v, err := f(c)
		if err != nil {
			return v, err
		}
		ln := func(i int) (int, bool) { return varcharLen(c.argType(i)) }
		x, ok := ln(0)
		if !ok {
			if strings.HasPrefix(v.typ, "array") {
				v.typ = "array(varchar)"
			} else {
				v.typ = "varchar"
			}
			return v, nil
		}
		y, _ := ln(1)
		z, _ := ln(2)
		n := min(rule(x, y, z), 2147483647)
		if strings.HasPrefix(v.typ, "array") {
			v.typ = fmt.Sprintf("array(varchar(%d))", n)
		} else {
			v.typ = fmt.Sprintf("varchar(%d)", n)
		}
		return v, nil
	}
}

func sameLen(x, _, _ int) int { return x }

// jsonArg：字段值不一定是合法的 JSON，SLS 的 JSON 函数遇到非法 JSON 返回 NULL（DuckDB 会报错）。
func jsonArg(v val) string { return "(CASE WHEN json_valid(" + v.sql + ") THEN " + v.sql + " END)" }

func jsonFn(duck, typ string) fn {
	return func(c *fcall) (val, error) {
		args := []string{jsonArg(c.args[0])}
		for _, a := range c.args[1:] {
			args = append(args, a.sql)
		}
		return val{duck + "(" + strings.Join(args, ", ") + ")", typ}, nil
	}
}

// windowFuncs 是窗口函数（只能带 OVER 使用）。
var windowFuncs = map[string]string{
	"row_number": "bigint", "rank": "bigint", "dense_rank": "bigint", "percent_rank": "double", "cume_dist": "double",
	"ntile": "bigint", "lag": "$0", "lead": "$0", "first_value": "$0", "last_value": "$0", "nth_value": "$0",
}

func init() {
	for n, typ := range windowFuncs {
		funcs[n] = rename(n, typ)
	}
	agg(rename("count", "bigint"), "count")
	agg(rename("count_if", "bigint"), "count_if")
	agg(func(c *fcall) (val, error) {
		t := c.argType(0)
		if t == "integer" {
			t = "bigint"
		}
		return val{"sum(" + c.argsSQL() + ")", t}, nil
	}, "sum")
	agg(rename("avg", "double"), "avg")
	agg(rename("min", "$0"), "min")
	agg(rename("max", "$0"), "max")
	agg(rename("arbitrary", "$0"), "arbitrary", "any_value")
	agg(rename("max_by", "$0"), "max_by")
	agg(rename("min_by", "$0"), "min_by")
	agg(rename("bool_and", "boolean"), "bool_and", "every")
	agg(rename("bool_or", "boolean"), "bool_or")
	for _, n := range []string{"stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop"} {
		agg(rename(n, "double"), n)
	}
	// 两个参数都是整数时 Presto 选的是 (real, real) 的重载，结果是 real
	for _, n := range []string{"corr", "covar_pop", "covar_samp", "regr_slope", "regr_intercept"} {
		agg(func(c *fcall) (val, error) {
			if c.args[0].integral() && c.args[1].integral() {
				return val{"CAST(" + c.name + "(" + c.argsSQL() + ") AS FLOAT)", "real"}, nil
			}
			return val{c.name + "(" + c.argsSQL() + ")", "double"}, nil
		}, n)
	}
	// Presto：exp(avg(ln(x)))，ln(负数) 是 NaN、ln(0) 是 -Infinity（DuckDB 会报错）
	agg(tmpl("exp(avg(CASE WHEN $1 > 0 THEN ln($1) WHEN $1 = 0 THEN CAST('-inf' AS DOUBLE) WHEN $1 < 0 THEN CAST('nan' AS DOUBLE) END))", "double"), "geometric_mean")
	// SLS 用 HyperLogLog 估算，误差约 2%；DuckDB 的 approx_count_distinct 误差大得多，这里用精确值
	agg(func(c *fcall) (val, error) { return val{"count(DISTINCT " + c.args[0].sql + ")", "bigint"}, nil }, "approx_distinct")
	agg(func(c *fcall) (val, error) {
		// Presto 返回数据里实际出现的值（离散分位数），类型与参数相同
		return val{"quantile_disc(" + c.args[0].sql + ", " + c.args[1].sql + ")", c.argType(0)}, nil
	}, "approx_percentile")
	agg(rename("histogram", "map"), "histogram")
	agg(rename("array_agg", "array"), "array_agg")
	agg(tmpl("map(list($1), list($2))", "map"), "map_agg")

	funcs["if"] = func(c *fcall) (val, error) {
		if len(c.args) == 2 {
			return val{"(CASE WHEN " + c.args[0].sql + " THEN " + c.args[1].sql + " END)", c.argType(1)}, nil
		}
		return val{"(CASE WHEN " + c.args[0].sql + " THEN " + c.args[1].sql + " ELSE " + c.args[2].sql + " END)", c.argType(1)}, nil
	}
	funcs["try"] = func(c *fcall) (val, error) { return c.args[0], nil }
	funcs["typeof"] = func(c *fcall) (val, error) { return val{"lower(typeof(" + c.args[0].sql + "))", "varchar"}, nil }
	funcs["replace"] = func(c *fcall) (val, error) {
		if len(c.args) == 2 {
			return val{"replace(" + c.args[0].sql + ", " + c.args[1].sql + ", '')", "varchar"}, nil
		}
		return rename("replace", "varchar")(c)
	}
	// Presto 没有匹配时返回 NULL，DuckDB 返回空串
	funcs["regexp_extract"] = func(c *fcall) (val, error) {
		return val{"(CASE WHEN regexp_matches(" + c.args[0].sql + ", " + c.args[1].sql + ") THEN regexp_extract(" + c.argsSQL() + ") END)", "varchar"}, nil
	}
	// Presto 替换全部匹配，分组引用写作 $1
	funcs["regexp_replace"] = func(c *fcall) (val, error) {
		repl := "''"
		if len(c.args) > 2 {
			repl = "regexp_replace(" + c.args[2].sql + ", '\\$(\\d)', '\\\\\\1', 'g')"
		}
		return val{"regexp_replace(" + c.args[0].sql + ", " + c.args[1].sql + ", " + repl + ", 'g')", "varchar"}, nil
	}
	funcs["element_at"] = func(c *fcall) (val, error) {
		if c.argType(0) == "map" {
			return val{"map_extract(" + c.args[0].sql + ", " + c.args[1].sql + ")[1]", ""}, nil
		}
		return val{"list_extract(" + c.args[0].sql + ", " + c.args[1].sql + ")", ""}, nil
	}
	funcs["date_format"] = func(c *fcall) (val, error) {
		return val{"strftime(" + tsArg(c.args[0]) + ", " + dateFormat(c.args[1].sql) + ")", "varchar"}, nil
	}
	funcs["date_parse"] = func(c *fcall) (val, error) {
		return val{"strptime(" + c.args[0].sql + ", " + dateFormat(c.args[1].sql) + ")", "timestamp"}, nil
	}
	funcs["date_trunc"] = func(c *fcall) (val, error) {
		return val{"date_trunc(" + c.args[0].sql + ", " + tsArg(c.args[1]) + ")", "timestamp"}, nil
	}
	funcs["date_diff"] = func(c *fcall) (val, error) {
		return val{"date_diff(" + c.args[0].sql + ", " + tsArg(c.args[1]) + ", " + tsArg(c.args[2]) + ")", "bigint"}, nil
	}
	// Presto：date_add(unit, n, ts)
	funcs["date_add"] = func(c *fcall) (val, error) {
		n := "CAST(" + c.args[1].sql + " AS BIGINT)"
		var iv string
		switch u := strings.ToLower(strings.Trim(c.args[0].sql, "'")); u {
		case "millisecond", "second", "minute", "hour", "day", "week", "month", "year":
			iv = "to_" + u + "s(" + n + ")"
		case "quarter":
			iv = "to_months(3 * " + n + ")"
		default:
			return val{}, &Error{"Unsupported date_add unit: " + u}
		}
		return val{"(" + tsArg(c.args[2]) + " + " + iv + ")", "timestamp"}, nil
	}
	// time_series(time, window, format, padding)：按窗口对齐时间并格式化。SLS 还会用 padding 补齐查询时间范围内
	// 没有数据的桶，这里没有实现补齐
	funcs["time_series"] = func(c *fcall) (val, error) {
		if len(c.args) != 4 {
			return val{}, &Error{"time_series needs 4 arguments"}
		}
		w, err := windowSeconds(c.args[1].sql)
		if err != nil {
			return val{}, err
		}
		t := c.args[0].sql
		bucket := "sls_from_unixtime(" + t + " - " + t + " % " + strconv.FormatInt(w, 10) + ")"
		if w%86400 == 0 { // 按天及以上对齐到本地时间的零点
			bucket = "date_trunc('day', sls_from_unixtime(" + t + "))"
		}
		return val{"strftime(" + bucket + ", " + dateFormat(c.args[2].sql) + ")", "varchar"}, nil
	}
	funcs["url_extract_host"] = tmpl(`nullif(regexp_extract($1, '^[A-Za-z][A-Za-z0-9+.-]*://(?:[^@/?#]*@)?([^:/?#]*)', 1), '')`, "varchar")
	funcs["url_extract_path"] = tmpl(`regexp_extract($1, '^(?:[A-Za-z][A-Za-z0-9+.-]*://[^/?#]*)?([^?#]*)', 1)`, "varchar")
	funcs["url_extract_protocol"] = tmpl(`nullif(regexp_extract($1, '^([A-Za-z][A-Za-z0-9+.-]*)://', 1), '')`, "varchar")
	funcs["url_extract_query"] = tmpl(`nullif(regexp_extract($1, '\?([^#]*)', 1), '')`, "varchar")
	funcs["url_extract_fragment"] = tmpl(`nullif(regexp_extract($1, '#(.*)$', 1), '')`, "varchar")
	funcs["url_extract_port"] = tmpl(`TRY_CAST(nullif(regexp_extract($1, '^[A-Za-z][A-Za-z0-9+.-]*://(?:[^@/?#]*@)?[^:/?#]*:([0-9]+)', 1), '') AS BIGINT)`, "bigint")
	funcs["url_extract_parameter"] = tmpl(`url_decode(nullif(regexp_extract($1, '[?&]' || $2 || '=([^&#]*)', 1), ''))`, "varchar")
}

// call 翻译函数调用。没登记的函数按 SLS 的格式报错。
func (t *tr) call(e *parser.FunctionCallContext, sc *scope, m mode) (val, error) {
	nameCtx := e.QualifiedName()
	name := strings.ToLower(nameCtx.GetText())
	f, ok := funcs[name]
	if !ok {
		return val{}, t.semErrorf(nameCtx.GetStart(), "Function %s not registered", name)
	}
	c := &fcall{name: name, star: e.ASTERISK() != nil}
	if sq := e.SetQuantifier(); sq != nil {
		c.distinct = strings.EqualFold(sq.GetText(), "distinct")
	}
	for _, a := range e.AllExpression() {
		v, err := t.expr(a, sc, m)
		if err != nil {
			return val{}, err
		}
		c.args = append(c.args, v)
	}
	if items := e.AllSortItem(); len(items) > 0 {
		o, err := t.sortItems(items, sc, false)
		if err != nil {
			return val{}, err
		}
		c.order = o
	}
	v, err := f(c)
	if err != nil {
		return val{}, err
	}
	if flt := e.Filter(); flt != nil {
		w, err := t.boolExpr(flt.BooleanExpression(), sc, m)
		if err != nil {
			return val{}, err
		}
		v.sql += " FILTER (WHERE " + w.sql + ")"
	}
	if over := e.Over(); over != nil {
		s, err := t.over(over.(*parser.OverContext), sc, m)
		if err != nil {
			return val{}, err
		}
		v.sql += " OVER (" + s + ")"
	}
	return v, nil
}

func (t *tr) over(o *parser.OverContext, sc *scope, m mode) (string, error) {
	var parts []string
	if ps := o.GetPartition(); len(ps) > 0 {
		var exprs []string
		for _, p := range ps {
			v, err := t.expr(p, sc, m)
			if err != nil {
				return "", err
			}
			exprs = append(exprs, v.sql)
		}
		parts = append(parts, "PARTITION BY "+strings.Join(exprs, ", "))
	}
	if items := o.AllSortItem(); len(items) > 0 {
		s, err := t.sortItems(items, sc, false)
		if err != nil {
			return "", err
		}
		parts = append(parts, "ORDER BY "+s)
	}
	if wf := o.WindowFrame(); wf != nil {
		s, err := t.windowFrame(wf.(*parser.WindowFrameContext), sc, m)
		if err != nil {
			return "", err
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, " "), nil
}

// windowFrame 逐项翻译窗口边界（边界里的表达式同样要经过翻译，不能把原文拼进 SQL）。
func (t *tr) windowFrame(wf *parser.WindowFrameContext, sc *scope, m mode) (string, error) {
	typ := strings.ToUpper(wf.GetFrameType().GetText())
	if typ == "GROUPS" {
		return "", syntaxErrorf(wf.GetFrameType(), "GROUPS frames are %v", ErrUnsupported)
	}
	bounds := wf.AllFrameBound()
	var out []string
	for _, b := range bounds {
		switch b := b.(type) {
		case *parser.UnboundedFrameContext:
			out = append(out, "UNBOUNDED "+strings.ToUpper(b.GetBoundType().GetText()))
		case *parser.CurrentRowBoundContext:
			out = append(out, "CURRENT ROW")
		case *parser.BoundedFrameContext:
			v, err := t.expr(b.Expression(), sc, m)
			if err != nil {
				return "", err
			}
			out = append(out, v.sql+" "+strings.ToUpper(b.GetBoundType().GetText()))
		default:
			return "", unsupported(b)
		}
	}
	if len(out) == 2 {
		return typ + " BETWEEN " + out[0] + " AND " + out[1], nil
	}
	return typ + " " + out[0], nil
}

// dateFormat 把 MySQL 风格的格式串字面量（'%Y-%m-%d %H:%i:%s'）换成 DuckDB strftime 的写法；
// 不是字面量时原样返回。
func dateFormat(lit string) string {
	if len(lit) < 2 || lit[0] != '\'' {
		return lit
	}
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

// 结果长度规则包装在所有函数登记之后（同一文件里的 init 按出现顺序执行）。
func init() {
	for _, n := range []string{"lower", "upper", "reverse", "trim", "ltrim", "rtrim", "substr", "substring",
		"regexp_extract", "split_part", "json_extract_scalar", "url_extract_host", "url_extract_path",
		"url_extract_parameter", "url_extract_query", "url_extract_fragment", "url_extract_protocol", "url_decode",
		"split", "regexp_extract_all"} {
		funcs[n] = lenRule(funcs[n], sameLen)
	}
	funcs["replace"] = lenRule(funcs["replace"], func(x, _, z int) int { return x + z*(x+1) })
	funcs["regexp_replace"] = lenRule(funcs["regexp_replace"], func(x, y, _ int) int { return x + x*y/2*(x+1) })
	funcs["url_encode"] = lenRule(funcs["url_encode"], func(x, _, _ int) int { return 12 * x })
}

// windowSeconds 解析 time_series 的窗口：'30s'、'5m'、'1h'、'1d'。
func windowSeconds(lit string) (int64, error) {
	s := strings.Trim(lit, "'")
	if len(s) < 2 {
		return 0, &Error{"invalid time_series window " + lit}
	}
	n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
	if err != nil || n <= 0 {
		return 0, &Error{"invalid time_series window " + lit}
	}
	unit := map[byte]int64{'s': 1, 'm': 60, 'h': 3600, 'd': 86400}[s[len(s)-1]]
	if unit == 0 {
		return 0, &Error{"invalid time_series window " + lit}
	}
	return n * unit, nil
}
