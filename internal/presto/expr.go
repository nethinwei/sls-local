package presto

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/antlr4-go/antlr/v4"

	"github.com/nethinwei/sls-local/internal/presto/parser"
)

// val 是翻译好的表达式与它的 Presto 类型（尽力推断，不确定时为空）。类型用于需要按参数类型选择写法的
// 函数（date_format(__time__, ...) 等）、整数除以零的报错。
type val struct {
	sql string
	typ string // bigint、integer、double、varchar、boolean、timestamp、date、interval、array、map、json、unknown
}

func (v val) numeric() bool {
	return v.typ == "bigint" || v.typ == "integer" || v.typ == "double"
}

func (v val) integral() bool { return v.typ == "bigint" || v.typ == "integer" }

// mode 控制名字解析：outputs 表示可以引用 select 列表的输出名（group by、having、order by），
// outputsFirst 表示输出名优先于字段（order by，与 Presto 一致）。
type mode struct {
	outputs      bool
	outputsFirst bool
	lambda       map[string]bool // lambda 参数
}

func (t *tr) expr(e parser.IExpressionContext, sc *scope, m mode) (val, error) {
	return t.boolExpr(e.BooleanExpression(), sc, m)
}

func (t *tr) boolExpr(e parser.IBooleanExpressionContext, sc *scope, m mode) (val, error) {
	switch e := e.(type) {
	case *parser.PredicatedContext:
		v, err := t.valueExpr(e.ValueExpression(), sc, m)
		if err != nil || e.Predicate() == nil {
			return v, err
		}
		return t.predicate(e.Predicate(), v, sc, m)
	case *parser.LogicalNotContext:
		v, err := t.boolExpr(e.BooleanExpression(), sc, m)
		return val{"(NOT " + v.sql + ")", "boolean"}, err
	case *parser.LogicalBinaryContext:
		l, err := t.boolExpr(e.GetLeft(), sc, m)
		if err != nil {
			return val{}, err
		}
		r, err := t.boolExpr(e.GetRight(), sc, m)
		return val{"(" + l.sql + " " + strings.ToUpper(e.GetOperator().GetText()) + " " + r.sql + ")", "boolean"}, err
	}
	return val{}, unsupported(e)
}

func not(n antlr.TerminalNode) string {
	if n != nil {
		return "NOT "
	}
	return ""
}

func (t *tr) predicate(p parser.IPredicateContext, left val, sc *scope, m mode) (val, error) {
	b := func(s string) (val, error) { return val{"(" + left.sql + " " + s + ")", "boolean"}, nil }
	switch p := p.(type) {
	case *parser.ComparisonContext:
		r, err := t.valueExpr(p.GetRight(), sc, m)
		if err != nil {
			return val{}, err
		}
		op := p.ComparisonOperator().GetText()
		if (left.numeric() && strings.HasPrefix(r.typ, "varchar")) || (strings.HasPrefix(left.typ, "varchar") && r.numeric()) {
			if t.resolve != nil {
				return val{}, splSignatureError(op, left.typ, r.typ)
			}
			return val{}, t.semErrorf(p.ComparisonOperator().GetStart(), "'%s' cannot be applied to %s, %s", op, left.typ, r.typ)
		}
		if op == "!=" {
			op = "<>"
		}
		return b(op + " " + r.sql)
	case *parser.BetweenContext:
		lo, err := t.valueExpr(p.GetLower(), sc, m)
		if err != nil {
			return val{}, err
		}
		hi, err := t.valueExpr(p.GetUpper(), sc, m)
		if err != nil {
			return val{}, err
		}
		return b(not(p.NOT()) + "BETWEEN " + lo.sql + " AND " + hi.sql)
	case *parser.InListContext:
		var items []string
		for _, e := range p.AllExpression() {
			v, err := t.expr(e, sc, m)
			if err != nil {
				return val{}, err
			}
			items = append(items, v.sql)
		}
		return b(not(p.NOT()) + "IN (" + strings.Join(items, ", ") + ")")
	case *parser.InSubqueryContext:
		if t.resolve != nil {
			return val{}, unsupported(p)
		}
		sql, _, err := t.query(p.Query(), sc, false)
		if err != nil {
			return val{}, err
		}
		return b(not(p.NOT()) + "IN (" + sql + ")")
	case *parser.LikeContext:
		pat, err := t.valueExpr(p.GetPattern(), sc, m)
		if err != nil {
			return val{}, err
		}
		s := not(p.NOT()) + "LIKE " + pat.sql
		if p.GetEscape() != nil {
			esc, err := t.valueExpr(p.GetEscape(), sc, m)
			if err != nil {
				return val{}, err
			}
			s += " ESCAPE " + esc.sql
		}
		return b(s)
	case *parser.NullPredicateContext:
		return b("IS " + not(p.NOT()) + "NULL")
	case *parser.DistinctFromContext:
		r, err := t.valueExpr(p.GetRight(), sc, m)
		if err != nil {
			return val{}, err
		}
		return b("IS " + not(p.NOT()) + "DISTINCT FROM " + r.sql)
	}
	return val{}, unsupported(p)
}

func (t *tr) valueExpr(e parser.IValueExpressionContext, sc *scope, m mode) (val, error) {
	switch e := e.(type) {
	case *parser.ValueExpressionDefaultContext:
		return t.primary(e.PrimaryExpression(), sc, m)
	case *parser.ArithmeticUnaryContext:
		v, err := t.valueExpr(e.ValueExpression(), sc, m)
		return val{"(" + e.GetOperator().GetText() + v.sql + ")", v.typ}, err
	case *parser.ArithmeticBinaryContext:
		l, err := t.valueExpr(e.GetLeft(), sc, m)
		if err != nil {
			return val{}, err
		}
		r, err := t.valueExpr(e.GetRight(), sc, m)
		if err != nil {
			return val{}, err
		}
		return arith(e.GetOperator().GetText(), l, r), nil
	case *parser.ConcatenationContext:
		l, err := t.valueExpr(e.GetLeft(), sc, m)
		if err != nil {
			return val{}, err
		}
		r, err := t.valueExpr(e.GetRight(), sc, m)
		return val{"(" + l.sql + " || " + r.sql + ")", "varchar"}, err
	case *parser.AtTimeZoneContext:
		v, err := t.valueExpr(e.ValueExpression(), sc, m)
		if err != nil {
			return val{}, err
		}
		tz, ok := e.TimeZoneSpecifier().(*parser.TimeZoneStringContext)
		if !ok || !strings.EqualFold(tz.String_().GetText(), "'UTC'") {
			return val{}, unsupported(e) // 只支持 UTC：其他时区需要时区库
		}
		// 本地时间（-sql-utc-offset 的时区）换算到 UTC，SLS 输出形如 2026-11-14 13:15:00.000 UTC
		return val{"(strftime(sls_utc(" + tsArg(v) + "), '%Y-%m-%d %H:%M:%S.%g') || ' UTC')", "timestamp with time zone"}, nil
	}
	return val{}, unsupported(e)
}

// arith 生成算术表达式。整数除以零与 Presto 一样报错（DuckDB 返回 NULL）。
func arith(op string, l, r val) val {
	typ := "bigint"
	switch {
	case l.typ == "double" || r.typ == "double":
		typ = "double"
	case l.typ == "timestamp" || r.typ == "timestamp":
		typ = "timestamp"
	case l.typ == "date" || r.typ == "date":
		typ = "date"
	case !l.integral() || !r.integral():
		typ = ""
	}
	if (op == "/" || op == "%") && l.integral() && r.integral() {
		return val{"(CASE WHEN " + r.sql + " = 0 THEN error('" + errMarker + "/ by zero') ELSE " + l.sql + " " + op + " " + r.sql + " END)", typ}
	}
	return val{"(" + l.sql + " " + op + " " + r.sql + ")", typ}
}

func (t *tr) primary(e parser.IPrimaryExpressionContext, sc *scope, m mode) (val, error) {
	switch e := e.(type) {
	case *parser.NullLiteralContext:
		return val{"NULL", "unknown"}, nil
	case *parser.NumericLiteralContext:
		s := e.GetText()
		switch e.Number().(type) {
		case *parser.IntegerLiteralContext:
			if _, err := strconv.ParseInt(s, 10, 64); err != nil {
				return val{}, errorf(e.GetStart(), "Invalid numeric literal: %s", s)
			}
			n, _ := strconv.ParseInt(s, 10, 64)
			if n > 1<<31-1 {
				return val{s, "bigint"}, nil
			}
			return val{s, "integer"}, nil
		default: // Presto 把小数字面量当作 double
			return val{"CAST(" + s + " AS DOUBLE)", "double"}, nil
		}
	case *parser.BooleanLiteralContext:
		return val{strings.ToUpper(e.GetText()), "boolean"}, nil
	case *parser.StringLiteralContext:
		lit, ok := e.String_().(*parser.BasicStringLiteralContext)
		if !ok {
			return val{}, unsupported(e)
		}
		text := lit.GetText()
		n := utf8.RuneCountInString(strings.ReplaceAll(text[1:len(text)-1], "''", "'"))
		return val{text, fmt.Sprintf("varchar(%d)", n)}, nil
	case *parser.TypeConstructorContext:
		lit := e.String_().GetText()
		var typ string
		if e.DOUBLE_PRECISION() != nil {
			typ = "double"
		} else {
			typ = strings.ToLower(identName(e.Identifier()))
		}
		switch typ {
		case "date":
			return val{"DATE " + lit, "date"}, nil
		case "timestamp":
			return val{"TIMESTAMP " + lit, "timestamp"}, nil
		case "time":
			return val{"TIME " + lit, "time"}, nil
		}
		dt, err := duckType(typ)
		if err != nil {
			return val{}, errorf(e.GetStart(), "Unknown type: %s", typ)
		}
		return val{"CAST(" + lit + " AS " + dt + ")", typ}, nil
	case *parser.IntervalLiteralContext:
		return t.interval(e.Interval().(*parser.IntervalContext))
	case *parser.ParenthesizedExpressionContext:
		v, err := t.expr(e.Expression(), sc, m)
		return val{"(" + v.sql + ")", v.typ}, err
	case *parser.ColumnReferenceContext:
		id := e.Identifier()
		_, quoted := id.(*parser.QuotedIdentifierContext)
		return t.column([]string{identName(id)}, id.GetStart(), quoted, sc, m)
	case *parser.DereferenceContext:
		parts, ok := dottedName(e)
		if !ok {
			return val{}, unsupported(e)
		}
		return t.column(parts, e.GetStart(), false, sc, m)
	case *parser.FunctionCallContext:
		return t.call(e, sc, m)
	case *parser.CastContext:
		v, err := t.expr(e.Expression(), sc, m)
		if err != nil {
			return val{}, err
		}
		typ := strings.ToLower(e.Type_().GetText())
		dt, err := duckType(typ)
		if err != nil {
			return val{}, errorf(e.Type_().GetStart(), "Unknown type: %s", typ)
		}
		fn := "CAST"
		if e.TRY_CAST() != nil || t.resolve != nil { // SPL 的 cast 转换失败时是 NULL
			fn = "TRY_CAST"
		}
		return val{fn + "(" + v.sql + " AS " + dt + ")", baseType(typ)}, nil
	case *parser.SimpleCaseContext:
		subject, err := t.valueExpr(e.ValueExpression(), sc, m)
		if err != nil {
			return val{}, err
		}
		return t.caseExpr("CASE "+subject.sql, e.AllWhenClause(), e.GetElseExpression(), sc, m)
	case *parser.SearchedCaseContext:
		return t.caseExpr("CASE", e.AllWhenClause(), e.GetElseExpression(), sc, m)
	case *parser.ArrayConstructorContext:
		var items []string
		for _, x := range e.AllExpression() {
			v, err := t.expr(x, sc, m)
			if err != nil {
				return val{}, err
			}
			items = append(items, v.sql)
		}
		return val{"[" + strings.Join(items, ", ") + "]", "array"}, nil
	case *parser.SubscriptContext:
		base, err := t.primary(e.GetValue(), sc, m)
		if err != nil {
			return val{}, err
		}
		idx, err := t.valueExpr(e.GetIndex(), sc, m)
		return val{base.sql + "[" + idx.sql + "]", ""}, err
	case *parser.SpecialDateTimeFunctionContext:
		switch strings.ToLower(e.GetName().GetText()) {
		case "current_date":
			return val{"CAST(sls_now() AS DATE)", "date"}, nil
		case "current_timestamp", "localtimestamp":
			return val{"sls_now()", "timestamp"}, nil
		}
		return val{}, unsupported(e)
	case *parser.SubstringContext:
		var args []string
		for _, x := range e.AllValueExpression() {
			v, err := t.valueExpr(x, sc, m)
			if err != nil {
				return val{}, err
			}
			args = append(args, v.sql)
		}
		return val{"substring(" + strings.Join(args, ", ") + ")", "varchar"}, nil
	case *parser.ExtractContext:
		v, err := t.valueExpr(e.ValueExpression(), sc, m)
		if err != nil {
			return val{}, err
		}
		field := strings.ToLower(identName(e.Identifier()))
		part, ok := extractFields[field]
		if !ok {
			return val{}, errorf(e.Identifier().GetStart(), "Invalid EXTRACT field: %s", field)
		}
		return val{"extract(" + part + " FROM " + tsArg(v) + ")", "bigint"}, nil
	case *parser.PositionContext:
		needle, err := t.valueExpr(e.ValueExpression(0), sc, m)
		if err != nil {
			return val{}, err
		}
		hay, err := t.valueExpr(e.ValueExpression(1), sc, m)
		return val{"strpos(" + hay.sql + ", " + needle.sql + ")", "bigint"}, err
	case *parser.RowConstructorContext:
		var items []string
		for _, x := range e.AllExpression() {
			v, err := t.expr(x, sc, m)
			if err != nil {
				return val{}, err
			}
			items = append(items, v.sql)
		}
		return val{"row(" + strings.Join(items, ", ") + ")", "row"}, nil
	case *parser.SubqueryExpressionContext:
		if t.resolve != nil {
			return val{}, unsupported(e)
		}
		sql, _, err := t.query(e.Query(), sc, false)
		return val{"(" + sql + ")", ""}, err
	case *parser.ExistsContext:
		if t.resolve != nil {
			return val{}, unsupported(e)
		}
		sql, _, err := t.query(e.Query(), sc, false)
		return val{"EXISTS (" + sql + ")", "boolean"}, err
	case *parser.LambdaContext:
		params := identNames(e.AllIdentifier())
		lm := m
		lm.lambda = map[string]bool{}
		for k := range m.lambda {
			lm.lambda[k] = true
		}
		for _, p := range params {
			lm.lambda[strings.ToLower(p)] = true
		}
		body, err := t.expr(e.Expression(), sc, lm)
		if err != nil {
			return val{}, err
		}
		head := quote(params[0])
		if len(params) != 1 {
			head = "(" + quoteList(params) + ")"
		}
		return val{head + " -> " + body.sql, ""}, nil
	}
	return val{}, unsupported(e)
}

func (t *tr) caseExpr(head string, whens []parser.IWhenClauseContext, els parser.IExpressionContext, sc *scope, m mode) (val, error) {
	var b strings.Builder
	b.WriteString(head)
	typ := ""
	for _, w := range whens {
		c, err := t.expr(w.GetCondition(), sc, m)
		if err != nil {
			return val{}, err
		}
		r, err := t.expr(w.GetResult(), sc, m)
		if err != nil {
			return val{}, err
		}
		typ = mergeTypes(typ, r.typ)
		b.WriteString(" WHEN " + c.sql + " THEN " + r.sql)
	}
	if els != nil {
		e, err := t.expr(els, sc, m)
		if err != nil {
			return val{}, err
		}
		typ = mergeTypes(typ, e.typ)
		b.WriteString(" ELSE " + e.sql)
	}
	b.WriteString(" END")
	return val{"(" + b.String() + ")", typ}, nil
}

func (t *tr) interval(e *parser.IntervalContext) (val, error) {
	if e.GetTo() != nil {
		return val{}, unsupported(e)
	}
	lit := e.String_().GetText()
	unit := strings.ToUpper(e.GetFrom().GetText())
	s := "(CAST(" + lit + " AS BIGINT) * INTERVAL 1 " + unit + ")"
	if e.GetSign() != nil && e.GetSign().GetText() == "-" {
		s = "(-" + s + ")"
	}
	return val{s, "interval"}, nil
}

// dottedName 把 a.b.c 拆成标识符；基表达式不是纯标识符链时返回 false。
func dottedName(e *parser.DereferenceContext) ([]string, bool) {
	var parts []string
	var walk func(p parser.IPrimaryExpressionContext) bool
	walk = func(p parser.IPrimaryExpressionContext) bool {
		switch p := p.(type) {
		case *parser.ColumnReferenceContext:
			parts = append(parts, identName(p.Identifier()))
			return true
		case *parser.DereferenceContext:
			if !walk(p.GetBase()) {
				return false
			}
			parts = append(parts, identName(p.GetFieldName()))
			return true
		}
		return false
	}
	return parts, walk(e)
}

// column 解析名字：lambda 参数 → （order by）输出名 → 各层作用域里的表（派生表的列、log 的字段）
// → （group by、having）输出名。
func (t *tr) column(parts []string, tok antlr.Token, quoted bool, sc *scope, m mode) (val, error) {
	name := strings.Join(parts, ".")
	if len(parts) == 1 && m.lambda[strings.ToLower(name)] {
		return val{quote(name), ""}, nil
	}
	if t.resolve != nil {
		sql, typ := t.resolve(name)
		return val{sql, typ}, nil
	}
	output := func() (val, bool) {
		if !m.outputs || len(parts) != 1 || sc == nil {
			return val{}, false
		}
		for _, o := range sc.outputs {
			if strings.EqualFold(o, name) {
				return val{quote(o), ""}, true
			}
		}
		return val{}, false
	}
	if m.outputsFirst {
		if v, ok := output(); ok {
			return v, nil
		}
	}
	for s := sc; s != nil; s = s.parent {
		for _, r := range s.rels {
			if r.log {
				if v, ok := t.logField(name); ok {
					return v, nil
				}
				continue
			}
			if len(parts) >= 2 && strings.EqualFold(parts[0], r.alias) {
				rest := strings.Join(parts[1:], ".")
				for _, c := range r.cols {
					if strings.EqualFold(c, rest) {
						return val{quote(r.alias) + "." + quote(c), ""}, nil
					}
				}
			}
			for _, c := range r.cols {
				if strings.EqualFold(c, name) {
					return val{quote(r.alias) + "." + quote(c), ""}, nil
				}
			}
		}
	}
	if v, ok := output(); ok {
		return v, nil
	}
	if quoted && strings.HasPrefix(name, "__tag__:") {
		return val{}, errorf(tok, "Column '%s' cannot be resolved; it seems %s is wrapped by \"; if %s is a string rather than a column, please use '%s'", name, name, name, name)
	}
	return val{}, errorf(tok, "Column '%s' cannot be resolved; please add the column in the index attribute", name)
}

// logField 把 log 的字段换成 CTE 上的取值表达式，按索引类型转换。没有字段索引或没开统计时返回 false。
func (t *tr) logField(name string) (val, bool) {
	switch strings.ToLower(name) {
	case "__time__":
		return val{ColTime, "bigint"}, true
	// __time_ns_part__ 在 SLS 的 SQL 里不能用（报 Column ... cannot be resolved）
	case "__topic__":
		return val{"NULLIF(" + ColTopic + ", '')", "varchar"}, true // SLS 的 SQL 里空 topic 是 null
	case "__source__":
		return val{ColSource, "varchar"}, true
	}
	f, path, ok := t.ix.LookupSQL(name)
	if !ok {
		return val{}, false
	}
	typ := "varchar"
	switch f.Type {
	case "long":
		typ = "bigint"
	case "double":
		typ = "double"
	}
	if !f.DocValue { // 有字段索引但没开统计：SQL 里能引用，值总是 null（与 SLS 一致）
		dt, _ := duckType(typ)
		return val{"CAST(NULL AS " + dt + ")", typ}, true
	}
	col := ColContents
	if strings.HasPrefix(f.Name, "__tag__:") {
		col = ColTags
	}
	// JSON 路径内联成字符串字面量而不是参数：同一字段在 select 与 group by 里必须写成同一个表达式
	expr := "json_extract_string(" + col + ", " + sqlString(jsonPath(f.Name)) + ")"
	if len(path) > 0 {
		// 字段值不一定是合法的 JSON（SLS 取不到子键时是 null）
		p := "$"
		for _, seg := range path {
			p += "." + strings.TrimPrefix(jsonPath(seg), "$.")
		}
		expr = "(CASE WHEN json_valid(" + expr + ") THEN json_extract_string(" + expr + ", " + sqlString(p) + ") END)"
	}
	switch typ {
	case "bigint":
		expr = "TRY_CAST(" + expr + " AS BIGINT)"
	case "double":
		expr = "TRY_CAST(" + expr + " AS DOUBLE)"
	}
	return val{expr, typ}, true
}

func jsonPath(key string) string {
	return `$."` + strings.ReplaceAll(strings.ReplaceAll(key, `\`, `\\`), `"`, `\"`) + `"`
}

// duckType 把 Presto 的类型名换成 DuckDB 的。
func duckType(typ string) (string, error) {
	typ = strings.ToLower(strings.TrimSpace(typ))
	if inner, ok := strings.CutPrefix(typ, "array("); ok && strings.HasSuffix(inner, ")") {
		el, err := duckType(inner[:len(inner)-1])
		return el + "[]", err
	}
	if inner, ok := strings.CutPrefix(typ, "map("); ok && strings.HasSuffix(inner, ")") {
		k, v, ok := strings.Cut(inner[:len(inner)-1], ",")
		if !ok {
			return "", fmt.Errorf("bad map type")
		}
		kt, err := duckType(k)
		if err != nil {
			return "", err
		}
		vt, err := duckType(v)
		return "MAP(" + kt + ", " + vt + ")", err
	}
	switch base := baseType(typ); base {
	case "bigint", "integer", "smallint", "tinyint", "double", "boolean", "date", "timestamp", "time", "json":
		return strings.ToUpper(base), nil
	case "real":
		return "FLOAT", nil
	case "varchar", "char":
		return "VARCHAR", nil
	case "varbinary":
		return "BLOB", nil
	case "decimal":
		if !decimalType.MatchString(typ) {
			return "", fmt.Errorf("bad decimal type %s", typ)
		}
		return strings.ToUpper(typ), nil
	}
	return "", fmt.Errorf("unknown type %s", typ)
}

// baseType 去掉类型参数：varchar(10) → varchar。
func baseType(typ string) string {
	typ = strings.ToLower(typ)
	if i := strings.IndexByte(typ, '('); i > 0 && !strings.HasPrefix(typ, "array") && !strings.HasPrefix(typ, "map") {
		typ = typ[:i]
	}
	if typ == "int" {
		return "integer"
	}
	if strings.HasPrefix(typ, "array") {
		return "array"
	}
	if strings.HasPrefix(typ, "map") {
		return "map"
	}
	return typ
}

// tsArg 让时间函数也能接受 Unix 秒（SLS 允许 date_format(__time__, ...) 这样的写法）。
func tsArg(v val) string {
	if v.numeric() {
		return "sls_from_unixtime(" + v.sql + ")"
	}
	return v.sql
}

// errMarker 标记翻译器生成的运行时错误，store 把其后的文本原样返回（SLS 的原文）。
const errMarker = "SLS_ERROR:"

// varcharLen 解析 varchar(n)。
func varcharLen(typ string) (int, bool) {
	if inner, ok := strings.CutPrefix(typ, "varchar("); ok && strings.HasSuffix(inner, ")") {
		n, err := strconv.Atoi(inner[:len(inner)-1])
		return n, err == nil
	}
	return 0, false
}

// mergeTypes 是 CASE、if 各分支的公共类型：都是 varchar(n) 时取最长的。
func mergeTypes(a, b string) string {
	switch {
	case a == "" || a == "unknown":
		return b
	case b == "" || b == "unknown":
		return a
	}
	if x, ok := varcharLen(a); ok {
		if y, ok := varcharLen(b); ok {
			return fmt.Sprintf("varchar(%d)", max(x, y))
		}
	}
	if strings.HasPrefix(a, "varchar") && strings.HasPrefix(b, "varchar") {
		return "varchar"
	}
	return a
}

// extractFields 是 EXTRACT 能取的部分（Presto 的写法 → DuckDB 的写法）；只接受这些，字段名不会拼进 SQL。
var extractFields = map[string]string{
	"year": "year", "quarter": "quarter", "month": "month", "week": "week", "day": "day",
	"day_of_month": "day", "day_of_week": "isodow", "dow": "isodow", "day_of_year": "doy", "doy": "doy",
	"year_of_week": "isoyear", "yow": "isoyear", "hour": "hour", "minute": "minute", "second": "second",
	"millisecond": "millisecond", "timezone_hour": "timezone_hour", "timezone_minute": "timezone_minute",
}

var decimalType = regexp.MustCompile(`^decimal(\(\d+(,\s*\d+)?\))?$`)
