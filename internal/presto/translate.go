package presto

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/nethinwei/sls-local/internal/index"
	"github.com/nethinwei/sls-local/internal/presto/parser"
)

// Stmt 是翻译好的语句，接在 WITH log AS (...) 之后执行（log 的列见 Columns）。
type Stmt struct {
	SQL  string
	Args []any
	// InsertAt 是最外层查询没写 FROM 时，SLS 在原文里插入 " from <logstore>" 的位置（字节偏移，即下一个记号
	// 的起点），回填 meta.insertedSQL；写了 FROM 时为 -1。
	InsertAt int
	// Types 是推断出的输出列类型（SLS 的写法）；只填有把握而 DuckDB 给不出的（varchar(n)、unknown、json、real），
	// 其余为空，由执行结果的类型决定。
	Types []string
}

// NotFoundError 是 FROM 引用了 log 以外的表：SLS 把它当成另一个 logstore。
type NotFoundError struct{ Name string }

func (e *NotFoundError) Error() string { return "logstore (" + e.Name + ") is not found" }

// ErrUnsupported 表示写法合法，但 sls-local 还没实现。
var ErrUnsupported = errors.New("not supported by sls-local yet")

// log 这张表的列（store 建的 CTE）。列名带 __sls_ 前缀，不会与字段或别名冲突。
const (
	ColTime     = "__sls_t"
	ColNs       = "__sls_ns"
	ColTopic    = "__sls_topic"
	ColSource   = "__sls_source"
	ColTags     = "__sls_tags"
	ColContents = "__sls_contents"
)

// defaultLimit 是最外层查询没写 LIMIT 时 SLS 返回的行数。
const defaultLimit = 100

var setSession = regexp.MustCompile(`(?is)^\s*set\s+session\s+[^;]*;`)

// Translate 翻译 "|" 之后的 SQL。src 前面可以有若干 set session ...; 会话设置，在本地没有意义，
// 换成等长的空白（报错的列号仍按原文计算）。logstore 是查询的 logstore 名：SLS 先把 SQL 改写（from log 换成
// from "<logstore>"、没写 FROM 时插入 from "<logstore>"）再交给 Presto，部分报错的列号按改写后的文本计算。
func Translate(src string, ix *index.Index, logstore string) (*Stmt, error) {
	body := []byte(src)
	for off := 0; ; {
		m := setSession.FindIndex(body[off:])
		if m == nil {
			break
		}
		for i := off + m[0]; i < off+m[1]; i++ {
			if body[i] != '\n' {
				body[i] = ' '
			}
		}
		off += m[1]
	}
	tree, tokens, perr := parse(string(body))
	t := &tr{ix: ix, tokens: tokens, srcLen: len(src), insertAt: -1, quotedLogstore: quote(logstore)}
	t.scanFrom()
	if perr != nil {
		// 语法错误的两个列号都按 SLS 改写后的文本计算
		col := perr.col + 1
		for _, off := range t.logRefs {
			if perr.start >= 0 && off < perr.start {
				col += len(t.quotedLogstore) - len("log")
			}
		}
		return nil, &Error{fmt.Sprintf("ErrorType:SyntaxError.ErrorPosition,line:%d,column:%d.ErrorMessage:line %d:%d: %s", perr.line, col, perr.line, col, perr.msg)}
	}
	st, ok := tree.Statement().(*parser.StatementDefaultContext)
	if !ok {
		return nil, syntaxErrorf(tree.GetStart(), "only select statements are allowed after |")
	}
	if !t.hasFrom {
		t.insertAt = insertPoint(st.Query(), tokens, len(src))
	}
	sql, _, err := t.query(st.Query(), nil, true)
	if err != nil {
		return nil, err
	}
	return &Stmt{SQL: sql, Args: t.args, InsertAt: t.insertAt, Types: t.topTypes}, nil
}

// scanFrom 在记号流里找 FROM/JOIN：原文有没有 from（决定 SLS 是否补 from 子句），以及 from log 的位置。
func (t *tr) scanFrom() {
	all := t.tokens.GetAllTokens()
	for i, tok := range all {
		if tok.GetTokenType() != parser.SqlBaseParserFROM && tok.GetTokenType() != parser.SqlBaseParserJOIN {
			continue
		}
		t.hasFrom = true
		for j := i + 1; j < len(all); j++ {
			if all[j].GetChannel() != antlr.TokenDefaultChannel {
				continue // 空白与注释
			}
			if strings.EqualFold(all[j].GetText(), "log") {
				t.logRefs = append(t.logRefs, all[j].GetStart())
			}
			break
		}
	}
}

// tr 是一次翻译的状态。
type tr struct {
	ix       *index.Index
	tokens   *antlr.CommonTokenStream
	srcLen   int
	args     []any
	ctes     []map[string][]string // WITH 定义的表（小写名 → 列名），内层在后
	insertAt int
	topSpec  bool // 已经处理过最外层的第一个 select
	topTypes []string
	nAlias   int
	hasFrom  bool // 原文里出现过 FROM：SLS 这时不会补 from <logstore>
	// SLS 改写后的坐标：logRefs 是 from log 里 log 的位置（换成带引号的 logstore 名），insertAt 处插入 from 子句
	quotedLogstore string
	logRefs        []int
}

// rel 是 FROM 里的一张表。
type rel struct {
	log   bool     // log：列是索引里的字段
	alias string   // 引用名（派生表）；log 不能用限定名引用（与 SLS 一致：log.level 会被当成一个字段名）
	cols  []string // 派生表的列名
}

// scope 是一层 select 的名字空间。
type scope struct {
	parent  *scope
	rels    []*rel
	outputs []string // select 列表的输出名：group by、having、order by 可以引用
	types   []string // 输出列的类型（见 Stmt.Types）
}

// query 翻译 [WITH ...] queryNoWith，返回 SQL 与输出列名。top 表示最外层（补默认 LIMIT）。
func (t *tr) query(q parser.IQueryContext, outer *scope, top bool) (string, []string, error) {
	var with string
	if w := q.With(); w != nil {
		if w.RECURSIVE() != nil {
			return "", nil, syntaxErrorf(w.GetStart(), "WITH RECURSIVE is %v", ErrUnsupported)
		}
		frame := map[string][]string{}
		t.ctes = append(t.ctes, frame)
		defer func() { t.ctes = t.ctes[:len(t.ctes)-1] }()
		var defs []string
		for _, nq := range w.AllNamedQuery() {
			sql, cols, err := t.query(nq.Query(), outer, false)
			if err != nil {
				return "", nil, err
			}
			if ca := nq.ColumnAliases(); ca != nil {
				cols = identNames(ca.AllIdentifier())
			}
			name := identName(nq.GetName())
			frame[strings.ToLower(name)] = cols
			defs = append(defs, quote(name)+" AS ("+renameCols(sql, cols)+")")
		}
		with = "WITH " + strings.Join(defs, ", ") + " "
	}
	sql, cols, err := t.queryNoWith(q.QueryNoWith(), outer, top)
	return with + sql, cols, err
}

// renameCols 给子查询的输出列换名（WITH t(a, b) AS (...)）。
func renameCols(sql string, cols []string) string {
	return "SELECT * FROM (" + sql + ") AS __sls_cte(" + quoteList(cols) + ")"
}

func (t *tr) queryNoWith(q parser.IQueryNoWithContext, outer *scope, top bool) (string, []string, error) {
	if q.OFFSET() != nil { // SLS 的 Presto 版本还不支持 OFFSET；语法错误的两个列号都按改写后的文本
		tok := q.OFFSET().GetSymbol()
		l, c := tok.GetLine(), t.rewrittenCol(tok)
		return "", nil, &Error{fmt.Sprintf("ErrorType:SyntaxError.ErrorPosition,line:%d,column:%d.ErrorMessage:line %d:%d: mismatched input '%s' expecting <EOF>", l, c, l, c, tok.GetText())}
	}
	sql, cols, sc, err := t.queryTerm(q.QueryTerm(), outer)
	if err != nil {
		return "", nil, err
	}
	if top {
		t.topTypes = sc.types
	}
	var b strings.Builder
	b.WriteString(sql)
	if items := q.AllSortItem(); len(items) > 0 {
		order, err := t.sortItems(items, sc, true)
		if err != nil {
			return "", nil, err
		}
		b.WriteString(" ORDER BY " + order)
	}
	switch {
	case q.GetLimit() != nil && q.ALL() == nil:
		b.WriteString(" LIMIT " + q.GetLimit().GetText())
	case q.GetFetchFirstNRows() != nil:
		b.WriteString(" LIMIT " + q.GetFetchFirstNRows().GetText())
	case top && q.GetLimit() == nil:
		fmt.Fprintf(&b, " LIMIT %d", defaultLimit)
	}
	return b.String(), cols, nil
}

func (t *tr) sortItems(items []parser.ISortItemContext, sc *scope, outputsFirst bool) (string, error) {
	var parts []string
	for _, it := range items {
		v, err := t.expr(it.Expression(), sc, mode{outputs: true, outputsFirst: outputsFirst})
		if err != nil {
			return "", err
		}
		s := v.sql
		if it.GetOrdering() != nil {
			s += " " + strings.ToUpper(it.GetOrdering().GetText())
		}
		// Presto 不论升降序都默认 NULLS LAST
		if it.GetNullOrdering() != nil {
			s += " NULLS " + strings.ToUpper(it.GetNullOrdering().GetText())
		} else {
			s += " NULLS LAST"
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", "), nil
}

// queryTerm 返回 SQL、输出列名，以及 ORDER BY 用的作用域。
func (t *tr) queryTerm(q parser.IQueryTermContext, outer *scope) (string, []string, *scope, error) {
	switch q := q.(type) {
	case *parser.QueryTermDefaultContext:
		return t.queryPrimary(q.QueryPrimary(), outer)
	case *parser.SetOperationContext:
		l, cols, lsc, err := t.queryTerm(q.GetLeft(), outer)
		if err != nil {
			return "", nil, nil, err
		}
		r, _, _, err := t.queryTerm(q.GetRight(), outer)
		if err != nil {
			return "", nil, nil, err
		}
		op := strings.ToUpper(q.GetOperator().GetText())
		if sq := q.SetQuantifier(); sq != nil && strings.EqualFold(sq.GetText(), "all") {
			op += " ALL"
		}
		return "(" + l + ") " + op + " (" + r + ")", cols, &scope{parent: outer, outputs: cols, types: lsc.types}, nil
	}
	return "", nil, nil, unsupported(q)
}

func (t *tr) queryPrimary(q parser.IQueryPrimaryContext, outer *scope) (string, []string, *scope, error) {
	switch q := q.(type) {
	case *parser.QueryPrimaryDefaultContext:
		return t.querySpec(q.QuerySpecification().(*parser.QuerySpecificationContext), outer)
	case *parser.SubqueryContext:
		sql, cols, err := t.queryNoWith(q.QueryNoWith(), outer, false)
		if err != nil {
			return "", nil, nil, err
		}
		return "(" + sql + ")", cols, &scope{parent: outer, outputs: cols, types: make([]string, len(cols))}, nil
	}
	return "", nil, nil, unsupported(q)
}

func (t *tr) querySpec(q *parser.QuerySpecificationContext, outer *scope) (string, []string, *scope, error) {
	sc := &scope{parent: outer}
	top := !t.topSpec && outer == nil
	t.topSpec = true
	var from []string
	switch {
	case q.FROM() == nil && (t.hasFrom || !top):
		// SLS 只在原文里没有 from 时给最外层的 select 补 from <logstore>（extract(hour from ts) 里的 from 也算）；
		// 其他没写 FROM 的 select 没有表，只算一次
	case q.FROM() == nil:
		sc.rels = []*rel{{log: true}}
		from = []string{"log"}
	default:
		for _, r := range q.AllRelation() {
			s, err := t.relation(r, sc)
			if err != nil {
				return "", nil, nil, err
			}
			from = append(from, s)
		}
	}

	var items []string
	for i, it := range q.AllSelectItem() {
		switch it := it.(type) {
		case *parser.SelectSingleContext:
			v, err := t.expr(it.Expression(), sc, mode{})
			if err != nil {
				return "", nil, nil, err
			}
			name := fmt.Sprintf("_col%d", i)
			if id := it.Identifier(); id != nil {
				name = identName(id)
			} else if n, ok := exprName(it.Expression()); ok {
				name = n
			}
			sc.outputs = append(sc.outputs, name)
			sc.types = append(sc.types, overrideType(v.typ))
			if v.typ == "json" { // JSON 按原文输出（DuckDB 会把它解析成值）
				v.sql = "CAST(" + v.sql + " AS VARCHAR)"
			}
			items = append(items, v.sql+" AS "+quote(name))
		case *parser.SelectAllContext:
			cols, sql, err := t.selectAll(it, sc)
			if err != nil {
				return "", nil, nil, err
			}
			sc.outputs = append(sc.outputs, cols...)
			sc.types = append(sc.types, make([]string, len(cols))...)
			items = append(items, sql...)
		}
	}

	var b strings.Builder
	b.WriteString("SELECT ")
	if sq := q.SetQuantifier(); sq != nil && strings.EqualFold(sq.GetText(), "distinct") {
		b.WriteString("DISTINCT ")
	}
	b.WriteString(strings.Join(items, ", "))
	if len(from) > 0 {
		b.WriteString(" FROM " + strings.Join(from, ", "))
	}
	if w := q.GetWhere(); w != nil {
		v, err := t.boolExpr(w, sc, mode{})
		if err != nil {
			return "", nil, nil, err
		}
		b.WriteString(" WHERE " + v.sql)
	}
	if g := q.GroupBy(); g != nil {
		s, err := t.groupBy(g, sc)
		if err != nil {
			return "", nil, nil, err
		}
		b.WriteString(" GROUP BY " + s)
	}
	if h := q.GetHaving(); h != nil {
		v, err := t.boolExpr(h, sc, mode{outputs: true})
		if err != nil {
			return "", nil, nil, err
		}
		b.WriteString(" HAVING " + v.sql)
	}
	return b.String(), sc.outputs, sc, nil
}

// selectAll 展开 * 与 x.*：只支持派生表（log 的字段由索引决定，SLS 的展开规则尚未对照）。
func (t *tr) selectAll(it *parser.SelectAllContext, sc *scope) ([]string, []string, error) {
	var qual string
	if qn := it.QualifiedName(); qn != nil {
		qual = qn.GetText()
	}
	var cols, items []string
	for _, r := range sc.rels {
		if qual != "" && !strings.EqualFold(r.alias, qual) {
			continue
		}
		if r.log {
			return nil, nil, syntaxErrorf(it.GetStart(), "select * on log is %v, list the columns", ErrUnsupported)
		}
		for _, c := range r.cols {
			cols = append(cols, c)
			items = append(items, quote(r.alias)+"."+quote(c)+" AS "+quote(c))
		}
	}
	if len(items) == 0 {
		return nil, nil, errorf(it.GetStart(), "Relation '%s' not found", qual)
	}
	return cols, items, nil
}

func (t *tr) groupBy(g parser.IGroupByContext, sc *scope) (string, error) {
	if sq := g.SetQuantifier(); sq != nil {
		return "", syntaxErrorf(sq.GetStart(), "GROUP BY %s is %v", sq.GetText(), ErrUnsupported)
	}
	var parts []string
	for _, el := range g.AllGroupingElement() {
		single, ok := el.(*parser.SingleGroupingSetContext)
		if !ok {
			return "", syntaxErrorf(el.GetStart(), "%s is %v", strings.ToUpper(el.GetStart().GetText()), ErrUnsupported)
		}
		gs := single.GroupingSet()
		exprs := gs.AllExpression()
		if len(exprs) != 1 || gs.GetStart().GetText() == "(" && len(exprs) != 1 {
			return "", syntaxErrorf(gs.GetStart(), "grouping sets are %v", ErrUnsupported)
		}
		v, err := t.expr(exprs[0], sc, mode{outputs: true})
		if err != nil {
			return "", err
		}
		parts = append(parts, v.sql)
	}
	return strings.Join(parts, ", "), nil
}

// relation 翻译 FROM 里的一项，并把它登记到 sc。
func (t *tr) relation(r parser.IRelationContext, sc *scope) (string, error) {
	switch r := r.(type) {
	case *parser.RelationDefaultContext:
		return t.sampled(r.SampledRelation(), sc)
	case *parser.JoinRelationContext:
		left, err := t.relation(r.GetLeft(), sc)
		if err != nil {
			return "", err
		}
		switch {
		case r.CROSS() != nil:
			right, err := t.sampled(r.GetRight(), sc)
			return left + " CROSS JOIN " + right, err
		case r.NATURAL() != nil:
			return "", syntaxErrorf(r.NATURAL().GetSymbol(), "NATURAL JOIN is %v", ErrUnsupported)
		}
		right, err := t.relation(r.GetRightRelation(), sc)
		if err != nil {
			return "", err
		}
		jt := strings.ToUpper(strings.TrimSpace(r.JoinType().GetText()))
		if jt == "" {
			jt = "INNER"
		}
		jt = strings.Replace(jt, "OUTER", " OUTER", 1)
		crit := r.JoinCriteria()
		if crit.USING() != nil {
			return left + " " + jt + " JOIN " + right + " USING (" + quoteList(identNames(crit.AllIdentifier())) + ")", nil
		}
		on, err := t.boolExpr(crit.BooleanExpression(), sc, mode{})
		if err != nil {
			return "", err
		}
		return left + " " + jt + " JOIN " + right + " ON " + on.sql, nil
	}
	return "", unsupported(r)
}

func (t *tr) sampled(s parser.ISampledRelationContext, sc *scope) (string, error) {
	if s.TABLESAMPLE() != nil {
		return "", syntaxErrorf(s.TABLESAMPLE().GetSymbol(), "TABLESAMPLE is %v", ErrUnsupported)
	}
	a := s.AliasedRelation()
	var alias string
	if id := a.Identifier(); id != nil {
		alias = identName(id)
	}
	var colAliases []string
	if ca := a.ColumnAliases(); ca != nil {
		colAliases = identNames(ca.AllIdentifier())
	}
	switch p := a.RelationPrimary().(type) {
	case *parser.TableNameContext:
		ids := p.QualifiedName().AllIdentifier()
		name := identName(ids[len(ids)-1])
		if len(ids) == 1 {
			if cols, ok := t.cte(name); ok {
				if colAliases != nil {
					cols = colAliases
				}
				if alias == "" {
					alias = name
				}
				sc.rels = append(sc.rels, &rel{alias: alias, cols: cols})
				return quote(name) + " AS " + quote(alias) + colList(colAliases), nil
			}
			if strings.EqualFold(name, "log") {
				if colAliases != nil {
					return "", syntaxErrorf(a.ColumnAliases().GetStart(), "column aliases on log are %v", ErrUnsupported)
				}
				sc.rels = append(sc.rels, &rel{log: true})
				return "log", nil
			}
		}
		return "", &NotFoundError{Name: p.QualifiedName().GetText()}
	case *parser.SubqueryRelationContext:
		sql, cols, err := t.query(p.Query(), sc.parent, false)
		if err != nil {
			return "", err
		}
		if colAliases != nil {
			cols = colAliases
		}
		if alias == "" {
			t.nAlias++
			alias = fmt.Sprintf("__sls_sq%d", t.nAlias)
		}
		sc.rels = append(sc.rels, &rel{alias: alias, cols: cols})
		return "(" + sql + ") AS " + quote(alias) + colList(colAliases), nil
	case *parser.ParenthesizedRelationContext:
		s, err := t.relation(p.Relation(), sc)
		return "(" + s + ")", err
	case *parser.UnnestContext:
		if p.ORDINALITY() != nil {
			return "", syntaxErrorf(p.ORDINALITY().GetSymbol(), "WITH ORDINALITY is %v", ErrUnsupported)
		}
		var args []string
		for _, e := range p.AllExpression() {
			v, err := t.expr(e, sc, mode{})
			if err != nil {
				return "", err
			}
			args = append(args, "unnest("+v.sql+")")
		}
		if alias == "" {
			t.nAlias++
			alias = fmt.Sprintf("__sls_un%d", t.nAlias)
		}
		cols := colAliases
		if cols == nil {
			for i := range args {
				cols = append(cols, fmt.Sprintf("_col%d", i))
			}
		}
		sc.rels = append(sc.rels, &rel{alias: alias, cols: cols})
		return "(SELECT " + strings.Join(args, ", ") + ") AS " + quote(alias) + "(" + quoteList(cols) + ")", nil
	}
	return "", unsupported(a.RelationPrimary())
}

func (t *tr) cte(name string) ([]string, bool) {
	for i := len(t.ctes) - 1; i >= 0; i-- {
		if cols, ok := t.ctes[i][strings.ToLower(name)]; ok {
			return cols, true
		}
	}
	return nil, false
}

// exprName 是没写别名的 select 项的列名：单个字段引用用字段名（Presto 的规则），其他由调用方编号。
func exprName(e parser.IExpressionContext) (string, bool) {
	p, ok := e.BooleanExpression().(*parser.PredicatedContext)
	if !ok || p.Predicate() != nil {
		return "", false
	}
	d, ok := p.ValueExpression().(*parser.ValueExpressionDefaultContext)
	if !ok {
		return "", false
	}
	switch x := d.PrimaryExpression().(type) {
	case *parser.ColumnReferenceContext:
		return identName(x.Identifier()), true
	case *parser.DereferenceContext:
		return identName(x.GetFieldName()), true
	}
	return "", false
}

func identName(id parser.IIdentifierContext) string {
	s := id.GetText()
	switch id.(type) {
	case *parser.QuotedIdentifierContext:
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	case *parser.BackQuotedIdentifierContext:
		return strings.ReplaceAll(s[1:len(s)-1], "``", "`")
	}
	return s
}

func identNames(ids []parser.IIdentifierContext) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = identName(id)
	}
	return out
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func quoteList(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = quote(n)
	}
	return strings.Join(q, ", ")
}

func colList(names []string) string {
	if names == nil {
		return ""
	}
	return "(" + quoteList(names) + ")"
}

func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func unsupported(n antlr.ParserRuleContext) error {
	return syntaxErrorf(n.GetStart(), "%q is %v", n.GetText(), ErrUnsupported)
}

// overrideType 是要覆盖执行结果类型的类型名（见 Stmt.Types）。
func overrideType(typ string) string {
	switch {
	case strings.HasPrefix(typ, "varchar("), strings.HasPrefix(typ, "array(varchar("),
		typ == "unknown", typ == "json", typ == "real", typ == "timestamp with time zone":
		return typ
	}
	return ""
}

// insertPoint 是 SLS 插入 from <logstore> 的位置：最外层第一个 select 的列表之后的下一个记号。
func insertPoint(q parser.IQueryContext, tokens *antlr.CommonTokenStream, srcLen int) int {
	term := q.QueryNoWith().QueryTerm()
	for {
		switch x := term.(type) {
		case *parser.SetOperationContext:
			term = x.GetLeft()
			continue
		case *parser.QueryTermDefaultContext:
			spec, ok := x.QueryPrimary().(*parser.QueryPrimaryDefaultContext)
			if !ok {
				return -1
			}
			items := spec.QuerySpecification().AllSelectItem()
			for i := items[len(items)-1].GetStop().GetTokenIndex() + 1; ; i++ {
				next := tokens.Get(i)
				if next.GetTokenType() == antlr.TokenEOF {
					return srcLen
				}
				if next.GetChannel() == antlr.TokenDefaultChannel {
					return next.GetStart()
				}
			}
		}
		return -1
	}
}

// rewrittenCol 是 tok 在 SLS 改写后的 SQL 里的列号（从 1 起）。
func (t *tr) rewrittenCol(tok antlr.Token) int {
	col := tok.GetColumn() + 1
	for _, off := range t.logRefs {
		if off < tok.GetStart() {
			col += len(t.quotedLogstore) - len("log")
		}
	}
	if t.insertAt >= 0 && t.insertAt <= tok.GetStart() {
		col += len(" from " + t.quotedLogstore + " ")
	}
	return col
}

// semErrorf 是语义错误：ErrorPosition 按原文，消息里的列号按改写后的文本（与 SLS 一致）。
func (t *tr) semErrorf(tok antlr.Token, format string, a ...any) error {
	l := tok.GetLine()
	return &Error{fmt.Sprintf("ErrorType:SyntaxError.ErrorPosition,line:%d,column:%d.ErrorMessage:line %d:%d: ", l, tok.GetColumn()+1, l, t.rewrittenCol(tok)) + fmt.Sprintf(format, a...)}
}
