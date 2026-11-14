package query

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/nethinwei/sls-local/internal/index"
)

// Cond 是一段 DuckDB WHERE 条件及其参数。
type Cond struct {
	SQL   string
	Args  []any
	Terms []Term // meta.terms：检索语句里的词（SLS 用来高亮）
}

// Term 是 meta.terms 的一项。
type Term struct {
	Term string `json:"term"`
	Key  string `json:"key"`
}

// Error 是语句本身的错误，按 SLS 的错误码与原文返回（见 docs/wire-findings.md）。
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

// SLS 的原文，空格与标点照抄（插件按这些文案识别错误类型）。
const (
	msgKeyNotIndexed   = `key (%s) is not config as key value config,if symbol : is  in your log,please wrap : with quotation mark "`
	msgLeadingWildcard = `only support * or ?  in the  middle or end  of the query`
	msgSyntaxPrefix    = `parse search query error,please read query syntax document,error detail:syntax error, `
	// 没有全文索引时的报错没有与真实 SLS 对照过（对照用的 logstore 开着全文索引）
	msgNoLineIndex = `full text index is not config, search with key: value instead`
)

// valueExpr 返回字段在 logs 表上的取值表达式（VARCHAR，字段不存在时为 NULL）。
// __topic__、__source__ 是列；__tag__:x 在 tags 里；其余字段在 contents 里；path 是 JSON 字段的子键。
func valueExpr(name string, path []string) (string, []any) {
	var expr string
	var args []any
	switch strings.ToLower(name) {
	case "__topic__":
		expr = "topic"
	case "__source__":
		expr = "source"
	default:
		col := "contents"
		if strings.HasPrefix(name, "__tag__:") {
			col = "tags"
		}
		expr, args = "json_extract_string("+col+", ?)", []any{jsonPath(name)}
	}
	if len(path) > 0 {
		// 嵌套路径 $."a"."b"；字段值不是合法 JSON 时为 NULL（DuckDB 会报错）
		jp := "$"
		for _, p := range path {
			jp += "." + strings.TrimPrefix(jsonPath(p), "$.")
		}
		inner, innerArgs := expr, args
		expr = "(CASE WHEN json_valid(" + inner + ") THEN json_extract_string(" + inner + ", ?) END)"
		args = append(append(append([]any{}, innerArgs...), innerArgs...), jp)
	}
	return expr, args
}

// jsonPath 生成 DuckDB 的 JSON 路径 $."key"，字段名原样保留（含点号、冒号）。
func jsonPath(key string) string {
	return `$."` + strings.ReplaceAll(strings.ReplaceAll(key, `\`, `\\`), `"`, `\"`) + `"`
}

// Search 按索引 ix 解析检索语句，返回 WHERE 条件。空语句与 "*" 匹配全部。
func Search(q string, ix *index.Index) (Cond, error) {
	toks, err := lex(q)
	if err != nil {
		return Cond{}, err
	}
	if len(toks) == 0 {
		// 空语句：SLS 为每个中文分词字段列出一个空词，再加上 *
		var terms []Term
		for _, k := range ix.ChnKeys() {
			terms = append(terms, Term{"", k})
		}
		return Cond{SQL: "TRUE", Terms: append(terms, Term{"*", ""})}, nil
	}
	p := &parser{toks: toks}
	n, _, err := p.or()
	if err != nil {
		return Cond{}, err
	}
	if !p.done() {
		return Cond{}, p.unexpected(p.peek(), "")
	}
	c := &compiler{ix: ix}
	sql, err := n.compile(c)
	if err != nil {
		return Cond{}, err
	}
	return Cond{SQL: sql, Args: c.args, Terms: dedupTerms(terms(n, ix))}, nil
}

// terms 按 SLS 的规则列出检索词：与（and）保持顺序，或（or）与二元 not 反序，一元 not 末尾补 *。
// 数值字段记作 *；text 字段按分词符切词，不区分大小写时转小写，短语里的词按字母序。
func terms(n node, ix *index.Index) []Term {
	switch n := n.(type) {
	case binary:
		l, r := terms(n.l, ix), terms(n.r, ix)
		if n.op == "AND" {
			return append(l, r...)
		}
		return append(r, l...)
	case not:
		return append(terms(n.x, ix), Term{"*", ""})
	case notBinary:
		return append(terms(n.r, ix), terms(n.l, ix)...)
	case matchAll:
		return []Term{{"*", ""}}
	case term:
		if n.key == "" {
			return wordTerms(n.value, ix.Line, "", n.quoted)
		}
		lower := strings.ToLower(n.key)
		if strings.HasPrefix(lower, "__tag__:") || lower == "__topic__" || lower == "__source__" {
			return []Term{{n.value, n.key}}
		}
		f, path, ok := ix.Lookup(n.key)
		key := n.key
		if ok && len(path) == 0 && !ix.Auto {
			key = f.Name // 键名按索引里的写法（LEVEL: ERROR → level）
		}
		if ok && (f.Type == "long" || f.Type == "double") {
			return []Term{{"*", key}}
		}
		return wordTerms(n.value, f, key, n.quoted)
	case cmp:
		return compareTerms(n.key, n.value, ix)
	case rng:
		return compareTerms(n.key, n.lo, ix)
	}
	return nil
}

func compareTerms(key, value string, ix *index.Index) []Term {
	if f, _, ok := ix.Lookup(key); ok && (f.Type == "long" || f.Type == "double") {
		return []Term{{"*", key}}
	}
	return []Term{{strings.ToLower(key), ""}, {strings.ToLower(value), ""}} // 非数值字段上的比较按全文的两个词
}

// wordTerms 把值按字段的分词符切成词。
func wordTerms(value string, f *index.Field, key string, quoted bool) []Term {
	toks := strings.Join(index.DefaultTokens, "")
	cs := false
	if f != nil {
		toks, cs = f.Tokens, f.CaseSensitive
	}
	if !cs {
		value = strings.ToLower(value)
	}
	if !quoted { // 不加引号时 * 与 ? 是通配符，不是分词符
		toks = strings.NewReplacer("*", "", "?", "").Replace(toks)
	}
	words := strings.FieldsFunc(value, func(r rune) bool { return strings.ContainsRune(toks, r) })
	if len(words) > 1 {
		sort.Strings(words)
	}
	out := make([]Term, 0, len(words))
	for _, w := range words {
		out = append(out, Term{w, key})
	}
	if len(out) == 0 {
		out = append(out, Term{value, key})
	}
	return out
}

func dedupTerms(ts []Term) []Term {
	seen := map[Term]bool{}
	out := ts[:0]
	for _, t := range ts {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}
	return out
}

// ---------------- 词法 ----------------

type tokKind int

const (
	tWord   tokKind = iota // 不加引号的词，可能带 key: 前缀
	tQuoted                // 双引号字符串（已去掉转义）
	tLParen
	tRParen
	tLBrack
	tRBrack
)

type token struct {
	kind       tokKind
	text       string
	start, end int // 在语句中的字节位置 [start, end)
}

var singles = map[byte]tokKind{'(': tLParen, ')': tRParen, '[': tLBrack, ']': tRBrack}

func lex(q string) ([]token, error) {
	var out []token
	for i := 0; i < len(q); {
		c := q[i]
		if k, ok := singles[c]; ok {
			out = append(out, token{k, q[i : i+1], i, i + 1})
			i++
			continue
		}
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '"':
			var b strings.Builder
			j := i + 1
			for ; j < len(q) && q[j] != '"'; j++ {
				if q[j] == '\\' && j+1 < len(q) {
					j++
				}
				b.WriteByte(q[j])
			}
			if j >= len(q) {
				return nil, &Error{msgSyntaxPrefix + fmt.Sprintf("unterminated quotation mark at column:%d", i+1)}
			}
			out = append(out, token{tQuoted, b.String(), i, j + 1})
			i = j + 1
		default:
			j := i
			for j < len(q) && !strings.ContainsRune(" \t\n\r()[]\"", rune(q[j])) {
				j++
			}
			out = append(out, token{tWord, q[i:j], i, j})
			i = j
		}
	}
	return out, nil
}

// ---------------- 语法 ----------------
//
//	or      := and ("or" and)*
//	and     := unary (("and" | "not") unary | 裸词 裸词)*   key: value 之后必须写 and/or/not
//	unary   := "not" unary | primary
//	primary := "(" or ")" | 裸词 | "引号串" | key: value | key op 数值 | key in [a b]

type node interface {
	compile(c *compiler) (string, error)
}

type parser struct {
	toks  []token
	pos   int
	depth int // 括号嵌套层数
}

func (p *parser) done() bool  { return p.pos >= len(p.toks) }
func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token { t := p.toks[p.pos]; p.pos++; return t }

func (p *parser) isKeyword(kw string) bool {
	return !p.done() && p.peek().kind == tWord && strings.EqualFold(p.peek().text, kw)
}

func isKeyword(t token) bool {
	if t.kind != tWord {
		return false
	}
	switch strings.ToLower(t.text) {
	case "and", "or", "not":
		return true
	}
	return false
}

// unexpected 生成与 SLS 相同格式的语法错误。
func (p *parser) unexpected(t token, expecting string) error {
	name := "TERM"
	switch t.kind {
	case tLParen, tRParen, tLBrack, tRBrack:
		name = "'" + t.text + "'"
	case tWord:
		if isKeyword(t) {
			name = strings.ToUpper(t.text)
		}
	}
	msg := msgSyntaxPrefix + "unexpected " + name
	if expecting != "" {
		msg += ", expecting " + expecting
	}
	near := t.text
	if k, _, ok := splitKey(near); ok && k != "" {
		near = k
	}
	return &Error{msg + fmt.Sprintf(" error position is from column:%d to column:%d,error near < %s >",
		t.start+1, t.start+len(near)+1, near)}
}

// unexpectedEnd 是语句在不该结束的地方结束了：位置是最后一个记号。括号没有闭合、而最后一个记号
// 是一个完整的检索词时，SLS 列出可以接着写的记号。
func (p *parser) unexpectedEnd() error {
	msg := msgSyntaxPrefix + "unexpected $end"
	if len(p.toks) == 0 {
		return &Error{msg}
	}
	last := p.toks[len(p.toks)-1]
	if p.depth > 0 && !isKeyword(last) && last.kind != tLParen {
		msg += ", expecting AND or OR or NOT or RIGHTBRACKET"
	}
	return &Error{msg + fmt.Sprintf(" error position is from column:%d to column:%d,error near < %s >",
		last.start+1, last.end+1, last.text)}
}

// colonInValue 是值里出现了不带引号的冒号（SLS 报语法错误，位置是这个冒号）。
func colonInValue(pos int) error {
	return &Error{msgSyntaxPrefix + fmt.Sprintf("unexpected COLON, expecting $end error position is from column:%d to column:%d,error near < : >", pos+1, pos+2)}
}

func (p *parser) or() (node, bool, error) {
	l, bare, err := p.and()
	if err != nil {
		return nil, false, err
	}
	for p.isKeyword("or") {
		p.next()
		r, _, err := p.and()
		if err != nil {
			return nil, false, err
		}
		l, bare = binary{"OR", l, r}, false
	}
	return l, bare, nil
}

func (p *parser) and() (node, bool, error) {
	l, bare, err := p.unary()
	if err != nil {
		return nil, false, err
	}
	for !p.done() && p.peek().kind != tRParen && !p.isKeyword("or") {
		switch {
		case p.isKeyword("and"):
			p.next()
			r, _, err := p.unary()
			if err != nil {
				return nil, false, err
			}
			l, bare = binary{"AND", l, r}, false
		case p.isKeyword("not"): // 二元 not：a not b = a and not b
			p.next()
			r, _, err := p.unary()
			if err != nil {
				return nil, false, err
			}
			l, bare = notBinary{l, r}, false
		case bare && p.bareTermAhead():
			r, _, err := p.unary()
			if err != nil {
				return nil, false, err
			}
			l = binary{"AND", l, r}
		default:
			return nil, false, p.unexpected(p.peek(), "$end")
		}
	}
	return l, bare, nil
}

// bareTermAhead 判断下一个记号是不是裸词（全文检索词）：可以与前一个裸词隐式 and。
func (p *parser) bareTermAhead() bool {
	t := p.peek()
	if t.kind == tQuoted {
		return true
	}
	if t.kind != tWord || isKeyword(t) {
		return false
	}
	if _, _, ok := splitKey(t.text); ok {
		return false
	}
	return !p.comparisonAhead()
}

// comparisonAhead 判断当前位置是不是 key op 值 或 key in [a b]。
func (p *parser) comparisonAhead() bool {
	if p.pos+1 >= len(p.toks) || p.toks[p.pos+1].kind != tWord {
		return false
	}
	switch op := p.toks[p.pos+1].text; op {
	case ">", ">=", "<", "<=", "=":
		return true
	default:
		return strings.EqualFold(op, "in") && p.pos+2 < len(p.toks) &&
			(p.toks[p.pos+2].kind == tLBrack || p.toks[p.pos+2].kind == tLParen)
	}
}

func (p *parser) unary() (node, bool, error) {
	if p.isKeyword("not") {
		p.next()
		x, _, err := p.unary()
		if err != nil {
			return nil, false, err
		}
		return not{x}, false, nil
	}
	return p.primary()
}

func (p *parser) primary() (node, bool, error) {
	if p.done() {
		return nil, false, p.unexpectedEnd()
	}
	if p.peek().kind == tWord && !isKeyword(p.peek()) && p.comparisonAhead() {
		n, err := p.comparison()
		return n, false, err
	}
	t := p.next()
	switch t.kind {
	case tLParen:
		p.depth++
		defer func() { p.depth-- }()
		n, _, err := p.or()
		if err != nil {
			return nil, false, err
		}
		if p.done() {
			return nil, false, p.unexpectedEnd()
		}
		if r := p.next(); r.kind != tRParen {
			return nil, false, p.unexpected(r, "')'")
		}
		return n, false, nil
	case tQuoted:
		return term{value: t.text, quoted: true}, true, nil
	case tWord:
		if isKeyword(t) {
			return nil, false, p.unexpected(t, "")
		}
	default:
		return nil, false, p.unexpected(t, "")
	}
	key, value, hasKey := splitKey(t.text)
	if !hasKey && !p.done() && p.peek().kind == tWord && p.peek().text == ":" {
		// key : value（冒号前后都有空白）
		p.next()
		key, value, hasKey = t.text, "", true
	}
	if !hasKey {
		if value == "*" {
			return matchAll{}, false, nil
		}
		return term{value: value}, true, nil
	}
	if key == "" {
		return nil, false, p.unexpected(t, "")
	}
	if value != "" {
		if i := strings.IndexByte(value, ':'); i >= 0 {
			return nil, false, colonInValue(t.end - len(value) + i)
		}
		return term{key: key, value: value}, false, nil
	}
	// key: 后面隔着空白跟值：可以是词或引号字符串
	if p.done() {
		return nil, false, p.unexpectedEnd()
	}
	v := p.next()
	switch {
	case v.kind == tQuoted:
		return term{key: key, value: v.text, quoted: true}, false, nil
	case v.kind == tWord && !isKeyword(v):
		if i := strings.IndexByte(v.text, ':'); i >= 0 {
			return nil, false, colonInValue(v.start + i)
		}
		return term{key: key, value: v.text}, false, nil
	}
	return nil, false, p.unexpected(v, "")
}

// comparison 解析 key op 值 与 key in [a b]（方括号闭区间、圆括号开区间，可混用）。
func (p *parser) comparison() (node, error) {
	key := p.next().text
	op := p.next().text
	if !strings.EqualFold(op, "in") {
		if p.done() {
			return nil, p.unexpectedEnd()
		}
		v := p.next()
		if v.kind != tWord || isKeyword(v) {
			return nil, p.unexpected(v, "")
		}
		return cmp{key: key, op: op, value: v.text}, nil
	}
	open := p.next()
	var bounds []string
	for !p.done() && p.peek().kind == tWord && len(bounds) < 2 {
		bounds = append(bounds, p.next().text)
	}
	if p.done() {
		return nil, p.unexpectedEnd()
	}
	if len(bounds) != 2 {
		return nil, p.unexpected(p.peek(), "")
	}
	closing := p.next()
	if closing.kind != tRBrack && closing.kind != tRParen {
		return nil, p.unexpected(closing, "']'")
	}
	return rng{key: key, lo: bounds[0], hi: bounds[1], loIncl: open.kind == tLBrack, hiIncl: closing.kind == tRBrack}, nil
}

// splitKey 把 "key:value" 拆开；__tag__:x 的第一个冒号属于字段名。
func splitKey(w string) (key, value string, ok bool) {
	from := 0
	if strings.HasPrefix(w, "__tag__:") {
		from = len("__tag__:")
	}
	i := strings.IndexByte(w[from:], ':')
	if i < 0 {
		return "", w, false
	}
	i += from
	return w[:i], w[i+1:], true
}

// ---------------- 编译 ----------------

type compiler struct {
	ix   *index.Index
	args []any
}

// field 找到字段；没有索引的普通字段报 SLS 同款错误，没有索引的 tag 不报错（ok=false，按整值匹配）。
func (c *compiler) field(key string) (*index.Field, []string, bool, error) {
	f, path, ok := c.ix.Lookup(key)
	if ok {
		return f, path, true, nil
	}
	if strings.HasPrefix(key, "__tag__:") {
		return nil, nil, false, nil
	}
	return nil, nil, false, &Error{fmt.Sprintf(msgKeyNotIndexed, key)}
}

type binary struct {
	op   string
	l, r node
}

func (b binary) compile(c *compiler) (string, error) {
	l, err := b.l.compile(c)
	if err != nil {
		return "", err
	}
	r, err := b.r.compile(c)
	if err != nil {
		return "", err
	}
	return "(" + l + " " + b.op + " " + r + ")", nil
}

type not struct{ x node }

func (n not) compile(c *compiler) (string, error) {
	x, err := n.x.compile(c)
	if err != nil {
		return "", err
	}
	return "(NOT " + x + ")", nil
}

// notBinary 是二元 not：a not b = a and not b（单独一种节点，meta.terms 的顺序与 and 不同）。
type notBinary struct{ l, r node }

func (n notBinary) compile(c *compiler) (string, error) {
	return binary{"AND", n.l, not{n.r}}.compile(c)
}

type matchAll struct{}

func (matchAll) compile(*compiler) (string, error) { return "TRUE", nil }

// term 是一个检索词：有 key 时匹配该字段，否则做全文检索。
type term struct {
	key    string
	value  string
	quoted bool
}

func (t term) compile(c *compiler) (string, error) {
	wildcard := !t.quoted
	if wildcard && t.value != "*" && (strings.HasPrefix(t.value, "*") || strings.HasPrefix(t.value, "?")) {
		return "", &Error{msgLeadingWildcard}
	}
	if t.key == "" {
		if c.ix.Line == nil {
			return "", &Error{msgNoLineIndex}
		}
		// 全文列用换行连接各字段（见 store.fulltext），SLS 按字段分别分词，所以字段边界总是词边界
		line := *c.ix.Line
		if !strings.ContainsRune(line.Tokens, '\n') {
			line.Tokens += "\n"
		}
		return c.textMatch("fulltext", nil, &line, t.value, wildcard), nil
	}
	f, path, ok, err := c.field(t.key)
	if err != nil {
		return "FALSE", err
	}
	if !ok { // 没有索引的 tag：按整个值匹配，区分大小写，支持词尾通配（实测如此）
		return c.tagMatch(t.key, t.value, wildcard), nil
	}
	expr, args := valueExpr(f.Name, path)
	if wildcard && t.value == "*" {
		c.args = append(c.args, args...)
		switch strings.ToLower(t.key) {
		case "__topic__": // 实测 __topic__: * 什么也匹配不到（有 topic 的日志也不行）
			return "FALSE", nil
		case "__source__":
			return "(" + expr + " <> '')", nil
		}
		return "(" + expr + " IS NOT NULL)", nil
	}
	if f.Type == "long" || f.Type == "double" {
		// 数值字段：key: 值 是数值相等，非数值与通配都匹配不到
		n, ok := parseExactNumber(t.value, f.Type) // key: 值 要求整个值是数（6* 匹配不到）
		if !ok {
			return "FALSE", nil
		}
		c.args = append(c.args, args...)
		c.args = append(c.args, n)
		return "coalesce(" + numeric(expr, f.Type) + " = ?, false)", nil
	}
	return c.textMatch(expr, args, f, t.value, wildcard), nil
}

// tagMatch 是没有索引的 tag 上的检索：整个值相等；不加引号时 * 匹配任意多个字符、? 匹配一个。
func (c *compiler) tagMatch(key, value string, wildcard bool) string {
	expr, args := valueExpr(key, nil)
	c.args = append(c.args, args...)
	if wildcard && value == "*" {
		return "(" + expr + " IS NOT NULL)"
	}
	pattern := regexp.QuoteMeta(value)
	if wildcard {
		pattern = strings.NewReplacer(`\*`, ".*", `\?`, ".").Replace(pattern)
	}
	c.args = append(c.args, "^"+pattern+"$")
	return "coalesce(regexp_matches(" + expr + ", ?), false)"
}

// textMatch 按字段的分词符把值切成词，要求它们在字段文本中连续出现（短语），两端是分词符或文本边界。
// 不加引号的值里 * 与 ? 是词内通配符。值中没有任何词时匹配不到。
func (c *compiler) textMatch(expr string, exprArgs []any, f *index.Field, value string, wildcard bool) string {
	if !f.CaseSensitive {
		expr, value = "lower("+expr+")", strings.ToLower(value)
	}
	pattern, ok := tokenPattern(value, f.Tokens, wildcard)
	if !ok {
		return "FALSE"
	}
	match := "coalesce(regexp_matches(" + expr + ", ?), false)"
	// 直接取列的（全文、__topic__、__source__）先用子串查找排除大部分行，正则只跑剩下的（约快一倍）。
	// 正则匹配时其中的每个字面片段都必然出现在文本里，所以不会漏。JSON 字段上不加：再取一遍值比正则还贵。
	if lit := longestLiteral(value, f.Tokens, wildcard); len(exprArgs) == 0 && lit != "" {
		c.args = append(c.args, lit, pattern)
		return "(contains(" + expr + ", ?) AND " + match + ")"
	}
	c.args = append(c.args, exprArgs...)
	c.args = append(c.args, pattern)
	return match
}

// longestLiteral 是检索值里最长的一段不含通配符与分词符的文字。分词符可以是字母或汉字（索引配置允许），
// 所以除了字母数字以外的字符，也要按 tokens 切开。
func longestLiteral(value, tokens string, wildcard bool) string {
	best := ""
	for _, piece := range strings.FieldsFunc(value, func(r rune) bool {
		return (wildcard && (r == '*' || r == '?')) || !(unicode.IsLetter(r) || unicode.IsDigit(r)) || strings.ContainsRune(tokens, r)
	}) {
		if len(piece) > len(best) {
			best = piece
		}
	}
	return best
}

// cmp 是数值比较：key > 值 等。text 字段上的比较匹配不到（与 SLS 一致，不报错）。
type cmp struct {
	key, op, value string
}

func (m cmp) compile(c *compiler) (string, error) {
	f, path, ok, err := c.field(m.key)
	if err != nil || !ok || (f.Type != "long" && f.Type != "double") {
		return "FALSE", err
	}
	n, ok := parseNumber(m.value, f.Type)
	if !ok {
		return "FALSE", nil
	}
	expr, args := valueExpr(f.Name, path)
	c.args = append(c.args, args...)
	c.args = append(c.args, n)
	return "coalesce(" + numeric(expr, f.Type) + " " + m.op + " ?, false)", nil
}

// rng 是数值区间：key in [lo hi]，方括号闭、圆括号开。
type rng struct {
	key, lo, hi    string
	loIncl, hiIncl bool
}

func (r rng) compile(c *compiler) (string, error) {
	f, path, ok, err := c.field(r.key)
	if err != nil || !ok || (f.Type != "long" && f.Type != "double") {
		return "FALSE", err
	}
	lo, ok1 := parseNumber(r.lo, f.Type)
	hi, ok2 := parseNumber(r.hi, f.Type)
	if !ok1 || !ok2 {
		return "FALSE", nil
	}
	loOp, hiOp := ">", "<"
	if r.loIncl {
		loOp = ">="
	}
	if r.hiIncl {
		hiOp = "<="
	}
	expr, args := valueExpr(f.Name, path)
	v := numeric(expr, f.Type)
	c.args = append(c.args, args...)
	c.args = append(c.args, lo)
	c.args = append(c.args, args...)
	c.args = append(c.args, hi)
	return "coalesce(" + v + " " + loOp + " ? AND " + v + " " + hiOp + " ?, false)", nil
}

func numeric(expr, typ string) string {
	if typ == "long" {
		return "TRY_CAST(" + expr + " AS BIGINT)"
	}
	return "TRY_CAST(" + expr + " AS DOUBLE)"
}

// parseExactNumber 要求整个值是一个数（key: 值 的相等匹配）。
func parseExactNumber(s, typ string) (any, bool) {
	if typ == "long" {
		n, err := strconv.ParseInt(s, 10, 64)
		return n, err == nil
	}
	n, err := strconv.ParseFloat(s, 64)
	return n, err == nil
}

// parseNumber 解析比较与区间里的数值。long 字段与 SLS 一样只取开头的整数部分（像 C 的 atoll：
// 300.5 → 300、3e2 → 3，实测如此）；double 字段按浮点数解析。
func parseNumber(s, typ string) (any, bool) {
	if typ == "long" {
		end := 0
		if end < len(s) && (s[end] == '-' || s[end] == '+') {
			end++
		}
		digits := end
		for end < len(s) && s[end] >= '0' && s[end] <= '9' {
			end++
		}
		if end == digits {
			return nil, false
		}
		n, err := strconv.ParseInt(s[:end], 10, 64)
		return n, err == nil
	}
	n, err := strconv.ParseFloat(s, 64)
	return n, err == nil
}

// tokenPattern 把检索值编译成 RE2：值内各词以分词符相连，两端是分词符或文本边界。
// tokens 是字段的分词符；wildcard 时 * 与 ? 是词内通配符而不是分词符。值中没有任何词时 ok=false。
func tokenPattern(value, tokens string, wildcard bool) (string, bool) {
	// 查询值里的 * 与 ? 是通配符，不切词；数据仍按全部分词符切（url 里的 ? 是分隔符）
	splitBy := tokens
	if wildcard {
		splitBy = strings.NewReplacer("*", "", "?", "").Replace(tokens)
	}
	words := strings.FieldsFunc(value, func(r rune) bool { return strings.ContainsRune(splitBy, r) })
	if len(words) == 0 {
		return "", false
	}
	delim := charClass(tokens, false)
	word := charClass(tokens, true)
	parts := make([]string, len(words))
	for i, w := range words {
		q := regexp.QuoteMeta(w)
		if wildcard {
			q = strings.ReplaceAll(q, `\*`, word+`*`)
			q = strings.ReplaceAll(q, `\?`, word)
		}
		parts[i] = q
	}
	return `(^|` + delim + `)` + strings.Join(parts, delim+`+`) + `($|` + delim + `)`, true
}

// charClass 生成分词符的 RE2 字符类；negate 时是「非分词符」。
func charClass(tokens string, negate bool) string {
	var b strings.Builder
	b.WriteByte('[')
	if negate {
		b.WriteByte('^')
	}
	for _, r := range tokens {
		switch r {
		case '\\', ']', '[', '^', '-':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte(']')
	return b.String()
}
