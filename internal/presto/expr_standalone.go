package presto

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	"github.com/nethinwei/sls-local/internal/presto/parser"
)

// Resolver 把表达式里的字段名换成 SQL 与类型；SPL 里没有的字段也要能引用（值是 NULL）。
type Resolver func(name string) (sql, typ string)

// ExprSyntaxError 是表达式的语法错误；Col 是相对于表达式开头的列号（从 0 起），由调用方换算成语句里的位置。
type ExprSyntaxError struct {
	Col int
	Msg string
}

func (e *ExprSyntaxError) Error() string { return fmt.Sprintf("col %d: %s", e.Col, e.Msg) }

// TranslateExpr 翻译 SPL 里的一个表达式（where 的条件、extend 的值）。语义与 SQL 相同，区别是：
// 字段都由 resolve 决定（SPL 里字段都是 varchar），cast 转换失败时是 NULL，不能有子查询与聚合。
func TranslateExpr(src string, resolve Resolver) (string, error) {
	lex := parser.NewSqlBaseLexer(upperStream{antlr.NewInputStream(src)})
	el := &errorListener{DefaultErrorListener: antlr.NewDefaultErrorListener()}
	lex.RemoveErrorListeners()
	lex.AddErrorListener(el)
	tokens := antlr.NewCommonTokenStream(lex, antlr.TokenDefaultChannel)
	p := parser.NewSqlBaseParser(tokens)
	p.RemoveErrorListeners()
	p.AddErrorListener(el)
	tree := p.StandaloneExpression()
	if el.err != nil {
		return "", &ExprSyntaxError{Col: el.err.col, Msg: el.err.msg}
	}
	// SPL 的标识符不能带冒号（Presto 的可以）：__tag__:x 要写成 "__tag__:x"
	for _, tok := range tokens.GetAllTokens() {
		if tok.GetTokenType() == parser.SqlBaseLexerIDENTIFIER {
			if i := strings.IndexByte(tok.GetText(), ':'); i >= 0 {
				return "", &ExprSyntaxError{Col: tok.GetColumn() + i, Msg: "mismatched input ':' expecting <EOF>"}
			}
		}
	}
	t := &tr{tokens: tokens, srcLen: len(src), insertAt: -1, resolve: resolve}
	v, err := t.expr(tree.Expression(), &scope{}, mode{})
	if err != nil {
		return "", err
	}
	return v.sql, nil
}

// splSignatureError 是 SPL 里 varchar 与数值比较时 SLS 的报错（Presto 的函数签名错误，原文照抄）。
func splSignatureError(op, left, right string) error {
	name := map[string]string{">": "gt", ">=": "gte", "<": "lt", "<=": "lte", "=": "eq", "<>": "neq", "!=": "neq"}[op]
	return &Error{fmt.Sprintf("invalid query: line 1:4: Scalar function signature is not supported: %s(%s, %s). Supported signatures: "+
		"(uuid,uuid) -> boolean, (__user_T1,__user_T1) -> boolean, (ipaddress,ipaddress) -> boolean, (varbinary,varbinary) -> boolean, "+
		"(varchar,varchar) -> boolean, (timestamp,timestamp) -> boolean, (boolean,boolean) -> boolean, "+
		"(timestamp with time zone,timestamp with time zone) -> boolean, (tinyint,tinyint) -> boolean, (smallint,smallint) -> boolean, "+
		"(integer,integer) -> boolean, (bigint,bigint) -> boolean, (real,real) -> boolean, (double,double) -> boolean, (date,date) -> boolean, "+
		"(interval day to second,interval day to second) -> boolean, (interval year to month,interval year to month) -> boolean, "+
		"(DECIMAL(a_precision,a_scale),DECIMAL(a_precision,a_scale)) -> boolean.",
		name, strings.ToUpper(baseType(left)), strings.ToUpper(baseType(right)))}
}
