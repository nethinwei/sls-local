// Package presto 把 SLS 的 SQL（Presto 方言）翻译成 DuckDB 的 SQL。
//
// 语法分析用 prestodb 的 SqlBase.g4（grammar/，ANTLR 生成的代码在 parser/），所以能接受的写法和语法错误的
// 文案与 Presto 一致。翻译在语法树上进行：解析每个名字所在的作用域（log、子查询、CTE、select 别名），
// 把 log 的字段换成按索引类型转换的取值表达式，把 Presto 函数换成 DuckDB 的等价写法。
package presto

import (
	"fmt"
	"unicode"

	"github.com/antlr4-go/antlr/v4"

	"github.com/nethinwei/sls-local/internal/presto/parser"
)

// Error 是返回给客户端的 SQL 错误，Msg 是 SLS 的原文格式。
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

// errorf 生成带位置的错误：line L:C: ...（C 从 1 起，与 Presto 一致）。
func errorf(tok antlr.Token, format string, a ...any) error {
	return &Error{fmt.Sprintf("line %d:%d: ", tok.GetLine(), tok.GetColumn()+1) + fmt.Sprintf(format, a...)}
}

// syntaxErrorf 是 SLS 包装过的语法类错误：前面多一段 ErrorType 与 ErrorPosition。
func syntaxErrorf(tok antlr.Token, format string, a ...any) error {
	l, c := tok.GetLine(), tok.GetColumn()+1
	return &Error{fmt.Sprintf("ErrorType:SyntaxError.ErrorPosition,line:%d,column:%d.ErrorMessage:line %d:%d: ", l, c, l, c) + fmt.Sprintf(format, a...)}
}

// upperStream 让词法分析不区分大小写：语法里的关键字都是大写，Presto 也是这样包装输入流的。
// 记号的文本仍取自原文。
type upperStream struct{ *antlr.InputStream }

func (s upperStream) LA(i int) int {
	c := s.InputStream.LA(i)
	if c <= 0 {
		return c
	}
	return int(unicode.ToUpper(rune(c)))
}

type errorListener struct {
	*antlr.DefaultErrorListener
	err *parseError
}

// parseError 是第一个语法错误；列号在 Translate 里换算成 SLS 改写后的坐标再输出。
type parseError struct {
	line, col int // col 从 0 起
	start     int // 出错记号在原文中的字节偏移
	msg       string
}

func (l *errorListener) SyntaxError(_ antlr.Recognizer, sym any, line, column int, msg string, _ antlr.RecognitionException) {
	if l.err != nil {
		return
	}
	e := &parseError{line: line, col: column, start: -1, msg: msg}
	if tok, ok := sym.(antlr.Token); ok && tok != nil {
		e.start = tok.GetStart()
	}
	l.err = e
}

// parse 把一条语句解析成语法树。解析失败时也返回记号流（用于换算报错坐标）。
func parse(src string) (parser.ISingleStatementContext, *antlr.CommonTokenStream, *parseError) {
	lex := parser.NewSqlBaseLexer(upperStream{antlr.NewInputStream(src)})
	el := &errorListener{DefaultErrorListener: antlr.NewDefaultErrorListener()}
	lex.RemoveErrorListeners()
	lex.AddErrorListener(el)
	tokens := antlr.NewCommonTokenStream(lex, antlr.TokenDefaultChannel)
	p := parser.NewSqlBaseParser(tokens)
	p.RemoveErrorListeners()
	p.AddErrorListener(el)
	tree := p.SingleStatement()
	if el.err != nil {
		tokens.Fill()
		return nil, tokens, el.err
	}
	return tree, tokens, nil
}
