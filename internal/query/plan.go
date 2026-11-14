package query

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nethinwei/sls-local/internal/index"
)

// ErrUnsupported 表示语句合法但 sls-local 还不支持（例如 SPL 中除 where 之外的指令）。
var ErrUnsupported = errors.New("not supported by sls-local yet")

// Plan 是一条检索/SPL 语句的执行计划。
type Plan struct {
	Search   string // 检索部分原文，回填 meta.whereQuery
	Where    Cond   // 检索条件与各个 | where 的合取
	PackMeta bool   // 带 | with_pack_meta：每行返回 __pack_meta__
	SPL      bool   // 带 SPL 指令（with_pack_meta 不算）
	SQL      *SQLStmt
}

// Parse 按索引 ix 解析一条语句：检索、检索 | SPL，或检索 | SQL。
func Parse(q string, ix *index.Index) (*Plan, error) {
	search, rest := Split(q)
	where, err := Search(search, ix)
	if err != nil {
		return nil, err
	}
	p := &Plan{Search: search, Where: where}
	if sqlStart.MatchString(rest) {
		p.SQL, err = TranslateSQL(rest[1:], ix)
		if err != nil {
			return nil, err
		}
		return p, nil
	}
	for _, cmd := range pipes(rest) {
		name, arg, _ := strings.Cut(cmd, " ")
		switch strings.ToLower(name) {
		case "with_pack_meta":
			p.PackMeta = true
		case "where":
			c, err := Expr(arg)
			if err != nil {
				return nil, err
			}
			p.Where.SQL = "(" + p.Where.SQL + " AND " + c.SQL + ")"
			p.Where.Args = append(p.Where.Args, c.Args...)
			p.SPL = true
		case "":
			return nil, &Error{"invalid query: empty pipe"}
		default:
			return nil, fmt.Errorf("SPL %q: %w", name, ErrUnsupported)
		}
	}
	return p, nil
}
