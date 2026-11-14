package query

import (
	"errors"
	"os"

	"github.com/nethinwei/sls-local/internal/index"
	"github.com/nethinwei/sls-local/internal/presto"
)

// sqlEngine 选择 SQL 的翻译器：默认是基于 Presto 语法树的翻译（internal/presto）；SLS_LOCAL_SQL=legacy 时用
// 原来的逐记号改写（过渡期保留，确认没有回退后删除）。
var sqlEngine = func() string {
	if os.Getenv("SLS_LOCAL_SQL") == "legacy" {
		return "legacy"
	}
	return "antlr"
}()

func translatePresto(src string, ix *index.Index, logstore string) (*SQLStmt, error) {
	st, err := presto.Translate(src, ix, logstore)
	if err != nil {
		var pe *presto.Error
		var nf *presto.NotFoundError
		switch {
		case errors.As(err, &pe):
			return nil, &Error{pe.Msg}
		case errors.As(err, &nf):
			return nil, &LogstoreNotExistError{Name: nf.Name}
		case errors.Is(err, presto.ErrUnsupported):
			return nil, err
		}
		return nil, err
	}
	return &SQLStmt{SQL: st.SQL, Args: st.Args, InsertAt: st.InsertAt, Types: st.Types}, nil
}
