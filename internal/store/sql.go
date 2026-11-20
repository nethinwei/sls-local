package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/nethinwei/sls-local/internal/presto"
	"github.com/nethinwei/sls-local/internal/query"
)

// SQLResult 是 SQL 语句的结果，格式与 SLS 一致（docs/wire-findings.md）。
type SQLResult struct {
	Keys      []string // 列名
	Types     []string // SLS 的列类型名：long、double、varchar、boolean、timestamp、date、array(bigint)……
	Rows      [][]byte // 每行一个 JSON 对象，值全部是字符串，null 写作 "null"
	Processed int64    // 参与计算的日志条数（时间范围内满足检索条件的）
}

// RunSQL 在 p 限定的日志（project、时间范围、检索条件）上执行改写好的 SQL。
func (s *Store) RunSQL(ctx context.Context, p SearchParams, stmt *query.SQLStmt) (*SQLResult, error) {
	sn := s.acquire()
	defer s.release(sn)
	base, baseArgs := p.where()
	keys := p.keys(stmt.SQL, stmt.Processed) // 检索条件与语句用到的字段列
	// log 与 compare 用的 log__shift_<n>（时间范围往前平移 n 秒，检索条件相同）
	from, err := s.source(ctx, sn, &p, stmt.SQL, stmt.Processed)
	if err != nil {
		return nil, err
	}
	defs := []string{query.CTEDef("log", from, base, keys)}
	cteArgs := append([]any{}, baseArgs...)
	for _, n := range stmt.Shifts {
		sp := p
		sp.From, sp.To = p.From-n, p.To-n
		b, a := sp.where()
		shifted, err := s.source(ctx, sn, &sp, stmt.SQL, stmt.Processed)
		if err != nil {
			return nil, err
		}
		defs = append(defs, query.CTEDef(presto.ShiftTable(n), shifted, b, keys))
		cteArgs = append(cteArgs, a...)
	}
	with := "WITH " + strings.Join(defs, ", ") + " "
	q := with + stmt.SQL
	if rest, ok := strings.CutPrefix(stmt.SQL, "WITH "); ok { // 语句自己的 WITH 合并进来
		q = with[:len(with)-1] + ", " + rest
	}
	// processedRows 与结果互不依赖，另开一个连接同时算
	processed := make(chan error, 1)
	res := &SQLResult{}
	go func() {
		var err error
		if stmt.Processed != "" {
			err = s.db.QueryRowContext(ctx, with+"SELECT "+stmt.Processed, append(cteArgs, stmt.Args...)...).Scan(&res.Processed)
		} else {
			var from string
			if from, err = s.source(ctx, sn, &p); err == nil {
				err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM `+from+` WHERE `+base, baseArgs...).Scan(&res.Processed)
			}
		}
		processed <- err
	}()
	// 每条返回路径都要等它结束：它还在用快照 sn，release 之后快照里的头部行与分段文件可能被删掉
	wait := sync.OnceValue(func() error { return <-processed })
	defer wait()
	rows, err := s.db.QueryContext(ctx, q, append(cteArgs, stmt.Args...)...)
	if err != nil {
		return nil, sqlError(stmt, err)
	}
	defer rows.Close()
	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	dbTypes := make([]string, len(cts))
	for i, ct := range cts {
		res.Keys = append(res.Keys, ct.Name())
		dbTypes[i] = ct.DatabaseTypeName()
		t := slsType(dbTypes[i])
		if i < len(stmt.Types) && stmt.Types[i] != "" { // 翻译器推断出的类型更准（varchar(n)、unknown 等）
			t = stmt.Types[i]
		}
		res.Types = append(res.Types, t)
	}
	// SLS 在每行最后多带 __source__ 与 __time__（不在 keys 里）。结果里已有 __source__ 列时不再重复；
	// 已有 __time__ 列时改名为 __time___0（实测如此）
	var tail strings.Builder
	if !slices.Contains(res.Keys, "__source__") {
		tail.WriteString(`,"__source__":""`)
	}
	timeKey := "__time__"
	if slices.Contains(res.Keys, timeKey) {
		timeKey = "__time___0"
	}
	tail.WriteString(`,"` + timeKey + `":"` + strconv.FormatInt(p.From, 10) + `"`)
	tail.WriteByte('}')
	vals := make([]any, len(cts))
	ptrs := make([]any, len(cts))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		var b bytes.Buffer
		b.WriteByte('{')
		for i, v := range vals {
			if i > 0 {
				b.WriteByte(',')
			}
			writeString(&b, res.Keys[i])
			b.WriteByte(':')
			writeString(&b, formatValue(v, dbTypes[i]))
		}
		if t := tail.String(); len(vals) == 0 && len(t) > 1 {
			b.WriteString(t[1:])
		} else {
			b.WriteString(t)
		}
		res.Rows = append(res.Rows, b.Bytes())
	}
	if err := rows.Err(); err != nil {
		return nil, sqlError(stmt, err)
	}
	if err := wait(); err != nil {
		return nil, sqlError(stmt, err)
	}
	return res, nil
}

func sqlError(stmt *query.SQLStmt, err error) error {
	if e := stmt.FuncNotRegistered(err); e != nil {
		return e
	}
	return queryError(err)
}

// slsType 把 DuckDB 的类型名换成 SLS 返回的列类型名。
func slsType(t string) string {
	if elem, ok := strings.CutSuffix(t, "[]"); ok {
		return "array(" + elemType(elem) + ")"
	}
	if inner, ok := strings.CutPrefix(t, "MAP("); ok && strings.HasSuffix(inner, ")") {
		k, v, _ := strings.Cut(inner[:len(inner)-1], ", ")
		return "map(" + elemType(k) + "," + elemType(v) + ")"
	}
	switch {
	case t == "BIGINT", t == "INTEGER", t == "SMALLINT", t == "TINYINT", t == "HUGEINT",
		t == "UBIGINT", t == "UINTEGER", t == "USMALLINT", t == "UTINYINT", t == "UHUGEINT":
		return "long"
	case t == "DOUBLE", t == "FLOAT", strings.HasPrefix(t, "DECIMAL"):
		return "double"
	case strings.HasPrefix(t, "TIMESTAMP"):
		return "timestamp"
	}
	return strings.ToLower(t)
}

// elemType 是数组、map 里的元素类型名：与顶层不同，整数保留具体宽度（array(integer)、map(varchar,bigint)）。
func elemType(t string) string {
	switch t {
	case "INTEGER", "SMALLINT", "TINYINT":
		return "integer"
	case "BIGINT", "UBIGINT", "UINTEGER", "HUGEINT":
		return "bigint"
	}
	return slsType(t)
}

// formatValue 按 SLS 的写法把一个值转成字符串。
func formatValue(v any, dbType string) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case int8, int16, int32, int64, uint8, uint16, uint32, uint64, int, uint:
		return fmt.Sprint(x)
	case *big.Int:
		return x.String()
	case float64:
		return formatFloat(x)
	case float32:
		return formatFloat(float64(x))
	case duckdb.Decimal:
		return formatFloat(x.Float64())
	case time.Time:
		if dbType == "DATE" {
			return x.Format("2006-01-02")
		}
		return x.Format("2006-01-02 15:04:05.000")
	case []any:
		elem, _ := strings.CutSuffix(dbType, "[]")
		parts := make([]string, len(x))
		for i, e := range x {
			if s, ok := e.(string); ok {
				b, _ := json.Marshal(s)
				parts[i] = string(b)
			} else if f, ok := e.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
				parts[i] = `"` + formatFloat(f) + `"` // SLS 在数组里把它们写成字符串
			} else {
				parts[i] = formatValue(e, elem)
			}
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// formatFloat 与 SLS 一致：Grisu2 生成数字（dtoa.go），整数值补 ".0"（1 → "1.0"，1e20 → "100000000000000000000.0"）。
func formatFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return formatGrisu(f)
}
