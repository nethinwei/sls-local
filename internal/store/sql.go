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
	"time"

	"github.com/duckdb/duckdb-go/v2"

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
	base, baseArgs := p.where()
	q := query.CTE(base) + stmt.SQL
	rows, err := s.db.QueryContext(ctx, q, append(baseArgs, stmt.Args...)...)
	if err != nil {
		return nil, sqlError(stmt, err)
	}
	defer rows.Close()
	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	res := &SQLResult{}
	dbTypes := make([]string, len(cts))
	for i, ct := range cts {
		res.Keys = append(res.Keys, ct.Name())
		dbTypes[i] = ct.DatabaseTypeName()
		res.Types = append(res.Types, slsType(dbTypes[i]))
	}
	// SLS 在每行最后多带 __source__ 与 __time__（不在 keys 里；结果里已有同名列时不再重复），照抄
	var tail strings.Builder
	for _, kv := range [][2]string{{"__source__", ""}, {"__time__", strconv.FormatInt(p.From, 10)}} {
		if !slices.Contains(res.Keys, kv[0]) {
			tail.WriteString(`,"` + kv[0] + `":"` + kv[1] + `"`)
		}
	}
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
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM logs WHERE `+base, baseArgs...).Scan(&res.Processed); err != nil {
		return nil, queryError(err)
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
		inner := slsType(elem)
		if inner == "long" {
			inner = "bigint"
		}
		return "array(" + inner + ")"
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

// formatFloat 与 SLS 一致：不用科学计数法，整数值补 ".0"（1 → "1.0"，1e20 → "100000000000000000000.0"）。
func formatFloat(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}
