package presto

import (
	"errors"
	"strings"
	"testing"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/index"
)

// 所有 project 的日志在同一张表里：SQL 只能读到 log（store 建的 CTE，只含当前 logstore）。
// 任何能引用到其他表、执行任意 SQL 的写法都必须被拒绝。
func TestIsolation(t *testing.T) {
	ix := index.Auto()
	notFound := []string{
		" select count(*) from logs",
		` select count(*) from "logs"`,
		" select count(*) from main.logs",
		" select count(*) from log as l, logs",
		" select count(*) from log l join logstores on true",
		" select count(*) from (select count(*) from logs)",
		" select (select count(*) from logs) as x",
		" select exists (select 1 from logs) as x",
		" select level from log union all select contents from logs",
		" with logs as (select count(*) as c from logs) select c from logs",
		" select count(*) over (rows between (select count(*) from logs) preceding and current row) as x",
	}
	for _, in := range notFound {
		_, err := Translate(in, ix, "store")
		var nf *NotFoundError
		if !errors.As(err, &nf) {
			t.Errorf("Translate(%q) = %v, want NotFoundError", in, err)
		}
	}
	rejected := []string{
		" select count(*) from query('select * from logs')",
		" select count(*) from read_csv('/etc/hosts')",
		" select read_text('/etc/passwd') as x",
		" select query('select 1') as x",
		" select 1; drop table logs",
		" drop table logs",
		" select * ",
		" pragma database_list",
		` select extract("hour from now()) from logs --" from __time__) as x`,
		" select cast(1 as decimal(1,2)) as x from log",
		" select current_setting('enable_external_access') as x",
		" select count(*) from range(10)",
		" select count(*) where level in (select level from logs)",
	}
	for _, in := range rejected {
		st, err := Translate(in, ix, "store")
		if err == nil && strings.Contains(strings.ToLower(st.SQL), "logs") {
			t.Errorf("Translate(%q) = %q, references another table", in, st.SQL)
		} else if err == nil && in != " select cast(1 as decimal(1,2)) as x from log" {
			t.Errorf("Translate(%q): want error, got %q", in, st.SQL)
		}
	}
	// 内部列名（contents、project）只会被当成字段名
	st, err := Translate(" select contents, project", ix, "store")
	if err != nil || strings.Contains(st.SQL, " contents") || strings.Contains(st.SQL, " project") {
		t.Errorf("internal columns: %v %v", st, err)
	}
}

func TestTranslate(t *testing.T) {
	ix := index.Auto()
	ok := []struct{ in, want string }{
		// 不写 AS 的别名、乘号、子查询的列、CTE、集合运算（golden 用例里这些在旧的逐记号改写上都失败过）
		{" select count(*) c", `SELECT count(*) AS "c" FROM log LIMIT 100`},
		{" select count(*) * 2 as x", `SELECT (count(*) * 2) AS "x" FROM log LIMIT 100`},
		{" select c from (select count(*) as c from log)", `SELECT "__sls_sq1"."c" AS "c" FROM (SELECT count(*) AS "c" FROM log) AS "__sls_sq1" LIMIT 100`},
		{" with t as (select 1 as a from log) select a from t", `WITH "t" AS (SELECT * FROM (SELECT 1 AS "a" FROM log) AS __sls_cte("a")) SELECT "t"."a" AS "a" FROM "t" AS "t" LIMIT 100`},
		// SLS 只给第一个 select 补 from 子句（insertedSQL 只有一处）
		{" select 1 as a union all select 2 as a", `(SELECT 1 AS "a" FROM log) UNION ALL (SELECT 2 AS "a") LIMIT 100`},
		{" select count(*) as c group by level order by c desc limit 5", `SELECT count(*) AS "c" FROM log GROUP BY "__f_6c6576656c" ORDER BY "c" DESC NULLS LAST LIMIT 5`},
		// 原文里有 from（extract 里的）时 SLS 不补 from 子句：只算一次
		{" select extract(hour from from_unixtime(0)) as h", `SELECT extract(hour FROM sls_from_unixtime(0)) AS "h" LIMIT 100`},
	}
	for _, c := range ok {
		st, err := Translate(c.in, ix, "store")
		if err != nil {
			t.Errorf("Translate(%q): %v", c.in, err)
			continue
		}
		if st.SQL != c.want {
			t.Errorf("Translate(%q)\n got %s\nwant %s", c.in, st.SQL, c.want)
		}
	}
}

// 报错文案与列号与 SLS 一致（列号按 SLS 改写后的 SQL：log 换成 "<logstore>"、补上 from 子句）。
func TestErrors(t *testing.T) {
	ix := index.FromConfig(mustIndex(t))
	cases := []struct{ in, want string }{
		{" select nonexist limit 1", "line 1:9: Column 'nonexist' cannot be resolved; please add the column in the index attribute"},
		{" select foo(1) as x", "ErrorType:SyntaxError.ErrorPosition,line:1,column:9.ErrorMessage:line 1:9: Function foo not registered"},
		{" select count(*) as c where n > '100'", "ErrorType:SyntaxError.ErrorPosition,line:1,column:31.ErrorMessage:line 1:45: '>' cannot be applied to bigint, varchar(3)"},
		{" select level from log group by level order by level limit 1 offset 1", "ErrorType:SyntaxError.ErrorPosition,line:1,column:66.ErrorMessage:line 1:66: mismatched input 'offset' expecting <EOF>"},
	}
	for _, c := range cases {
		_, err := Translate(c.in, ix, "store")
		if err == nil || err.Error() != c.want {
			t.Errorf("Translate(%q)\n got %v\nwant %s", c.in, err, c.want)
		}
	}
	st, err := Translate(" select count(*) as c where n > 1", ix, "store")
	if err != nil || st.InsertAt != 22 {
		t.Errorf("InsertAt = %v, %v; want 22", st, err)
	}
}

func mustIndex(t *testing.T) *sls.Index {
	t.Helper()
	return &sls.Index{Keys: map[string]sls.IndexKey{
		"level": {Type: "text", DocValue: true},
		"n":     {Type: "long", DocValue: true},
	}}
}
