package query

import (
	"errors"
	"strings"
	"testing"

	"github.com/nethinwei/sls-local/internal/fieldcol"
	"github.com/nethinwei/sls-local/internal/index"
)

func TestTranslateSQL(t *testing.T) {
	ix := devIndex(t)
	str := fieldcol.Ref // 字段列（存储层提供，CTE 里原样带出）
	long := "TRY_CAST(" + str("cost_ms") + " AS BIGINT)"
	cases := []struct{ in, want string }{
		// 没写 FROM、LIMIT：补上 FROM log 与 LIMIT 100；没有别名的列补列名
		{" select count(*)", ` select count(*) AS "_col0" FROM log LIMIT 100`},
		{" select level", ` select ` + str("level") + ` AS "level" FROM log LIMIT 100`},
		{" select count(*) as c from log where cost_ms > 100", ` select count(*) as "c" from log where ` + long + ` > 100 LIMIT 100`},
		// 看板的写法：别名在 group by / order by 里保留，字段在 select、where 里改写
		{" select ((__time__) - (__time__) % 60) as time, level, count(*) as count from log where service_name in ('a') group by time, level order by time limit 10000",
			` select ((__sls_t) - (__sls_t) % 60) as "time", ` + str("level") + ` AS "level", count(*) as "count" from log where ` +
				str("service_name") + ` in ('a') group by "time", ` + str("level") + ` order by "time" limit 10000`},
		// 插件的日志量查询："level" as level
		{` select __time__ as time, "level" as level, count(*) as count group by time, level order by time limit 100000`,
			` select __sls_t as "time", ` + str("level") + ` as "level", count(*) as "count" FROM log group by "time", "level" order by "time" limit 100000`},
		{" select date_format(from_unixtime(__time__), '%Y-%m-%d %H:%i:%s') as d limit 1",
			` select strftime(sls_from_unixtime(__sls_t), '%Y-%m-%d %H:%M:%S') as "d" FROM log limit 1`},
		{" select approx_percentile(cost_ms, array[0.5, 0.9]) as p, approx_distinct(trace_id) as u",
			` select approx_quantile(` + long + `, array[0.5, 0.9]) as "p", approx_count_distinct(` + str("trace_id") + `) as "u" FROM log LIMIT 100`},
		{" select cast(cost_ms as varchar) as s, extract(hour from from_unixtime(__time__)) as h, current_date as d",
			` select cast(` + long + ` as varchar) as "s", extract(hour from sls_from_unixtime(__sls_t)) as "h", CAST(sls_now() AS DATE) as "d" FROM log LIMIT 100`},
		{" set session mode=scan; select count(*) as c", ` select count(*) as "c" FROM log LIMIT 100`},
		{" select __topic__, __source__ limit 1", ` select NULLIF(__sls_topic, '') AS "__topic__", __sls_source AS "__source__" FROM log limit 1`},
		{" select c from (select count(*) as c from log)", ` select "c" AS "c" from (select count(*) as "c" from log) LIMIT 100`},
	}
	for _, c := range cases {
		got, err := TranslateSQL(c.in, ix)
		if err != nil {
			t.Errorf("TranslateSQL(%q): %v", c.in, err)
			continue
		}
		if got.SQL != c.want {
			t.Errorf("TranslateSQL(%q)\n got  %s\n want %s", c.in, got.SQL, c.want)
		}
	}
}

// 报错原文来自真实 SLS（docs/wire-findings.md）；列号从 "|" 之后算起。
func TestTranslateSQLErrors(t *testing.T) {
	ix := devIndex(t)
	cases := map[string]string{
		" select product_id limit 1": "line 1:9: Column 'product_id' cannot be resolved; please add the column in the index attribute",
		" select nosuch limit 1":     "line 1:9: Column 'nosuch' cannot be resolved; please add the column in the index attribute",
		` select "__tag__:__path__" limit 1`: `line 1:9: Column '__tag__:__path__' cannot be resolved; it seems __tag__:__path__ is wrapped by "; ` +
			`if __tag__:__path__ is a string rather than a column, please use '__tag__:__path__'`,
	}
	for in, want := range cases {
		_, err := TranslateSQL(in, ix)
		if err == nil || err.Error() != want {
			t.Errorf("TranslateSQL(%q) = %v\nwant %s", in, err, want)
		}
	}
}

// 所有 project 共用一张表：语句只能读到 FROM log（CTE），任何别的表、表函数、改配置都要拒绝。
func TestTranslateSQLIsolation(t *testing.T) {
	ix := index.Auto()
	notExist := []string{
		" select count(*) from logs",
		" select count(*) from log as l, logs",
		" select count(*) from log l join logstores on true",
		" select count(*) from query('select * from logs')",
		" select count(*) from (select count(*) from logs)",
		" select count(*) from range(10)",
		" select count(*) from read_csv('/etc/hosts')",
	}
	for _, in := range notExist {
		_, err := TranslateSQL(in, ix)
		var ne *LogstoreNotExistError
		if !errors.As(err, &ne) {
			t.Errorf("TranslateSQL(%q) = %v, want LogstoreNotExist", in, err)
		}
	}
	rejected := []string{
		" select (from logs)",
		" select count(*) order by (from logs)",
		" select 1; drop table logs",
		" drop table logs",
		" select *",
		" select count(*) from (select * from log)",
		" select log.level from log",
		" pragma database_list",
	}
	for _, in := range rejected {
		if _, err := TranslateSQL(in, ix); err == nil {
			t.Errorf("TranslateSQL(%q): want error", in)
		}
	}
	// 内部列名（contents、project）只会被当成字段名
	got, err := TranslateSQL(" select contents, project", ix)
	if err != nil || strings.Contains(got.SQL, " contents") || strings.Contains(got.SQL, " project") {
		t.Errorf("internal columns: %v %v", got, err)
	}
}

func TestTranslateDateFormat(t *testing.T) {
	if got := translateDateFormat("'%Y-%m-%d %H:%i:%s.%f %T'"); got != "'%Y-%m-%d %H:%M:%S.%f %H:%M:%S'" {
		t.Fatal(got)
	}
}
