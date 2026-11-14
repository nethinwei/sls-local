package api

import (
	"strings"
	"testing"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

// SQL 的期望格式来自真实 SLS（docs/wire-findings.md）；语句取自常见的看板查询与插件。
func TestSQL(t *testing.T) {
	c := newTestEnv(t)
	for _, ls := range []string{"devstore", "autostore"} {
		if err := c.PutLogs("devproj", ls, demoLogs(0, 125, "ABC-1")); err != nil {
			t.Fatal(err)
		}
		if err := c.PutLogs("devproj", ls, demoLogs(125, 250, "ABC-2")); err != nil {
			t.Fatal(err)
		}
	}
	from, to := base.Unix(), base.Add(250*time.Second).Unix()+1
	run := func(t *testing.T, logstore, q string) *logsResult {
		t.Helper()
		return mustGetLogs(t, c, logstore, &sls.GetLogRequest{From: from, To: to, Query: q})
	}
	check := func(t *testing.T, res *logsResult, keys, types string) {
		t.Helper()
		if !res.meta.HasSQL || strings.Join(res.meta.Keys, ",") != keys || strings.Join(res.meta.ColumnTypes, ",") != types {
			t.Fatalf("hasSQL=%v keys=%v types=%v\nwant keys=%s types=%s", res.meta.HasSQL, res.meta.Keys, res.meta.ColumnTypes, keys, types)
		}
	}

	t.Run("aggregates and types", func(t *testing.T) {
		res := run(t, "devstore", "* | select count(*) as c, avg(cost_ms) as a, max(elapsed_ms) as m, sum(cost_ms) as s, min(level) as l")
		check(t, res, "c,a,m,s,l", "long,double,double,long,varchar")
		r := res.rows[0]
		if r["c"] != "250" || r["a"] != "124.5" || r["m"] != "24.9" || r["s"] != "31125" || r["l"] != "ERROR" {
			t.Fatalf("row = %v", r)
		}
		// 与 SLS 一样，每行末尾多带 __source__ 与 __time__（不在 keys 里）
		if r["__source__"] != "" || r["__time__"] == "" || res.meta.ProcessedRows != 250 {
			t.Fatalf("tail = %q %q processed=%d", r["__source__"], r["__time__"], res.meta.ProcessedRows)
		}
	})

	t.Run("dashboard: volume by level", func(t *testing.T) {
		res := run(t, "devstore", "* | select ((__time__) - (__time__) % 60) as time, level, count(*) as count from log "+
			"where service_name in ('order_api') and level in ('INFO', 'ERROR') group by time, level order by time limit 10000")
		check(t, res, "time,level,count", "long,varchar,long")
		var sum int
		for _, r := range res.rows {
			n := 0
			for _, ch := range r["count"] {
				n = n*10 + int(ch-'0')
			}
			sum += n
			if r["time"] == "" || r["level"] == "" {
				t.Fatalf("row = %v", r)
			}
		}
		if sum != 250 {
			t.Fatalf("sum = %d", sum)
		}
	})

	t.Run("plugin: logs volume query", func(t *testing.T) {
		res := run(t, "devstore", `* | select ((__time__) - (__time__) % 30) as time, "level" as level, count(*) as count group by time, level order by time limit 100000`)
		check(t, res, "time,level,count", "long,varchar,long")
		if len(res.rows) == 0 || res.rows[0]["time"] > res.rows[len(res.rows)-1]["time"] {
			t.Fatalf("rows = %v", res.rows)
		}
	})

	t.Run("dashboard: service list and error count", func(t *testing.T) {
		res := run(t, "devstore", "* | select service_name from log where service_name is not null group by service_name order by service_name limit 500")
		check(t, res, "service_name", "varchar")
		if len(res.rows) != 1 || res.rows[0]["service_name"] != "order_api" {
			t.Fatalf("rows = %v", res.rows)
		}
		res = run(t, "devstore", "level: ERROR | select count(*) as count from log where service_name in ('order_api')")
		if res.rows[0]["count"] != "25" || res.meta.ProcessedRows != 25 {
			t.Fatalf("count = %v processed=%d", res.rows[0], res.meta.ProcessedRows)
		}
	})

	t.Run("default limit 100", func(t *testing.T) {
		if res := run(t, "devstore", "* | select level"); len(res.rows) != 100 {
			t.Fatalf("rows = %d", len(res.rows))
		}
		if res := run(t, "devstore", "* | select level limit 150"); len(res.rows) != 150 {
			t.Fatalf("rows = %d", len(res.rows))
		}
	})

	t.Run("value formats", func(t *testing.T) {
		res := run(t, "devstore", "* | select 5/2 as d, null as n, true as b, 1.5 as x, cast(1 as double) as y, 1e20 as z limit 1")
		r := res.rows[0]
		want := map[string]string{"d": "2", "n": "null", "b": "true", "x": "1.5", "y": "1.0", "z": "100000000000000000000.0"}
		for k, v := range want {
			if r[k] != v {
				t.Errorf("%s = %q, want %q", k, r[k], v)
			}
		}
		if ts := strings.Join(res.meta.ColumnTypes, ","); !strings.HasPrefix(ts, "long,") || !strings.Contains(ts, ",boolean,") {
			t.Errorf("types = %s", ts)
		}
		res = run(t, "devstore", "* | select uid, __topic__, __source__ from log where cost_ms = 0")
		if r := res.rows[0]; r["uid"] != "null" || r["__topic__"] != "null" || r["__source__"] != "10.0.0.1" {
			t.Errorf("nulls = %v", r)
		}
	})

	t.Run("time functions in UTC+8", func(t *testing.T) {
		res := run(t, "devstore", "* | select from_unixtime(__time__) as ts, date_format(from_unixtime(__time__), '%Y-%m-%d %H:%i:%s') as d, "+
			"date_trunc('minute', from_unixtime(__time__)) as m, to_unixtime(from_unixtime(__time__)) as u from log where cost_ms = 0")
		check(t, res, "ts,d,m,u", "timestamp,varchar,timestamp,double")
		// base 是 UTC 06:14:49，东八区 14:14:49
		r := res.rows[0]
		if r["ts"] != "2026-11-14 14:14:49.000" || r["d"] != "2026-11-14 14:14:49" || r["m"] != "2026-11-14 14:14:00.000" || r["u"] != "1794636889.0" {
			t.Fatalf("row = %v", r)
		}
	})

	t.Run("approx functions", func(t *testing.T) {
		res := run(t, "devstore", "* | select approx_percentile(cost_ms, 0.5) as p, approx_percentile(cost_ms, array[0.5, 0.9]) as ps, approx_distinct(trace_id) as u")
		check(t, res, "p,ps,u", "long,array(bigint),long")
		if r := res.rows[0]; !strings.HasPrefix(r["ps"], "[") || r["p"] == "null" || r["u"] == "null" {
			t.Fatalf("row = %v", r)
		}
	})

	t.Run("auto index and set session", func(t *testing.T) {
		res := run(t, "autostore", "* | select product_id, count(*) as c group by product_id order by product_id limit 1")
		check(t, res, "product_id,c", "varchar,long")
		if r := res.rows[0]; r["product_id"] != "2000" || r["c"] != "13" {
			t.Fatalf("row = %v", r)
		}
		if res := run(t, "devstore", "* | set session mode=scan; select count(*) as c"); res.rows[0]["c"] != "250" {
			t.Fatalf("row = %v", res.rows[0])
		}
	})

	t.Run("errors match SLS", func(t *testing.T) {
		cases := map[string]struct {
			code, msg string
			status    int32
		}{
			"* | select product_id limit 1": {"ParameterInvalid", "line 1:9: Column 'product_id' cannot be resolved; please add the column in the index attribute", 400},
			"* | select foo(1) as x":        {"ParameterInvalid", "ErrorType:SyntaxError.ErrorPosition,line:1,column:9.ErrorMessage:line 1:9: Function foo not registered", 400},
			"* | select count(*) from logs": {"LogStoreNotExist", "logstore (logs) is not found", 404},
		}
		for q, want := range cases {
			_, err := getLogs(c, "devstore", &sls.GetLogRequest{From: from, To: to, Query: q})
			se := slsError(err)
			if se == nil || se.Code != want.code || se.Message != want.msg || se.HTTPCode != want.status {
				t.Errorf("%q: err = %v\nwant %d %s %s", q, err, want.status, want.code, want.msg)
			}
		}
	})

	// 所有 project、logstore 写进同一张表：SQL 只能读到自己的那份
	t.Run("isolation", func(t *testing.T) {
		if err := c.PutLogs("otherproj", "devstore", demoLogs(0, 125, "XYZ-1")); err != nil {
			t.Fatal(err)
		}
		for _, ls := range []string{"devstore", "autostore"} {
			if res := run(t, ls, "* | select count(*) as c"); res.rows[0]["c"] != "250" {
				t.Fatalf("%s count = %v", ls, res.rows[0])
			}
		}
	})
}

// docs/compatibility.md 列出的函数与写法都要真的能跑。
func TestDocumentedSQLFunctions(t *testing.T) {
	c := newTestEnv(t)
	if err := c.PutLogs("devproj", "devstore", demoLogs(0, 20, "ABC-1")); err != nil {
		t.Fatal(err)
	}
	from, to := base.Unix(), base.Add(20*time.Second).Unix()+1
	exprs := []string{
		"from_unixtime(__time__)", "to_unixtime(from_unixtime(__time__))", "now()", "current_timestamp", "current_date",
		"date_format(from_unixtime(__time__), '%Y-%m-%d %H:%i:%s')", "date_parse('2026-11-14 10:00:00', '%Y-%m-%d %H:%i:%s')",
		"date_trunc('hour', from_unixtime(__time__))", "date_diff('second', from_unixtime(__time__), now())",
		"extract(hour from from_unixtime(__time__))",
		"approx_percentile(cost_ms, 0.9)", "approx_distinct(trace_id)", "regexp_like(msg, 'paid')",
		"json_extract_scalar('{\"a\":\"b\"}', '$.a')", "cardinality(array[1, 2])",
		"count(*)", "sum(cost_ms)", "avg(cost_ms)", "min(level)", "max(level)", "count_if(cost_ms > 5)",
		"arbitrary(level)", "max_by(trace_id, cost_ms)", "min_by(trace_id, cost_ms)",
		"if(cost_ms > 5, 'big', 'small')", "coalesce(uid, 'none')", "round(avg(elapsed_ms), 1)",
		"split_part(msg, ' ', 1)", "strpos(msg, 'paid')", "regexp_extract(trace_id, '[0-9]+')",
		"lower(level)", "upper(msg)", "length(msg)", "concat(level, '-', msg)",
		"cast(cost_ms as varchar)", "try_cast(msg as bigint)",
	}
	aggregates := map[string]bool{}
	for _, e := range []string{"approx_percentile", "approx_distinct", "count(", "sum(", "avg(", "min(", "max(", "count_if", "arbitrary", "max_by", "min_by"} {
		aggregates[e] = true
	}
	for _, e := range exprs {
		q := "* | select " + e + " as v from log limit 1"
		for a := range aggregates {
			if strings.Contains(e, a) {
				q = "* | select " + e + " as v from log"
			}
		}
		if _, err := getLogs(c, "devstore", &sls.GetLogRequest{From: from, To: to, Query: q}); err != nil {
			t.Errorf("%s: %v", e, err)
		}
	}
	// 窗口函数、子查询、having
	for _, q := range []string{
		"* | select trace_id, row_number() over (order by __time__) as n from log limit 3",
		"* | select c from (select level, count(*) as c from log group by level) where c > 1",
		"* | select level, count(*) as c from log group by level having count(*) > 1 order by c desc",
	} {
		if _, err := getLogs(c, "devstore", &sls.GetLogRequest{From: from, To: to, Query: q}); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}
