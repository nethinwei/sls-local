package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/store"
)

// 端到端测试用官方 Go SDK 当客户端：SDK 把请求发往 {project}.{endpoint}，
// 这里让它的 HTTP 客户端无论域名都连到测试服务。
// devstore 用一个真实 SLS logstore 的索引（testdata/index/sample-logstore.json），期望结果来自
// 同一组查询在真实 SLS 上的行为（docs/wire-findings.md）；autostore 没有配置索引。
func newTestEnv(t *testing.T) *sls.Client {
	t.Helper()
	st, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	b, err := os.ReadFile("../../testdata/index/sample-logstore.json")
	if err != nil {
		t.Fatal(err)
	}
	var devIndex sls.Index
	if err := json.Unmarshal(b, &devIndex); err != nil {
		t.Fatal(err)
	}
	h := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), map[string]*sls.Index{"devproj/devstore": &devIndex})
	h.now = func() time.Time { return base.Add(time.Hour) } // 测试数据在 base 附近：自动索引只看最近一天写入的字段
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	client := &sls.Client{Endpoint: "sls-local:80", AccessKeyID: "local", AccessKeySecret: "local"}
	client.SetHTTPClient(&http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}})
	return client
}

func ptr[T any](v T) *T { return &v }

var base = time.Unix(1794636889, 123000000) // 不对齐到 10 秒，直方图会有首尾两个不完整的桶

// demoLogs 生成第 from..to-1 条日志（第 i 条在 base+i 秒），字段是常见 Go 服务的 JSON 日志。
func demoLogs(from, to int, packID string) *sls.LogGroup {
	g := &sls.LogGroup{
		Topic:  ptr(""),
		Source: ptr("10.0.0.1"),
		LogTags: []*sls.LogTag{
			{Key: ptr("__path__"), Value: ptr("/logs/order_api.log")},
		},
	}
	if packID != "" {
		g.LogTags = append(g.LogTags, &sls.LogTag{Key: ptr("__pack_id__"), Value: ptr(packID)})
	}
	for i := from; i < to; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		kv := func(k, v string) *sls.LogContent { return &sls.LogContent{Key: ptr(k), Value: ptr(v)} }
		level, msg := "INFO", "request received"
		if i%10 == 0 {
			level = "ERROR"
		}
		if i%2 == 0 {
			msg = "order paid"
		}
		contents := []*sls.LogContent{
			kv("level", level), kv("msg", msg), kv("service_name", "order_api"),
			kv("trace_id", fmt.Sprintf("trace%03d", i)), kv("cost_ms", fmt.Sprint(i)),
			kv("elapsed_ms", fmt.Sprintf("%.1f", float64(i)/10)), kv("product_id", fmt.Sprint(2000+i%20)),
		}
		if i%2 == 1 {
			contents = append(contents, kv("uid", fmt.Sprint(100+i)))
		}
		g.Logs = append(g.Logs, &sls.Log{Time: ptr(uint32(ts.Unix())), TimeNs: ptr(uint32(ts.Nanosecond())), Contents: contents})
	}
	return g
}

type logsResult struct {
	meta sls.GetLogsV3ResponseMeta
	rows []map[string]string
	keys [][]string
}

// getLogs 发 GetLogsV3 原始请求，返回每行的内容与键顺序（插件就是这样读的）。
func getLogs(c *sls.Client, logstore string, req *sls.GetLogRequest) (*logsResult, error) {
	body, _ := json.Marshal(req)
	p, _ := c.GetProject("devproj")
	r, err := p.RawRequest("POST", "/logstores/"+logstore+"/logs", map[string]string{
		"x-log-bodyrawsize": fmt.Sprint(len(body)), "Content-Type": "application/json",
	}, body)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	raw, _ := io.ReadAll(r.Body)
	var out struct {
		Meta sls.GetLogsV3ResponseMeta `json:"meta"`
		Data []json.RawMessage         `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode: %w\n%s", err, raw)
	}
	res := &logsResult{meta: out.Meta}
	for _, d := range out.Data {
		var m map[string]string
		if err := json.Unmarshal(d, &m); err != nil {
			return nil, err
		}
		res.rows = append(res.rows, m)
		res.keys = append(res.keys, orderedKeys(d))
	}
	return res, nil
}

func mustGetLogs(t *testing.T, c *sls.Client, logstore string, req *sls.GetLogRequest) *logsResult {
	t.Helper()
	res, err := getLogs(c, logstore, req)
	if err != nil {
		t.Fatalf("GetLogsV3(%q): %v", req.Query, err)
	}
	return res
}

func orderedKeys(obj []byte) []string {
	dec := json.NewDecoder(strings.NewReader(string(obj)))
	var keys []string
	if _, err := dec.Token(); err != nil {
		return nil
	}
	for dec.More() {
		k, _ := dec.Token()
		keys = append(keys, k.(string))
		if _, err := dec.Token(); err != nil {
			return nil
		}
	}
	return keys
}

// contextLogs 调 SDK 的 GetContextLogs（它挂在 LogStore 上）。
func contextLogs(t *testing.T, c *sls.Client, back, forward int32, packID, packMeta string) (*sls.GetContextLogsResponse, error) {
	t.Helper()
	ls, err := c.GetLogStore("devproj", "devstore")
	if err != nil {
		t.Fatal(err)
	}
	return ls.GetContextLogs(back, forward, packID, packMeta)
}

func slsError(err error) *sls.Error {
	var se *sls.Error
	if errors.As(err, &se) {
		return se
	}
	return nil
}

func TestEndToEnd(t *testing.T) {
	c := newTestEnv(t)
	for _, ls := range []string{"devstore", "autostore"} {
		if err := c.PutLogs("devproj", ls, demoLogs(0, 125, "ABC-1")); err != nil {
			t.Fatalf("PutLogs: %v", err)
		}
		if err := c.PutLogs("devproj", ls, demoLogs(125, 250, "ABC-2")); err != nil {
			t.Fatalf("PutLogs: %v", err)
		}
	}
	from, to := base.Unix(), base.Add(250*time.Second).Unix()+1
	count := func(t *testing.T, logstore, q string) (int64, error) {
		// 命中总数用直方图数：GetLogsV3 的 processedRows 只是这次返回的行数（与 SLS 一致）
		h, err := c.GetHistograms("devproj", logstore, "", from, to, q)
		if err != nil {
			return 0, err
		}
		return h.Count, nil
	}

	t.Run("logstore", func(t *testing.T) {
		names, err := c.ListLogStoreV2("devproj", 0, 10, "")
		if err != nil || strings.Join(names, ",") != "autostore,devstore" {
			t.Fatalf("ListLogStoreV2 = %v, %v", names, err)
		}
		if _, err := c.GetLogStore("devproj", "devstore"); err != nil {
			t.Fatalf("GetLogStore: %v", err)
		}
		_, err = c.GetLogStore("devproj", "nope")
		if se := slsError(err); se == nil || se.Code != "LogStoreNotExist" || se.HTTPCode != 404 {
			t.Fatalf("GetLogStore(nope) = %v", err)
		}
	})

	t.Run("newest first, field order, pack meta, keys", func(t *testing.T) {
		res := mustGetLogs(t, c, "devstore", &sls.GetLogRequest{From: from, To: to, Query: "* | with_pack_meta", Lines: 100, Reverse: true})
		if res.meta.Progress != "Complete" || res.meta.Count != 100 || len(res.rows) != 100 || res.meta.ProcessedRows != 100 {
			t.Fatalf("meta = %+v rows=%d", res.meta, len(res.rows))
		}
		if res.rows[0]["trace_id"] != "trace249" || res.rows[99]["trace_id"] != "trace150" {
			t.Fatalf("order: first %s last %s", res.rows[0]["trace_id"], res.rows[99]["trace_id"])
		}
		// __pack_meta__ 紧跟写入字段、在 __topic__ 之前（golden：spl/with-pack-meta）
		want := "level,msg,service_name,trace_id,cost_ms,elapsed_ms,product_id,uid,__pack_meta__,__topic__,__source__," +
			"__tag__:__path__,__tag__:__pack_id__,__time__,__time_ns_part__"
		if got := strings.Join(res.keys[0], ","); got != want {
			t.Fatalf("keys = %s\nwant %s", got, want)
		}
		if res.rows[0]["__time_ns_part__"] != "123000000" {
			t.Fatalf("ns = %s", res.rows[0]["__time_ns_part__"])
		}
		// meta.keys 也包含 __pack_meta__（golden：spl/with-pack-meta）
		wantKeys := "level,msg,service_name,trace_id,cost_ms,elapsed_ms,product_id,uid,__pack_meta__,__topic__,__source__,__tag__:__path__,__tag__:__pack_id__"
		if got := strings.Join(res.meta.Keys, ","); got != wantKeys {
			t.Fatalf("meta.keys = %s", got)
		}
	})

	t.Run("paging", func(t *testing.T) {
		res := mustGetLogs(t, c, "devstore", &sls.GetLogRequest{From: from, To: to, Query: "*", Lines: 100, Offset: 200, Reverse: true})
		if len(res.rows) != 50 || res.rows[49]["trace_id"] != "trace000" {
			t.Fatalf("rows = %d", len(res.rows))
		}
		if _, ok := res.rows[0]["__pack_meta__"]; ok {
			t.Fatal("__pack_meta__ without | with_pack_meta")
		}
		res = mustGetLogs(t, c, "devstore", &sls.GetLogRequest{From: from, To: to, Query: "*", Lines: 5})
		if res.rows[0]["trace_id"] != "trace000" {
			t.Fatalf("ascending first = %s", res.rows[0]["trace_id"])
		}
	})

	// 期望来自真实 SLS 对同类语句的行为（docs/wire-findings.md）
	t.Run("search semantics", func(t *testing.T) {
		cases := map[string]int64{
			"level: ERROR":                             25,
			"Level: error":                             25, // 字段名、值都不区分大小写
			"level:ERROR":                              25,
			"level: ERROR and trace_id: trace010":      1,
			"trace_id: trace010 or trace_id: trace011": 2,
			"not level: ERROR":                         225,
			"level: INFO not msg: paid":                125, // 二元 not
			"paid":                                     125,
			"order paid":                               125, // 裸词相邻 = and
			"paid order":                               125,
			`msg: "order paid"`:                        125,
			`msg: "paid order"`:                        125, // 引号里的各词都出现即可，不管顺序（实测）
			`msg: #"paid order"`:                       0,   // 短语 #"..." 要求顺序
			`msg: #"order paid"`:                       125,
			"msg: pai*":                                125,
			"msg: p*d":                                 125,
			`msg: "pai*"`:                              0,  // 引号内 * 是字面量
			"2005":                                     13, // 全文覆盖没有字段索引的 product_id
			"cost_ms > 200":                            49,
			"cost_ms >= 200":                           50,
			"cost_ms = 68":                             1,
			"cost_ms: 68":                              1,
			"cost_ms in [50 60]":                       11,
			"cost_ms in (50 60]":                       10,
			"cost_ms: abc":                             0,
			"cost_ms: 6*":                              0,
			"elapsed_ms > 24.5":                        4,
			"level > a":                                0, // text 字段比较：不报错、匹配不到
			"uid: *":                                   125,
			"__tag__:__path__: order_api.log":          0, // tag 没有索引：不报错、匹配不到
			"__source__: 10.0.0.1":                     250,
			"__topic__: x":                             0,
			"* | where cost_ms > '50'":                 53, // 字段是 varchar：按字符串比较
			"* | where cast(cost_ms as bigint) > 50":   199,
			"* | where level = 'info'":                 0, // SPL 区分大小写
			"* | where level = 'INFO'":                 225,
			"* | where nosuchkey is null and level = 'INFO'":                 225,
			"* | where __time__ > 0":                                         250,
			"* | where level = 'INFO' | where cast(cost_ms as bigint) > 200": 45,
			`* | where "__tag__:__path__" like '%order_api%'`:                250,
			"level: ERROR | where regexp_like(trace_id, '^trace0[0-4]')":     5,
		}
		for q, want := range cases {
			got, err := count(t, "devstore", q)
			if err != nil {
				t.Errorf("%q: %v", q, err)
			} else if got != want {
				t.Errorf("%q matched %d, want %d", q, got, want)
			}
		}
		// 自动索引：所有字段都可检索（包括 tag）
		for q, want := range map[string]int64{"product_id: 2005": 13, "__tag__:__path__: order_api.log": 250} {
			if got, err := count(t, "autostore", q); err != nil || got != want {
				t.Errorf("autostore %q matched %d, %v; want %d", q, got, err, want)
			}
		}
	})

	t.Run("errors match SLS", func(t *testing.T) {
		notIndexed := `key (%s) is not config as key value config,if symbol : is  in your log,please wrap : with quotation mark "`
		cases := map[string]string{
			"nosuchkey: x":              fmt.Sprintf(notIndexed, "nosuchkey"),
			"product_id: 2005":          fmt.Sprintf(notIndexed, "product_id"),
			"__tag__:__path__: *order*": "only support * or ?  in the  middle or end  of the query",
			"level: INFO msg: paid":     "parse search query error,please read query syntax document,error detail:syntax error, unexpected TERM, expecting $end error position is from column:13 to column:16,error near < msg >",
		}
		for q, want := range cases {
			_, err := count(t, "devstore", q)
			se := slsError(err)
			if se == nil || se.HTTPCode != 400 || se.Code != "ParameterInvalid" || se.Message != want {
				t.Errorf("%q: err = %v\nwant ParameterInvalid %s", q, err, want)
			}
		}
		// SPL 的表达式只针对一行，不能有子查询（读不到其他日志）
		if _, err := count(t, "devstore", "* | where (select 1) = 1"); slsError(err) == nil || slsError(err).Code != "ParameterInvalid" {
			t.Errorf("subquery in where: err = %v", err)
		}
		// 与 SLS 一样，SPL 里 varchar 与整数比较是 Presto 的函数签名错误（golden：spl/where-long）
		_, err := count(t, "devstore", "* | where cost_ms > 50")
		wantPrefix := "invalid query: line 1:4: Scalar function signature is not supported: gt(VARCHAR, INTEGER). Supported signatures: (uuid,uuid) -> boolean"
		if se := slsError(err); se == nil || se.Code != "ParameterInvalid" || !strings.HasPrefix(se.Message, wantPrefix) {
			t.Errorf("varchar > int: err = %v", err)
		}
	})

	t.Run("time range is [from, to) in nanoseconds", func(t *testing.T) {
		at := base.Add(10 * time.Second)
		res := mustGetLogs(t, c, "devstore", &sls.GetLogRequest{
			From: at.Unix(), FromNsPart: int32(at.Nanosecond()), To: at.Unix() + 2, ToNsPart: int32(at.Nanosecond()), Query: "*"})
		if res.meta.ProcessedRows != 2 {
			t.Fatalf("matched %d, want 2", res.meta.ProcessedRows)
		}
	})

	t.Run("GetIndex", func(t *testing.T) {
		ix, err := c.GetIndex("devproj", "devstore")
		if err != nil || ix.Keys["cost_ms"].Type != "long" || ix.Line == nil {
			t.Fatalf("configured: %+v, %v", ix, err)
		}
		ix, err = c.GetIndex("devproj", "autostore")
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range []string{"level", "product_id", "uid", "__tag__:__path__"} {
			if f, ok := ix.Keys[k]; !ok || f.Type != "text" || !f.DocValue {
				t.Errorf("auto index key %q = %+v, %v", k, f, ok)
			}
		}
	})

	t.Run("GetHistograms", func(t *testing.T) {
		h, err := c.GetHistograms("devproj", "devstore", "", from, from+250, "level: ERROR")
		if err != nil {
			t.Fatal(err)
		}
		// 250 秒 → 间隔 10 秒；from 不对齐：首桶 [from, 下一个 10 秒边界)，末桶到 to 为止
		if h.Progress != "Complete" || h.Count != 25 || len(h.Histograms) != 26 {
			t.Fatalf("progress=%s count=%d buckets=%d", h.Progress, h.Count, len(h.Histograms))
		}
		first, last := h.Histograms[0], h.Histograms[25]
		if first.From != from || first.To != from-from%10+10 || last.To != from+250 || last.From != (from+250)-(from+250)%10 {
			t.Fatalf("first=%+v last=%+v", first, last)
		}
		var sum int64
		for i, b := range h.Histograms {
			sum += b.Count
			if i > 0 && b.From != h.Histograms[i-1].To {
				t.Fatalf("gap before bucket %d", i)
			}
		}
		if sum != 25 {
			t.Fatalf("sum = %d", sum)
		}
		for span, want := range map[int64]int64{30: 1, 120: 10, 900: 30, 7200: 300, 43200: 1800, 172800: 3600, 604800: 14400} {
			if got := histogramInterval(span); got != want {
				t.Errorf("interval(%d) = %d, want %d", span, got, want)
			}
		}
	})

	t.Run("GetContextLogs", func(t *testing.T) {
		res := mustGetLogs(t, c, "devstore", &sls.GetLogRequest{From: from, To: to, Query: "trace_id: trace124 | with_pack_meta", Lines: 1})
		anchor := res.rows[0]
		ctx, err := contextLogs(t, c, 3, 3, anchor["__tag__:__pack_id__"], anchor["__pack_meta__"])
		if err != nil {
			t.Fatal(err)
		}
		if ctx.BackLines != 3 || ctx.ForwardLines != 3 || len(ctx.Logs) != 7 {
			t.Fatalf("back=%d forward=%d logs=%d", ctx.BackLines, ctx.ForwardLines, len(ctx.Logs))
		}
		// 锚点是第一个 pack 的最后一条，后面三条来自下一个 pack（同一来源）
		for i, l := range ctx.Logs {
			if l["__index_number__"] != fmt.Sprint(i-3) || l["trace_id"] != fmt.Sprintf("trace%03d", 121+i) {
				t.Fatalf("row %d: index=%s trace=%s", i, l["__index_number__"], l["trace_id"])
			}
			if _, ok := l["__source__"]; ok {
				t.Fatal("context rows have no __source__")
			}
		}
		// 最新一条之后没有日志
		res = mustGetLogs(t, c, "devstore", &sls.GetLogRequest{From: from, To: to, Query: "trace_id: trace249 | with_pack_meta", Lines: 1})
		ctx, err = contextLogs(t, c, 2, 2, res.rows[0]["__tag__:__pack_id__"], res.rows[0]["__pack_meta__"])
		if err != nil || ctx.BackLines != 2 || ctx.ForwardLines != 0 || len(ctx.Logs) != 3 {
			t.Fatalf("newest: %+v, %v", ctx, err)
		}
		_, err = contextLogs(t, c, 0, 0, "x", "0|1|0|0")
		if se := slsError(err); se == nil || se.Code != "ParameterInvalid" {
			t.Fatalf("0/0 lines: %v", err)
		}
	})
}
