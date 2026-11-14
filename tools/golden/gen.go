package main

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"

	"github.com/nethinwei/sls-local/internal/golden"
)

// stream 模拟一个采集源（一台主机上的一个文件），它的日志按包写入。
type stream struct {
	topic, source, host, path string
	extraTags                 [][2]string
	packPrefix                string
	kinds                     []kind
}

// kind 是一类日志：服务、级别、消息与字段。fields 按写入顺序返回字段。
type kind struct {
	weight int
	gen    func(r *rand.Rand) [][2]string
}

// generate 生成确定性的数据集：同样的代码永远得到同样的数据。改动生成逻辑后要重新录制。
func generate() *golden.Dataset {
	r := rand.New(rand.NewPCG(20261114, 1))
	hex := func(n int) string {
		const digits = "0123456789abcdef"
		b := make([]byte, n)
		for i := range b {
			b[i] = digits[r.IntN(16)]
		}
		return string(b)
	}
	trace := func() [][2]string { return [][2]string{{"trace_id", hex(32)}, {"span_id", hex(16)}} }
	order := func() string { return fmt.Sprintf("ORD%08d", r.IntN(100000000)) }
	latency := func() string { return fmt.Sprint(int(r.ExpFloat64()*120) + 1) }
	cat := func(parts ...[][2]string) [][2]string {
		var out [][2]string
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	f := func(kv ...string) [][2]string {
		var out [][2]string
		for i := 0; i+1 < len(kv); i += 2 {
			out = append(out, [2]string{kv[i], kv[i+1]})
		}
		return out
	}

	checkout := []kind{
		{30, func(r *rand.Rand) [][2]string {
			return cat(f("level", "INFO", "msg", "request completed", "service", "checkout",
				"method", []string{"GET", "POST"}[r.IntN(2)], "url", fmt.Sprintf("/api/v1/orders/%d", r.IntN(500)),
				"http_status", []string{"200", "200", "200", "201", "404", "500"}[r.IntN(6)], "latency_ms", latency()), trace())
		}},
		{15, func(r *rand.Rand) [][2]string {
			return cat(f("level", "INFO", "msg", "order created", "service", "checkout", "order_id", order(),
				"user_id", fmt.Sprint(10000+r.IntN(90000)), "amount", fmt.Sprintf("%d.%02d", r.IntN(500), r.IntN(100)), "currency", []string{"USD", "EUR", "JPY"}[r.IntN(3)]), trace())
		}},
		{10, func(r *rand.Rand) [][2]string {
			return cat(f("level", "INFO", "msg", "payment authorized", "service", "checkout", "order_id", order(),
				"latency_ms", latency(), "ratio", fmt.Sprintf("%.3f", r.Float64())), trace())
		}},
		{5, func(r *rand.Rand) [][2]string {
			return cat(f("level", "ERROR", "msg", "payment failed: card declined", "service", "checkout", "order_id", order(),
				"error", []string{"card declined", "insufficient funds", "issuer unavailable"}[r.IntN(3)], "latency_ms", latency(), "code", []string{"AbC-12", "abc-12", "XYZ-9"}[r.IntN(3)]), trace())
		}},
		{4, func(r *rand.Rand) [][2]string {
			return cat(f("level", "WARN", "msg", "upstream timeout", "service", "checkout", "upstream", "payments",
				"timeout_ms", "3000", "latency_ms", fmt.Sprint(3000+r.IntN(200))), trace())
		}},
		{3, func(r *rand.Rand) [][2]string {
			return cat(f("level", "WARN", "msg", "slow query", "service", "checkout", "sql", "select * from orders where id = ?",
				"elapsed_ms", fmt.Sprintf("%.3f", 200+r.Float64()*800), "rows", fmt.Sprint(r.IntN(50))), trace())
		}},
		{3, func(r *rand.Rand) [][2]string {
			return cat(f("level", "INFO", "msg", "cart updated", "service", "checkout",
				"ctx", fmt.Sprintf(`{"user":{"id":%d,"tier":"%s"},"items":[%d,%d],"flag":%v}`, r.IntN(1000), []string{"gold", "silver", "basic"}[r.IntN(3)], r.IntN(9), r.IntN(9), r.IntN(2) == 1)), trace())
		}},
	}
	inventory := []kind{
		{25, func(r *rand.Rand) [][2]string {
			return cat(f("level", "INFO", "msg", "stock reserved", "service", "inventory", "sku", fmt.Sprintf("SKU-%04d", r.IntN(2000)), "qty", fmt.Sprint(1+r.IntN(5))), trace())
		}},
		{15, func(r *rand.Rand) [][2]string {
			return cat(f("level", "INFO", "msg", "cache miss for key sku:1024", "service", "inventory", "key", "sku:1024", "latency_ms", latency()), trace())
		}},
		{5, func(r *rand.Rand) [][2]string {
			return cat(f("level", "ERROR", "msg", "库存不足，无法预占", "service", "inventory", "sku", fmt.Sprintf("SKU-%04d", r.IntN(2000)),
				"remark", []string{"华东一号仓库存不足", "华南二号仓暂停发货", "上海仓盘点中"}[r.IntN(3)]), trace())
		}},
		{3, func(r *rand.Rand) [][2]string {
			return cat(f("level", "WARN", "msg", "stock low", "service", "inventory", "sku", fmt.Sprintf("SKU-%04d", r.IntN(2000)), "qty", fmt.Sprint(r.IntN(3))), trace())
		}},
	}
	audit := []kind{
		{10, func(r *rand.Rand) [][2]string {
			return f("level", "INFO", "msg", "user login", "service", "auth", "user_id", fmt.Sprint(10000+r.IntN(90000)),
				"ip", fmt.Sprintf("203.0.113.%d", r.IntN(255)), "ua", "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0)")
		}},
		{3, func(r *rand.Rand) [][2]string {
			return f("level", "WARN", "msg", "permission denied", "service", "auth", "user_id", fmt.Sprint(10000+r.IntN(90000)), "resource", "/admin/users")
		}},
	}
	streams := []stream{
		{"", "10.0.0.11", "host-a", "/var/log/app/checkout.log", nil, "5EED00000000A001", checkout},
		{"", "10.0.0.12", "host-b", "/var/log/app/inventory.log", [][2]string{{"env", "staging"}}, "5EED00000000B002", inventory},
		{"audit", "10.0.0.11", "host-a", "/var/log/app/audit.log", nil, "5EED00000000C003", audit},
	}
	pick := func(ks []kind) [][2]string {
		total := 0
		for _, k := range ks {
			total += k.weight
		}
		n := r.IntN(total)
		for _, k := range ks {
			if n < k.weight {
				return k.gen(r)
			}
			n -= k.weight
		}
		panic("unreachable")
	}

	ds := &golden.Dataset{}
	for si, s := range streams {
		var logs []golden.Log
		for sec := int64(0); sec < golden.DatasetSpan; sec++ {
			n := r.IntN(3) // checkout 平均每秒 1 条，其余每秒 0.5 条
			if si > 0 {
				n = r.IntN(2)
			}
			if si == 0 && sec%97 == 0 {
				n = 6 // 偶尔一秒内多条，检验同一秒内按纳秒排序
			}
			var ns []uint32
			for range n {
				ns = append(ns, uint32(r.IntN(1_000_000_000)))
			}
			if sec%211 == 0 && n > 0 {
				ns[0] = 0 // 纳秒部分为 0 的日志
			}
			sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
			for _, x := range ns {
				logs = append(logs, golden.Log{T: sec, Ns: x, Contents: pick(s.kinds)})
			}
		}
		if si == 0 {
			addSpecialLogs(logs)
		}
		// 每 40 条一个包，模拟采集器批量发送
		for i, seq := 0, 1; i < len(logs); i, seq = i+40, seq+1 {
			g := golden.Group{Topic: s.topic, Source: s.source, Tags: [][2]string{
				{"__hostname__", s.host}, {"__path__", s.path}, {"__pack_id__", fmt.Sprintf("%s-%X", s.packPrefix, seq)},
			}}
			g.Tags = append(g.Tags, s.extraTags...)
			g.Logs = logs[i:min(i+40, len(logs))]
			ds.Groups = append(ds.Groups, g)
		}
	}
	return ds
}

// addSpecialLogs 把一些边界值放进固定位置的日志，检验特殊字符、空值、非法数值等。
func addSpecialLogs(logs []golden.Log) {
	specials := [][][2]string{
		{{"level", "INFO"}, {"msg", `say "hello" to <world> & friends`}, {"service", "checkout"}, {"note", ""}},
		{{"level", "INFO"}, {"msg", "latency not measured"}, {"service", "checkout"}, {"latency_ms", "n/a"}},
		{{"level", "INFO"}, {"msg", "negative latency"}, {"service", "checkout"}, {"latency_ms", "-5"}},
		{{"level", "INFO"}, {"msg", "tiny ratio"}, {"service", "checkout"}, {"ratio", "1e-3"}},
		{{"level", "INFO"}, {"msg", "leading zeros"}, {"service", "checkout"}, {"user_id", "007"}},
		{{"level", "Error"}, {"msg", "mixed case level"}, {"service", "checkout"}},
		{{"level", "INFO"}, {"msg", "unicode ✓ 🚀 测试"}, {"service", "checkout"}},
		{{"level", "INFO"}, {"msg", "long value"}, {"service", "checkout"}, {"blob", strings.Repeat("abcdefghij", 200)}},
		{{"level", "INFO"}, {"msg", "path with query"}, {"service", "checkout"}, {"url", "/api/v1/search?q=red+shoes&page=2"}},
		{{"level", "INFO"}, {"msg", "tab\tand\nnewline"}, {"service", "checkout"}},
		{{"level", "INFO"}, {"msg", "duplicate-ish key case"}, {"service", "checkout"}, {"Level", "debug"}},
		{{"level", "INFO"}, {"msg", "json array"}, {"service", "checkout"}, {"ctx", `[1,2,3]`}},
		{{"level", "INFO"}, {"msg", "invalid json"}, {"service", "checkout"}, {"ctx", `{"user":`}},
	}
	for i, s := range specials {
		pos := 50 + i*61
		if pos < len(logs) {
			logs[pos].Contents = s
		}
	}
}
