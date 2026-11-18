package query

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/fieldcol"
	"github.com/nethinwei/sls-local/internal/index"
)

// filter-syntax.json 来自 Grafana 插件，插件前后端与 sls-local 三方共用同一组用例。
func loadPluginCases(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	b, err := os.ReadFile("../../testdata/plugin/filter-syntax.json")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSplitMatchesPlugin(t *testing.T) {
	var cases []struct{ In, Search, Rest string }
	if err := json.Unmarshal(loadPluginCases(t)["splitStatement"], &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		s, r := Split(c.In)
		if s != c.Search || r != c.Rest {
			t.Errorf("Split(%q) = %q, %q; want %q, %q", c.In, s, r, c.Search, c.Rest)
		}
	}
}

func TestIsSQLMatchesPlugin(t *testing.T) {
	var cases []struct {
		In   string
		Want bool
	}
	if err := json.Unmarshal(loadPluginCases(t)["isSQL"], &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if got := IsSQL(c.In); got != c.Want {
			t.Errorf("IsSQL(%q) = %v, want %v", c.In, got, c.Want)
		}
	}
}

// devIndex 是一个真实 SLS logstore 的索引（testdata/index/sample-logstore.json）。
func devIndex(t *testing.T) *index.Index {
	t.Helper()
	b, err := os.ReadFile("../../testdata/index/sample-logstore.json")
	if err != nil {
		t.Fatal(err)
	}
	var raw sls.Index
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	return index.FromConfig(&raw)
}

func TestSearchCompile(t *testing.T) {
	ix := devIndex(t)
	f := fieldcol.Ref // 字段列（存储层提供）
	text := func(key string) string {
		return "(contains(lower(" + f(key) + "), ?) AND coalesce(regexp_matches(lower(" + f(key) + "), ?), false))"
	}
	cases := []struct {
		in, sql string
		nargs   int
	}{
		{"", "TRUE", 0},
		{"*", "TRUE", 0},
		{"level: ERROR", text("level"), 2},
		{"Level:ERROR", text("level"), 2},
		{"error", "(contains(lower(fulltext), ?) AND coalesce(regexp_matches(lower(fulltext), ?), false))", 2},
		{"order paid", "((contains(lower(fulltext), ?) AND coalesce(regexp_matches(lower(fulltext), ?), false)) AND (contains(lower(fulltext), ?) AND coalesce(regexp_matches(lower(fulltext), ?), false)))", 4},
		{"level: a and msg: b", "(" + text("level") + " AND " + text("msg") + ")", 4},
		{"level: a not msg: b", "(" + text("level") + " AND (NOT " + text("msg") + "))", 4},
		{"level: a or not msg: b", "(" + text("level") + " OR (NOT " + text("msg") + "))", 4},
		{"uid: *", "(" + f("uid") + " IS NOT NULL)", 0},
		{"__topic__: x", "(contains(lower(topic), ?) AND coalesce(regexp_matches(lower(topic), ?), false))", 2},
		// 没有索引的 tag 按整个值匹配（golden：__tag__:__path__: /var/log/app/audit.log 命中，: audit 不命中）
		{"__tag__:__path__: x", "coalesce(regexp_matches(json_extract_string(tags, ?), ?), false)", 2},
		{"cost_ms > 50", "coalesce(TRY_CAST(" + f("cost_ms") + " AS BIGINT) > ?, false)", 1},
		{"cost_ms: 68", "coalesce(TRY_CAST(" + f("cost_ms") + " AS BIGINT) = ?, false)", 1},
		{"cost_ms in (50 60]", "coalesce(TRY_CAST(" + f("cost_ms") + " AS BIGINT) > ? AND TRY_CAST(" + f("cost_ms") + " AS BIGINT) <= ?, false)", 2},
		{"elapsed_ms > 1.5", "coalesce(TRY_CAST(" + f("elapsed_ms") + " AS DOUBLE) > ?, false)", 1},
		{"cost_ms: abc", "FALSE", 0},
		{"cost_ms: 6*", "FALSE", 0},
		{"level > a", "FALSE", 0},
		{`msg: ", ;"`, "FALSE", 0},
	}
	for _, c := range cases {
		got, err := Search(c.in, ix)
		if err != nil {
			t.Errorf("Search(%q): %v", c.in, err)
			continue
		}
		if got.SQL != c.sql || len(got.Args) != c.nargs {
			t.Errorf("Search(%q) = %s %v\nwant %s (%d args)", c.in, got.SQL, got.Args, c.sql, c.nargs)
		}
	}
}

// 报错文案来自真实 SLS（docs/wire-findings.md），插件靠它们识别错误类型。
func TestSearchErrorsMatchSLS(t *testing.T) {
	ix := devIndex(t)
	cases := map[string]string{
		"nosuchkey: x":                  "key (nosuchkey) is not config as key value config,if symbol : is  in your log,please wrap : with quotation mark \"",
		"product_id: *":                 "key (product_id) is not config as key value config,if symbol : is  in your log,please wrap : with quotation mark \"",
		"__tag__:__path__: *order_api*": "only support * or ?  in the  middle or end  of the query",
		"level: INFO msg: paid": "parse search query error,please read query syntax document,error detail:syntax error, " +
			"unexpected TERM, expecting $end error position is from column:13 to column:16,error near < msg >",
		"msg: order paid": "parse search query error,please read query syntax document,error detail:syntax error, " +
			"unexpected TERM, expecting $end error position is from column:12 to column:16,error near < paid >",
	}
	for in, want := range cases {
		_, err := Search(in, ix)
		if err == nil || err.Error() != want {
			t.Errorf("Search(%q) error = %v\nwant %s", in, err, want)
		}
	}
	for _, in := range []string{"(a", "a)", `msg: "x`, "level:", "and a", "level: (INFO or WARN)", "cost_ms in [1"} {
		if _, err := Search(in, ix); err == nil {
			t.Errorf("Search(%q): want error", in)
		}
	}
}

func TestSearchTagKeyPath(t *testing.T) {
	got, err := Search(`__tag__:__path__: x`, index.Auto())
	if err != nil {
		t.Fatal(err)
	}
	if got.Args[0] != `$."__tag__:__path__"` {
		t.Fatalf("path = %v", got.Args[0])
	}
}

// 词匹配语义（实测 SLS）：普通值的各词都要出现、不管顺序；短语 #"..." 各词按顺序相邻、不跨字段；
// 通配符只在词内；全文列里的字段分隔符总是词边界（大小写由 SQL 里的 lower() 处理）。
func TestTextPatterns(t *testing.T) {
	tokens := strings.Join(index.DefaultTokens, "")
	sep := FieldSeparator
	cases := []struct {
		value            string
		wildcard, phrase bool
		text             string
		want             bool
	}{
		{"error", true, false, "call payment failed: error", true},
		{"err", true, false, "error", false},
		{"err*", true, false, "error happened", true},
		{"p*d", true, false, "order paid", true},
		{"abc21", true, false, "trace abc21", true},
		{"abc2", true, false, "abc21", false},
		{"a.b", true, false, "x a.b y", true},
		{"pai*", false, false, "paid", false},
		// 普通值：各词都出现即可
		{"probe 21", false, false, "probe 21", true},
		{"probe 21", false, false, "probe 3 21", true},
		{"declined card", false, false, "card declined", true},
		{"declined,card", true, false, "card declined", true},
		{"alpha zzz", false, false, "alpha beta", false},
		{"beta gamma", false, false, "alpha beta" + sep + "gamma", true},
		// 短语：按顺序相邻，词之间可以隔任意分词符，不跨字段
		{"card declined", false, true, "card declined", true},
		{"card, declined", false, true, "x card ,  declined y", true},
		{"declined card", false, true, "card declined", false},
		{"probe 21", false, true, "probe 3 21", false},
		{"beta gamma", false, true, "alpha beta" + sep + "gamma", false},
		{"probekey probeval", false, true, "probekey" + sep + "probeval", false},
		{"linea lineb", false, true, "linea\nlineb", true},
		// 字段分隔符是词边界：字段末尾的词能匹配，通配符不跨字段
		{"timeout", true, false, "upstream timeout" + sep + "level", true},
		{"time*level", true, false, "timeout" + sep + "level", false},
	}
	for _, c := range cases {
		pats := textPatterns(c.value, tokens, sep, c.wildcard, c.phrase)
		got := len(pats) > 0
		for _, p := range pats {
			got = got && regexp.MustCompile(p.re).MatchString(c.text) && strings.Contains(c.text, p.literal)
		}
		if got != c.want {
			t.Errorf("textPatterns(%q, phrase=%v) on %q = %v, want %v (%+v)", c.value, c.phrase, c.text, got, c.want, pats)
		}
	}
	// 没有分词符时字符类仍是合法的正则
	for _, p := range textPatterns("a b", "", "", false, false) {
		regexp.MustCompile(p.re)
	}
}

func TestExpr(t *testing.T) {
	f := fieldcol.Ref
	cases := []struct{ in, sql string }{
		{"cost_ms > '50'", "(" + f("cost_ms") + " > '50')"},
		{`"__tag__:__path__" like '%order%'`, "(json_extract_string(tags, ?) like '%order%')"},
		{"cast(cost_ms as bigint) > 50", "(cast(" + f("cost_ms") + " as bigint) > 50)"},
		{"__time__ > 0 and nosuch is null", "(t > 0 and " + f("nosuch") + " is null)"},
		{"regexp_like(msg, 'a''b')", "(regexp_matches(" + f("msg") + ", 'a''b'))"},
	}
	for _, c := range cases {
		got, err := Expr(c.in)
		if err != nil || got.SQL != c.sql {
			t.Errorf("Expr(%q) = %q, %v\nwant %q", c.in, got.SQL, err, c.sql)
		}
	}
	for _, in := range []string{"1; drop table logs", "(select 1) = 1", "a = 'x", ""} {
		if _, err := Expr(in); err == nil {
			t.Errorf("Expr(%q): want error", in)
		}
	}
}

func TestParsePipes(t *testing.T) {
	p, err := Parse("level: ERROR | with_pack_meta", index.Auto())
	if err != nil || !p.PackMeta || p.Search != "level: ERROR" {
		t.Fatalf("Parse = %+v, %v", p, err)
	}
	if p, err := Parse("level: ERROR | select count(*)", index.Auto()); err != nil || p.SQL == nil || p.Search != "level: ERROR" {
		t.Fatalf("SQL: %+v, %v", p, err)
	}
	if _, err := Parse("* | parse-csv a as b", index.Auto()); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("SPL: %v", err)
	}
	p, err = Parse("* | extend a = 1 | where a > 0 | project a", index.Auto())
	if err != nil || len(p.Stages) != 3 || p.Stages[0].Kind != "extend" || p.Stages[2].Fields[0] != "a" {
		t.Fatalf("SPL stages: %+v, %v", p, err)
	}
}

// 短语 #"..."：几种写法都能解析，meta.terms 记作原文；普通引号的词按字母序拆开（实测 SLS）。
func TestPhraseSyntax(t *testing.T) {
	ix := devIndex(t)
	cases := []struct {
		in    string
		terms []Term
	}{
		{`#"card declined"`, []Term{{"card declined", ""}}},
		{`#"Card, Declined"`, []Term{{"Card, Declined", ""}}},
		{`"declined card"`, []Term{{"card", ""}, {"declined", ""}}},
		{`msg: #"Card Declined"`, []Term{{"Card Declined", "msg"}}},
		{`msg:#"card declined"`, []Term{{"card declined", "msg"}}},
		{`#"card declined" and level: error`, []Term{{"card declined", ""}, {"error", "level"}}},
		{`#alpha`, []Term{{"#alpha", ""}}}, // # 后面不是引号：普通的词
	}
	for _, c := range cases {
		got, err := Search(c.in, ix)
		if err != nil {
			t.Errorf("Search(%q): %v", c.in, err)
			continue
		}
		if !slices.Equal(got.Terms, c.terms) {
			t.Errorf("Search(%q).Terms = %v, want %v", c.in, got.Terms, c.terms)
		}
	}
	if _, err := Search(`#"unterminated`, ix); err == nil {
		t.Error(`#"unterminated: want error`)
	}
}
