package query

import (
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"

	sls "github.com/aliyun/aliyun-log-go-sdk"

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
	text := "coalesce(regexp_matches(lower(json_extract_string(contents, ?)), ?), false)"
	cases := []struct {
		in, sql string
		nargs   int
	}{
		{"", "TRUE", 0},
		{"*", "TRUE", 0},
		{"level: ERROR", text, 2},
		{"Level:ERROR", text, 2},
		{"error", "(contains(lower(fulltext), ?) AND coalesce(regexp_matches(lower(fulltext), ?), false))", 2},
		{"order paid", "((contains(lower(fulltext), ?) AND coalesce(regexp_matches(lower(fulltext), ?), false)) AND (contains(lower(fulltext), ?) AND coalesce(regexp_matches(lower(fulltext), ?), false)))", 4},
		{"level: a and msg: b", "(" + text + " AND " + text + ")", 4},
		{"level: a not msg: b", "(" + text + " AND (NOT " + text + "))", 4},
		{"level: a or not msg: b", "(" + text + " OR (NOT " + text + "))", 4},
		{"uid: *", "(json_extract_string(contents, ?) IS NOT NULL)", 1},
		{"__topic__: x", "(contains(lower(topic), ?) AND coalesce(regexp_matches(lower(topic), ?), false))", 2},
		// 没有索引的 tag 按整个值匹配（golden：__tag__:__path__: /var/log/app/audit.log 命中，: audit 不命中）
		{"__tag__:__path__: x", "coalesce(regexp_matches(json_extract_string(tags, ?), ?), false)", 2},
		{"cost_ms > 50", "coalesce(TRY_CAST(json_extract_string(contents, ?) AS BIGINT) > ?, false)", 2},
		{"cost_ms: 68", "coalesce(TRY_CAST(json_extract_string(contents, ?) AS BIGINT) = ?, false)", 2},
		{"cost_ms in (50 60]", "coalesce(TRY_CAST(json_extract_string(contents, ?) AS BIGINT) > ? AND TRY_CAST(json_extract_string(contents, ?) AS BIGINT) <= ?, false)", 4},
		{"elapsed_ms > 1.5", "coalesce(TRY_CAST(json_extract_string(contents, ?) AS DOUBLE) > ?, false)", 2},
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

// 词匹配语义：按分词符切词、短语连续、通配符只在词内（大小写由 SQL 里的 lower() 处理）。
func TestTokenPattern(t *testing.T) {
	tokens := strings.Join(index.DefaultTokens, "")
	cases := []struct {
		value    string
		wildcard bool
		text     string
		want     bool
	}{
		{"error", true, "call payment failed: error", true},
		{"err", true, "error", false},
		{"err*", true, "error happened", true},
		{"p*d", true, "order paid", true},
		{"probe 21", false, "probe 21", true},
		{"probe 21", false, "probe 3 21", false},
		{"abc21", true, "trace abc21", true},
		{"abc2", true, "abc21", false},
		{"a.b", true, "x a.b y", true},
		{"pai*", false, "paid", false},
	}
	for _, c := range cases {
		p, ok := tokenPattern(c.value, tokens, c.wildcard)
		got := ok && regexp.MustCompile(p).MatchString(c.text)
		if got != c.want {
			t.Errorf("tokenPattern(%q) on %q = %v, want %v (pattern %s)", c.value, c.text, got, c.want, p)
		}
	}
}

func TestExpr(t *testing.T) {
	cases := []struct{ in, sql string }{
		{"cost_ms > '50'", "(json_extract_string(contents, ?) > '50')"},
		{`"__tag__:__path__" like '%order%'`, "(json_extract_string(tags, ?) like '%order%')"},
		{"cast(cost_ms as bigint) > 50", "(cast(json_extract_string(contents, ?) as bigint) > 50)"},
		{"__time__ > 0 and nosuch is null", "(t > 0 and json_extract_string(contents, ?) is null)"},
		{"regexp_like(msg, 'a''b')", "(regexp_matches(json_extract_string(contents, ?), 'a''b'))"},
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
