package store

import (
	"context"
	"strings"
	"testing"
)

func TestRebuildFulltext(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, "", Options{SQLOffset: DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	g := &Group{Source: "src", NumericKeys: map[string]bool{"lat": true}, Logs: []Log{
		{Time: 1_800_000_000, Contents: []KV{{"msg", "hello"}, {"lat", "123"}}},
		{Time: 1_800_000_001, Contents: []KV{{"x", "a"}, {"x", "b"}, {"lat", "7"}}},
	}}
	if err := s.Append(ctx, "p", "l", g); err != nil {
		t.Fatal(err)
	}
	read := func() []string {
		rows, err := s.db.QueryContext(ctx, `SELECT fulltext FROM logs ORDER BY seq`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var f string
			rows.Scan(&f)
			out = append(out, f)
		}
		return out
	}
	written := read()
	if want := "src\x1fmsg\x1fhello\x1flat"; written[0] != want {
		t.Fatalf("written = %q, want %q", written[0], want)
	}
	numeric := func(string, string) map[string]bool { return map[string]bool{"lat": true} }

	// 没有版本号（升级前的库）：重建，结果与写入时相同（写入与重建出自同一份数据）
	if n, err := s.RebuildFulltext(ctx, numeric, false); err != nil || n != 2 {
		t.Fatalf("first rebuild = %d, %v", n, err)
	}
	if got := read(); strings.Join(got, "|") != strings.Join(written, "|") {
		t.Fatalf("rebuilt = %q, want %q", got, written)
	}
	// 已是当前版本：不再重建
	if n, err := s.RebuildFulltext(ctx, numeric, false); err != nil || n != 0 {
		t.Fatalf("second rebuild = %d, %v", n, err)
	}
	// 索引改了（lat 不再是数值）：-reindex 按新配置重建
	if n, err := s.RebuildFulltext(ctx, func(string, string) map[string]bool { return nil }, true); err != nil || n != 2 {
		t.Fatalf("forced rebuild = %d, %v", n, err)
	}
	if got := read(); !strings.HasSuffix(got[0], "\x1flat\x1f123") || got[1] != "src\x1fx\x1fa\x1fx_0\x1fb\x1flat\x1f7" {
		t.Fatalf("after reindex = %q", got)
	}
}
