package store

import (
	"strconv"
	"testing"
)

// 期望值都是真实 SLS 的输出（同一个 double，SLS 用 Grisu2 排版）。
func TestFormatGrisu(t *testing.T) {
	parse := func(s string) float64 { f, _ := strconv.ParseFloat(s, 64); return f }
	a, b := parse("0.1"), parse("0.2")
	cases := []struct {
		in   float64
		want string
	}{
		{a + b, "0.30000000000000007"},
		{341256.0 / 1056, "323.15909090909096"},
		{parse("0.039541320680110716"), "0.039541320680110719"},
		{parse("168.63859649122807"), "168.63859649122808"},
		{parse("125.44415492957746"), "125.44415492957747"},
		{parse("6.524317912218268"), "6.524317912218268"},
		{parse("80.54567022538552"), "80.54567022538552"},
		{parse("591348.0125807842"), "591348.0125807842"},
		{parse("768.9915555978389"), "768.9915555978389"},
		{parse("35626.14"), "35626.14"},
		{1, "1.0"},
		{2529, "2529.0"},
		{1e20, "100000000000000000000.0"},
		{0.1, "0.1"},
		{-3.5, "-3.5"},
		{0, "0.0"},
	}
	for _, c := range cases {
		if got := formatGrisu(c.in); got != c.want {
			t.Errorf("formatGrisu(%v) = %s, want %s", c.in, got, c.want)
		}
	}
}
