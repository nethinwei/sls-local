package main

import (
	"errors"
	"testing"
)

func TestParseWriterID(t *testing.T) {
	host := func(h string) func() (string, error) { return func() (string, error) { return h, nil } }
	for _, c := range []struct {
		v, host string
		want    int64
		ok      bool
	}{
		{"3", "", 3, true},
		{"auto", "sls-local-write-2", 2, true},
		{"auto", "sls-local-write-0", 0, true},
		{"auto", "laptop", 0, false},
		{"auto", "sls-local-write-x", 0, false},
		{"x", "", 0, false},
	} {
		got, err := parseWriterID(c.v, host(c.host))
		if (err == nil) != c.ok || got != c.want {
			t.Errorf("parseWriterID(%q) on %q = %d, %v", c.v, c.host, got, err)
		}
	}
	if _, err := parseWriterID("auto", func() (string, error) { return "", errors.New("no hostname") }); err == nil {
		t.Error("hostname error ignored")
	}
}
