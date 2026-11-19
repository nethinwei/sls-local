package main

import (
	"errors"
	"testing"
	"time"

	"github.com/nethinwei/sls-local/internal/store"
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

// 默认刷段间隔：单机不带对象存储时不变，带对象存储时 30 秒，写入节点 2 秒；命令行显式给了就不改。
func TestApplyTargetFlushInterval(t *testing.T) {
	for _, c := range []struct {
		target, storage string
		want            time.Duration
	}{
		{"all", "", store.DefaultFlushInterval},
		{"all", "file:///tmp/x", singleBackupFlush},
		{"write", "file:///tmp/x", 2 * time.Second},
	} {
		opts := store.Options{Storage: c.storage, SegmentDir: "/tmp/seg", FlushInterval: store.DefaultFlushInterval}
		data, https := "/data/sls.duckdb", ":443"
		if err := applyTarget(c.target, &opts, &data, &https, false); err != nil {
			t.Fatal(err)
		}
		if opts.FlushInterval != c.want {
			t.Errorf("%s with storage %q: flush interval = %s, want %s", c.target, c.storage, opts.FlushInterval, c.want)
		}
	}
}
