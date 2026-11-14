package wire

import (
	"os"
	"testing"
)

// 样本来自 LoongCollector 3.4.1 的真实请求，见 docs/wire-findings.md。
func TestDecodeLoongCollectorSample(t *testing.T) {
	body, err := os.ReadFile("../../testdata/wire/putlogs_lz4_loongcollector341.bin")
	if err != nil {
		t.Fatal(err)
	}
	g, err := DecodeLogGroup(body, "lz4", 1391)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Logs) != 10 || g.GetSource() != "172.27.0.3" {
		t.Fatalf("logs=%d source=%q", len(g.Logs), g.GetSource())
	}
	tags := map[string]string{}
	for _, tag := range g.LogTags {
		tags[tag.GetKey()] = tag.GetValue()
	}
	if tags["__pack_id__"] != "9893B9C2046C38EC-1" || tags["__path__"] != "/logs/probe.log" {
		t.Fatalf("tags = %v", tags)
	}
	if c := g.Logs[0].Contents; len(c) == 0 || c[1].GetKey() != "level" || c[1].GetValue() != "INFO" {
		t.Fatalf("first log contents = %v", c)
	}
}

func TestDecompressRejectsBadSize(t *testing.T) {
	if _, err := Decompress([]byte{1}, "lz4", 0); err == nil {
		t.Fatal("want error for zero raw size")
	}
	if _, err := Decompress([]byte{1}, "snappy", 1); err == nil {
		t.Fatal("want error for unsupported compression")
	}
}
