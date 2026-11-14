package config

import (
	"os"
	"path/filepath"
	"testing"
)

func write(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.yaml")
	if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoad(t *testing.T) {
	c, err := Load(write(t, `
logstores:
  - project: devproj
    logstore: devstore
    index:
      line: {token: [" ", ","], caseSensitive: false}
      keys:
        level: {type: text, token: [" "], doc_value: true}
        cost_ms: {type: long, doc_value: true}
  - project: devproj
    logstore: auto
`))
	if err != nil {
		t.Fatal(err)
	}
	ix := c.Logstores[0].Index
	if ix == nil || ix.Line == nil || ix.Keys["cost_ms"].Type != "long" || !ix.Keys["level"].DocValue {
		t.Fatalf("index = %+v", ix)
	}
	if c.Logstores[1].Index != nil {
		t.Fatal("auto logstore should have no index")
	}
}

func TestLoadRejects(t *testing.T) {
	for name, s := range map[string]string{
		"missing logstore": "logstores: [{project: p}]",
		"duplicate":        "logstores: [{project: p, logstore: l}, {project: p, logstore: l}]",
		"unknown type":     "logstores: [{project: p, logstore: l, index: {keys: {a: {type: int}}}}]",
		"unknown field":    "logstore: []",
	} {
		if _, err := Load(write(t, s)); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}
