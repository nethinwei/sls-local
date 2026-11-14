package store

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestWriteStringMatchesMarshal(t *testing.T) {
	cases := []string{"", "abc", "a b-c_d.e/f:g", `q"uote`, `back\slash`, "<a>&b", "tab\tnl\n", "\x00\x1f",
		"中文", "  ", "bad\xffutf8", "~!@#$%^*()+={}[]|;',?"}
	for c := 0; c < 128; c++ {
		cases = append(cases, string(rune(c)))
	}
	for _, s := range cases {
		var b bytes.Buffer
		writeString(&b, s)
		want, _ := json.Marshal(s)
		if b.String() != string(want) {
			t.Errorf("writeString(%q) = %s, want %s", s, b.String(), want)
		}
	}
}

func FuzzWriteString(f *testing.F) {
	f.Add("abc")
	f.Fuzz(func(t *testing.T, s string) {
		var b bytes.Buffer
		writeString(&b, s)
		if want, _ := json.Marshal(s); b.String() != string(want) {
			t.Fatalf("writeString(%q) = %s, want %s", s, b.String(), want)
		}
	})
}
