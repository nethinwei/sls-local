package fieldcol

import (
	"slices"
	"testing"
)

func TestKeys(t *testing.T) {
	keys := []string{"level", "Level", "a.b", `x"y`, "", "中文", "__f_6c"}
	sql := ""
	for _, k := range keys {
		if got, ok := Key(Name(k)); !ok || got != k {
			t.Errorf("Key(Name(%q)) = %q, %v", k, got, ok)
		}
		sql += Ref(k) + " = 1 AND "
	}
	sql += Ref("level") // 重复
	want := slices.Clone(keys)
	slices.Sort(want)
	if got := Keys(sql, "no fields here"); !slices.Equal(got, want) {
		t.Errorf("Keys = %q, want %q", got, want)
	}
	if Name("Level") == Name("level") {
		t.Error("field names that differ in case must map to different columns")
	}
	if _, ok := Key("contents"); ok {
		t.Error("Key(contents) should not be a field column")
	}
}
