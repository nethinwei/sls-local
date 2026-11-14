package api

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"testing"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

// 重复的字段名：期望值是同一组字段写进真实 SLS 后 GetLogsV3 的原始响应（data 与 meta.keys）。
// 同名字段第 n 次出现改名为 名字_{n-2}，不检查与已有字段重名；keys 是原始字段名去重。
func TestDuplicateKeys(t *testing.T) {
	c := newTestEnv(t)
	now := uint32(base.Unix())
	kv := func(k, v string) *sls.LogContent { return &sls.LogContent{Key: ptr(k), Value: ptr(v)} }
	g := &sls.LogGroup{Topic: ptr("dup"), Source: ptr("probe"), Logs: []*sls.Log{{Time: &now, Contents: []*sls.LogContent{
		kv("zz", "1"), kv("x", "a"), kv("y_0", "taken"), kv("x", "b"), kv("aa", "2"), kv("x", "c"), kv("y", "p"), kv("y", "q"), kv("x_0", "late"),
	}}}}
	if err := c.PutLogs("devproj", "autostore", g); err != nil {
		t.Fatal(err)
	}
	req := &sls.GetLogRequest{From: int64(now), To: int64(now) + 1, Query: "__topic__: dup", Lines: 10}
	res := mustGetLogs(t, c, "autostore", req)
	if len(res.rows) != 1 {
		t.Fatalf("%d rows", len(res.rows))
	}
	wantData := `"zz":"1","x":"a","y_0":"taken","x_0":"b","aa":"2","x_1":"c","y":"p","y_0":"q","x_0":"late","__topic__":"dup","__source__":"probe"`
	if got := strings.Join(res.keys[0], ","); !strings.HasPrefix(got, "zz,x,y_0,x_0,aa,x_1,y,y_0,x_0,__topic__,__source__") {
		t.Errorf("data keys = %s", got)
	}
	raw := rawData(t, c, req)
	if !strings.Contains(raw, wantData) {
		t.Errorf("data = %s\nwant it to contain %s", raw, wantData)
	}
	if want := []string{"zz", "x", "y_0", "aa", "y", "x_0", "__topic__", "__source__"}; !slices.Equal(res.meta.Keys, want) {
		t.Errorf("meta.keys = %v, want %v", res.meta.Keys, want)
	}
	// 全文能搜到每个值；按名字取值时取第一个
	for q, want := range map[string]int{"__topic__: dup and b": 1, "__topic__: dup and late": 1, "__topic__: dup and c": 1} {
		if n := len(mustGetLogs(t, c, "autostore", &sls.GetLogRequest{From: int64(now), To: int64(now) + 1, Query: q, Lines: 10}).rows); n != want {
			t.Errorf("%q: %d rows, want %d", q, n, want)
		}
	}
}

func rawData(t *testing.T, c *sls.Client, req *sls.GetLogRequest) string {
	t.Helper()
	body, _ := json.Marshal(req)
	p, _ := c.GetProject("devproj")
	r, err := p.RawRequest("POST", "/logstores/autostore/logs", map[string]string{
		"x-log-bodyrawsize": strconv.Itoa(len(body)), "Content-Type": "application/json"}, body)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := r.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return b.String()
}
