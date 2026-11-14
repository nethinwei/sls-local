// Package golden 是 sls-local 与真实 SLS 的对照测试：同一份合成数据写进云端的一个专用 logstore 和 sls-local，
// 同一组请求发给两边，云端的响应录成 testdata/golden/recorded/，测试回放时要求 sls-local 的响应与之一致。
//
// 录制：go run ./tools/golden（需要云端 AccessKey，见 docs/golden.md）。回放：go test ./internal/golden。
package golden

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// LocalLogstore 是回放时 sls-local 里的 logstore 名。云端的 logstore 名由录制工具的参数决定，
// 两边的名字都会在归一化时替换成 {logstore}。
const LocalLogstore = "sls-local-golden"

// Case 是一个请求。时间是相对数据集起点（Recording.Base）的秒数；From、To 都为 0 时取整个数据集。
type Case struct {
	Name    string `json:"name"`
	Op      string `json:"op"` // logs、histogram、context、index、logstore、shards、cursor
	Query   string `json:"query,omitempty"`
	From    int64  `json:"from,omitempty"`
	To      int64  `json:"to,omitempty"`
	Line    int64  `json:"line,omitempty"`
	Offset  int64  `json:"offset,omitempty"`
	Reverse bool   `json:"reverse,omitempty"`
	Back    int32  `json:"back,omitempty"`    // context：向前几行
	Forward int32  `json:"forward,omitempty"` // context：向后几行
	Cursor  string `json:"cursor,omitempty"`  // cursor：begin、end
	Note    string `json:"note,omitempty"`
}

// Response 是归一化后的响应：只保留与语义有关的头，Body 去掉耗时等易变字段、替换掉环境相关的名字。
type Response struct {
	Status int               `json:"status"`
	Header map[string]string `json:"header,omitempty"`
	Body   json.RawMessage   `json:"body,omitempty"`
	Text   string            `json:"text,omitempty"` // 不是 JSON 的响应体
}

// Recording 是一个用例文件的录制结果。
type Recording struct {
	Base int64 `json:"base"` // 数据集起点（Unix 秒）
	// LogstoreLen 是云端 logstore 名的长度：SLS 部分报错的列号按改写后的 SQL（log 换成 logstore 名）计算，
	// 回放时本地用同样长度的名字
	LogstoreLen int            `json:"logstoreLen,omitempty"`
	RecordedAt  string         `json:"recordedAt"`
	Results     []RecordedCase `json:"results"`
	byName      map[string]*Response
}

type RecordedCase struct {
	Name     string   `json:"name"`
	Response Response `json:"response"`
}

func (r *Recording) Lookup(name string) (*Response, bool) {
	if r.byName == nil {
		r.byName = map[string]*Response{}
		for i := range r.Results {
			r.byName[r.Results[i].Name] = &r.Results[i].Response
		}
	}
	resp, ok := r.byName[name]
	return resp, ok
}

// SuiteDir 是 testdata/golden 的路径。
type SuiteDir string

// CaseFiles 返回 cases/ 下的用例文件名（不含扩展名），按名字排序。
func (d SuiteDir) CaseFiles() ([]string, error) {
	m, err := filepath.Glob(filepath.Join(string(d), "cases", "*.json"))
	if err != nil {
		return nil, err
	}
	var names []string
	for _, p := range m {
		names = append(names, strings.TrimSuffix(filepath.Base(p), ".json"))
	}
	sort.Strings(names)
	return names, nil
}

func (d SuiteDir) Cases(file string) ([]Case, error) {
	var cs []Case
	if err := readJSON(filepath.Join(string(d), "cases", file+".json"), &cs); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, c := range cs {
		if c.Name == "" || seen[c.Name] {
			return nil, fmt.Errorf("%s: empty or duplicate case name %q", file, c.Name)
		}
		seen[c.Name] = true
	}
	return cs, nil
}

func (d SuiteDir) Recording(file string) (*Recording, error) {
	var r Recording
	if err := readJSON(filepath.Join(string(d), "recorded", file+".json"), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (d SuiteDir) WriteRecording(file string, r *Recording) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	p := filepath.Join(string(d), "recorded", file+".json")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, append(b, '\n'), 0o644)
}

// IndexJSON 返回对照用 logstore 的索引（SLS CreateIndex 的 JSON）。
func (d SuiteDir) IndexJSON() ([]byte, error) {
	return os.ReadFile(filepath.Join(string(d), "index.json"))
}

func (d SuiteDir) Index() (*sls.Index, error) {
	var ix sls.Index
	if err := readJSON(filepath.Join(string(d), "index.json"), &ix); err != nil {
		return nil, err
	}
	return &ix, nil
}

// KnownDiffs 是目前已知与 SLS 不一致的用例（文件/用例名 → 原因）。回放时这些用例不一致不算失败，
// 一致了反而失败，提醒把它从名单里删掉。
func (d SuiteDir) KnownDiffs() (map[string]string, error) {
	m := map[string]string{}
	err := readJSON(filepath.Join(string(d), "known-diffs.json"), &m)
	if os.IsNotExist(err) {
		return m, nil
	}
	return m, err
}

func (d SuiteDir) WriteKnownDiffs(m map[string]string) error {
	b, err := json.MarshalIndent(m, "", "  ") // map 按键排序输出
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(string(d), "known-diffs.json"), append(b, '\n'), 0o644)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// Target 是被测的一端：云端 SLS 或 sls-local。
type Target struct {
	Client   *sls.Client
	Project  string
	Logstore string
	cap      *capture
}

// NewTarget 用 rt 发请求（nil 时用 http.DefaultTransport），并记录每个响应的原文。
func NewTarget(c *sls.Client, project, logstore string, rt http.RoundTripper) *Target {
	if rt == nil {
		rt = http.DefaultTransport
	}
	cp := &capture{rt: rt}
	c.SetHTTPClient(&http.Client{Transport: cp})
	return &Target{Client: c, Project: project, Logstore: logstore, cap: cp}
}

type rawResponse struct {
	status int
	header http.Header
	body   []byte
}

// capture 记录最后一个响应，并解开 SLS 的 lz4 / zstd 压缩。
type capture struct {
	rt   http.RoundTripper
	mu   sync.Mutex
	last *rawResponse
}

func (c *capture) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := c.rt.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(b))
	plain := b
	switch ct := resp.Header.Get("x-log-compresstype"); ct {
	case "":
	case "lz4":
		size, _ := strconv.Atoi(resp.Header.Get("x-log-bodyrawsize"))
		dst := make([]byte, size)
		n, err := lz4.UncompressBlock(b, dst)
		if err != nil {
			return nil, fmt.Errorf("lz4 response: %w", err)
		}
		plain = dst[:n]
	case "zstd":
		d, _ := zstd.NewReader(nil)
		defer d.Close()
		if plain, err = d.DecodeAll(b, nil); err != nil {
			return nil, fmt.Errorf("zstd response: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown x-log-compresstype %q", ct)
	}
	c.mu.Lock()
	c.last = &rawResponse{status: resp.StatusCode, header: resp.Header.Clone(), body: plain}
	c.mu.Unlock()
	return resp, nil
}

// Run 用 SDK 的方法发出用例的请求（与真实客户端一样），返回最后一个响应（context 先查锚点日志，再取上下文）。
func (t *Target) Run(c Case, base int64) (*rawResponse, error) {
	from, to := base+c.From, base+c.To
	if c.From == 0 && c.To == 0 {
		from, to = base, base+DatasetSpan
	}
	t.cap.last = nil
	c2 := t.Client
	var err error
	switch c.Op {
	case "logs":
		_, err = c2.GetLogsV3(t.Project, t.Logstore, &sls.GetLogRequest{From: from, To: to, Query: c.Query, Lines: c.Line, Offset: c.Offset, Reverse: c.Reverse})
	case "histogram":
		_, err = c2.GetHistogramsV2(t.Project, t.Logstore, &sls.GetHistogramRequest{From: from, To: to, Query: c.Query})
	case "context":
		// 锚点：检索结果的第一条（带 __pack_meta__）
		var resp *sls.GetLogsV3Response
		resp, err = c2.GetLogsV3(t.Project, t.Logstore, &sls.GetLogRequest{From: from, To: to, Query: c.Query + " | with_pack_meta", Lines: 1, Reverse: c.Reverse})
		if err != nil || len(resp.Logs) == 0 {
			return nil, fmt.Errorf("context %s: anchor query returned no log: %v", c.Name, err)
		}
		a := resp.Logs[0]
		var ls *sls.LogStore
		if ls, err = c2.GetLogStore(t.Project, t.Logstore); err != nil {
			return nil, fmt.Errorf("context %s: GetLogStore: %w", c.Name, err)
		}
		_, err = ls.GetContextLogs(c.Back, c.Forward, a["__tag__:__pack_id__"], a["__pack_meta__"])
	case "index":
		_, err = c2.GetIndex(t.Project, t.Logstore)
	case "logstore":
		_, err = c2.GetLogStore(t.Project, t.Logstore)
	case "shards":
		_, err = c2.ListShards(t.Project, t.Logstore)
	case "cursor":
		_, err = c2.GetCursor(t.Project, t.Logstore, 0, c.Cursor)
	default:
		return nil, fmt.Errorf("case %s: unknown op %q", c.Name, c.Op)
	}
	if t.cap.last == nil {
		return nil, fmt.Errorf("case %s: no response: %v", c.Name, err)
	}
	return t.cap.last, nil
}

// Names 是要从响应里替换掉的环境相关的名字。
type Names struct {
	Project  string
	Logstore string
	Region   string
	// IndexKeys 不为空时，GetIndex 的响应只保留这些字段：云端 logstore 可能还有与对照无关的字段
	IndexKeys map[string]bool
}

var (
	// 不参与比对的 meta 字段：执行开销（耗时、CPU、扫描字节）每次都不一样；powerSql 与 cpuCores 只在
	// SQL 没命中云端结果缓存时出现（同一条 SQL 第二次执行就没有了）
	volatileMeta = []string{"elapsedMillisecond", "cpuSec", "cpuCores", "processedBytes", "powerSql"}
	acsResource  = regexp.MustCompile(`acs:log:[^:"]*:\d+:`)
	// __pack_meta__ 形如 0|<游标 base64>|9|8，游标与写入时间有关
	packMetaCursor = regexp.MustCompile(`^(\d+\|)[A-Za-z0-9+/=]+(\|.*)$`)
)

// Normalize 把原始响应变成可比对的形式。
func Normalize(raw *rawResponse, names Names) Response {
	out := Response{Status: raw.status, Header: map[string]string{}}
	for _, h := range []string{"Content-Type", "x-log-count", "x-log-progress"} {
		if v := raw.header.Get(h); v != "" {
			out.Header[strings.ToLower(h)] = v
		}
	}
	text := string(raw.body)
	if names.Logstore != "" {
		text = strings.ReplaceAll(text, names.Logstore, "{logstore}")
	}
	if names.Project != "" {
		text = strings.ReplaceAll(text, names.Project, "{project}")
	}
	if names.Region != "" {
		text = strings.ReplaceAll(text, names.Region, "{region}")
	}
	text = acsResource.ReplaceAllString(text, "acs:log:{region}:{account}:")
	n, err := parseJSON([]byte(text))
	if err != nil {
		out.Text = text
		return out
	}
	if keys := n.get("keys"); keys != nil && keys.kind == 'o' && n.get("line") != nil && names.IndexKeys != nil {
		for _, k := range append([]string(nil), keys.keys...) {
			if !names.IndexKeys[k] {
				keys.del(k)
			}
		}
	}
	normalizeBody(n)
	out.Body = n.encode()
	return out
}

func normalizeBody(n *node) {
	switch n.kind {
	case 'o':
		if meta := n.get("meta"); meta != nil {
			for _, k := range volatileMeta {
				meta.del(k)
			}
		}
		n.del("accessDeniedDetail") // 含账号与诊断信息
		for _, k := range []string{"createTime", "lastModifyTime", "lastUserModifyTime"} {
			if v := n.get(k); v != nil && v.kind == 's' {
				v.setScalar(json.Number("0"))
			}
		}
		for _, k := range []string{"resourceGroupId", "cursor"} {
			if v := n.get(k); v != nil {
				v.setScalar("{" + k + "}")
			}
		}
		if v := n.get("__pack_meta__"); v != nil {
			if s, ok := v.str(); ok {
				v.setScalar(packMetaCursor.ReplaceAllString(s, "${1}{cursor}${2}"))
			}
		}
		for _, v := range n.vals {
			normalizeBody(v)
		}
	case 'a':
		for _, v := range n.vals {
			normalizeBody(v)
		}
	}
}

// canonical 把 JSON 重新排版（保持键顺序）：录制文件整体缩进时，嵌在里面的响应体也会被重新缩进。
func canonical(b []byte) []byte {
	if len(b) == 0 {
		return b
	}
	n, err := parseJSON(b)
	if err != nil {
		return b
	}
	return n.encode()
}

// Equal 比较两个归一化后的响应。
func Equal(a, b Response) bool {
	if a.Status != b.Status || a.Text != b.Text || !bytes.Equal(canonical(a.Body), canonical(b.Body)) || len(a.Header) != len(b.Header) {
		return false
	}
	for k, v := range a.Header {
		if b.Header[k] != v {
			return false
		}
	}
	return true
}

// Diff 给出两个响应第一处不同附近的内容，便于定位。
func Diff(want, got Response) string {
	if want.Status != got.Status {
		return fmt.Sprintf("status: want %d, got %d\nwant body: %s\ngot body:  %s", want.Status, got.Status, clip(want.Body, want.Text), clip(got.Body, got.Text))
	}
	wl := strings.Split(string(canonical(want.Body))+want.Text, "\n")
	gl := strings.Split(string(canonical(got.Body))+got.Text, "\n")
	for i := 0; i < len(wl) || i < len(gl); i++ {
		var w, g string
		if i < len(wl) {
			w = wl[i]
		}
		if i < len(gl) {
			g = gl[i]
		}
		if w != g {
			lo := max(0, i-3)
			return fmt.Sprintf("body line %d:\nwant: %s\ngot:  %s\n(context)\n%s", i+1, w, g, strings.Join(wl[lo:min(len(wl), i+1)], "\n"))
		}
	}
	return fmt.Sprintf("header: want %v, got %v", want.Header, got.Header)
}

func clip(b []byte, text string) string {
	s := string(b) + text
	if len(s) > 600 {
		return s[:600] + "..."
	}
	return s
}
