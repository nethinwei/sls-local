// golden 生成对照数据、写入云端 SLS 并录制响应，结果供 internal/golden 的回放测试使用。见 docs/golden.md。
//
//	go run ./tools/golden gen      # 重新生成 testdata/golden/dataset.json（改了生成逻辑才需要）
//	go run ./tools/golden upload   # 云端：补齐索引、把数据集写进一个空的时间段，记下起点时间
//	go run ./tools/golden record   # 云端：执行全部用例，录制到 testdata/golden/recorded/
//
// 云端参数取自环境变量：SLS_ENDPOINT（如 cn-hangzhou.log.aliyuncs.com）、SLS_GOLDEN_PROJECT、
// SLS_GOLDEN_LOGSTORE（默认 sls-local-golden）、SLS_ACCESS_KEY_ID、SLS_ACCESS_KEY_SECRET；
// SLS_GOLDEN_DENY 是录制结果里不允许出现的额外词（逗号分隔，不区分大小写），例如公司或项目名。
// logstore 可以是在用的 logstore：数据写在没有其他日志的时间段，索引只追加字段、不改已有字段。
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/golden"
)

const dir = golden.SuiteDir("testdata/golden")

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		log.Fatal("usage: golden gen|upload|record [flags]")
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "gen":
		ds := generate()
		check(writeDataset("testdata/golden/dataset.json", ds))
		log.Printf("dataset: %d logs in %d groups", ds.Count(), len(ds.Groups))
	case "upload":
		upload(args)
	case "record":
		record(args)
	default:
		log.Fatalf("unknown command %q", cmd)
	}
}

type cloud struct {
	client                    *sls.Client
	project, logstore, region string
	endpoint, akID            string
}

func newCloud() *cloud {
	c := &cloud{endpoint: os.Getenv("SLS_ENDPOINT"), project: os.Getenv("SLS_GOLDEN_PROJECT"),
		logstore: os.Getenv("SLS_GOLDEN_LOGSTORE"), akID: os.Getenv("SLS_ACCESS_KEY_ID")}
	if c.endpoint == "" || c.project == "" || c.akID == "" || os.Getenv("SLS_ACCESS_KEY_SECRET") == "" {
		log.Fatal("set SLS_ENDPOINT, SLS_GOLDEN_PROJECT, SLS_ACCESS_KEY_ID and SLS_ACCESS_KEY_SECRET")
	}
	if c.logstore == "" {
		c.logstore = "sls-local-golden"
	}
	c.region = strings.SplitN(strings.TrimSuffix(c.endpoint, "-intranet"), ".", 2)[0]
	c.client = &sls.Client{Endpoint: c.endpoint, AccessKeyID: c.akID, AccessKeySecret: os.Getenv("SLS_ACCESS_KEY_SECRET")}
	return c
}

// upload 补齐索引，再把数据集写进一个没有日志的时间段，等到全部可查。
func upload(args []string) {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)
	skipIndex := fs.Bool("skip-index", false, "不检查、不修改索引")
	baseFlag := fs.Int64("base", 0, "数据集起点（Unix 秒）；0 表示自动找一个空的时间段")
	fs.Parse(args)
	c := newCloud()
	ds, err := dir.Dataset()
	check(err)
	if !*skipIndex {
		applyIndex(c)
	}
	base := *baseFlag
	if base == 0 {
		base = findEmptyWindow(c)
	}
	log.Printf("writing %d logs at base %d (%s)", ds.Count(), base, time.Unix(base, 0).UTC())
	for i, g := range ds.LogGroups(base) {
		// 公网偶尔超时：重试。超时的请求可能其实已经写入，重复写入会在下面的条数检查里暴露
		for attempt := 1; ; attempt++ {
			err := c.client.PutLogs(c.project, c.logstore, g)
			if err == nil {
				break
			}
			if attempt == 5 {
				log.Fatalf("PutLogs group %d: %v", i, err)
			}
			log.Printf("PutLogs group %d failed (attempt %d): %v; retrying", i, attempt, err)
			time.Sleep(time.Duration(attempt) * 3 * time.Second)
		}
	}
	writeJSON("testdata/golden/upload.json", map[string]any{"base": base})
	log.Printf("uploaded %d logs at base %d (%s); waiting until they are searchable", ds.Count(), base, time.Unix(base, 0).UTC())
	for deadline := time.Now().Add(5 * time.Minute); ; {
		n, complete, err := count(c, base, base+golden.DatasetSpan)
		if err == nil && n == ds.Count() && complete {
			log.Print("all logs searchable")
			return
		}
		if err == nil && n > ds.Count() && complete {
			log.Fatalf("%d logs in the window, expected %d: a retried write was duplicated; run upload again (it picks a new window)", n, ds.Count())
		}
		if time.Now().After(deadline) {
			log.Fatalf("logs not searchable after 5 minutes: %d of %d, complete=%v, err=%v", n, ds.Count(), complete, err)
		}
		time.Sleep(10 * time.Second)
	}
}

// count 用直方图数日志。不用 SQL：SLS 对不带 FROM 的 select count(*) 走统计快速路径，结果不精确。
func count(c *cloud, from, to int64) (int, bool, error) {
	resp, err := c.client.GetHistograms(c.project, c.logstore, "", from, to, "*")
	if err != nil {
		return 0, false, err
	}
	return int(resp.Count), resp.IsComplete(), nil
}

// findEmptyWindow 从 logstore 创建前一小时开始往前找，返回第一个前后 1 分钟内都没有日志的时间段的起点
// （对齐到分钟再加 7 秒，直方图首尾会有不完整的桶）。不早于 TTL 到期前 3 天，保证录制时数据还在。
func findEmptyWindow(c *cloud) int64 {
	ls, err := c.client.GetLogStore(c.project, c.logstore)
	check(err)
	earliest := time.Now().Unix() - int64(ls.TTL-3)*86400
	start := time.Unix(int64(ls.CreateTime), 0).Add(-time.Hour - golden.DatasetSpan*time.Second).Truncate(time.Minute).Unix()
	if now := time.Now().Add(-time.Hour - golden.DatasetSpan*time.Second).Truncate(time.Minute).Unix(); start > now {
		start = now
	}
	for b := start + 7; b >= earliest; b -= 3600 {
		n, complete, err := count(c, b-60, b+golden.DatasetSpan+60)
		check(err)
		if n == 0 && complete {
			return b
		}
		log.Printf("window at %s has %d logs, trying an earlier one", time.Unix(b, 0).UTC(), n)
	}
	log.Fatal("no empty window within the logstore TTL")
	return 0
}

// applyIndex 把 testdata/golden/index.json 的字段追加到云端索引：已有的同名字段必须与之完全一致，
// 其他字段原样保留，所以不影响这个 logstore 现有的查询。
func applyIndex(c *cloud) {
	raw, err := dir.IndexJSON()
	check(err)
	var want map[string]any
	check(json.Unmarshal(raw, &want))
	cur := map[string]any{}
	curRaw, err := c.client.GetIndexString(c.project, c.logstore)
	if err != nil {
		log.Fatalf("GetIndex: %v (the logstore needs an index; create one with at least the line index first)", err)
	}
	check(json.Unmarshal([]byte(curRaw), &cur))

	changed := false
	if curLine, ok := cur["line"]; ok {
		if !sameIndexKey(curLine, want["line"]) {
			log.Fatalf("the full-text index differs from testdata/golden/index.json; refusing to change it\ncloud: %v\nwant:  %v", curLine, want["line"])
		}
	} else {
		cur["line"], changed = want["line"], true
	}
	curKeys, _ := cur["keys"].(map[string]any)
	if curKeys == nil {
		curKeys = map[string]any{}
		cur["keys"] = curKeys
	}
	var added []string
	for k, v := range want["keys"].(map[string]any) {
		if old, ok := curKeys[k]; ok {
			if !sameIndexKey(old, v) {
				log.Fatalf("index key %q differs from testdata/golden/index.json; refusing to change it\ncloud: %v\nwant:  %v", k, old, v)
			}
			continue
		}
		curKeys[k], changed = v, true
		added = append(added, k)
	}
	if !changed {
		log.Print("index already contains every golden key")
		return
	}
	delete(cur, "lastModifyTime")
	delete(cur, "lastUserModifyTime")
	b, err := json.Marshal(cur)
	check(err)
	check(c.client.UpdateIndexString(c.project, c.logstore, string(b)))
	sort.Strings(added)
	log.Printf("added index keys %v; waiting 90s for them to take effect on new data", added)
	time.Sleep(90 * time.Second)
}

// sameIndexKey 比较两个索引字段的定义，缺省值视为相同（SLS 返回时会补上 alias 等字段）。
func sameIndexKey(a, b any) bool {
	norm := func(v any) map[string]any {
		m, _ := v.(map[string]any)
		out := map[string]any{"alias": "", "caseSensitive": false, "chn": false, "doc_value": false}
		for k, x := range m {
			out[k] = x
		}
		if out["type"] == "long" || out["type"] == "double" {
			delete(out, "token")
			delete(out, "caseSensitive")
			delete(out, "chn")
		}
		return out
	}
	return reflect.DeepEqual(norm(a), norm(b))
}

// record 执行全部用例两遍，两遍结果不同的用例报警（结果不稳定，不适合精确比对）。
func record(args []string) {
	fs := flag.NewFlagSet("record", flag.ExitOnError)
	only := fs.String("file", "", "只录制这个用例文件（不含 .json）")
	fs.Parse(args)
	c := newCloud()
	var up struct {
		Base int64 `json:"base"`
	}
	b, err := os.ReadFile("testdata/golden/upload.json")
	check(err)
	check(json.Unmarshal(b, &up))
	t := golden.NewTarget(c.client, c.project, c.logstore, nil)
	ix, err := dir.Index()
	check(err)
	names := golden.Names{Project: c.project, Logstore: c.logstore, Region: c.region, IndexKeys: map[string]bool{}}
	for k := range ix.Keys {
		names.IndexKeys[k] = true
	}
	ds, err := dir.Dataset()
	check(err)
	dsKeys := map[string]bool{}
	for _, g := range ds.Groups {
		for _, l := range g.Logs {
			for _, kv := range l.Contents {
				dsKeys[kv[0]] = true
			}
		}
	}
	foreign := foreignKeys(c, ix)
	files, err := dir.CaseFiles()
	check(err)
	unstable := 0
	for _, file := range files {
		if *only != "" && file != *only {
			continue
		}
		cases, err := dir.Cases(file)
		check(err)
		rec := &golden.Recording{Base: up.Base, RecordedAt: time.Now().UTC().Format(time.DateOnly), LogstoreLen: len(c.logstore)}
		for _, cs := range cases {
			var got [2]golden.Response
			for i := range got {
				raw, err := t.Run(cs, up.Base)
				check(err)
				got[i] = golden.Normalize(raw, names)
			}
			if !golden.Equal(got[0], got[1]) {
				unstable++
				log.Printf("UNSTABLE %s/%s:\n%s", file, cs.Name, golden.Diff(got[0], got[1]))
			}
			check(leakCheck(c, file+"/"+cs.Name, got[0]))
			check(foreignKeyCheck(file+"/"+cs.Name, got[0], foreign))
			check(rowCheck(file+"/"+cs.Name, got[0], dsKeys, up.Base))
			rec.Results = append(rec.Results, golden.RecordedCase{Name: cs.Name, Response: got[0]})
		}
		check(dir.WriteRecording(file, rec))
		log.Printf("%s: %d cases", file, len(cases))
	}
	if unstable > 0 {
		log.Printf("%d unstable cases: mark them in the case notes or remove them", unstable)
	}
}

// 账号 ID 出现在 acs:log:<region>:<account>: 资源名里，Normalize 会替换成占位符
var rawACS = regexp.MustCompile(`acs:log:[^{]`)

// leakCheck 确保录制结果里没有账号、project、region、AccessKey 等环境信息（结果会提交到公开仓库）。
func leakCheck(c *cloud, name string, r golden.Response) error {
	s := string(r.Body) + r.Text
	deny := []string{c.project, c.logstore, c.region, c.akID, c.endpoint}
	for _, w := range strings.Split(os.Getenv("SLS_GOLDEN_DENY"), ",") { // 额外不能出现的词，如公司名
		if w = strings.TrimSpace(w); w != "" {
			deny = append(deny, w)
		}
	}
	for _, bad := range deny {
		if strings.Contains(strings.ToLower(s), strings.ToLower(bad)) {
			return fmt.Errorf("%s: response still contains %q after normalization", name, bad)
		}
	}
	if rawACS.MatchString(s) {
		return fmt.Errorf("%s: response contains an unnormalized acs resource name", name)
	}
	return nil
}

// writeDataset 每条日志写一行，便于审阅 diff。
func writeDataset(path string, ds *golden.Dataset) error {
	var b strings.Builder
	b.WriteString("{\"groups\": [\n")
	for i, g := range ds.Groups {
		head, err := json.Marshal(struct {
			Topic  string      `json:"topic"`
			Source string      `json:"source"`
			Tags   [][2]string `json:"tags"`
		}{g.Topic, g.Source, g.Tags})
		if err != nil {
			return err
		}
		b.WriteString(strings.TrimSuffix(string(head), "}") + ", \"logs\": [\n")
		for j, l := range g.Logs {
			line, err := json.Marshal(l)
			if err != nil {
				return err
			}
			b.WriteString("  " + string(line))
			if j < len(g.Logs)-1 {
				b.WriteByte(',')
			}
			b.WriteByte('\n')
		}
		b.WriteString("]}")
		if i < len(ds.Groups)-1 {
			b.WriteByte(',')
		}
		b.WriteByte('\n')
	}
	b.WriteString("]}\n")
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// foreignKeys 是云端 logstore 索引里、golden 索引之外的字段（共用的 logstore 原有的字段）。
func foreignKeys(c *cloud, ix *sls.Index) map[string]bool {
	raw, err := c.client.GetIndexString(c.project, c.logstore)
	check(err)
	var cur struct {
		Keys map[string]any `json:"keys"`
	}
	check(json.Unmarshal([]byte(raw), &cur))
	out := map[string]bool{}
	for k := range cur.Keys {
		if _, ok := ix.Keys[k]; !ok {
			out[strings.ToLower(k)] = true
		}
	}
	return out
}

// foreignKeyCheck 确认 SQL 结果的列名与行字段里没有 logstore 原有的字段（例如 select * 会列出全部索引字段）。
func foreignKeyCheck(name string, r golden.Response, foreign map[string]bool) error {
	var body struct {
		Meta *struct {
			Keys []string `json:"keys"`
		} `json:"meta"`
		Data []map[string]any `json:"data"`
	}
	if r.Status != 200 || json.Unmarshal(r.Body, &body) != nil || body.Meta == nil {
		return nil
	}
	names := append([]string(nil), body.Meta.Keys...)
	for _, row := range body.Data {
		for k := range row {
			names = append(names, k)
		}
	}
	for _, k := range names {
		if foreign[strings.ToLower(k)] {
			return fmt.Errorf("%s: result contains field %q of the logstore's own index; refusing to record (do not use select * on a shared logstore)", name, k)
		}
	}
	return nil
}

// rowCheck 确认检索与上下文返回的每一行都来自合成数据集：时间在数据集的时间段内（录制前已确认这段时间只有
// 数据集的日志）；没有时间的行，字段都要在数据集里（或是 __ 开头的系统字段）。logstore 可能还有别的日志，这道检查防止把它们录进公开仓库。
// SQL 结果的列由查询决定，不检查；用例里不要写会突破请求时间范围的 __time__ 条件。
func rowCheck(name string, r golden.Response, dsKeys map[string]bool, base int64) error {
	var body struct {
		Meta *struct {
			HasSQL bool `json:"hasSQL"`
		} `json:"meta"`
		Data []map[string]any `json:"data"`
		Logs []map[string]any `json:"logs"`
	}
	if r.Status != 200 || json.Unmarshal(r.Body, &body) != nil {
		return nil
	}
	rows := body.Logs
	if body.Meta != nil && !body.Meta.HasSQL {
		rows = append(rows, body.Data...)
	}
	for _, row := range rows {
		t, hasTime := row["__time__"].(string)
		if hasTime {
			var ts int64
			fmt.Sscan(t, &ts)
			if ts < base || ts >= base+golden.DatasetSpan {
				return fmt.Errorf("%s: row time %s is outside the dataset window; refusing to record", name, t)
			}
			continue // 时间在数据集的时间段内：这一行来自数据集（SPL extend 等会加出新字段）
		}
		for k := range row {
			if !dsKeys[k] && !strings.HasPrefix(k, "__") {
				return fmt.Errorf("%s: row without __time__ has field %q that is not in the dataset; refusing to record", name, k)
			}
		}
	}
	return nil
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", " ")
	check(err)
	check(os.WriteFile(path, append(b, '\n'), 0o644))
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
