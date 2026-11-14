package golden

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"

	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/nethinwei/sls-local/internal/api"
	"github.com/nethinwei/sls-local/internal/store"
)

var (
	updateKnown = flag.Bool("golden.update-known", false, "把当前不一致的用例写进 known-diffs.json（保留已有的原因说明）")
	dumpDir     = flag.String("golden.dump", "", "把 sls-local 的响应按录制文件的格式写到这个目录，便于与 recorded/ 对比")
)

const localProject = "golden-proj"

// TestGolden 回放录制的云端响应：sls-local 对同样的数据、同样的请求要给出同样的响应。
// known-diffs.json 里的用例允许不一致；它们一旦一致，测试会要求把它们从名单里删掉，保证只进不退。
func TestGolden(t *testing.T) {
	d := SuiteDir("../../testdata/golden")
	files, err := d.CaseFiles()
	if err != nil {
		t.Fatal(err)
	}
	ds, err := d.Dataset()
	if err != nil {
		t.Fatal(err)
	}
	ix, err := d.Index()
	if err != nil {
		t.Fatal(err)
	}
	known, err := d.KnownDiffs()
	if err != nil {
		t.Fatal(err)
	}

	recs := map[string]*Recording{}
	var base int64
	for _, f := range files {
		rec, err := d.Recording(f)
		if os.IsNotExist(err) {
			t.Logf("%s: not recorded yet, skipped", f)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if base != 0 && rec.Base != base {
			t.Fatalf("%s was recorded against a different upload (base %d, others %d); re-record all files", f, rec.Base, base)
		}
		base, recs[f] = rec.Base, rec
	}
	if len(recs) == 0 {
		t.Skip("no recordings")
	}

	logstore := LocalLogstore
	for _, r := range recs {
		if r.LogstoreLen > 0 {
			logstore = localLogstoreName(r.LogstoreLen)
		}
	}
	target := newLocalTarget(t, ix, logstore)
	for _, g := range ds.LogGroups(base) {
		if err := target.Client.PutLogs(localProject, logstore, g); err != nil {
			t.Fatalf("PutLogs: %v", err)
		}
	}

	names := Names{Project: localProject, Logstore: logstore, IndexKeys: map[string]bool{}}
	for k := range ix.Keys {
		names.IndexKeys[k] = true
	}
	stillDiff := map[string]string{}
	pass, total := 0, 0
	for _, f := range files {
		rec := recs[f]
		if rec == nil {
			continue
		}
		cases, err := d.Cases(f)
		if err != nil {
			t.Fatal(err)
		}
		dump := &Recording{Base: base}
		for _, c := range cases {
			key := f + "/" + c.Name
			t.Run(key, func(t *testing.T) {
				want, ok := rec.Lookup(c.Name)
				if !ok {
					t.Skip("not recorded; run go run ./tools/golden record")
				}
				total++
				raw, err := target.Run(c, base)
				if err != nil {
					t.Fatal(err)
				}
				got := Normalize(raw, names)
				dump.Results = append(dump.Results, RecordedCase{Name: c.Name, Response: got})
				reason, isKnown := known[key]
				if strings.HasPrefix(reason, "不稳定") {
					// 结果本身不确定（并列行的顺序等）：一致与否都不作判断
					t.Skipf("unstable: %s", reason)
				}
				if Equal(*want, got) {
					pass++
					if isKnown && !*updateKnown {
						t.Errorf("now matches SLS: remove %q from testdata/golden/known-diffs.json", key)
					}
					return
				}
				stillDiff[key] = reason
				msg := "query: " + c.Query + "\n" + Diff(*want, got)
				switch {
				case isKnown || *updateKnown:
					t.Skipf("known difference (%s)\n%s", reason, msg)
				default:
					t.Errorf("differs from SLS\n%s", msg)
				}
			})
		}
		if *dumpDir != "" {
			if err := SuiteDir(*dumpDir).WriteRecording(f, dump); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("%d/%d cases match SLS", pass, total)
	if *updateKnown {
		if err := d.WriteKnownDiffs(stillDiff); err != nil {
			t.Fatal(err)
		}
		keys := make([]string, 0, len(stillDiff))
		for k := range stillDiff {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		t.Logf("known-diffs.json: %d entries", len(keys))
	}
}

// newLocalTarget 在进程内起 sls-local；SDK 发往 {project}.{endpoint} 的请求都连到这个服务。
func newLocalTarget(t *testing.T, ix *sls.Index, logstore string) *Target {
	t.Helper()
	st, err := store.Open(context.Background(), "", store.Options{SQLOffset: store.DefaultSQLOffset})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	h := api.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), map[string]*sls.Index{localProject + "/" + logstore: ix})
	h.TTLDays = 30 // 与录制用的云端 logstore 一致（只影响 GetLogStore、GetIndex 里的 ttl）
	srv := httptest.NewServer(h.Handler())
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	c := &sls.Client{Endpoint: "sls-local:80", AccessKeyID: "local", AccessKeySecret: "local"}
	return NewTarget(c, localProject, logstore, &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	})
}

// localLogstoreName 返回长度为 n 的 logstore 名（与云端等长，报错列号才一致）。
func localLogstoreName(n int) string {
	name := LocalLogstore
	for len(name) < n {
		name += "-golden"
	}
	return name[:n]
}
