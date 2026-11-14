package golden

import (
	"path/filepath"

	sls "github.com/aliyun/aliyun-log-go-sdk"
)

// DatasetSpan 是数据集覆盖的秒数：日志时间落在 [Base, Base+DatasetSpan)。
const DatasetSpan = 1200

// Dataset 是合成的日志（tools/golden gen 生成），时间相对起点。
type Dataset struct {
	Groups []Group `json:"groups"`
}

// Group 对应一次 PutLogs 的 LogGroup，模拟采集器的一个包。
type Group struct {
	Topic  string      `json:"topic"`
	Source string      `json:"source"`
	Tags   [][2]string `json:"tags"`
	Logs   []Log       `json:"logs"`
}

type Log struct {
	T        int64       `json:"t"`  // 相对起点的秒
	Ns       uint32      `json:"ns"` // 纳秒部分
	Contents [][2]string `json:"c"`
}

func (d SuiteDir) Dataset() (*Dataset, error) {
	var ds Dataset
	if err := readJSON(filepath.Join(string(d), "dataset.json"), &ds); err != nil {
		return nil, err
	}
	return &ds, nil
}

// Count 返回日志条数。
func (ds *Dataset) Count() int {
	n := 0
	for _, g := range ds.Groups {
		n += len(g.Logs)
	}
	return n
}

// LogGroups 把数据集放到 base 开始的时间上。
func (ds *Dataset) LogGroups(base int64) []*sls.LogGroup {
	ptr := func(s string) *string { return &s }
	var out []*sls.LogGroup
	for _, g := range ds.Groups {
		lg := &sls.LogGroup{Topic: ptr(g.Topic), Source: ptr(g.Source)}
		for _, kv := range g.Tags {
			lg.LogTags = append(lg.LogTags, &sls.LogTag{Key: ptr(kv[0]), Value: ptr(kv[1])})
		}
		for _, l := range g.Logs {
			t, ns := uint32(base+l.T), l.Ns
			log := &sls.Log{Time: &t, TimeNs: &ns}
			for _, kv := range l.Contents {
				log.Contents = append(log.Contents, &sls.LogContent{Key: ptr(kv[0]), Value: ptr(kv[1])})
			}
			lg.Logs = append(lg.Logs, log)
		}
		out = append(out, lg)
	}
	return out
}
