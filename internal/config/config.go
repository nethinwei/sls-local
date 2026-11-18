// Package config 读取 sls-local 的配置文件（YAML；JSON 也是合法的 YAML）。
package config

import (
	"fmt"
	"os"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"sigs.k8s.io/yaml"
)

// Config 是配置文件的内容。
//
//	logstores:
//	  - project: devproj
//	    logstore: devstore
//	    index:            # 与 SLS GetIndex 返回的 JSON 结构相同，可直接从生产导出；省略时自动索引全部字段
//	      line: {token: [...], caseSensitive: false}
//	      keys:
//	        level: {type: text, token: [...], doc_value: true}
//	        cost_ms: {type: long, doc_value: true}
//	access_keys:          # 设置后校验请求签名（v1）；省略时不校验（本地开发）
//	  - {id: LTAI..., secret: ...}
type Config struct {
	Logstores  []Logstore  `json:"logstores"`
	AccessKeys []AccessKey `json:"access_keys"`
}

// AccessKey 是允许访问的一对 AccessKey。
type AccessKey struct {
	ID     string `json:"id"`
	Secret string `json:"secret"`
}

// Logstore 是一个预建的 logstore 及其索引。
type Logstore struct {
	Project  string     `json:"project"`
	Logstore string     `json:"logstore"`
	Index    *sls.Index `json:"index,omitempty"`
}

// Load 读取并校验配置文件。
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.UnmarshalStrict(b, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	seen := map[string]bool{}
	for i, l := range c.Logstores {
		if l.Project == "" || l.Logstore == "" {
			return nil, fmt.Errorf("%s: logstores[%d]: project and logstore are required", path, i)
		}
		key := l.Project + "/" + l.Logstore
		if seen[key] {
			return nil, fmt.Errorf("%s: logstore %s is declared twice", path, key)
		}
		seen[key] = true
		if l.Index != nil {
			for name, k := range l.Index.Keys {
				switch k.Type {
				case "", "text", "long", "double", "json":
				default:
					return nil, fmt.Errorf("%s: %s: index key %q has unknown type %q", path, key, name, k.Type)
				}
			}
		}
	}
	for i, k := range c.AccessKeys {
		if k.ID == "" || k.Secret == "" {
			return nil, fmt.Errorf("%s: access_keys[%d]: id and secret are required", path, i)
		}
	}
	return &c, nil
}
