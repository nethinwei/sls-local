package golden

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// node 是保持键顺序的 JSON 值。SLS 返回日志时保持字段的写入顺序，比对时顺序也要算在内，
// 所以不能解码成 map。数字保留原文（1.0 与 1 不同）。
type node struct {
	kind   byte // 'o' 对象、'a' 数组、's' 其他（字符串、数字、布尔、null）
	keys   []string
	vals   []*node
	scalar any
}

func parseJSON(b []byte) (*node, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	n, err := parseNode(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing data after JSON value")
	}
	return n, nil
}

func parseNode(dec *json.Decoder) (*node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			n := &node{kind: 'o'}
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return nil, err
				}
				v, err := parseNode(dec)
				if err != nil {
					return nil, err
				}
				n.keys = append(n.keys, k.(string))
				n.vals = append(n.vals, v)
			}
			_, err := dec.Token()
			return n, err
		case '[':
			n := &node{kind: 'a'}
			for dec.More() {
				v, err := parseNode(dec)
				if err != nil {
					return nil, err
				}
				n.vals = append(n.vals, v)
			}
			_, err := dec.Token()
			return n, err
		}
		return nil, fmt.Errorf("unexpected delimiter %v", t)
	default:
		return &node{kind: 's', scalar: t}, nil
	}
}

func (n *node) get(key string) *node {
	if n == nil || n.kind != 'o' {
		return nil
	}
	for i, k := range n.keys {
		if k == key {
			return n.vals[i]
		}
	}
	return nil
}

func (n *node) del(key string) {
	if n == nil || n.kind != 'o' {
		return
	}
	for i, k := range n.keys {
		if k == key {
			n.keys = append(n.keys[:i], n.keys[i+1:]...)
			n.vals = append(n.vals[:i], n.vals[i+1:]...)
			return
		}
	}
}

// str 返回字符串值；不是字符串时返回 ""、false。
func (n *node) str() (string, bool) {
	if n == nil || n.kind != 's' {
		return "", false
	}
	s, ok := n.scalar.(string)
	return s, ok
}

func (n *node) setScalar(v any) { n.kind, n.keys, n.vals, n.scalar = 's', nil, nil, v }

// encode 输出带缩进的 JSON，键顺序与输入相同，不转义 HTML 字符（与 SLS 一致）。
func (n *node) encode() []byte {
	var buf bytes.Buffer
	n.write(&buf, "")
	return buf.Bytes()
}

func (n *node) write(buf *bytes.Buffer, indent string) {
	switch n.kind {
	case 'o':
		if len(n.keys) == 0 {
			buf.WriteString("{}")
			return
		}
		buf.WriteString("{\n")
		for i, k := range n.keys {
			buf.WriteString(indent + "  ")
			writeScalar(buf, k)
			buf.WriteString(": ")
			n.vals[i].write(buf, indent+"  ")
			if i < len(n.keys)-1 {
				buf.WriteByte(',')
			}
			buf.WriteByte('\n')
		}
		buf.WriteString(indent + "}")
	case 'a':
		if len(n.vals) == 0 {
			buf.WriteString("[]")
			return
		}
		buf.WriteString("[\n")
		for i, v := range n.vals {
			buf.WriteString(indent + "  ")
			v.write(buf, indent+"  ")
			if i < len(n.vals)-1 {
				buf.WriteByte(',')
			}
			buf.WriteByte('\n')
		}
		buf.WriteString(indent + "]")
	default:
		writeScalar(buf, n.scalar)
	}
}

func writeScalar(buf *bytes.Buffer, v any) {
	if num, ok := v.(json.Number); ok {
		buf.WriteString(num.String())
		return
	}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	buf.Truncate(buf.Len() - 1) // Encode 末尾带换行
}
