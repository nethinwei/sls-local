package query

import (
	"slices"
	"strings"

	"github.com/nethinwei/sls-local/internal/index"
)

// Need 是一条检索语句要求日志里必须出现的词，存储层用分段的 bloom 过滤跳过不可能命中的分段。
// nil 表示没有要求（不能跳过任何分段）。
//
// 只收集「一定要出现」的词：and 两边的要求都要满足，or 至少满足一边（任一边没有要求则整体没有要求），not
// 里的不算。词是数据按分词符切出来的整词（小写）：Sig 是切词用的分词符（见 TokenSig）。
type Need struct {
	Op   string // "and"、"or"、"word"
	Sig  string
	Word string
	Kids []*Need
}

// TokenSig 是一组分词符的规范写法（去重排序，总带上字段分隔符）：bloom 与检索按它对应，分词符不同的不能互用。
func TokenSig(tokens string) string {
	rs := []rune(tokens + FieldSeparator)
	slices.Sort(rs)
	return string(slices.Compact(rs))
}

// Eval 判断 has（某个 bloom 可能含有某个词）之下这个分段是否可能命中。
func (n *Need) Eval(has func(sig, word string) bool) bool {
	if n == nil {
		return true
	}
	switch n.Op {
	case "word":
		return has(n.Sig, n.Word)
	case "and":
		for _, k := range n.Kids {
			if !k.Eval(has) {
				return false
			}
		}
		return true
	default: // or
		for _, k := range n.Kids {
			if k.Eval(has) {
				return true
			}
		}
		return false
	}
}

func needAll(op string, kids ...*Need) *Need {
	var ks []*Need
	for _, k := range kids {
		if k == nil {
			if op == "or" {
				return nil // 有一边没有要求：or 整体没有要求
			}
			continue // and：没有要求的一边不影响
		}
		ks = append(ks, k)
	}
	switch len(ks) {
	case 0:
		return nil
	case 1:
		return ks[0]
	}
	return &Need{Op: op, Kids: ks}
}

// needs 从语法树收集必须出现的词，与 compile 的语义对应：全文检索与 text 字段（不含 tag、__topic__、
// __source__、JSON 子键、中文分词）上的普通词。带通配符的词、非 ASCII 的词（大小写转换可能与 DuckDB 不同）
// 不收集。
func needs(n node, ix *index.Index) *Need {
	switch n := n.(type) {
	case binary:
		return needAll(strings.ToLower(n.op), needs(n.l, ix), needs(n.r, ix))
	case notBinary:
		return needs(n.l, ix)
	case term:
		var f *index.Field
		if n.key == "" {
			f = ix.Line
		} else {
			lower := strings.ToLower(n.key)
			if strings.HasPrefix(lower, "__tag__:") || lower == "__topic__" || lower == "__source__" {
				return nil
			}
			ff, path, ok := ix.Lookup(n.key)
			if !ok || len(path) > 0 || ff.Type != "text" {
				return nil
			}
			f = ff
		}
		if f == nil || f.Chn {
			return nil
		}
		wildcard := !n.quoted
		if wildcard && n.value == "*" {
			return nil
		}
		sig := TokenSig(f.Tokens)
		var kids []*Need
		for _, w := range splitWords(n.value, f.Tokens, wildcard) {
			if strings.ContainsAny(w, "*?") && wildcard || !isASCII(w) {
				continue
			}
			kids = append(kids, &Need{Op: "word", Sig: sig, Word: strings.ToLower(w)})
		}
		return needAll("and", kids...)
	}
	return nil // not、matchAll、数值比较
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
