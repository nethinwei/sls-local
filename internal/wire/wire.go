// Package wire 解码 SLS PutLogs 的请求体：按 x-log-compresstype 解压，再按 protobuf LogGroup 解码。
package wire

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// MaxRawSize 是解压后请求体的上限。SLS 单次 PutLogs 限 10MB，这里放宽一些，只用来挡住异常的 x-log-bodyrawsize。
const MaxRawSize = 64 << 20

var zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))

// Decompress 按 compressType（lz4、zstd、deflate 或空）还原请求体，rawSize 取自 x-log-bodyrawsize。
func Decompress(body []byte, compressType string, rawSize int) ([]byte, error) {
	switch compressType {
	case "":
		return body, nil
	case "lz4":
		if rawSize <= 0 || rawSize > MaxRawSize {
			return nil, fmt.Errorf("invalid x-log-bodyrawsize %d", rawSize)
		}
		out := make([]byte, rawSize)
		n, err := lz4.UncompressBlock(body, out)
		if err != nil {
			return nil, fmt.Errorf("lz4: %w", err)
		}
		return out[:n], nil
	case "zstd":
		out, err := zstdDecoder.DecodeAll(body, make([]byte, 0, max(rawSize, 0)))
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}
		return out, nil
	case "deflate":
		r, err := zlib.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("deflate: %w", err)
		}
		defer r.Close()
		out, err := io.ReadAll(io.LimitReader(r, MaxRawSize+1))
		if err != nil {
			return nil, fmt.Errorf("deflate: %w", err)
		}
		if len(out) > MaxRawSize {
			return nil, fmt.Errorf("deflate: body larger than %d bytes", MaxRawSize)
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported x-log-compresstype %q", compressType)
}

// DecodeLogGroup 解压并解码一个 PutLogs 请求体。
func DecodeLogGroup(body []byte, compressType string, rawSize int) (*sls.LogGroup, error) {
	raw, err := Decompress(body, compressType, rawSize)
	if err != nil {
		return nil, err
	}
	g := &sls.LogGroup{}
	if err := g.Unmarshal(raw); err != nil {
		return nil, fmt.Errorf("decode LogGroup: %w", err)
	}
	return g, nil
}
