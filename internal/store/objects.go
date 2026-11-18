package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// objects 是放分段的对象存储。键是分段目录下的相对路径（/ 分隔）。
type objects interface {
	// Put 上传本地文件。
	Put(ctx context.Context, key, local string) error
	// Get 下载到本地文件（先写临时文件再改名）。对象不存在时返回 fs.ErrNotExist。
	Get(ctx context.Context, key, local string) error
	// List 列出 prefix 下的全部键。
	List(ctx context.Context, prefix string) ([]string, error)
	// ListVersions 列出 prefix 下的全部键与它们的版本（内容变了版本就变，如 S3 的 ETag）。
	ListVersions(ctx context.Context, prefix string) (map[string]string, error)
	// Delete 删除对象，不存在不算错。
	Delete(ctx context.Context, key string) error
	// ReadSmall、WriteSmall 读写小对象（写入节点的租约）。对象不存在时 ReadSmall 返回 fs.ErrNotExist。
	ReadSmall(ctx context.Context, key string) ([]byte, error)
	WriteSmall(ctx context.Context, key string, data []byte) error
	String() string
}

// OpenObjects 按 URL 打开对象存储：
//
//	file:///path              本地或挂载的目录（NFS 等）
//	s3://bucket/prefix        S3 兼容存储；端点与凭证取环境变量 SLS_S3_ENDPOINT（默认 s3.amazonaws.com）、
//	                          AWS_ACCESS_KEY_ID、AWS_SECRET_ACCESS_KEY；SLS_S3_INSECURE=1 时走 HTTP，
//	                          SLS_S3_REGION 指定区域
func OpenObjects(rawURL string) (objects, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("storage %q: %w", rawURL, err)
	}
	switch u.Scheme {
	case "file":
		if u.Path == "" {
			return nil, fmt.Errorf("storage %q: want file:///absolute/path", rawURL)
		}
		return fsObjects{root: filepath.FromSlash(u.Path)}, nil
	case "s3":
		if u.Host == "" {
			return nil, fmt.Errorf("storage %q: want s3://bucket/prefix", rawURL)
		}
		endpoint := os.Getenv("SLS_S3_ENDPOINT")
		if endpoint == "" {
			endpoint = "s3.amazonaws.com"
		}
		c, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewEnvAWS(),
			Secure: os.Getenv("SLS_S3_INSECURE") != "1",
			Region: os.Getenv("SLS_S3_REGION"),
		})
		if err != nil {
			return nil, fmt.Errorf("storage %q: %w", rawURL, err)
		}
		prefix := strings.Trim(u.Path, "/")
		if prefix != "" {
			prefix += "/"
		}
		return &s3Objects{c: c, bucket: u.Host, prefix: prefix}, nil
	}
	return nil, fmt.Errorf("storage %q: scheme must be file or s3", rawURL)
}

// ---------------- 目录 ----------------

type fsObjects struct{ root string }

func (o fsObjects) String() string { return "file://" + filepath.ToSlash(o.root) }

func (o fsObjects) path(key string) string { return filepath.Join(o.root, filepath.FromSlash(key)) }

func (o fsObjects) Put(_ context.Context, key, local string) error {
	return copyFile(local, o.path(key))
}

func (o fsObjects) Get(_ context.Context, key, local string) error {
	return copyFile(o.path(key), local)
}

func (o fsObjects) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	err := filepath.WalkDir(o.path(prefix), func(p string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil || d.IsDir() || strings.HasSuffix(p, ".tmp") {
			return err
		}
		rel, err := filepath.Rel(o.root, p)
		if err != nil {
			return err
		}
		keys = append(keys, filepath.ToSlash(rel))
		return nil
	})
	return keys, err
}

func (o fsObjects) ListVersions(_ context.Context, prefix string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.WalkDir(o.path(prefix), func(p string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil || d.IsDir() || strings.HasSuffix(p, ".tmp") {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return nil // 刚被删
		}
		rel, err := filepath.Rel(o.root, p)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = fmt.Sprintf("%d-%d", info.ModTime().UnixNano(), info.Size())
		return nil
	})
	return out, err
}

func (o fsObjects) Delete(_ context.Context, key string) error { return removeIfExists(o.path(key)) }

func (o fsObjects) ReadSmall(_ context.Context, key string) ([]byte, error) {
	return os.ReadFile(o.path(key))
}

func (o fsObjects) WriteSmall(_ context.Context, key string, data []byte) error {
	dst := o.path(key)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

// copyFile 复制文件：先写临时文件、同步后改名，读的一方不会看到写了一半的文件。
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

// ---------------- S3 ----------------

type s3Objects struct {
	c      *minio.Client
	bucket string
	prefix string
}

func (o *s3Objects) String() string { return "s3://" + o.bucket + "/" + o.prefix }

func (o *s3Objects) Put(ctx context.Context, key, local string) error {
	_, err := o.c.FPutObject(ctx, o.bucket, o.prefix+key, local, minio.PutObjectOptions{})
	return err
}

func (o *s3Objects) Get(ctx context.Context, key, local string) error {
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return err
	}
	tmp := local + ".tmp"
	if err := o.c.FGetObject(ctx, o.bucket, o.prefix+key, tmp, minio.GetObjectOptions{}); err != nil {
		os.Remove(tmp)
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return fmt.Errorf("%s: %w", key, fs.ErrNotExist)
		}
		return err
	}
	return os.Rename(tmp, local)
}

func (o *s3Objects) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range o.c.ListObjects(ctx, o.bucket, minio.ListObjectsOptions{Prefix: o.prefix + prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, strings.TrimPrefix(obj.Key, o.prefix))
	}
	return keys, nil
}

func (o *s3Objects) ListVersions(ctx context.Context, prefix string) (map[string]string, error) {
	out := map[string]string{}
	for obj := range o.c.ListObjects(ctx, o.bucket, minio.ListObjectsOptions{Prefix: o.prefix + prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out[strings.TrimPrefix(obj.Key, o.prefix)] = obj.ETag
	}
	return out, nil
}

func (o *s3Objects) Delete(ctx context.Context, key string) error {
	return o.c.RemoveObject(ctx, o.bucket, o.prefix+key, minio.RemoveObjectOptions{})
}

func (o *s3Objects) ReadSmall(ctx context.Context, key string) ([]byte, error) {
	obj, err := o.c.GetObject(ctx, o.bucket, o.prefix+key, minio.GetObjectOptions{})
	if err == nil {
		var data []byte
		data, err = io.ReadAll(obj)
		obj.Close()
		if err == nil {
			return data, nil
		}
	}
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return nil, fmt.Errorf("%s: %w", key, fs.ErrNotExist)
	}
	return nil, err
}

func (o *s3Objects) WriteSmall(ctx context.Context, key string, data []byte) error {
	_, err := o.c.PutObject(ctx, o.bucket, o.prefix+key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/json"})
	return err
}

// objectKey 是分段（或旁路文件）在对象存储里的键：分段目录下的相对路径。
func (s *Store) objectKey(local string) string {
	rel, err := filepath.Rel(s.segDir, local)
	if err != nil {
		return path.Base(local)
	}
	return filepath.ToSlash(rel)
}
