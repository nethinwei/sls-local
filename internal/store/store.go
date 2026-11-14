// Package store 用 DuckDB 保存日志。所有 project/logstore 共用一张 logs 表：
// 固定列放时间与元数据，字段按写入顺序存成 JSON 文本（SLS 返回日志时保持字段顺序，插件依赖这一点）。
package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

const schema = `
CREATE TABLE IF NOT EXISTS logstores (
	project  VARCHAR NOT NULL,
	logstore VARCHAR NOT NULL,
	created  BIGINT  NOT NULL,
	PRIMARY KEY (project, logstore)
);
CREATE TABLE IF NOT EXISTS logs (
	project  VARCHAR NOT NULL,
	logstore VARCHAR NOT NULL,
	seq      BIGINT  NOT NULL,  -- 写入顺序，同一纳秒内的先后与 pack_meta 都用它
	t        BIGINT  NOT NULL,  -- __time__，unix 秒
	ns       INTEGER,           -- __time_ns_part__，采集端未开启纳秒时为 NULL
	topic    VARCHAR NOT NULL,
	source   VARCHAR NOT NULL,
	tags     VARCHAR NOT NULL,  -- JSON 对象，键带 __tag__: 前缀，按写入顺序
	contents VARCHAR NOT NULL,  -- JSON 对象，字段按写入顺序
	fulltext VARCHAR NOT NULL   -- 全部字段值以换行连接，供全文检索
);
-- 上下文：同一来源（pack_id 去掉最后一段序号）的日志按写入顺序相邻
ALTER TABLE logs ADD COLUMN IF NOT EXISTS pack_ctx VARCHAR;`

// KV 是一个字段或 tag。
type KV struct{ Key, Value string }

// Log 是写入的一条日志。
type Log struct {
	Time     uint32
	TimeNs   *uint32
	Contents []KV
}

// Group 是一次 PutLogs 写入的日志组，对应 protobuf LogGroup。
type Group struct {
	Topic, Source string
	Tags          []KV // 原始 tag 键（如 __hostname__），写入时加上 __tag__: 前缀
	Logs          []Log
}

// Store 是 DuckDB 上的日志存储。写入经同一个 Appender 串行进行，查询走连接池。
type Store struct {
	db  *sql.DB
	mu  sync.Mutex // 保护 app、conn、seq 与 known
	app *duckdb.Appender
	con driver.Conn
	seq int64

	known map[string]bool // 已登记的 project/logstore
}

// dsnOptions 关掉 DuckDB 访问外部文件与网络、安装扩展的能力：| where 与 SQL 接受用户写的表达式，
// 不能借 read_csv 之类的函数读到宿主机上的文件。integer_division 让整数相除得整数（与 SLS/Presto 一致）。
// lock_configuration 防止语句里用 SET 改回来。
const dsnOptions = "?enable_external_access=false&autoinstall_known_extensions=false&autoload_known_extensions=false" +
	"&integer_division=true&lock_configuration=true"

// Options 是存储的可选设置。
type Options struct {
	// SQLOffset 是 SQL 里时间函数的时区偏移：from_unixtime 返回这个时区的本地时间（不带时区），
	// date_format、now() 同理。SLS 默认东八区。
	SQLOffset time.Duration
}

// DefaultSQLOffset 是 SLS 的默认时区 UTC+8。
const DefaultSQLOffset = 8 * time.Hour

// macros 在每个连接上建临时宏，实现 SLS（Presto）里 DuckDB 没有的时间函数（query.funcMap 把调用改写成它们）。
func macros(offsetSec int64) string {
	off := strconv.FormatInt(offsetSec, 10)
	return `CREATE OR REPLACE TEMP MACRO sls_from_unixtime(x) AS
	make_timestamp(CAST(round(CAST(x AS DOUBLE) * 1000000) AS BIGINT)) + to_seconds(` + off + `);
CREATE OR REPLACE TEMP MACRO sls_to_unixtime(ts) AS CAST(epoch(ts) AS DOUBLE) - ` + off + `;
CREATE OR REPLACE TEMP MACRO sls_now() AS make_timestamp(epoch_us(get_current_timestamp())) + to_seconds(` + off + `);
CREATE OR REPLACE TEMP MACRO sls_utc(ts) AS ts - to_seconds(` + off + `);`
}

// Open 打开（或创建）path 上的数据库。path 为空时使用内存库（测试用）。
func Open(ctx context.Context, path string, opts Options) (*Store, error) {
	init := macros(int64(opts.SQLOffset / time.Second))
	connector, err := duckdb.NewConnector(path+dsnOptions, func(ex driver.ExecerContext) error {
		_, err := ex.ExecContext(context.Background(), init, nil)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("open duckdb %q: %w", path, err)
	}
	db := sql.OpenDB(connector)
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	s := &Store{db: db, known: map[string]bool{}}
	if err := db.QueryRowContext(ctx, `SELECT coalesce(max(seq), 0) FROM logs`).Scan(&s.seq); err != nil {
		db.Close()
		return nil, err
	}
	if s.con, err = connector.Connect(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if s.app, err = duckdb.NewAppenderFromConn(s.con, "", "logs"); err != nil {
		s.con.Close()
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close 刷出未写完的数据并关闭数据库。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.app.Close()
	if cerr := s.con.Close(); err == nil {
		err = cerr
	}
	if cerr := s.db.Close(); err == nil {
		err = cerr
	}
	return err
}

// EnsureLogstore 登记一个 logstore（已存在时什么也不做）。
func (s *Store) EnsureLogstore(ctx context.Context, project, logstore string) error {
	key := project + "/" + logstore
	s.mu.Lock()
	ok := s.known[key]
	s.mu.Unlock()
	if ok {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO logstores VALUES (?, ?, ?)`,
		project, logstore, time.Now().Unix()); err != nil {
		return err
	}
	s.mu.Lock()
	s.known[key] = true
	s.mu.Unlock()
	return nil
}

// LogstoreInfo 是一个 logstore 的元数据。
type LogstoreInfo struct {
	Name    string
	Created int64
}

// Logstores 按名字列出 project 下的 logstore。
func (s *Store) Logstores(ctx context.Context, project string) ([]LogstoreInfo, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT logstore, created FROM logstores WHERE project = ? ORDER BY logstore`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LogstoreInfo
	for rows.Next() {
		var i LogstoreInfo
		if err := rows.Scan(&i.Name, &i.Created); err != nil {
			return nil, err
		}
		out = append(out, i)
	}
	return out, rows.Err()
}

// Logstore 返回一个 logstore，不存在时 ok 为 false。
func (s *Store) Logstore(ctx context.Context, project, logstore string) (LogstoreInfo, bool, error) {
	i := LogstoreInfo{Name: logstore}
	err := s.db.QueryRowContext(ctx, `SELECT created FROM logstores WHERE project = ? AND logstore = ?`,
		project, logstore).Scan(&i.Created)
	if err == sql.ErrNoRows {
		return i, false, nil
	}
	return i, err == nil, err
}

// Append 写入一个日志组，返回后即可查到。
func (s *Store) Append(ctx context.Context, project, logstore string, g *Group) error {
	if len(g.Logs) == 0 {
		return nil
	}
	if err := s.EnsureLogstore(ctx, project, logstore); err != nil {
		return err
	}
	tags := make([]KV, 0, len(g.Tags))
	var packCtx any
	for _, t := range g.Tags {
		tags = append(tags, KV{"__tag__:" + t.Key, t.Value})
		if t.Key == "__pack_id__" {
			if i := strings.LastIndexByte(t.Value, '-'); i > 0 {
				packCtx = t.Value[:i]
			}
		}
	}
	tagsJSON := objectJSON(tags)

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range g.Logs {
		s.seq++
		var ns any
		if l.TimeNs != nil {
			ns = int32(*l.TimeNs)
		}
		if err := s.app.AppendRow(project, logstore, s.seq, int64(l.Time), ns, g.Topic, g.Source,
			tagsJSON, objectJSON(dedupe(l.Contents)), fulltext(l.Contents), packCtx); err != nil {
			return fmt.Errorf("append: %w", err)
		}
	}
	if err := s.app.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// dedupe 去掉重复的字段名（保留第一个），JSON 对象里重复的键无法按名字取值。
func dedupe(kvs []KV) []KV {
	seen := make(map[string]bool, len(kvs))
	out := kvs[:0:0]
	for _, kv := range kvs {
		if !seen[kv.Key] {
			seen[kv.Key] = true
			out = append(out, kv)
		}
	}
	return out
}

// objectJSON 按给定顺序编码成 JSON 对象。
func objectJSON(kvs []KV) string {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, kv := range kvs {
		if i > 0 {
			b.WriteByte(',')
		}
		writeString(&b, kv.Key)
		b.WriteByte(':')
		writeString(&b, kv.Value)
	}
	b.WriteByte('}')
	return b.String()
}

func writeString(b *bytes.Buffer, s string) {
	enc, _ := json.Marshal(s) // string 编码不会失败
	b.Write(enc)
}

func fulltext(kvs []KV) string {
	vals := make([]string, len(kvs))
	for i, kv := range kvs {
		vals[i] = kv.Value
	}
	return strings.Join(vals, "\n")
}

// DeleteBefore 删除 __time__ 早于 cutoff 的日志，返回删除条数。
func (s *Store) DeleteBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	r, err := s.db.ExecContext(ctx, `DELETE FROM logs WHERE t < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	return r.RowsAffected()
}
