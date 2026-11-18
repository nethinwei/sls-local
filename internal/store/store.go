// Package store 用 DuckDB 保存日志。所有 project/logstore 共用一张 logs 表：
// 固定列放时间与元数据，字段按写入顺序存成 JSON 文本（SLS 返回日志时保持字段顺序，插件依赖这一点）。
package store

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/nethinwei/sls-local/internal/query"
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
	fulltext VARCHAR NOT NULL   -- 全文检索的文本：__source__、字段名与字段值，以 \x1f 分隔（见 fulltext()）
);
-- 上下文：同一来源（pack_id 去掉最后一段序号）的日志按写入顺序相邻
ALTER TABLE logs ADD COLUMN IF NOT EXISTS pack_ctx VARCHAR;
-- __pack_meta__ 的后两段：日志所在包（LogGroup）的条数与它在包里的序号
ALTER TABLE logs ADD COLUMN IF NOT EXISTS pack_n INTEGER;
ALTER TABLE logs ADD COLUMN IF NOT EXISTS pack_i INTEGER;
-- 有重复字段名时，原始字段名去重后的列表（JSON 数组，meta.keys 用）；没有重复时为 NULL
ALTER TABLE logs ADD COLUMN IF NOT EXISTS orig_keys VARCHAR;
-- 数据格式的版本等元信息
CREATE TABLE IF NOT EXISTS sls_meta (key VARCHAR PRIMARY KEY, value VARCHAR);`

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
	// NumericKeys 是按 long/double 建了索引的字段（小写）：它们的值不进全文索引（与 SLS 一致）
	NumericKeys map[string]bool
}

// Store 是 DuckDB 上的日志存储。写入经同一个 Appender 串行进行，查询走连接池。
// 写入先进 logs 表（头部），再定期刷成 Parquet 分段（见 segment.go）。
type Store struct {
	db  *sql.DB
	mu  sync.Mutex // 保护 app、conn、seq 与 known
	app *duckdb.Appender
	con driver.Conn
	seq int64

	known map[string]bool // 已登记的 project/logstore

	// 分段
	segDir      string
	flushRows   int64
	syncFlush   bool       // 测试用：行数到了就在 Append 里同步刷段
	tempSegDir  string     // 测试用的临时分段目录，Close 时删除
	flushMu     sync.Mutex // 刷段、保留、读回分段互斥
	segMu       sync.Mutex // 保护 cur、live、cleanups
	cur         *snapshot
	live        []*snapshot // 已被替换、但还有查询在用的快照
	cleanups    []cleanup
	flushNow    chan struct{}
	stopFlusher context.CancelFunc
	flusherDone chan struct{}
	onError     func(error)
	blooms      *bloomCache
	sigs        atomic.Pointer[func(project, logstore string) []string] // 见 SetTokenSigs
}

// dsnOptions 关掉 DuckDB 安装扩展的能力；integer_division 让整数相除得整数（与 SLS/Presto 一致）。
const dsnOptions = "?autoinstall_known_extensions=false&autoload_known_extensions=false&integer_division=true"

// lockDown 关掉 DuckDB 访问外部文件与网络的能力：| where 与 SQL 接受用户写的表达式，不能借 read_csv 之类的
// 函数读到宿主机上的文件。只放行分段目录（读写 Parquet 分段）。lock_configuration 防止语句里用 SET 改回来。
// 这些是全局设置，打开数据库后、处理任何请求前执行一次。
func lockDown(segDir string) string {
	dirs := "[]"
	if segDir != "" {
		dirs = "[" + sqlQuote(segDir+string(filepath.Separator)) + "]"
	}
	return "SET allowed_directories = " + dirs + "; SET enable_external_access = false; " +
		"SET parquet_metadata_cache = true; SET lock_configuration = true"
}

// Options 是存储的可选设置。
type Options struct {
	// SQLOffset 是 SQL 里时间函数的时区偏移：from_unixtime 返回这个时区的本地时间（不带时区），
	// date_format、now() 同理。SLS 默认东八区。
	SQLOffset time.Duration

	// SegmentDir 是 Parquet 分段的目录；为空时不刷段，日志全部留在 logs 表里。
	SegmentDir string
	// FlushInterval、FlushRows 是刷段的时机：间隔到了，或头部积累的行数到了。为 0 时用默认值。
	FlushInterval time.Duration
	FlushRows     int64
	// OnError 接收后台刷段、清理时的错误（可为空）。
	OnError func(error)
	// BloomCache 是分段 bloom 的内存缓存上限（字节），为 0 时用默认值。
	BloomCache int
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
	s := &Store{db: db, known: map[string]bool{}, onError: opts.OnError,
		blooms: newBloomCache(cmp.Or(opts.BloomCache, DefaultBloomCache))}
	if err := s.initSegments(&opts); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, lockDown(s.segDir)); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure duckdb: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	if s.cur, err = s.loadSegments(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := db.QueryRowContext(ctx, `SELECT coalesce(max(seq), 0) FROM logs`).Scan(&s.seq); err != nil {
		db.Close()
		return nil, err
	}
	s.seq = max(s.seq, s.cur.boundary)
	if s.con, err = connector.Connect(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if s.app, err = duckdb.NewAppenderFromConn(s.con, "", "logs"); err != nil {
		s.con.Close()
		db.Close()
		return nil, err
	}
	if s.segDir != "" && !s.syncFlush {
		fctx, cancel := context.WithCancel(context.Background())
		s.stopFlusher, s.flushNow, s.flusherDone = cancel, make(chan struct{}, 1), make(chan struct{})
		go s.flusher(fctx, cmp.Or(opts.FlushInterval, DefaultFlushInterval))
	}
	return s, nil
}

// initSegments 定下分段目录与刷段阈值。测试可以设 SLS_LOCAL_TEST_FLUSH_ROWS=n：没有配置分段目录时用一个
// 临时目录，头部每满 n 行就在 Append 里同步刷段，让整套测试也覆盖「一部分在头部、一部分在分段」的情形。
func (s *Store) initSegments(opts *Options) error {
	s.flushRows = cmp.Or(opts.FlushRows, DefaultFlushRows)
	if v := os.Getenv("SLS_LOCAL_TEST_FLUSH_ROWS"); v != "" && opts.SegmentDir == "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return fmt.Errorf("SLS_LOCAL_TEST_FLUSH_ROWS=%q: want a positive integer", v)
		}
		dir, err := os.MkdirTemp("", "sls-segments-")
		if err != nil {
			return err
		}
		opts.SegmentDir, s.flushRows, s.syncFlush, s.tempSegDir = dir, n, true, dir
	}
	if opts.SegmentDir == "" {
		return nil
	}
	dir, err := filepath.Abs(opts.SegmentDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("segment directory: %w", err)
	}
	if dir, err = filepath.EvalSymlinks(dir); err != nil { // DuckDB 按真实路径检查 allowed_directories
		return err
	}
	s.segDir = dir
	return nil
}

// fulltextVersion 是全文列的生成规则版本：2 起包含 __source__ 与字段名、不含数值字段的值；3 起各部分以
// \x1f 分隔（短语不能跨字段）、重复的字段名按 SLS 的规则改名（v0.2.0）。
const fulltextVersion = "3"

// RebuildFulltext 把旧版本写入的全文列按当前规则重建（升级后第一次启动时执行一次）；force 时不管版本，
// 按当前的索引配置全部重建（sls-local -reindex）。numeric 返回 logstore 里按 long/double 建了索引的字段。
// 返回重建的行数；已经是当前版本且不 force 时为 0。
func (s *Store) RebuildFulltext(ctx context.Context, numeric func(project, logstore string) map[string]bool, force bool) (int64, error) {
	var v sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT value FROM sls_meta WHERE key = 'fulltext_version'`).Scan(&v)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if v.String == fulltextVersion && !force {
		return 0, nil
	}
	s.flushMu.Lock() // 重建期间不刷段
	defer s.flushMu.Unlock()
	if err := s.unflush(ctx); err != nil { // 分段里的日志读回头部一起重建，之后再刷出去
		return 0, err
	}
	s.mu.Lock() // 重建期间不接受写入
	defer s.mu.Unlock()
	if _, err := s.db.ExecContext(ctx, `CREATE OR REPLACE TABLE sls_fulltext_rebuild (seq BIGINT, fulltext VARCHAR)`); err != nil {
		return 0, err
	}
	defer s.db.ExecContext(context.Background(), `DROP TABLE IF EXISTS sls_fulltext_rebuild`)
	app, err := duckdb.NewAppenderFromConn(s.con, "", "sls_fulltext_rebuild")
	if err != nil {
		return 0, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT project, logstore, seq, source, contents FROM logs`)
	if err != nil {
		app.Close()
		return 0, err
	}
	var n int64
	cache := map[string]map[string]bool{}
	for rows.Next() {
		var project, logstore, source, contents string
		var seq int64
		if err := rows.Scan(&project, &logstore, &seq, &source, &contents); err != nil {
			rows.Close()
			app.Close()
			return 0, err
		}
		key := project + "/" + logstore
		num, ok := cache[key]
		if !ok {
			num = numeric(project, logstore)
			cache[key] = num
		}
		if err := app.AppendRow(seq, fulltext(source, orderedObject(contents), num)); err != nil {
			rows.Close()
			app.Close()
			return 0, err
		}
		n++
	}
	rows.Close()
	if err := app.Close(); err != nil {
		return 0, err
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE logs SET fulltext = r.fulltext FROM sls_fulltext_rebuild r WHERE logs.seq = r.seq`); err != nil {
		return 0, err
	}
	_, err = s.db.ExecContext(ctx, `INSERT OR REPLACE INTO sls_meta VALUES ('fulltext_version', ?)`, fulltextVersion)
	return n, err
}

// Close 刷出未写完的数据并关闭数据库。
func (s *Store) Close() error {
	if s.stopFlusher != nil {
		s.stopFlusher()
		<-s.flusherDone
	}
	if s.tempSegDir != "" {
		defer os.RemoveAll(s.tempSegDir)
	}
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

	// 编码在锁外做，锁内只剩 Appender 写入
	type encoded struct {
		ns, origKeys       any
		contents, fulltext string
	}
	enc := make([]encoded, len(g.Logs))
	for i, l := range g.Logs {
		if l.TimeNs != nil {
			enc[i].ns = int32(*l.TimeNs)
		}
		fields, orig := renameDuplicates(l.Contents)
		if orig != nil {
			enc[i].origKeys = *orig
		}
		enc[i].contents = objectJSON(fields)
		enc[i].fulltext = fulltext(g.Source, fields, g.NumericKeys)
	}

	seq, err := func() (int64, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, l := range g.Logs {
			s.seq++
			if err := s.app.AppendRow(project, logstore, s.seq, int64(l.Time), enc[i].ns, g.Topic, g.Source,
				tagsJSON, enc[i].contents, enc[i].fulltext, packCtx, int32(len(g.Logs)), int32(i), enc[i].origKeys); err != nil {
				return 0, fmt.Errorf("append: %w", err)
			}
		}
		if err := s.app.Flush(); err != nil {
			return 0, fmt.Errorf("flush: %w", err)
		}
		return s.seq, nil
	}()
	if err != nil {
		return err
	}
	s.maybeFlush(ctx, seq)
	return nil
}

// renameDuplicates 按 SLS 的规则给重复的字段名改名：同名字段第 n 次出现（n ≥ 2）改成 名字_{n-2}，
// 位置不变，也不检查改名后是否与其他字段重名（实测 SLS 的响应里会出现重复的键）。按名字取值时取第一个，
// 与 SLS 相同。有重复时另外返回原始字段名去重后的 JSON 数组：SLS 的 meta.keys 用的是它。
func renameDuplicates(kvs []KV) ([]KV, *string) {
	if !hasDuplicateKey(kvs) { // 绝大多数日志没有重复字段：不分配
		return kvs, nil
	}
	seen := make(map[string]int, len(kvs))
	out := make([]KV, len(kvs))
	var orig []string
	for i, kv := range kvs {
		n := seen[kv.Key]
		seen[kv.Key] = n + 1
		out[i] = kv
		if n == 0 {
			orig = append(orig, kv.Key)
		} else {
			out[i].Key = kv.Key + "_" + strconv.Itoa(n-1)
		}
	}
	b, _ := json.Marshal(orig)
	keys := string(b)
	return out, &keys
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

// writeString 按 encoding/json 的规则把 s 编码成 JSON 字符串。绝大多数值只含不需要转义的 ASCII，
// 直接写出；其余交给 json.Marshal（它还会转义 <>&、U+2028/2029 并替换非法 UTF-8，输出保持不变）。
func writeString(b *bytes.Buffer, s string) {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c >= 0x80 || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			enc, _ := json.Marshal(s) // string 编码不会失败
			b.Write(enc)
			return
		}
	}
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	b.WriteString(s)
	b.WriteByte('"')
}

func hasDuplicateKey(kvs []KV) bool {
	if len(kvs) <= 16 { // 字段少时两两比较比建 map 快
		for i := range kvs {
			for j := range i {
				if kvs[j].Key == kvs[i].Key {
					return true
				}
			}
		}
		return false
	}
	seen := make(map[string]bool, len(kvs))
	for _, kv := range kvs {
		if seen[kv.Key] {
			return true
		}
		seen[kv.Key] = true
	}
	return false
}

// fulltext 是全文检索的文本。实测 SLS 的全文检索也能匹配字段名（搜 currency 命中带这个字段的日志）
// 与 __source__，但匹配不到 long/double 字段的值，也匹配不到 tag。各部分以 query.FieldSeparator 分隔：
// SLS 按字段分别分词，短语（#"..."）不能跨字段。值里本身带 \x1f 的极少，那里也会被当成字段边界。
func fulltext(source string, kvs []KV, numeric map[string]bool) string {
	parts := make([]string, 0, 2*len(kvs)+1)
	parts = append(parts, source)
	for _, kv := range kvs {
		parts = append(parts, kv.Key)
		if !numeric[strings.ToLower(kv.Key)] {
			parts = append(parts, kv.Value)
		}
	}
	return strings.Join(parts, query.FieldSeparator)
}

// DeleteBefore 删除 __time__ 早于 cutoff 的日志，返回删除条数。分段按整个文件删除：一个分段里最晚的日志
// 也早于 cutoff 时才删（分段按小时划分，最多多留一小时）。
func (s *Store) DeleteBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	r, err := s.db.ExecContext(ctx, `DELETE FROM logs WHERE t < ?`, cutoff.Unix())
	if err != nil {
		return 0, err
	}
	n, err := r.RowsAffected()
	return n + s.dropSegmentsBefore(cutoff.Unix()), err
}
