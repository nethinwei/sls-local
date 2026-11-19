# Changelog

# v0.3.0（未发布）

从本地开发用的模拟服务扩展到可以自建在生产环境（多节点部署是 beta）：数据以 Parquet 分段放在对象存储上，写入节点与查询节点分开部署，开启签名校验。不配置这些时，单机用法与 v0.2 相同（数据文件会自动升级）。

**存储**

- 新写入的日志先进数据文件（头部），每 5 分钟或积累 100 万条时按 project / logstore / 小时写成 Parquet 分段（zstd，每个文件最多 100 万条）。查询看的是头部加分段的一份快照：刷段、合并期间的查询不会重复或漏掉日志。
- 常见的字段（出现在 1% 以上的日志里，前 64 个）在分段里单独成列，聚合与字段检索直接读列，不再逐行解析 JSON。
- 每个分段带 bloom 过滤器：检索罕见的值时跳过不可能命中的分段。与索引的分词符对应，索引改过之后旧分段不跳过（结果不变）。
- 小分段定期合并：还在写入的小时分层合并（同样大小的 8 个合成一个，每条日志只被重写几次），安静 10 分钟后把剩下的小文件合并完；保留期按分段删除（最多多留一小时）；数据目录不存在时自动创建。
- 新参数：`-segments`、`-flush-interval`、`-flush-rows`。

**对象存储与多节点**

- `-storage s3://bucket/prefix`（OSS、S3、MinIO 等，minio-go）或 `file:///path`：分段上传到对象存储，本地目录成为缓存（`-cache-size`，默认 50 GB；合并掉、过期的分段不等淘汰直接删）。先在本地提交再上传，上传失败会重试；换了磁盘时从对象存储恢复。S3 凭证依次取环境变量、`~/.aws/credentials`、AWS 的 IAM 角色（IRSA 等）。
- `-target=write|read`：写入节点只接受写入，默认每 2 秒刷段、退出前刷段；查询节点只做查询，看到的数据比写入晚 2～4 秒。
- 写入节点按小时维护自己的分段清单（`_manifests/<编号>/<小时>.json`），查询节点每 2 秒（`-refresh-interval`）只列出清单、下载变了的，不再列出全部分段；新分段的 bloom 在刷新时预先下载，查询用到的分段并行下载，查询取消时不再等。
- `-writer-id` 区分写入节点（`auto` 取主机名末尾的序号，配合 StatefulSet）：写入节点在对象存储上持有编号的租约（`_writers/<编号>.json`，每 10 秒续约），编号重复的节点启动不了；运行中被别人拿走租约时停止写入、刷段与删除，`/ready` 失败。同一块盘上重启不用等租约过期。每个编号可写 2^48 条，用完时拒绝写入。
- 下线的写入节点（租约已释放，或 10 分钟没续）留下的过期分段由其他写入节点删除，它的清单也由其他写入节点按对象存储上的实际分段重写。
- 示例：`examples/production`（SeaweedFS、2 个写入节点、1 个查询节点、LoongCollector、Grafana）；`examples/kubernetes`（StatefulSet 写入节点、Deployment 查询节点）。

**运维**

- 配置文件的 `access_keys`：校验请求签名（v1，与 SLS SDK、LoongCollector 相同），核对 Content-MD5；不配置时不校验。`projects` 可以把密钥限定在某些 project。先核对签名再读请求体。写入、查询节点没配 AccessKey 时启动日志里警告。
- 单机（`-target=all`）开了 `-storage` 时默认 30 秒刷段（对象存储当备份，盘坏最多丢 30 秒）；查询节点 1 分钟以上没读到分段清单时 `/ready` 失败。告警规则示例 `examples/kubernetes/alerts.yaml`；README 增加「单点与容灾」。
- `/health`、`/ready`、`/metrics`（Prometheus）：各接口的请求数与耗时、写入量、刷段、合并、上传下载、bloom 跳过的分段、后台错误。镜像的健康检查改用 `/health`。
- `-query-timeout`（默认 1 分钟）、`-max-queries`（默认 CPU 数的 2 倍）、`-memory-limit`（DuckDB）、`-write-rate`（每个 logstore 每秒条数）。
- 报错码与限制见 docs/compatibility.md「鉴权、限流与多节点」：这些没有与真实 SLS 逐条对照。

**性能**（200 万条日志，落盘，与 v0.2 相比；`internal/api/bench_test.go`，`SLS_BENCH_DISK=1 SLS_BENCH_SEGMENTS=1`）

- 分组聚合 128 → 9.5 ms，检索后聚合 121 → 9.3 ms，直方图 59 → 6.3 ms，百分位 65 → 15 ms，计数 7.1 → 3.6 ms。
- 罕见值检索 70 → 0.9 ms，组合条件 12.9 → 5.7 ms，字段检索 6.8 → 3.9 ms。
- 混合查询的并发吞吐 48 → 266 次/秒。
- 变慢的：常见词的全文检索 7.0 → 9.4 ms，翻页 3.0 → 3.9 ms（只取 5 条 1.9 → 3.0 ms），写入 30 → 28 万条/秒。
- 2000 万条时，罕见值检索 7.6 ms、上下文 9.8 ms；聚合仍随数据量线性增长（分组聚合 80 ms）。

**其他**

- 测试：CI 另外用 `SLS_LOCAL_TEST_FLUSH_ROWS=3`（数据频繁刷成分段、不断合并）与 `SLS_LOCAL_TEST_REMOTE=1`（再放进对象存储、每次刷段后清空本地缓存）各跑一遍全部测试；`SLS_LOCAL_TEST_REMOTE=s3://…` 可以在真实的 S3 上跑。
- `tools/authcheck`：把同一组鉴权失败的请求发给真实 SLS 与 sls-local，并排对照报错。
- golden：4 个用例改标「不稳定」（并行扫描下 double 聚合的末位、并列行的顺序、`array_agg` 的元素顺序）。
- e2e 的 GetIndex 测试改用固定时钟（之前随日期失效）。

# v0.2.0 (2026-11-15)

**SQL**

- SQL 改用基于 Presto 语法（prestodb 的 SqlBase.g4，ANTLR 生成）的翻译器：不写 AS 的别名、乘号、子查询、CTE、UNION、窗口函数等写法都能正确处理；语法错误是 Presto 的原文。设置 `SLS_LOCAL_SQL=legacy` 可临时切回原来的逐记号改写。
- 列类型、报错文案与列号、double 的文本格式（Grisu2）、`__time___0` 等细节与真实 SLS 一致；新增 `time_series`、窗口函数、`url_extract_*`、`date_add` 等函数；支持索引别名，JSON 子键按嵌套路径取值。

**响应格式与检索**

- meta 的字段、顺序与取值（`processedRows`、`whereQuery`、`aggQuery`、`terms`、`insertedSQL`、`limited`……）、响应头、直方图、上下文（`total_lines`、`__pack_meta__` 的格式）、GetIndex、GetLogStore 与 SLS 一致；新增 ListShards、GetCursor。
- 检索：全文检索匹配字段名与 `__source__`、不匹配数值字段的值；没有索引的 tag 按整个值匹配；`key : value`；long 字段比较的数值解析与 SLS 相同；语法错误带位置。
- SPL：新增 `extend`、`project`、`project-away`、`parse-json`、`parse-regexp`、`parse-kv`；表达式用 Presto 语法翻译，报错与 SLS 一致。
- SQL：新增 `compare`（同比环比）。
- 修复：全文索引的分词符里没有换行时，全文检索什么也匹配不到（字段之间现在总是词边界）。
- v0.1 写入的数据启动时自动重建全文列。
- 修复：GetHistograms 的时间范围或 interval 极端时会生成海量的桶，占满内存。
- 检索的引号语义与 SLS 一致：`"a b"`、`a,b` 只要求各词都出现、不管顺序；新增短语 `#"a b"`（按顺序相邻、不跨字段），meta 的 `phraseQueryInfo` 等与 SLS 相同，`not` 里或与 SQL / SPL 同用时报 SLS 同款错误。
- 同一条日志里重复的字段名按 SLS 的规则改名（`x`、`x_0`、`x_1`……），不再丢掉后面的值；`meta.keys` 用原始字段名。
- 全文列的字段之间改用 `\x1f` 分隔（字段值里的换行不再被当成字段边界），升级后第一次启动自动重建。
- 翻页时各次查询在同一个快照里执行：查询期间写入的日志不会让一页里的结果重复或错开。
- 新增 `-reindex`：修改索引配置后按新配置重建旧日志（与 SLS 一样，改索引默认只影响之后写入的日志）。

**性能**（20 万条日志，见 `internal/api/bench_test.go`）

- 日志检索按时间窗口从排在前面的一端逐段扫描，凑够一页就停：`*` 9.8 → 2.0 ms，字段检索 30 → 2.4 ms，全文检索与组合条件快 15～20 倍；没有命中时与原来持平。
- 全文检索先做子串预过滤再跑正则；SQL 的 processedRows 与结果并发计算；去掉检索里没人用的总数窗口函数。
- 写入：编码移到锁外，常见字符串不再经过 `json.Marshal`，22 → 31 万条/秒。

**测试**

- golden 对照测试：合成数据写进云端与 sls-local，云端响应录制进仓库，CI 回放并要求只进不退（docs/golden.md）。

# v0.1.0 (2026-11-14)

首个版本：说 SLS 协议的本地服务，LoongCollector / SDK 写入，Grafana 的 SLS 数据源查询，数据存在本地 DuckDB。

**写入**

- PutLogs：protobuf LogGroup，lz4 / zstd / deflate 压缩；SDK 的 `/logstores/{logstore}` 与 LoongCollector 的 `/shards/lb` 两种路径。
- 同时监听 HTTP（查询）与自签 HTTPS:443（社区版 LoongCollector 只走 443、不校验证书）。
- 写入时自动创建 logstore；按 `__time__` 保留 3 天（`-ttl`）。

**查询**

- GetLogsV3：检索语法（and / or / not、短语、词内通配、数值比较与区间、`__tag__:`、`__topic__`、`__source__`、全文）、SPL `| where`、`| with_pack_meta`，纳秒精度时间范围，翻页。
- SQL（Presto 方言 → DuckDB）：聚合、分组、排序、`$__timeGroup` 展开后的写法、常用函数（`from_unixtime`、`date_format`、`date_trunc`、`approx_percentile`、`approx_distinct`、`regexp_like`、`json_extract_scalar` 等），时间函数默认东八区（`-sql-utc-offset`），整数除法，默认 LIMIT 100；返回格式与 SLS 一致。
- GetHistograms（自动分桶、边界对齐）、GetContextLogs（按来源跨 pack）、GetIndex、ListLogStores、GetLogStore、GetProject。

**索引**

- 配置文件声明索引，格式与 SLS GetIndex 的 JSON 相同；没有字段索引时报 SLS 同款错误，long / double 按数值匹配，SQL 只能用开启统计的字段。不配置时自动为所有字段建 text 索引。

**与 SLS 对齐的方式**

- 语义与报错文案按真实 SLS 的行为逐条对照（docs/wire-findings.md），端到端测试用官方 Go SDK 当客户端。已知差异见 docs/compatibility.md。

**安全**

- 没有鉴权，只用于本地开发与测试。DuckDB 关闭外部文件访问与改配置；SQL 只能读本 logstore 的数据。
