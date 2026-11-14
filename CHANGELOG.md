# Changelog

# 未发布

**SQL**

- SQL 改用基于 Presto 语法（prestodb 的 SqlBase.g4，ANTLR 生成）的翻译器：不写 AS 的别名、乘号、子查询、CTE、UNION、窗口函数等写法都能正确处理；语法错误是 Presto 的原文。设置 `SLS_LOCAL_SQL=legacy` 可临时切回原来的逐记号改写。
- 列类型、报错文案与列号、double 的文本格式（Grisu2）、`__time___0` 等细节与真实 SLS 一致；新增 `time_series`、窗口函数、`url_extract_*`、`date_add` 等函数；支持索引别名，JSON 子键按嵌套路径取值。

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
