# 兼容性

sls-local 的目标是：**SLS 的客户端（SDK、LoongCollector、Grafana 数据源）不改代码就能连上，常用查询的结果与报错和真实 SLS 一致**。每条行为与真实 SLS 的对照记录见 [wire-findings.md](wire-findings.md)，本文是速查表。

## 接口

| 接口 | 状态 | 说明 |
|---|---|---|
| PutLogs | ✅ | `POST /logstores/{ls}`（SDK）、`/logstores/{ls}/shards/lb`、`/shards/route`（LoongCollector）；lz4 / zstd / deflate |
| GetLogsV3 | ✅ | `POST /logstores/{ls}/logs`：检索、SPL、SQL |
| GetHistograms | ✅ | 自动间隔与 SLS 一致（7 天以上的档位是外推） |
| GetContextLogs | ✅ | 依赖 `\| with_pack_meta` 返回的 `__pack_meta__` |
| GetIndex | ✅ | 返回配置的索引；自动索引时列出最近一天出现过的字段 |
| ListShards、GetCursor | ✅ | 一个 shard；游标是写入序号 |
| ListLogStores、GetLogStore、GetProject | ✅ | |
| 旧版 GetLogs（`GET ?type=log`） | ❌ | 用 GetLogsV3 |
| CreateLogStore、CreateIndex、UpdateIndex | ❌ | logstore 在写入时自动创建，索引写在配置文件里 |
| PullLogs、消费组、MetricStore、数据加工、告警 | ❌ | |
| 签名校验、RAM 权限 | ❌ | 签名只解析不校验，任何 AccessKey 都能用 |

## 检索语法

| 写法 | 状态 |
|---|---|
| `key: value`、`key:value`、字段名不区分大小写 | ✅ |
| 全文检索、相邻裸词（隐式 and） | ✅ |
| `"a b"`、`a,b`：各词都出现即可，不管顺序（与 SLS 一致：引号不表示短语） | ✅ |
| 短语 `#"a b"`、`key: #"a b"`：按顺序相邻、不跨字段；meta 的 `phraseQueryInfo`、`new_mode`、`processedRows` 按 SLS 的规则；不能放在 `not` 里、不能与 SQL / SPL 同用（报错与 SLS 一致） | ✅ |
| 同一条日志里重复的字段名：按 SLS 的规则改名为 `名字_0`、`名字_1`……，按名字取值时取第一个 | ✅ |
| 修改索引配置：与 SLS 一样只影响之后写入的日志，`-reindex` 按新配置重建旧日志 | ✅ |
| `and`、`or`、`not`（一元与二元）、括号 | ✅ |
| 词内与词尾通配 `pai*`、`p*d`、`a?c`；`key: *` 字段存在 | ✅ |
| 全文检索匹配字段名、`__source__`（不匹配数值字段的值与 tag） | ✅ |
| 前导通配 `*abc` | 报错，与 SLS 一致 |
| long / double：`key: 68`、`>`、`>=`、`<`、`<=`、`=`、`in [a b]`、`in (a b]` | ✅ |
| `__topic__`、`__source__`、`__tag__:x` | ✅（没有索引的 tag 按整个值匹配） |
| 字段的分词符、大小写敏感、JSON 子键 `a.b` | ✅ 按索引配置 |
| 中文分词（chn）、全文索引的 include_keys / exclude_keys | ❌ |

## SPL

| 指令 | 状态 |
|---|---|
| `\| where <表达式>`（可多个）、`\| with_pack_meta` | ✅ 字段是 varchar，`__time__` 是数值；`cast` 失败时是 null |
| `extend`、`project`、`project-away`、`parse-json` | ✅ |
| `parse-regexp 字段, '正则' as a, b`、`parse-kv 字段, '分隔符', '键值分隔符'`、`parse-kv -regexp 字段, '正则'` | ✅ 解析出的字段可以被后面的命令引用 |
| `stats`、`sort`、`limit` | 与云端一样报错（云端这一版 SPL 不支持） |
| `parse-csv` 等其他指令 | ❌ 返回 NotSupported |

## SQL

语法是 Presto 的（用 prestodb 的 SqlBase.g4 解析），翻译成 DuckDB 执行。支持 `select … [from log] [where] [group by] [having] [order by] [limit]`、子查询、`with`、`union`/`intersect`/`except`、join、窗口函数、`filter (where …)`、`set session …;` 前缀。没写 FROM 时按 SLS 的规则补 `from log`，没写 LIMIT 时返回 100 行。字段类型按索引（long → bigint、double → double、text → varchar），没开统计的字段值是 null，索引别名可用。

| 类别 | 函数 |
|---|---|
| 聚合 | `count`、`count_if`、`sum`、`avg`、`min`、`max`、`arbitrary`、`max_by`、`min_by`、`bool_and`、`bool_or`、`stddev`、`variance` 等、`corr`、`covar_*`、`regr_*`、`geometric_mean`、`histogram`、`array_agg`、`map_agg`、`approx_distinct`（精确值）、`approx_percentile`（离散分位数） |
| 窗口 | `row_number`、`rank`、`dense_rank`、`percent_rank`、`cume_dist`、`ntile`、`lag`、`lead`、`first_value`、`last_value`、`nth_value` |
| 时间 | `from_unixtime`、`to_unixtime`、`now`、`current_timestamp`、`current_date`、`date_format`、`date_parse`、`date_trunc`、`date_diff`、`date_add`、`extract`、`year` … `second`、`day_of_week`、`day_of_year`、`time_series`（不补空桶）、`at time zone 'UTC'`；时间参数也可以直接给 Unix 秒 |
| 字符串 | `lower`、`upper`、`length`、`substr`、`concat`、`replace`、`reverse`、`trim`、`lpad`、`rpad`、`strpos`、`position`、`split`、`split_part`、`starts_with`、`regexp_like`、`regexp_extract`、`regexp_extract_all`、`regexp_replace`、`url_extract_*`、`url_encode`、`url_decode`、`to_utf8`、`from_utf8`、`md5`、`to_hex`、`to_base64`、`from_base64` |
| JSON | `json_extract`、`json_extract_scalar`、`json_array_length`、`json_size`、`json_parse`、`json_format`（字段值不是合法 JSON 时为 null） |
| 数组与 map | `array[…]`、`cardinality`、`contains`、`element_at`、`array_join`、`array_distinct`、`array_max`、`array_min`、`array_sort`、`array_position`、`slice`、`sequence`、`filter`、`transform`、`map`、`map_keys`、`map_values` |
| 数学与条件 | `abs`、`ceil`、`floor`、`round`、`sqrt`、`power`、`mod`、`ln`、`log2`、`log10`、三角函数、`greatest`、`least`、`if`、`coalesce`、`nullif`、`try`、`typeof`、`cast`、`try_cast` |
| 同比环比 | `compare(x, n1, n2…)`：x 是 FROM 子查询的一列，子查询在往前平移 n 秒的时间范围上再算一遍；可以按子查询的列 group by |
| 未实现 | `ip_to_*`、`numeric_histogram`、其他 SLS 特有函数（报 `Function … not registered`）；`select *`（log 上）、`grouping sets`、`rollup`、`cube` |

## 有意不实现

SLS 有、而日志系统通常不需要的能力（以 Loki 为参照：Loki 也没有的）不实现：

- 中文分词（`chn`）：中文仍按分词符切词，整段中文是一个词。
- `ip_to_*` 等 IP 地理函数（需要 IP 库；Loki 只在采集端做 geoip）。
- `numeric_histogram`、`time_series` 的补空桶。
- 估算函数的算法：`approx_distinct`、`approx_percentile` 返回精确值。

## 已知差异

逐条见 `testdata/golden/known-diffs.json`，概要见 [wire-findings.md](wire-findings.md) 末尾。

数据跨多个分段（或很多行）时 DuckDB 并行扫描，以下结果不确定，golden 里标为「不稳定」：double 聚合（`sum`、`avg`、`stddev`）的最后一位、`order by` 并列行的先后、`array_agg` 的元素顺序。SLS 本身是分布式计算，同样不保证这些。

## 鉴权、限流与多节点（v0.3）

以下行为没有与真实 SLS 逐条对照（报错码取自 SLS 文档，HTTP 状态码与文案是 sls-local 自定的），客户端靠报错码区分时可以依赖，靠文案时不要：

| 情形 | 返回 |
|---|---|
| 没有或看不懂 Authorization、AccessKeyId 不存在 | 401 `Unauthorized` |
| 签名不符 | 401 `SignatureNotMatch` |
| Date 与服务器时间相差超过 15 分钟 | 400 `RequestTimeTooSkewed` |
| 请求体与 Content-MD5 不符 | 400 `InvalidContentMD5` |
| 签名 v4（`SLS4-HMAC-SHA256`） | 401 `Unauthorized`：暂不支持，用 v1 |
| 同时执行的查询满了、排队 5 秒仍拿不到名额 | 403 `ReadQuotaExceed` |
| 查询超过 `-query-timeout` | 400 `RequestTimeout`（4xx：SDK 对 5xx 会自动重试约 90 秒） |
| 写入超过 `-write-rate` | 403 `WriteQuotaExceed` |
| 查询节点收到写入、写入节点收到查询 | 501 `NotSupported`，文案说明该发往哪类节点 |

- 权限只有「有没有合法的密钥」一级，没有 RAM 那样按 project / logstore / 操作的授权，也不支持 STS 临时凭证。
- 查询节点看到的数据比写入晚一个刷段间隔；在那之前新的 logstore 在查询节点上不存在（`LogStoreNotExist`），在配置文件里预先声明可以避免。
- 多个写入节点时，上下文（GetContextLogs）按写入序号排列：同一来源的日志落在不同写入节点上时，先列完一个节点的再列另一个，与 SLS 按包的先后不同。
- 保留期按分段删除：一个分段里最晚的日志也过期了才删，比 `-ttl` 最多多留一小时。
- 只有一个 shard（`ListShards` 返回 shard 0）；写入节点之间的分流交给负载均衡。
