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
| ListLogStores、GetLogStore、GetProject | ✅ | |
| 旧版 GetLogs（`GET ?type=log`） | ❌ | 用 GetLogsV3 |
| CreateLogStore、CreateIndex、UpdateIndex | ❌ | logstore 在写入时自动创建，索引写在配置文件里 |
| PullLogs、消费组、MetricStore、数据加工、告警 | ❌ | |
| 签名校验、RAM 权限 | ❌ | 签名只解析不校验，任何 AccessKey 都能用 |

## 检索语法

| 写法 | 状态 |
|---|---|
| `key: value`、`key:value`、字段名不区分大小写 | ✅ |
| 全文检索、相邻裸词（隐式 and）、`"短语"` | ✅ |
| `and`、`or`、`not`（一元与二元）、括号 | ✅ |
| 词内与词尾通配 `pai*`、`p*d`、`a?c`；`key: *` 字段存在 | ✅ |
| 前导通配 `*abc` | 报错，与 SLS 一致 |
| long / double：`key: 68`、`>`、`>=`、`<`、`<=`、`=`、`in [a b]`、`in (a b]` | ✅ |
| `__topic__`、`__source__`、`__tag__:x` | ✅（tag 需在索引里声明，自动索引时都可用） |
| 字段的分词符、大小写敏感、JSON 子键 `a.b` | ✅ 按索引配置 |
| 中文分词（chn）、全文索引的 include_keys / exclude_keys、别名（alias） | ❌ |

## SPL

| 指令 | 状态 |
|---|---|
| `\| where <表达式>`（可多个）、`\| with_pack_meta` | ✅ 字段是 varchar，`__time__` 是数值，与 SLS 一致 |
| `extend`、`project`、`parse-json`、`parse-regexp` 等 | ❌ 返回 NotSupported |

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
| 未实现 | `compare`、`ip_to_*`、`numeric_histogram`、其他 SLS 特有函数（报 `Function … not registered`）；`select *`（log 上）、`grouping sets`、`rollup`、`cube` |

## 已知差异

逐条见 `testdata/golden/known-diffs.json`，概要见 [wire-findings.md](wire-findings.md) 末尾。
