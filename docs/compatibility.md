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

支持的写法：`select … [from log] [where] [group by] [having] [order by] [limit]`、子查询、窗口函数、`set session …;` 前缀。没写 FROM 时默认 `from log`，没写 LIMIT 时返回 100 行。字段类型按索引（long → bigint、double → double、text → varchar），只能用开启统计（doc_value）的字段。

| SLS 函数 | 实现 |
|---|---|
| `from_unixtime`、`to_unixtime`、`now()`、`current_timestamp`、`current_date` | 按 `-sql-utc-offset`（默认东八区）返回不带时区的本地时间 |
| `date_format`、`date_parse` | MySQL 风格格式串（`%Y-%m-%d %H:%i:%s`）转换后交给 DuckDB |
| `date_trunc`、`date_diff`、`extract` | DuckDB 同名函数 |
| `approx_percentile`、`approx_distinct` | DuckDB `approx_quantile`、`approx_count_distinct` |
| `regexp_like`、`json_extract_scalar`、`cardinality` | DuckDB `regexp_matches`、`json_extract_string`、`len` |
| `count`、`sum`、`avg`、`min`、`max`、`count_if`、`arbitrary`、`max_by`、`min_by`、`if`、`coalesce`、`round`、`split_part`、`strpos`、`regexp_extract`、`lower`、`upper`、`length`、`concat`、`cast`、`try_cast` | DuckDB 同名函数 |
| `time_series`、`compare`、`histogram`、`ip_to_*` 等 SLS 特有函数 | ❌ 报 `Function … not registered` |
| `select *`、`log.level` 限定名、`at time zone`、Presto 参数顺序的 `date_add` | ❌ |

## 已知差异

- bigint 与字符串字面量比较（`cost_ms > '100'`）：SLS 报类型错误，sls-local 照常执行。
- decimal 字面量按精确值计算（`0.1+0.2` 得 `0.3`，SLS 得 `0.30000000000000007`）；null 字面量的列类型是 `long` 而不是 `unknown`。
- 上面对照过的报错之外，执行错误的文案来自 DuckDB（加 `invalid query: ` 前缀）。
- 自动索引时所有字段都是 varchar，数值统计要 `cast` 或在配置里声明 long / double。
- 没有全文索引时的报错文案未与真实 SLS 对照。
