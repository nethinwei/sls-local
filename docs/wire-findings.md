# 协议探针结论（阶段 0）

样本：`testdata/wire/putlogs_lz4_loongcollector341.*`（LoongCollector 社区版 3.4.1 + aliyun-log-go-sdk v0.1.131）。

## 写入端：LoongCollector flusher_sls

- 只调用一个接口：`POST /logstores/{logstore}/shards/lb`，启动阶段没有其他探测请求。
- **永远走 HTTPS:443**：社区版 `SLSClientManager::UsingHttps()` 恒为 true，端口写死为 `httpsFlag ? 443 : 80`；Endpoint 里的 `http://` 前缀和端口都会被忽略。
- **不校验证书**：`core/common/http/Curl.cpp` 在 https 时关闭 `VERIFYPEER/VERIFYHOST`，sls-local 启动时自签证书即可。
- Host = `{project}.{endpoint}`（原样带上配置里的端口，例如 `devproj.sls-local:8080`），容器网络里需要别名 `{project}.sls-local`。
- Body：protobuf `LogGroupList`/`LogGroup`（gogo），`x-log-compresstype: lz4`（lz4 raw block，解压长度取 `x-log-bodyrawsize`），签名 V1 `hmac-sha1`。
- LogGroup 自带 tag：`__host_ip__`、`__hostname__`、`__path__`、`__pack_id__`（形如 `9893B9C2046C38EC-1`，GetContextLogs 可直接使用）；`source` 为采集端 IP。

## 查询端：aliyun-log-go-sdk（Grafana 插件）

- 同样把 project 拼到域名前：`{scheme}{project}.{host}`（`log_project.go:1405`），IP endpoint 也一样，没有 path 风格的回退。
- 插件的 endpoint 可以带 `http://` 和端口，所以查询端走 HTTP 即可，例如 `http://sls-local:8080` → `devproj.sls-local:8080`。

## 对设计的影响

1. sls-local 同时监听 `:443`（TLS，自签）与 `:8080`（HTTP），project 一律从 Host 第一段取。
2. compose 中给 sls-local 配网络别名 `<project>.sls-local`；宿主机调试用 `<project>.localhost:<port>`。

# 查询语义对照（阶段 2，真实 SLS logstore，2026-11-14）

用一个真实 SLS logstore（索引见下，样本在 `testdata/index/sample-logstore.json`）逐条发查询，记录真实行为；sls-local 以此为准，测试用例同源。

索引：`line` 全文索引 + 字段索引（text：channel、error、level、msg、order_id、path、service_name、source、span_id、status、trace_id、uid；long：cost_ms；double：elapsed_ms），全部 `doc_value: true`，默认分词符 `, '";=()[]{}?@&<>/:\n\t\r`，不区分大小写。GetIndex 另带 `index_mode`、`storage`、`lastModifyTime`、`ttl`。

## 检索语法

| 语句 | 真实结果 |
|---|---|
| `nosuchkey: x`、`nosuchkey: *`、`product_id: 2010`（字段无索引） | 400 ParameterInvalid `key (nosuchkey) is not config as key value config,if symbol : is  in your log,please wrap : with quotation mark "`（`is  in` 两个空格，原文如此） |
| `__tag__:__path__: order_api.log`（tag 无索引） | 不报错，0 条 |
| `__tag__:__path__: *order_api*` | 400 `only support * or ?  in the  middle or end  of the query`（前导通配符不支持；空格原文如此） |
| `paid`、`2010`（全文） | 命中；全文覆盖没建字段索引的字段值 |
| `order paid`、`paid order` | 命中（裸词相邻 = and） |
| `level: INFO msg: paid`、`msg: order paid` | 400 `parse search query error,please read query syntax document,error detail:syntax error, unexpected TERM, expecting $end error position is from column:13 to column:16,error near < msg >`（key: value 之后不能隐式 and） |
| `level: INFO not msg: paid` | 命中（二元 not = and not） |
| `level: (INFO or WARN)` | 400 `... syntax error, unexpected OR ...` |
| `Level: info`、`level:INFO`、`level: INF*` | 命中（字段名不区分大小写；值不区分大小写；词尾通配） |
| `msg: p*d` | 命中（词中通配） |
| `msg: "pai*"` | 0 条（引号内 * 是字面量） |
| `level > a`（text 字段比较） | 不报错，0 条 |
| `cost_ms > 50`、`>=`、`= 68`、`cost_ms: 68`、`in [50 60]`、`in (50 60]`、`elapsed_ms > 1.5` | 命中（long/double） |
| `cost_ms: abc`、`cost_ms: 6*` | 不报错，0 条 |
| `uid: *` | 命中（字段存在） |
| `__topic__: x`、`__source__: 172.26.0.8` | 系统字段可直接检索 |

## SPL `| where`

| 语句 | 真实结果 |
|---|---|
| `* \| where cost_ms > 50` | 400 `invalid query: line 1:4: Scalar function signature is not supported: gt(VARCHAR, INTEGER) ...`（字段都是 varchar） |
| `* \| where cost_ms > '50'`、`cast(cost_ms as bigint) > 50` | 命中 |
| `* \| where level = 'info'` | 0 条（区分大小写） |
| `* \| where nosuchkey is null and level = 'INFO'` | 命中（不存在的字段为 null，不报错） |
| `* \| where __time__ > 0` | 命中（`__time__` 是数值） |
| `\| where a \| where b` | 命中（多个 where 依次过滤） |
| meta.whereQuery | 只含检索部分（`"* "`） |

## 返回的行

- 字段顺序：写入字段 → `__topic__` → `__source__` → `__tag__:*` → （`__pack_meta__`）→ `__time__` → `__time_ns_part__`。
- **没有** `__tag__:__receive_time__`。
- `meta.keys` 是返回行的键并集（不含 `__time__`、`__time_ns_part__`），按出现顺序。

## GetHistograms

`GET /logstores/{ls}?type=histogram&from&to&topic&query&interval`，body 是数组，总数与进度在头 `x-log-count`、`x-log-progress`（SDK 直接取 `Header["x-log-progress"][0]`，缺了会 panic）。

- 自动间隔取 {1, 10, 30, 60, 300, 600, 1800, 3600, 14400, …} 中使桶数 `(to-from)/interval ≤ 60` 的最小值。实测：30s→1、120s→10、300s→10、600s→10、900s→30、1800s→30、3600s→60、7200s→300、21600s→600、43200s→1800、86400s→1800、172800s→3600、604800s→14400。
- 桶边界对齐到 interval 的整数倍；首桶 `[from, 第一个边界)`、末桶 `[最后边界, to)`；空桶也返回（count 0）。

## GetContextLogs

`GET /logstores/{ls}?type=context_log&back_lines&forward_lines&pack_id&pack_meta`，返回 `{"progress","back_lines","forward_lines","logs"}`（没有 total_lines）。

- 每行 `__index_number__` 在最前：锚点 0，之前为负、之后为正，按 -n…0…+m 排列。
- 行内字段：写入字段 → `__pack_meta__` → `__tag__:*` → `__time__` → `__time_ns_part__`，**没有** `__topic__`、`__source__`。
- 跨 pack 取上下文：同一 pack_id 前缀（`EAA1AE0DB35A2514-CA19` 与 `-CA1A`）属于同一来源。
- `__pack_meta__` 真实格式 `0|<base64 cursor>|9|8`，对客户端不透明；sls-local 用 `0|<写入序号>|0|0`。

# SQL 对照（阶段 3，真实 SLS logstore，2026-11-14）

## 返回格式

- `meta.hasSQL: true`，`meta.keys` 是列名，`meta.columnTypes` 是 SLS 的类型名：`long`（所有整数，含 count、sum）、`double`、`varchar`、`boolean`、`timestamp`、`date`、`array(bigint)`；null 字面量是 `unknown`。
- 值全部是字符串，null 写作 `"null"`；double 不用科学计数法、整数值补 `.0`（`1.0`、`100000000000000000000.0`、`0.30000000000000007`）；timestamp 是 `2026-11-14 14:47:36.000`，date 是 `2026-11-14`，数组是 `[96,299]`。
- 每行末尾多带 `"__source__":""` 与 `"__time__":"<时间>"`，不在 keys 里；结果里已有同名列（`select __source__`）时不再重复。
- 不写 LIMIT 时返回 100 行；`lines`、`offset`、`reverse` 对 SQL 不生效。
- `processedRows` 是参与计算的日志条数。

## 语义

- 字段类型按索引：long → bigint、double → double、text → varchar；`__time__` 是 bigint；SQL 里空的 `__topic__` 是 null。
- 整数除法：`5/2` = 2。
- 时间函数按东八区：`from_unixtime(__time__)` 返回东八区的本地时间（不带时区），`date_format` 用 MySQL 风格格式（`%Y-%m-%d %H:%i:%s`），`to_unixtime` 返回 double。
- `set session mode=scan;` 前缀照常执行。

## 报错

| 语句 | 真实结果 |
|---|---|
| `select product_id`、`select nosuch`（无索引或无统计） | 400 ParameterInvalid `line 1:9: Column 'product_id' cannot be resolved; please add the column in the index attribute`（列号从 `\|` 之后算起） |
| `select "__tag__:__path__"`（tag 无索引） | 400 `line 1:9: Column '__tag__:__path__' cannot be resolved; it seems __tag__:__path__ is wrapped by "; if __tag__:__path__ is a string rather than a column, please use '__tag__:__path__'` |
| `select foo(1)` | 400 `ErrorType:SyntaxError.ErrorPosition,line:1,column:9.ErrorMessage:line 1:9: Function foo not registered` |
| `from log, log` / 其他表名 | 404 LogStoreNotExist `logstore (log) is not found` |
| `where cost_ms > '100'`（bigint 与 varchar） | 400 `'>' cannot be applied to bigint, varchar(3)` |

# 已知差异（sls-local 与真实 SLS）

- **bigint 与字符串字面量比较**：SLS 报类型错误，DuckDB 把字面量隐式转换后照常执行。
- **decimal 字面量**：`0.1+0.2` 在 DuckDB 是精确的 `0.3`，SLS 按 double 得 `0.30000000000000007`；null 字面量的类型是 `long` 而不是 `unknown`。
- **报错文案**：上表之外的执行错误是 DuckDB 的原文（加 `invalid query: ` 前缀），不是 Presto 的原文。
- **不支持**：`select *`、限定名 `log.level`、`time_series`、`compare` 等 SLS 特有函数、`at time zone`、`date_add` 的 Presto 参数顺序、UNION 中第二个 select 的自动列名、完整 SPL（只有 `where`）。
- **自动索引**（没有配置索引）时所有字段都是 varchar：数值统计要写 `cast`，或在配置里声明 long/double。
- **全文索引的 include_keys/exclude_keys** 不生效；没有全文索引时的报错文案未与真实 SLS 对照。
- **直方图间隔**超过 7 天跨度时的档位是外推的。
