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
| `"declined card"`、`msg: "declined card"`、`msg: declined,card`（数据是 card declined） | 命中：引号与分隔符连起来的词只要求各词都出现，不管顺序，可以分在不同字段 |
| `#"card declined"`、`msg: #"card declined"`、`msg:#"..."`、`#"Card, Declined"` | 命中：短语，按顺序相邻，词之间可隔任意分词符，不区分大小写 |
| `#"declined card"`；跨字段的 `#"beta gamma"`（f1=alpha beta、f2=gamma）、`#"key value"`（字段名与值） | 0 条：短语不跨字段，字段名与值也算不同部分；值里的换行只是普通分词符（`#"linea lineb"` 命中） |
| `#"card"`、`#""` | 与普通检索相同（一个词的短语不算短语，meta 没有 phraseQueryInfo） |
| `not #"..."`、`a not #"..."` | 400 `cannot use phrase query in not expression` |
| `#"..." \| select ...` | 400 `cannot use phrase query among with sql` |
| `#"..." \| where ...` | 400 `cannot use spl query together with phrase query`（`\| with_pack_meta` 可以） |

短语查询的 meta（两个词以上的短语，与其他条件组合时也一样）：`terms` 里短语记作引号里的原文（不转小写、保留标点），在 `terms` 之后依次多出 `new_mode: 1`、`mode: 1`（没有返回行时不带，偶尔也带）与 `phraseQueryInfo: {scanAll, beginOffset, endOffset, endTime}`：凑满一页时 `scanAll` 为 false、`processedRows` 是 offset + 行数；没凑满时 `scanAll` 为 true、`processedRows` 是候选行数（把短语放宽成各词都出现时的命中数）。`beginOffset` = max(0, offset − lines)，`endOffset` = offset + 行数，`endTime` 是最后一行的 `__time__`（没有行时为 0）。

同一条日志里字段名重复（写入 `x=a, y_0=taken, x=b, x=c, y=p, y=q, x_0=late`）：返回 `x:a, y_0:taken, x_0:b, x_1:c, y:p, y_0:q, x_0:late`，即同名字段第 n 次出现改名为 `名字_{n-2}`，位置不变，不检查改名后是否重名（响应 JSON 里会有重复的键）；`meta.keys` 是原始字段名去重（不含 `x_1`）；全文检索能匹配到每个值。
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

- 字段顺序：写入字段 → （`__pack_meta__`）→ `__topic__` → `__source__` → `__tag__:*` → `__time__` → `__time_ns_part__`（golden 更正：`__pack_meta__` 在 `__topic__` 之前）。
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

# SQL 对照（golden，2026-11-14）

以下来自 golden 对照测试（docs/golden.md）：同一份合成数据写进云端与 sls-local，逐条比对响应。

## 解析与改写

- SQL 是 Presto 方言，语法错误是 ANTLR 的原文（`mismatched input 'offset' expecting <EOF>`）；不支持 `OFFSET`。
- SLS 先按 `|` 切分语句再解析 SQL：`select a || b` 只有 `|` 之前的部分生效（实测 `select level || '/' || service` 返回的是 `level`）。
- SLS 把 SQL 改写后再交给 Presto：`from log` 换成 `from "<logstore>"`；原文里没有 `from` 这个词时，在最外层第一个 select 的列表之后插入 ` from "<logstore>" `，位置记在 `meta.insertedSQL`（`1,<位置>, from "<logstore>" `）。判断「有没有 from」看的是原文，`extract(hour from ts)` 里的也算：这时不插入，`select extract(hour from from_unixtime(0))` 只返回 1 行。
- 报错的列号：语法错误按改写后的文本；语义错误（类型不匹配、函数不存在）的 `ErrorPosition` 按原文、消息里的列号按改写后的文本（`ErrorPosition,line:1,column:40.ErrorMessage:line 1:62: '>' cannot be applied to bigint, varchar(3)`）。
- `where` 里写 `__time__` 条件会覆盖请求的时间范围（`* | where __time__ > 0` 查的是整个 logstore）。
- 不带 FROM 的 `* | select count(*)` 走统计快速路径，结果可能比实际少；`count(1)`、`from log`、加 group by 时精确。

## 名字

- 字段名不区分大小写；索引里配置的别名（alias）在 SQL 里可用。
- 有字段索引但没开统计的字段：SQL 里能引用，值总是 null；没有字段索引的字段报 `Column ... cannot be resolved`。
- `log.level` 不是限定名，被当成一个字段名（`Column 'log.level' cannot be resolved`）。
- JSON 字段的子键 `"ctx.user.id"` 按嵌套路径取值，类型按 json_keys 的配置；字段值不是合法 JSON 时为 null。
- 不写别名的列：单个字段引用用字段名，其他是 `_colN`。group by、having、order by 可以引用 select 的别名。

## 类型与格式

- 列类型：顶层的整数都是 `long`，数组与 map 里保留宽度（`array(integer)`、`map(varchar,bigint)`）；字符串字面量与由它算出的结果带长度，按 Presto 函数签名的长度公式（`lower('AbC')` → `varchar(3)`、`replace(x, y, z)` → `varchar(x+z·(x+1))`、`url_encode(x)` → `varchar(12x)`）；null 字面量是 `unknown`；`corr(bigint, bigint)` 是 `real`；`json_extract` 是 `json`，值是 JSON 原文。
- double 的文本：Grisu2 生成数字且不做末位修正，得到的是能还原成同一个 double 的数字串，但不一定是离真值最近的（`0.1+0.2` → `0.30000000000000007`，`341256/1056.0` → `323.15909090909096`）。
- 结果里已有 `__time__` 列时，行尾的 `__time__` 改名为 `__time___0`；已有 `__source__` 列时不再追加 `__source__`。
- `1/0` 报 `/ by zero`；`cast('a' as bigint)` 报 `ErrorType:SyntaxError.ErrorMessage:Can not cast 'a' to BIGINT`。
- `url_encode` 把空格编码成 `+`；`geometric_mean` 遇到负数是 `NaN`；`at time zone 'UTC'` 的类型是 `timestamp with time zone`，值形如 `2026-11-14 13:15:00.000 UTC`。

# 响应格式、检索与 SPL（golden，2026-11-14）

## GetLogsV3 的 meta

- 字段顺序固定：`count, progress, processedRows, processedBytes, elapsedMillisecond, hasSQL, telementryType, telemetryType, whereQuery, aggQuery`，SQL 再加 `cpuSec, cpuCores, powerSql, insertedSQL`，然后 `keys, terms`，SQL 没写 LIMIT 时 `limited: 100`，SPL 有 `new_mode: 4, scanBytes: 0`，最后 `isAccurate: true`，SQL 再加 `columnTypes`。拼错的 `telementryType` 与正确的 `telemetryType` 同时返回。
- `processedRows`：检索与 SPL 是返回的行数；SQL 是最外层读到的日志条数——WHERE 里能下推到索引的条件（数值字段与常量比较、文本字段 `=`/`in` 字符串，按索引不区分大小写）过滤后的条数，不下推的条件（like、is null 等）不算；没有表的 select 是 0；不聚合的 SQL 凑够 LIMIT 后提前停止扫描（不可预测）。
- `whereQuery`：只有检索时是原文；带 SQL 或 SPL 时是 `|` 之前的原文（含末尾空格）；只有 `| with_pack_meta` 时是整条语句。`aggQuery`：SQL 是 `|` 之后的原文，SPL 是 `*` 加上 `|` 及之后的原文。
- `keys`：结果为空时没有；最多 20 个。`terms`：检索词，键名按索引里的写法，数值字段记作 `*`，文本按分词符切词并转小写（短语里的词按字母序），通配符不切；`not x` 末尾补 `*`；空语句为每个中文分词字段列一个空词再加 `*`。
- GetLogsV3 不带 `x-log-count`、`x-log-progress` 头（只有 GetHistograms 带）。

## 检索

- 全文检索匹配 `__source__`、字段名与字段值，但匹配不到 long/double 字段的值，也匹配不到 tag。
- 没有索引的 tag 按整个值匹配，区分大小写，支持词尾通配与 `*`。
- `__topic__: *` 什么也匹配不到；`key : value`（冒号前有空格）可以；值里有不带引号的冒号报 `unexpected COLON`。
- long 字段的比较只取数值开头的整数部分（`300.5` → 300、`3e2` → 3）；`key: 值` 的相等匹配要求整个值是数（`6*` 匹配不到）。
- 语句意外结束的报错带位置：`unexpected $end error position is from column:14 to column:17,error near < and >`；括号没有闭合时还列出 `expecting AND or OR or NOT or RIGHTBRACKET`。
- `from > to` 报 400 `InvalidTimeRange`：`Time Range <from> to <to> is invalid`。

## compare

- `compare(x, n1, n2…)` 返回 `array(double)`：`[当前值, n1 秒前的值, n2 秒前的值…, 当前/n1 前的值, 当前/n2 前的值…]`；x 是 FROM 子查询的一列（直接写聚合报 `Cannot nest aggregations inside aggregation 'compare': ["count"(*)]`），子查询在往前平移 n 秒的时间范围上用同样的检索条件再算一遍；可以按子查询的列 group by，两边按这些列对应；历史值为 0 时比值是 `"Infinity"`（数组里写成字符串）。

## 上下文与其他接口

- `__pack_meta__` 是 `shard|游标（数字串的 base64）|包的条数|在包里的序号`；带 `| with_pack_meta` 时它在写入字段之后、`__topic__` 之前，也出现在 `meta.keys` 里。
- GetContextLogs 在 `logs` 之后返回 `total_lines`；超过 100 行按 100 返回；两边都为 0 时报 `back/forward_linesback_lines or forward_lines must be positive`（原文如此）。
- GetHistograms 每个桶的字段顺序是 `from, count, to, progress`；语句带 SQL 时忽略 SQL 部分。
- GetIndex：顶层字段按字母序；每个字段先是取了非默认值的属性（按字母序），再是取默认值的 `alias`、`caseSensitive`、`chn`。GetLogStore、ListShards、GetCursor 的字段见 internal/api/logstore.go。

## SPL

- 字段都是 varchar（`__time__` 是数值）；`where` 里 varchar 与整数比较报 Presto 的函数签名错误（`line 1:4: Scalar function signature is not supported: gt(VARCHAR, INTEGER). ...`），与字符串比较按字符串比较；`cast` 转换失败时是 null。
- `extend x = 表达式`、`project a, b`（保留 `__tag__:__pack_id__` 在最前，其余按列出的顺序）、`project-away a`、`parse-json f`（顶层成员展开成字段）。输出的列序按各行字段首次出现的顺序统一排列，新增的字段在后。
- `parse-regexp 字段, '正则' as a, b`：捕获组依次成为字段，没匹配上的行不增加字段。`parse-kv 字段, '分隔符', '键值分隔符'`：分隔符是整个字符串（`'?&'` 不会在 `?` 处切），每段在第一个键值分隔符处分开，没有分隔符的段忽略；`parse-kv -regexp 字段, '正则'` 的两个捕获组是键、值，取全部匹配。解析出的字段可以被后面的 `where` 引用。`parse-kv -delims=...` 这种写法报 `no viable alternative`。
- `stats count() by level` 报 `missing EQ at '('`；`sort`、`limit` 报 `this spl is not supported translate to sql, column cannot be predicted`；语法错误是 ANTLR 的原文 `line 1:<列> <消息>`，列号从 0 起、按 aggQuery 计算。

# 已知差异（sls-local 与真实 SLS）

逐条的差异与原因见 `testdata/golden/known-diffs.json`。主要的几类：

- **SQL 估算函数**：`approx_distinct`（HyperLogLog）、`approx_percentile`（qdigest）在 sls-local 上是精确值；`numeric_histogram`、`compare`、`ip_to_*` 未实现。
- **哈希顺序**：`array_agg`、`map_agg`、`histogram` 结果里元素的顺序，以及 order by 并列行的顺序，与 Presto 的内部顺序不同。
- **time_series** 不补齐没有数据的桶；`at time zone` 只支持 `'UTC'`。
- **SPL** 的 `stats`、`sort`、`limit` 等与云端一样报错；个别 SPL 在云端走另一种执行模式（meta 带 `mode`、`phraseQueryInfo`），字段顺序不同。
- **中文分词（chn）** 未实现：需要与 SLS 相同的词典；`terms` 里的少数内部编码与顺序无法复现。
- **自动索引**（没有配置索引）时所有字段都是 varchar：数值统计要写 `cast`，或在配置里声明 long/double。
- **全文索引的 include_keys/exclude_keys** 不生效；没有全文索引时的报错文案未与真实 SLS 对照。
- **直方图间隔**超过 7 天跨度时的档位是外推的。
