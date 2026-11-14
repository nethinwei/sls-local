# Golden 对照测试

sls-local 的目标是与真实 SLS 行为一致。Golden 测试用同一份合成数据、同一组请求对照两边：云端的响应录制下来提交到仓库，CI 回放时要求 sls-local 给出同样的响应，不需要云账号。

```
testdata/golden/
  dataset.json      合成日志（tools/golden gen 生成，确定性），时间相对起点
  index.json        对照用 logstore 的索引（SLS CreateIndex 的 JSON），云端与 sls-local 共用
  cases/*.json      请求，按类别分文件
  recorded/*.json   云端的响应（归一化后）
  known-diffs.json  目前已知不一致的用例及原因
```

## 比对规则

- 比对状态码、`Content-Type`、`x-log-count`、`x-log-progress` 与响应体；响应体按原样的键顺序比较（SLS 保持字段写入顺序）。
- 不比对：执行开销（`elapsedMillisecond`、`cpuSec`、`cpuCores`、`processedBytes`）、只在 SQL 没命中云端结果缓存时才出现的 `powerSql`、logstore 的创建与修改时间、游标值（`__pack_meta__` 里的游标、GetCursor 的结果）。
- project 名、region、账号 ID 替换成 `{project}`、`{region}`、`{account}`；录制工具在写文件前检查这些信息没有残留。
- 检索用例取 5 行检查格式与排序，另有一个同条件的直方图用例检查命中总数与分布。

## 只进不退

`known-diffs.json` 列出目前与 SLS 不一致的用例。回放时：

- 不在名单里的用例不一致：测试失败。
- 名单里的用例一致了：测试也失败，提示把它从名单里删掉。

修好一个差异就删掉对应的条目。确实需要整体刷新名单时（例如重新录制之后）：

```bash
go test ./internal/golden -run TestGolden -golden.update-known
```

已有条目的原因说明会保留，新增条目的原因为空，需要补上。

## 录制

需要一个云端 logstore（1 个 shard），可以是专用的，也可以是在用的开发 logstore：

- 数据写在一个前后都没有日志的时间段：从 logstore 创建前一小时开始往前找，不早于 TTL 到期前 3 天。
- 索引只追加 `index.json` 里的字段；已有的同名字段或全文索引与 `index.json` 不一致时，工具拒绝修改并退出。
- 录 GetIndex 时只保留 `index.json` 里的字段，logstore 原有的其他字段不会写进录制结果。

专用 logstore 的授权示例：

```json
{
  "Version": "1",
  "Statement": [{
    "Effect": "Allow",
    "Action": "log:*",
    "Resource": [
      "acs:log:*:*:project/<project>/logstore/sls-local-golden",
      "acs:log:*:*:project/<project>/logstore/sls-local-golden/*"
    ]
  }]
}
```

```bash
export SLS_ENDPOINT=<region>.log.aliyuncs.com SLS_GOLDEN_PROJECT=<project> SLS_GOLDEN_LOGSTORE=<logstore> \
       SLS_ACCESS_KEY_ID=... SLS_ACCESS_KEY_SECRET=...
export SLS_GOLDEN_DENY=<不能出现在录制结果里的词，逗号分隔，如公司名>
go run ./tools/golden upload   # 补齐索引（等待 90 秒生效），把数据集写进空的时间段，等到全部可查
go run ./tools/golden record   # 每个用例执行两遍，结果不稳定的会报出来
```

录制结果会提交到公开仓库。归一化把 project、logstore、region、账号替换成占位符；写文件前再检查这些名字、AccessKey ID 与 `SLS_GOLDEN_DENY` 里的词都没有出现，否则中止。

每次 upload 找一个新的空时间段，不会和之前写入的数据重叠。录制结果与起点绑定：数据集或索引改了，要重新 upload 再 record 全部用例。

## 加用例

在 `cases/` 里加一条（`op` 支持 `logs`、`histogram`、`context`、`index`、`logstore`、`shards`、`cursor`），然后只录这个文件：

```bash
go run ./tools/golden record -file sql
```

数据保留 30 天，超过之后要先重新 upload。

注意：

- SQL 或 SPL 的 where 里写 `__time__` 条件会覆盖请求的时间范围（实测 `* | where __time__ > 0` 查的是整个 logstore）。对照用的 logstore 可能还有别的日志，用例里不要这样写。录制工具会检查检索与上下文结果的每一行都来自数据集，发现别的日志就中止。
- 共用的 logstore 不要录 `select *`：它返回 logstore 全部索引字段，会把原有的字段名带进录制结果。录制工具会检查 SQL 结果里有没有 logstore 原有的字段，有就中止。
- 不带 FROM 的 `* | select count(*)` 在 SLS 上走统计快速路径，结果可能比实际少；`count(1)`、`from log` 或加了 group by 时是精确值。
