# sls-local

说阿里云日志服务（SLS）协议的日志服务：LoongCollector 和 SLS SDK 照常写入，Grafana 的 SLS 数据源照常查询。本地开发时一个进程、一个数据文件就能跑；生产环境里分成写入节点与查询节点，数据以 Parquet 分段放在对象存储（OSS / S3）上。

[English](README_EN.md)

> 社区项目，与阿里云无关。SLS 是阿里巴巴集团的商标，这里只用来说明兼容对象，见 [NOTICE](NOTICE)。

## 为什么

生产用 SLS 查日志，本地开发却只能二选一：接一个云上的开发 logstore（要 AccessKey、走公网、按量计费），或者换成 Loki（查询语法、看板、告警全都要另写一套）。

sls-local 让本地也说 SLS 的协议：

- **采集端不改**：LoongCollector 的 `flusher_sls` 只把 Endpoint 换成 `sls-local`。
- **查询端不改**：[nethinwei-sls-datasource](https://github.com/nethinwei/sls-grafana-datasource) 等 SLS 数据源只换 URL；生产的看板、检索语句、SQL 拿来就能用。
- **行为一致**：检索语法、SQL、索引规则、返回格式、报错文案按真实 SLS 逐条对照（[兼容性](docs/compatibility.md)）。本地能跑通的查询，到生产也能跑；生产会报的错，本地也报。

同一套协议也可以自建在生产环境（见[生产部署](#生产部署)）：采集端、看板、告警都沿用 SLS 的写法，日志存在自己的对象存储上。

## 快速开始

```bash
git clone https://github.com/nethinwei/sls-local.git
cd sls-local/examples/quickstart
docker compose up -d
```

打开 http://localhost:3000 （免登录）：「sls-local 快速上手」看板有按级别的日志量、P95 耗时和错误日志；也可以在 Explore 里选 SLS 数据源，写 `level: ERROR` 或 `* | select service_name, count(*) as c group by service_name`。

示例包含：造日志的容器 → LoongCollector 采集 → sls-local → Grafana 13（自动安装 SLS 插件）。端口被占用时用 `GRAFANA_PORT=3001 SLS_LOCAL_PORT=8081 docker compose up -d`。

## 接入已有环境

```yaml
services:
  sls-local:
    image: ghcr.io/nethinwei/sls-local:0.3.0
    command: ["-config", "/etc/sls-local.yaml"]   # 可选：预建 logstore、声明索引、配置 AccessKey
    volumes: ["./sls-local.yaml:/etc/sls-local.yaml:ro", "sls:/data"]
    ports: ["127.0.0.1:8080:8080"]               # 只绑本机：没有配置 AccessKey 时不鉴权
    networks:
      default:
        aliases: [myproject.sls-local]            # 每个 project 一个别名
```

**为什么要网络别名**：SLS 的 SDK 和采集器总是把请求发到 `{project}.{endpoint}`，sls-local 从域名的第一段取 project。

| 客户端 | 配置 |
|---|---|
| LoongCollector | `flusher_sls` 的 `Endpoint: sls-local`（只写主机名）。社区版固定走 HTTPS:443 且不校验证书，sls-local 用自签证书监听 443 |
| Grafana SLS 数据源 | URL `http://sls-local:8080`，project / logstore 照填；AccessKey 随便填，配置了 `access_keys` 时填其中一对 |
| SDK（容器内） | Endpoint `http://sls-local:8080` |
| SDK、curl（宿主机） | Endpoint `http://localhost:8080`，请求会发到 `myproject.localhost:8080`；macOS 和用 systemd-resolved 的 Linux 能直接解析 `*.localhost`，其他系统在 `/etc/hosts` 加一行 `127.0.0.1 myproject.localhost` |

### 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `-http` | `:8080` | 查询端口（HTTP） |
| `-https` | `:443` | 写入端口（HTTPS，启动时自签证书）；为空时不监听。查询节点默认不监听 |
| `-data` | `/data/sls.duckdb` | 数据文件；为空时用内存库 |
| `-ttl` | `72h` | 按日志时间保留多久；`0` 不删除 |
| `-config` | | 配置文件，见下 |
| `-logstore` | | 预建 `project/logstore`，可重复（写入时也会自动创建） |
| `-sql-utc-offset` | `8h` | SQL 时间函数的时区，SLS 默认东八区 |
| `-version` | | 打印版本 |
| `-reindex` | | 按 `-config` 里当前的索引重建全部日志的全文列后退出（先停掉正在运行的 sls-local；多节点时在写入节点上执行） |
| `-segments` | 数据文件旁的 `segments/` | Parquet 分段目录；用对象存储时是本地缓存 |
| `-flush-interval` | `5m`（写入节点 `10s`） | 把新写入的日志写成分段的间隔 |
| `-flush-rows` | `1000000` | 新写入的日志积累到这么多条时提前刷段 |
| `-storage` | | 对象存储：`s3://bucket/prefix` 或 `file:///path`，见[生产部署](#生产部署) |
| `-cache-size` | `53687091200`（50 GB） | 用对象存储时本地缓存的上限（字节） |
| `-target` | `all` | 节点角色：`all`（单机）、`write`（只写入）、`read`（只查询） |
| `-writer-id` | `0` | 写入节点的编号，共用一个对象存储的写入节点各不相同 |
| `-refresh-interval` | `10s` | 查询节点重新列出对象存储上分段的间隔 |
| `-query-timeout` | `1m` | 一次查询的最长执行时间，超时返回 `RequestTimeout` |
| `-max-queries` | CPU 数 × 2 | 同时执行的查询数，排队 5 秒拿不到名额返回 `ReadQuotaExceed` |
| `-memory-limit` | 物理内存的 80% | DuckDB 的内存上限，如 `8GB`；容器里设成低于容器的内存上限 |
| `-write-rate` | `0`（不限） | 每个 logstore 每秒最多写入的条数（允许 2 秒的突发），超过返回 `WriteQuotaExceed` |

### 配置文件与索引

```yaml
access_keys:        # 可选：设置后所有 SLS 接口都校验请求签名（v1）；省略时不校验
  - {id: my-collector, secret: ...}
logstores:
  - project: myproject
    logstore: app
    index:          # 与 SLS GetIndex 返回的 JSON 结构相同，可以直接从生产导出
      line: {token: [",", " ", ";", "=", "(", ")", "[", "]", "{", "}", "?", "@", "&", "<", ">", "/", ":", "\n", "\t", "\r"]}
      keys:
        level:   {type: text, token: [" "], doc_value: true}
        cost_ms: {type: long, doc_value: true}
```

声明了索引的 logstore 按 SLS 的规则执行：没有字段索引的字段不能检索（报 SLS 同款错误），long / double 按数值匹配，SQL 只能用开启统计（`doc_value`）的字段。**用生产的索引配置，本地就能提前发现「看板用了没建索引的字段」这类问题。**

没有声明索引的 logstore 自动为所有字段建 text 索引并开启统计，开箱即用；数值统计需要 `cast`。

与 SLS 一样，修改索引配置只影响之后写入的日志。要让旧日志也按新索引检索，停掉 sls-local 后执行一次 `sls-local -reindex -data <数据文件> -config <配置文件>`（相当于 SLS 控制台的「重建索引」）。

## 生产部署

```
LoongCollector / SDK ──PutLogs──► 负载均衡 ──► 写入节点 × N ──分段（每 10 秒）──► 对象存储（OSS / S3）
                                                                                   │
Grafana SLS 数据源 ──GetLogs 等──► 负载均衡 ──► 查询节点 × M ◄──按需下载、本地缓存───┘
```

- **写入节点**（`-target=write`）只接受写入：日志先进本地的数据文件，每 10 秒写成 Parquet 分段上传，合并小文件、按 `-ttl` 删除过期的分段。数据目录要用持久卷，`-writer-id` 在写入节点之间不能重复（StatefulSet 的序号正合适）。
- **查询节点**（`-target=read`）只做查询：每 10 秒列出对象存储上的分段，按需下载到本地缓存（`-cache-size`）。不存数据，可以随意增减。
- 写入与查询节点都要 `-storage s3://bucket/prefix`，S3 端点与凭证取环境变量 `SLS_S3_ENDPOINT`（OSS 填 `oss-cn-hangzhou.aliyuncs.com` 这样的端点）、`AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`，`SLS_S3_REGION` 可选。
- 在配置文件里写 `access_keys` 开启签名校验；采集器、SDK、Grafana 数据源用其中的密钥。

完整的示例（SeaweedFS 当 S3、2 个写入节点、1 个查询节点、LoongCollector、Grafana）：

```bash
cd examples/production
docker compose up -d     # 打开 http://localhost:3000
```

**要知道的事**

- 查询节点看到的数据比写入晚一个刷段间隔（写入节点默认 10 秒）。单机（`-target=all`）写入后立即可查。
- 一个 logstore 的第一个分段到达对象存储之前，查询节点上它不存在（返回 `LogStoreNotExist`）：在配置文件里预先声明 logstore。
- 写入节点前面放负载均衡或 Kubernetes Service（固定地址）。LoongCollector 解析一次域名后一直用那个地址，直接用 DNS 轮询分不了流，节点换了 IP 也不会跟着换。
- 第一次查询某个分段时要把它整个下载下来（约 100 万条一个文件）：冷数据的第一次查询会慢一些，缓存盘留够空间。
- 监控：每个节点的 `/metrics`（Prometheus 格式：各接口的请求数与耗时、写入量、刷段、上传下载、bloom 跳过的分段）、`/health`（存活）、`/ready`（就绪），都不需要签名。

## 兼容性

支持：PutLogs、GetLogsV3（检索、SPL `| where`、SQL）、GetHistograms、GetContextLogs、GetIndex、ListLogStores、GetLogStore。SQL 是 Presto 方言改写到 DuckDB，常用聚合与时间函数都可用。完整清单与已知差异见 [docs/compatibility.md](docs/compatibility.md)，每条行为与真实 SLS 的对照记录见 [docs/wire-findings.md](docs/wire-findings.md)。

## 工作原理

```
LoongCollector / SDK ──PutLogs (HTTPS 443 / HTTP)──►┐
                                                     ├─ sls-local ── DuckDB：头部表 + Parquet 分段（本地或对象存储）
Grafana SLS 数据源 ────GetLogs 等 (HTTP 8080)──────►┘
```

- 新写入的日志先进 DuckDB 的一张表（头部），每隔一段时间按 project / logstore / 小时写成 Parquet 分段（每个文件最多 100 万条，小文件定期合并）。查询看的是「头部 + 分段」的一份快照。
- 字段按写入顺序存成 JSON（SLS 返回日志时保持字段顺序）；每个分段还把常见的字段写成独立的列，聚合与字段检索直接读列。
- 每个分段带一个 bloom 过滤器，记着它出现过的词：检索罕见的值（订单号、trace id）时跳过不可能命中的分段。
- 检索语句按索引编译成 DuckDB 条件；SQL 用 Presto 的语法树翻译：字段换成对应的列，`FROM log` 指向本 logstore、时间范围内的数据，Presto 函数换成 DuckDB 的等价写法。
- 200 万条日志（单机、落盘）：分组聚合 128 → 9.6 ms，直方图 59 → 6.4 ms，罕见值检索 70 → 0.9 ms（与 v0.2 相比，见 CHANGELOG）。

## 安全

- **没有配置 `access_keys` 时不鉴权**（本地开发的默认），能访问端口的人能读写全部数据：只绑 `127.0.0.1`，不要暴露出去。
- 配置了 `access_keys` 时，所有 SLS 接口校验请求签名（v1，与 SLS SDK、LoongCollector 相同；v4 暂不支持），并核对 `Content-MD5` 与请求体。只区分「有没有合法的密钥」，不区分 project / logstore 的权限。
- 写入端口（443）是自签证书：放在负载均衡后面时由负载均衡终结 TLS。
- 查询被限制在当前 logstore 内：DuckDB 只能读写分段目录，不能访问其他文件、安装扩展或修改配置，SQL 只能读 `FROM log`。
- 对象存储的凭证只用环境变量传入。

## 开发

```bash
make test           # 全部测试（需要 cgo）
make test-segments  # 同一套测试，数据频繁刷成分段、放进对象存储（临时目录）再跑
make build          # 编译到 bin/sls-local
make image          # 构建镜像
```

- 端到端测试用官方 Go SDK 当客户端，期望值来自真实 SLS 的行为。
- 增加或修改语义时，先用真实 SLS 对照，把结论记进 [docs/wire-findings.md](docs/wire-findings.md)，再实现和写测试。
- `tools/wiredump` 记录收到的原始请求（HTTP 与自签 HTTPS），用来抓新客户端的协议细节。
- 基准测试：`go test ./internal/api -run '^$' -bench .`；`SLS_BENCH_N` 调数据量，`SLS_BENCH_DISK=1` 落盘，`SLS_BENCH_SEGMENTS=1` 全部刷成分段。
- 在真实的 S3 上跑测试：`SLS_S3_ENDPOINT=… AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… SLS_LOCAL_TEST_FLUSH_ROWS=3 SLS_LOCAL_TEST_REMOTE=s3://bucket/prefix go test ./...`。

## 许可证

[MIT](LICENSE)。第三方依赖见 [NOTICE](NOTICE)，镜像里附带全部依赖的许可证（`/usr/share/doc/sls-local`）。
