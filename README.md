# sls-local

说阿里云日志服务（SLS）协议的本地服务：LoongCollector 和 SLS SDK 照常写入，Grafana 的 SLS 数据源照常查询，数据存在本地的 DuckDB 里。不需要云账号，不产生费用。

[English](README_EN.md)

> 社区项目，与阿里云无关。SLS 是阿里巴巴集团的商标，这里只用来说明兼容对象，见 [NOTICE](NOTICE)。

## 为什么

生产用 SLS 查日志，本地开发却只能二选一：接一个云上的开发 logstore（要 AccessKey、走公网、按量计费），或者换成 Loki（查询语法、看板、告警全都要另写一套）。

sls-local 让本地也说 SLS 的协议：

- **采集端不改**：LoongCollector 的 `flusher_sls` 只把 Endpoint 换成 `sls-local`。
- **查询端不改**：[nethinwei-sls-datasource](https://github.com/nethinwei/sls-grafana-datasource) 等 SLS 数据源只换 URL；生产的看板、检索语句、SQL 拿来就能用。
- **行为一致**：检索语法、SQL、索引规则、返回格式、报错文案按真实 SLS 逐条对照（[兼容性](docs/compatibility.md)）。本地能跑通的查询，到生产也能跑；生产会报的错，本地也报。

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
    image: ghcr.io/nethinwei/sls-local:0.1.0
    command: ["-config", "/etc/sls-local.yaml"]   # 可选：预建 logstore、声明索引
    volumes: ["./sls-local.yaml:/etc/sls-local.yaml:ro", "sls:/data"]
    ports: ["127.0.0.1:8080:8080"]               # 只绑本机：sls-local 没有鉴权
    networks:
      default:
        aliases: [myproject.sls-local]            # 每个 project 一个别名
```

**为什么要网络别名**：SLS 的 SDK 和采集器总是把请求发到 `{project}.{endpoint}`，sls-local 从域名的第一段取 project。

| 客户端 | 配置 |
|---|---|
| LoongCollector | `flusher_sls` 的 `Endpoint: sls-local`（只写主机名）。社区版固定走 HTTPS:443 且不校验证书，sls-local 用自签证书监听 443 |
| Grafana SLS 数据源 | URL `http://sls-local:8080`，project / logstore 照填，AccessKey 随便填 |
| SDK（容器内） | Endpoint `http://sls-local:8080` |
| SDK、curl（宿主机） | Endpoint `http://localhost:8080`，请求会发到 `myproject.localhost:8080`；macOS 和用 systemd-resolved 的 Linux 能直接解析 `*.localhost`，其他系统在 `/etc/hosts` 加一行 `127.0.0.1 myproject.localhost` |

### 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `-http` | `:8080` | 查询端口（HTTP） |
| `-https` | `:443` | 写入端口（HTTPS，启动时自签证书）；为空时不监听 |
| `-data` | `/data/sls.duckdb` | 数据文件；为空时用内存库 |
| `-ttl` | `72h` | 按日志时间保留多久；`0` 不删除 |
| `-config` | | 配置文件，见下 |
| `-logstore` | | 预建 `project/logstore`，可重复（写入时也会自动创建） |
| `-sql-utc-offset` | `8h` | SQL 时间函数的时区，SLS 默认东八区 |
| `-version` | | 打印版本 |

### 配置文件与索引

```yaml
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

## 兼容性

支持：PutLogs、GetLogsV3（检索、SPL `| where`、SQL）、GetHistograms、GetContextLogs、GetIndex、ListLogStores、GetLogStore。SQL 是 Presto 方言改写到 DuckDB，常用聚合与时间函数都可用。完整清单与已知差异见 [docs/compatibility.md](docs/compatibility.md)，每条行为与真实 SLS 的对照记录见 [docs/wire-findings.md](docs/wire-findings.md)。

## 工作原理

```
LoongCollector / SDK ──PutLogs (HTTPS 443 / HTTP)──►┐
                                                     ├─ sls-local ── DuckDB（/data/sls.duckdb）
Grafana SLS 数据源 ────GetLogs 等 (HTTP 8080)──────►┘
```

- 所有日志存在一张表里：固定列放时间与元数据，字段按写入顺序存成 JSON（SLS 返回日志时保持字段顺序）。
- 检索语句按索引编译成 DuckDB 条件；SQL 逐个记号改写：字段换成按类型转换的取值表达式，`FROM log` 指向只含本 logstore 数据的子查询，Presto 函数换成 DuckDB 的等价写法。

## 安全

sls-local 只用于本地开发与测试：**没有鉴权**，签名只解析不校验，能访问端口的人能读写全部数据。不要暴露到公网；在宿主机上运行时，把端口绑在 `127.0.0.1`。

即便如此，查询也被限制在当前 logstore 内：DuckDB 关闭了读写外部文件、安装扩展和修改配置，SQL 只能读 `FROM log`。

## 开发

```bash
make test      # 全部测试（需要 cgo）
make build     # 编译到 bin/sls-local
make image     # 构建镜像
```

- 端到端测试用官方 Go SDK 当客户端，期望值来自真实 SLS 的行为。
- 增加或修改语义时，先用真实 SLS 对照，把结论记进 [docs/wire-findings.md](docs/wire-findings.md)，再实现和写测试。
- `tools/wiredump` 记录收到的原始请求（HTTP 与自签 HTTPS），用来抓新客户端的协议细节。

## 许可证

[MIT](LICENSE)。第三方依赖见 [NOTICE](NOTICE)，镜像里附带全部依赖的许可证（`/usr/share/doc/sls-local`）。
