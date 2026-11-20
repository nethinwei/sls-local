# 贡献指南

欢迎提交 issue 与 pull request。本项目的目标是**与真实 SLS 行为一致**：客户端不改代码就能连上，常用查询的结果与报错和阿里云 SLS 相同。所有改动都应围绕这个目标，并保持改动最小、可验证。

> English speakers: the docs are in Chinese, but issues and PRs in English are welcome.

## 开发环境

- Go 版本见 [`go.mod`](go.mod)。
- 需要 **cgo**：DuckDB 通过 cgo 链接，`CGO_ENABLED=1`（默认即可）。
- 构建镜像需要 Docker（可选）。

常用命令（见 [`Makefile`](Makefile)，`make help` 列出全部）：

```bash
make build          # 编译到 bin/sls-local
make test           # 全部测试（含 -race 竞态检测）
make test-segments  # 同一套测试，数据频繁刷成分段、再放进对象存储（临时目录）各跑一遍
make golden         # 与真实 SLS 的对照测试（docs/golden.md）
make lint           # gofmt 与 go vet
```

提交前请确保 `make lint` 与 `make test` 通过。CI 会额外以 `SLS_LOCAL_TEST_FLUSH_ROWS=3`（频繁刷段）和 `SLS_LOCAL_TEST_REMOTE=1`（再经对象存储）各跑一遍全部测试，建议本地先用 `make test-segments` 覆盖这两种情形。

## 怎样增加或修改行为

sls-local 的语义以真实 SLS 为准，不凭猜测。新增或修改一处行为时：

1. **先对照真实 SLS**：用一个真实 logstore 发出相同的请求，记录它的响应、报错文案、字段顺序等。`tools/wiredump` 可以抓取客户端发来的原始请求（HTTP 与自签 HTTPS），帮助还原协议细节。
2. **把结论记进 [`docs/wire-findings.md`](docs/wire-findings.md)**：这是行为对照的依据，评审时会参考。
3. **再实现并补测试**：端到端测试（`internal/api`）用官方 Go SDK 当客户端，期望值来自真实 SLS。
4. **golden 对照测试**（[`docs/golden.md`](docs/golden.md)）只进不退：已知差异登记在 `testdata/golden/known-diffs.json`，修好一个差异就删掉对应条目；让名单外的用例保持一致。

客户端靠报错**码**区分错误时，报错码需要与 SLS 一致；靠报错**文案**时，文案也要逐字一致（见 NOTICE 对报错文案的说明）。鉴权、限流、多节点的报错码取自 SLS 文档，尚未与真实服务逐条对照，详见 [`docs/compatibility.md`](docs/compatibility.md)。

## 代码规范

- `gofmt` 格式化；`go vet` 通过（`internal/presto/parser` 是 ANTLR 生成的代码，不做 vet）。
- 注释、文档使用简体中文，与现有代码保持一致；技术标识符保持原样。
- 改动精准：只动与本次目的相关的代码，不顺手重构、重排无关部分。
- 不引入非必要的依赖（见 NOTICE 的第三方清单；每个编译进二进制的依赖都要带许可证，CI 会检查）。

## 提交与 PR

- 提交信息使用 [Conventional Commits](https://www.conventionalcommits.org/) 前缀：`feat:`、`fix:`、`perf:`、`docs:`、`test:`、`refactor:`、`release:`。
- 一个 PR 聚焦一件事，附上动机与验证方式（跑了哪些测试、与 SLS 的对照结果）。
- 面向用户的改动在 [`CHANGELOG.md`](CHANGELOG.md) 顶部的「未发布」小节补一条。
- 发布由维护者完成：把「未发布」小节改成 `# vX.Y.Z (YYYY-MM-DD)` 标题后推送同名标签，`.github/workflows/release.yml` 会构建镜像，并以该小节作为 GitHub Release 的说明。

## 报告安全问题

**不要**在公开 issue 里提交安全漏洞。流程见 [`SECURITY.md`](SECURITY.md)。

## 许可证

提交即表示你同意你的贡献以本项目的 [MIT 许可证](LICENSE)授权。
