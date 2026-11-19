# Kubernetes 部署示例

`sls-local.yaml` 部署读写分离的 sls-local：

| 对象 | 作用 |
|---|---|
| StatefulSet `sls-local-write` × 2 | 写入节点，`-writer-id=auto` 取 Pod 序号，每个 Pod 一块持久卷 |
| Service `sls-local-write` | 采集器、SDK 写入（443 HTTPS、80 HTTP） |
| Deployment `sls-local-read` × 2 | 查询节点，缓存放 emptyDir，可随意增减 |
| Service `sls-local-read` | Grafana 数据源、SDK 查询（80） |
| Secret `sls-local` | 对象存储凭证、配置文件（索引与 AccessKey） |

```bash
kubectl create namespace logging
# 先改 Secret 里的凭证与密钥、-storage 的桶、SLS_S3_ENDPOINT
kubectl -n logging apply -f sls-local.yaml
```

## 域名

SLS 的客户端把 project 放在域名第一段：endpoint 填 `sls-write.internal` 时，写入 project `demo` 的请求发往
`demo.sls-write.internal`。集群里要让 `*.sls-write.internal` 解析到 `sls-local-write` 这个 Service
（`*.sls-read.internal` 同理）。两种做法：

- 在自己的 DNS 里加泛域名记录，指向 Service 的地址（集群外访问时用 LoadBalancer 或 Ingress）。
- 集群内用 CoreDNS 改写：在 `kube-system/coredns` 的 Corefile 里加

  ```
  rewrite stop {
      name regex (.+)\.sls-write\.internal sls-local-write.logging.svc.cluster.local
      answer auto
  }
  rewrite stop {
      name regex (.+)\.sls-read\.internal sls-local-read.logging.svc.cluster.local
      answer auto
  }
  ```

LoongCollector 解析一次域名后一直用那个地址：写入节点前面必须是固定地址（ClusterIP 或负载均衡），不要用
headless Service 做 DNS 轮询。ClusterIP 按连接分流，采集器保持长连接：采集器少时写入会集中在个别写入节点上
（每个节点一个采集器的 DaemonSet 时分得开）；要按请求分流，在前面放七层负载均衡（Ingress 等）。

## 扩缩容与运维

- 写入节点扩容直接改 replicas。缩容时被缩掉的 Pod 先把还没刷段的日志上传再退出（`terminationGracePeriodSeconds`
  留够），它的持久卷可以删；它留在对象存储上的分段照常可查，过期后由其余写入节点删除。
- 编号重复（比如同一个 StatefulSet 部署到两个集群却共用一个桶和前缀）时，后启动的节点拒绝启动，日志里说明谁占着
  编号。两套部署共用一个桶时用不同的 `-storage` 前缀。
- `/ready` 在租约被别的节点拿走、查询节点 1 分钟以上没读到分段清单时失败，`/metrics` 是 Prometheus 格式。告警规则示例见
  `alerts.yaml`（未就绪、后台错误、上传失败、未刷段的日志堆积、5xx、数据盘）。
- 镜像以非 root（uid 10001）运行，监听 8443 而不是 443。
