# sls-local

A log service that speaks the Alibaba Cloud Simple Log Service (SLS) API: LoongCollector and the SLS SDKs write to it, and SLS data sources for Grafana query it. For local development it is one process and one data file; in production it runs as write nodes and read nodes, with the data in Parquet segments on object storage (OSS / S3).

[中文](README.md)

> Community project, not affiliated with Alibaba Cloud. SLS is a trademark of Alibaba Group and is used here only to describe what this software is compatible with; see [NOTICE](NOTICE).

## Why

Production logs live in SLS, but local development has two poor options: a cloud logstore for development (AccessKeys, public network, metered billing), or Loki (a different query language, dashboards and alerts written twice).

sls-local makes your laptop speak SLS:

- **Collectors unchanged**: point LoongCollector's `flusher_sls` Endpoint at `sls-local`.
- **Queries unchanged**: SLS data sources such as [nethinwei-sls-datasource](https://github.com/nethinwei/sls-grafana-datasource) only change their URL; production dashboards, search statements and SQL work as they are.
- **Same behavior**: search syntax, SQL, index rules, response format and error messages are checked one by one against the real service ([compatibility](docs/compatibility.md)). A query that works locally works in production, and an error production would raise is raised locally too.

The same API can also run in production on your own infrastructure (see [Production](#production)): collectors, dashboards and alerts keep their SLS form, and the logs live in your own object storage.

## Quick start

```bash
git clone https://github.com/nethinwei/sls-local.git
cd sls-local/examples/quickstart
docker compose up -d
```

Open http://localhost:3000 (no login). The "sls-local 快速上手" dashboard shows log volume by level, P95 latency and error logs; in Explore, pick the SLS data source and try `level: ERROR` or `* | select service_name, count(*) as c group by service_name`.

The example runs a log generator → LoongCollector → sls-local → Grafana 13 (the SLS plugin is installed automatically). If ports are taken: `GRAFANA_PORT=3001 SLS_LOCAL_PORT=8081 docker compose up -d`.

## Use it in your environment

```yaml
services:
  sls-local:
    image: ghcr.io/nethinwei/sls-local:0.3.0
    command: ["-config", "/etc/sls-local.yaml"]   # optional: create logstores, declare indexes, set access keys
    volumes: ["./sls-local.yaml:/etc/sls-local.yaml:ro", "sls:/data"]
    ports: ["127.0.0.1:8080:8080"]               # localhost only: no authentication without access keys
    networks:
      default:
        aliases: [myproject.sls-local]            # one alias per project
```

**Why the network alias**: SLS SDKs and collectors always send requests to `{project}.{endpoint}`; sls-local takes the project from the first label of the host name.

| Client | Settings |
|---|---|
| LoongCollector | `flusher_sls` with `Endpoint: sls-local` (host name only). The community edition always uses HTTPS on port 443 without verifying the certificate; sls-local listens on 443 with a self-signed certificate |
| Grafana SLS data source | URL `http://sls-local:8080`, your project / logstore; any AccessKey, or one of `access_keys` when they are configured |
| SDK in a container | Endpoint `http://sls-local:8080` |
| SDK or curl on the host | Endpoint `http://localhost:8080`; requests go to `myproject.localhost:8080`, which macOS and Linux with systemd-resolved resolve to loopback. Elsewhere, add `127.0.0.1 myproject.localhost` to `/etc/hosts` |

### Flags

| Flag | Default | Description |
|---|---|---|
| `-http` | `:8080` | Query port (HTTP) |
| `-https` | `:443` | Write port (HTTPS, self-signed at startup); empty to disable. Read nodes do not listen by default |
| `-data` | `/data/sls.duckdb` | Data file; empty for an in-memory database |
| `-ttl` | `72h` | Retention by log time; `0` keeps everything |
| `-config` | | Configuration file, see below |
| `-logstore` | | Create `project/logstore` at startup, repeatable (logstores are also created on first write) |
| `-sql-utc-offset` | `8h` | Time zone of SQL time functions; SLS defaults to UTC+8 |
| `-version` | | Print the version |
| `-reindex` | | Rebuild the full-text column of all logs with the current indexes in `-config`, then exit (stop the running sls-local first; on a write node in a cluster) |
| `-segments` | `segments/` next to the data file | Parquet segment directory; the local cache with object storage |
| `-flush-interval` | `5m` (`2s` on write nodes) | How often new logs are written into segments |
| `-flush-rows` | `1000000` | Flush early once this many new logs are waiting |
| `-storage` | | Object storage: `s3://bucket/prefix` or `file:///path`, see [Production](#production) |
| `-cache-size` | `53687091200` (50 GB) | Local cache limit in bytes when using object storage |
| `-target` | `all` | Node role: `all` (single node), `write` (writes only), `read` (queries only) |
| `-writer-id` | `0` | Write node number; unique among write nodes sharing an object storage. `auto` takes the number at the end of the hostname (StatefulSet) |
| `-refresh-interval` | `2s` | How often read nodes reread the segment manifests |
| `-query-timeout` | `1m` | Longest a query may run; then `RequestTimeout` |
| `-max-queries` | 2 × CPUs | Queries running at once; a query that waits 5 s for a slot gets `ReadQuotaExceed` |
| `-memory-limit` | 80% of RAM | DuckDB memory limit, e.g. `8GB`. DuckDB fills its cache up to about this much (expected); leave about 1 GB of headroom below the container's memory limit |
| `-write-rate` | `0` (unlimited) | Logs per second each logstore accepts (2 s of burst), then `WriteQuotaExceed` |

### Configuration and indexes

```yaml
access_keys:        # optional: verify request signatures (v1) on every SLS API; none when omitted
  - {id: my-collector, secret: ..., projects: [myproject]}  # projects is optional: only these projects
  - {id: my-grafana, secret: ...}
logstores:
  - project: myproject
    logstore: app
    index:          # same JSON structure as SLS GetIndex returns: export it from production
      line: {token: [",", " ", ";", "=", "(", ")", "[", "]", "{", "}", "?", "@", "&", "<", ">", "/", ":", "\n", "\t", "\r"]}
      keys:
        level:   {type: text, token: [" "], doc_value: true}
        cost_ms: {type: long, doc_value: true}
```

Logstores with a declared index follow SLS rules: fields without an index cannot be searched (with the same error as SLS), long / double fields match numerically, and SQL can only use fields with statistics (`doc_value`). **With your production index, you find "the dashboard uses a field that has no index" before deploying.**

Logstores without a declared index get a text index with statistics on every field, so they work out of the box; numeric aggregation then needs `cast`.

As in SLS, changing an index only affects logs written afterwards. To search older logs with the new index, stop sls-local and run `sls-local -reindex -data <data file> -config <config file>` once (the equivalent of "rebuild index" in the SLS console).

## Production

> **Beta**: multi-node deployment is new in v0.3. It has been checked on one machine with 2 write nodes, 1–2 read
> nodes, object storage and a real collector, but not yet in large, long-running production use. Known limits are
> listed at the end of this section.

```
LoongCollector / SDK ──PutLogs──► load balancer ──► write nodes × N ──segments (every 2 s)──► object storage (OSS / S3)
                                                                                                │
Grafana SLS data source ──GetLogs etc.──► load balancer ──► read nodes × M ◄──download, local cache──┘
```

- **Write nodes** (`-target=write`) only accept writes: logs go to the local data file first and are uploaded as Parquet segments every 2 seconds; write nodes also merge small files and remove expired segments (`-ttl`). Give the data directory a persistent volume; `-writer-id` must differ between write nodes (in a StatefulSet, `-writer-id=auto` takes the pod ordinal): each id has a lease in the object storage, so a second node with the same id refuses to start; if the previous process died without stopping cleanly and the node comes back on a new disk, it waits for the lease to expire (1 minute). Expired segments of write nodes that were scaled away are removed by the remaining write nodes. Leases expire by time, so keep the nodes' clocks in sync (NTP).
- **Read nodes** (`-target=read`) only serve queries: every 2 seconds they reread the segment manifests kept by the write nodes (downloading only the changed ones) and download the segments a query needs into a local cache (`-cache-size`). They keep no data; add or remove them freely.
- Both need `-storage s3://bucket/prefix`. The S3 endpoint comes from `SLS_S3_ENDPOINT` (for OSS an endpoint such as `oss-cn-hangzhou.aliyuncs.com`); `SLS_S3_REGION` is optional. Credentials are taken from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, then `~/.aws/credentials`, then an AWS IAM role (EKS IRSA, ECS task role, EC2 instance role); for OSS use an AccessKey in the environment variables.
- Set `access_keys` in the configuration file to verify signatures; collectors, SDKs and Grafana data sources use those keys, and `projects` limits a key to some projects.

A complete example (SeaweedFS as S3, 2 write nodes, 1 read node, LoongCollector, Grafana):

```bash
cd examples/production
docker compose up -d     # then open http://localhost:3000
```

Kubernetes manifests (write nodes as a StatefulSet, read nodes as a Deployment, name resolution) are in [examples/kubernetes](examples/kubernetes).

**Good to know**

- Read nodes see writes 2–4 seconds late (the flush interval plus the manifest refresh interval). A single node (`-target=all`) shows a write immediately.
- Until a logstore's first segment reaches object storage, it does not exist on read nodes (`LogStoreNotExist`): declare logstores in the configuration file.
- Put write nodes behind a load balancer or a Kubernetes Service (a stable address). LoongCollector resolves the name once and keeps the address, so DNS round robin does not spread the load, and it does not follow a node to a new IP. Layer-4 load balancers (including Kubernetes Services) balance per connection and collectors keep connections open: with few collectors, writes concentrate on a few write nodes; they spread out with many collectors (such as one per node in a DaemonSet). Use a layer-7 load balancer to balance per request.
- The first query that needs a segment downloads the whole file (up to 1M logs, in parallel): cold data is slower the first time; give the cache disk enough room. Read nodes fetch the bloom filters of new segments as soon as they see them.
- Object storage requests: every 2 seconds each write node uploads a segment and its bloom filter for each logstore that received logs (rewritten a few more times by compaction), plus a manifest; read nodes list the manifests every 2 seconds. With 50 logstores written continuously, a write node makes about 50 PUTs per second (about 4.3 million a day). Raise `-flush-interval` to make fewer, at the cost of read nodes seeing data later.
- Known limits: a query runs on one read node (more read nodes add concurrency, not speed for a single query); read nodes do not see logs a write node has not flushed yet; the error codes for authentication and limits have not been checked one by one against SLS ([compatibility](docs/compatibility.md)).
- Monitoring: `/metrics` on every node (Prometheus format: requests and durations per API, ingestion, flushes, uploads and downloads, segments skipped by bloom filters), `/health` (liveness) and `/ready` (readiness), none of them signed.

## Compatibility

Supported: PutLogs, GetLogsV3 (search, SPL `| where`, SQL), GetHistograms, GetContextLogs, GetIndex, ListLogStores, GetLogStore. SQL is rewritten from the Presto dialect to DuckDB; common aggregate and time functions work. See [docs/compatibility.md](docs/compatibility.md) for the full list and known differences, and [docs/wire-findings.md](docs/wire-findings.md) for how each behavior was checked against SLS (both in Chinese).

## How it works

```
LoongCollector / SDK ──PutLogs (HTTPS 443 / HTTP)──►┐
                                                     ├─ sls-local ── DuckDB: head table + Parquet segments (local or object storage)
Grafana SLS data source ──GetLogs etc. (HTTP 8080)─►┘
```

- New logs go to a DuckDB table (the head) and are periodically written into Parquet segments per project / logstore / hour (up to 1M logs per file; small files are merged). A query reads a snapshot of head plus segments.
- Fields are stored as JSON in the order they were written (SLS returns fields in that order); each segment also stores frequent fields as their own columns, which aggregations and field searches read directly.
- Each segment has a bloom filter of its words: a search for a rare value (an order id, a trace id) skips the segments that cannot match.
- Search statements compile to DuckDB conditions according to the index. SQL is translated from a Presto syntax tree: fields become their columns, `FROM log` refers to the logstore's data in the time range, and Presto functions become their DuckDB equivalents.
- 2M logs (single node, on disk): group by 128 → 9.6 ms, histogram 59 → 6.4 ms, rare value 70 → 0.9 ms (against v0.2; see CHANGELOG).

## Security

- **Without `access_keys` there is no authentication** (the default for local development): anyone who reaches the ports can read and write all data. Bind them to `127.0.0.1` and do not expose them.
- With `access_keys`, every SLS API verifies the request signature (v1, as the SLS SDKs and LoongCollector sign; v4 is not supported yet) and checks `Content-MD5` against the body. A valid key grants everything; there are no per-project or per-logstore permissions.
- The write port (443) uses a self-signed certificate: behind a load balancer, terminate TLS there.
- Queries are confined to the current logstore: DuckDB can only read and write the segment directory, cannot reach other files, install extensions or change its settings, and SQL can only read `FROM log`.
- Object storage credentials are passed only through environment variables.

## Development

```bash
make test           # all tests (needs cgo)
make test-segments  # the same tests with data flushed into segments, then into object storage (a directory)
make build          # build bin/sls-local
make image          # build the image
```

- End-to-end tests use the official Go SDK as the client, with expectations taken from the real service.
- To add or change behavior, check it against SLS first, record it in [docs/wire-findings.md](docs/wire-findings.md), then implement and test it.
- `tools/wiredump` records raw requests (HTTP and self-signed HTTPS) to capture the protocol details of a new client.
- Benchmarks: `go test ./internal/api -run '^$' -bench .`; `SLS_BENCH_N` sets the number of logs, `SLS_BENCH_DISK=1` uses a data file, `SLS_BENCH_SEGMENTS=1` flushes everything into segments.
- Tests against a real S3: `SLS_S3_ENDPOINT=… AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… SLS_LOCAL_TEST_FLUSH_ROWS=3 SLS_LOCAL_TEST_REMOTE=s3://bucket/prefix go test ./...`.

## License

[MIT](LICENSE). Third-party software is listed in [NOTICE](NOTICE); the image ships the licenses of all dependencies under `/usr/share/doc/sls-local`.
