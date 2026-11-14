# sls-local

A local server that speaks the Alibaba Cloud Simple Log Service (SLS) API: LoongCollector and the SLS SDKs write to it, SLS data sources for Grafana query it, and the data lives in a local DuckDB. No cloud account, no cost.

[中文](README.md)

> Community project, not affiliated with Alibaba Cloud. SLS is a trademark of Alibaba Group and is used here only to describe what this software is compatible with; see [NOTICE](NOTICE).

## Why

Production logs live in SLS, but local development has two poor options: a cloud logstore for development (AccessKeys, public network, metered billing), or Loki (a different query language, dashboards and alerts written twice).

sls-local makes your laptop speak SLS:

- **Collectors unchanged**: point LoongCollector's `flusher_sls` Endpoint at `sls-local`.
- **Queries unchanged**: SLS data sources such as [nethinwei-sls-datasource](https://github.com/nethinwei/sls-grafana-datasource) only change their URL; production dashboards, search statements and SQL work as they are.
- **Same behavior**: search syntax, SQL, index rules, response format and error messages are checked one by one against the real service ([compatibility](docs/compatibility.md)). A query that works locally works in production, and an error production would raise is raised locally too.

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
    image: ghcr.io/nethinwei/sls-local:0.1.0
    command: ["-config", "/etc/sls-local.yaml"]   # optional: create logstores, declare indexes
    volumes: ["./sls-local.yaml:/etc/sls-local.yaml:ro", "sls:/data"]
    ports: ["127.0.0.1:8080:8080"]               # localhost only: sls-local has no authentication
    networks:
      default:
        aliases: [myproject.sls-local]            # one alias per project
```

**Why the network alias**: SLS SDKs and collectors always send requests to `{project}.{endpoint}`; sls-local takes the project from the first label of the host name.

| Client | Settings |
|---|---|
| LoongCollector | `flusher_sls` with `Endpoint: sls-local` (host name only). The community edition always uses HTTPS on port 443 without verifying the certificate; sls-local listens on 443 with a self-signed certificate |
| Grafana SLS data source | URL `http://sls-local:8080`, your project / logstore, any AccessKey |
| SDK in a container | Endpoint `http://sls-local:8080` |
| SDK or curl on the host | Endpoint `http://localhost:8080`; requests go to `myproject.localhost:8080`, which macOS and Linux with systemd-resolved resolve to loopback. Elsewhere, add `127.0.0.1 myproject.localhost` to `/etc/hosts` |

### Flags

| Flag | Default | Description |
|---|---|---|
| `-http` | `:8080` | Query port (HTTP) |
| `-https` | `:443` | Write port (HTTPS, self-signed at startup); empty to disable |
| `-data` | `/data/sls.duckdb` | Data file; empty for an in-memory database |
| `-ttl` | `72h` | Retention by log time; `0` keeps everything |
| `-config` | | Configuration file, see below |
| `-logstore` | | Create `project/logstore` at startup, repeatable (logstores are also created on first write) |
| `-sql-utc-offset` | `8h` | Time zone of SQL time functions; SLS defaults to UTC+8 |
| `-version` | | Print the version |

### Configuration and indexes

```yaml
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

## Compatibility

Supported: PutLogs, GetLogsV3 (search, SPL `| where`, SQL), GetHistograms, GetContextLogs, GetIndex, ListLogStores, GetLogStore. SQL is rewritten from the Presto dialect to DuckDB; common aggregate and time functions work. See [docs/compatibility.md](docs/compatibility.md) for the full list and known differences, and [docs/wire-findings.md](docs/wire-findings.md) for how each behavior was checked against SLS (both in Chinese).

## How it works

```
LoongCollector / SDK ──PutLogs (HTTPS 443 / HTTP)──►┐
                                                     ├─ sls-local ── DuckDB (/data/sls.duckdb)
Grafana SLS data source ──GetLogs etc. (HTTP 8080)─►┘
```

- All logs are kept in one table: fixed columns for time and metadata, fields stored as JSON in the order they were written (SLS returns fields in that order).
- Search statements compile to DuckDB conditions according to the index. SQL is rewritten token by token: fields become typed value expressions, `FROM log` refers to a subquery holding only the current logstore, and Presto functions become their DuckDB equivalents.

## Security

sls-local is for local development and testing only: **there is no authentication**, request signatures are parsed but not verified, and anyone who reaches the ports can read and write all data. Do not expose it to the internet; on a host, bind the ports to `127.0.0.1`.

Queries are still confined to the current logstore: DuckDB cannot read or write external files, install extensions or change its settings, and SQL can only read `FROM log`.

## Development

```bash
make test      # all tests (needs cgo)
make build     # build bin/sls-local
make image     # build the image
```

- End-to-end tests use the official Go SDK as the client, with expectations taken from the real service.
- To add or change behavior, check it against SLS first, record it in [docs/wire-findings.md](docs/wire-findings.md), then implement and test it.
- `tools/wiredump` records raw requests (HTTP and self-signed HTTPS) to capture the protocol details of a new client.

## License

[MIT](LICENSE). Third-party software is listed in [NOTICE](NOTICE); the image ships the licenses of all dependencies under `/usr/share/doc/sls-local`.
