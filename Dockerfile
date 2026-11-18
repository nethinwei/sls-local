# DuckDB 需要 cgo。构建阶段跑在构建机的平台上，用交叉编译器生成目标平台的二进制
# （DuckDB 是预编译的静态库，只需链接），比在 QEMU 里模拟编译快得多。
FROM --platform=$BUILDPLATFORM golang:1.25-bookworm AS build
ARG TARGETARCH
ARG VERSION=dev
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) pkg=x86-64-linux-gnu; cc=x86_64-linux-gnu ;; \
      arm64) pkg=aarch64-linux-gnu; cc=aarch64-linux-gnu ;; \
      *) echo "unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    apt-get update; \
    apt-get install -y --no-install-recommends "gcc-$pkg" "g++-$pkg"; \
    rm -rf /var/lib/apt/lists/*; \
    echo "$cc" > /tmp/cc-prefix
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN set -eux; \
    cc=$(cat /tmp/cc-prefix); \
    export CGO_ENABLED=1 GOOS=linux GOARCH="$TARGETARCH" CC="$cc-gcc" CXX="$cc-g++"; \
    go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/sls-local ./cmd/sls-local; \
    scripts/collect-go-licenses.sh /out/licenses; \
    cp LICENSE NOTICE /out/

FROM debian:bookworm-slim
ARG VERSION=dev
LABEL org.opencontainers.image.title="sls-local" \
      org.opencontainers.image.description="Log service speaking the Alibaba Cloud SLS API, for development and self-hosting" \
      org.opencontainers.image.source="https://github.com/nethinwei/sls-local" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="$VERSION"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /data sls \
    && mkdir -p /data && chown sls /data
COPY --from=build /out/sls-local /usr/local/bin/sls-local
COPY --from=build /out/LICENSE /out/NOTICE /usr/share/doc/sls-local/
COPY --from=build /out/licenses /usr/share/doc/sls-local/licenses
USER sls
VOLUME /data
# 8080：查询（HTTP）；443：写入（HTTPS 自签，LoongCollector 只走 443）
EXPOSE 8080 443
# /health 不要签名、不分节点角色（GET / 是 SLS 的接口：开启签名校验或在写入节点上不是 200）
HEALTHCHECK --interval=10s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["sls-local"]
