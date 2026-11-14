# sls-local 开发入口。make help 查看全部命令。
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE   ?= sls-local

.PHONY: help build test lint run image image-multi licenses clean

help: ## 显示帮助
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-12s %s\n", $$1, $$2}'

build: ## 编译到 bin/sls-local（需要 cgo）
	go build -trimpath -ldflags "-X main.version=$(VERSION)" -o bin/sls-local ./cmd/sls-local

test: ## 运行全部测试（含竞态检测）
	go test -race ./...

lint: ## gofmt 与 go vet
	@unformatted=$$(gofmt -l .); if [ -n "$$unformatted" ]; then echo "需要 gofmt：$$unformatted"; exit 1; fi
	go vet ./...

run: build ## 本机运行：HTTP :8080、HTTPS :8443，数据在 bin/sls.duckdb（LoongCollector 只走 443，采集请用容器）
	bin/sls-local -data bin/sls.duckdb -https :8443 -logstore demo/app

image: ## 构建本机架构的镜像 $(IMAGE):$(VERSION)
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

image-multi: ## 交叉构建 amd64 与 arm64 镜像（不推送）
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

licenses: ## 收集依赖许可证到 bin/licenses（缺许可证时失败）
	scripts/collect-go-licenses.sh bin/licenses

clean: ## 删除 bin/
	rm -rf bin
