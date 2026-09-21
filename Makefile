# weibo Makefile
#
# Common development tasks. All targets use bash and assume the working
# directory is the repo root.
#
# `make ci` mirrors the hosted CI jobs in .github/workflows/ci.yml step
# for step (build, fmt, vet incl. kubernetes tags + telemetry, race
# tests incl. kubernetes tags, coverage for root/control/telemetry,
# static checks, govulncheck). The hosted workflow calls these same
# make targets where practical so local and hosted CI stay equivalent
# (roadmap #25).
#
# Quick start:
#   make help        list available targets
#   make ci          run the same stable checks as hosted CI
#   make kafka-test  run the Kafka e2e test (requires local broker)
#   make dashboard-e2e  run the dashboard end-to-end test (requires docker + jq)

GO         ?= go
PKG        ?= ./...
COVER_FILE ?= coverage.out
COVER_HTML ?= coverage.html
GOFILES    := $(shell git ls-files '*.go' ':!:vendor/*')
FUZZTIME   ?= 10s

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "weibo make targets:\n\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[1;34m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# --- build --------------------------------------------------------------------

.PHONY: build
build: ## Compile all root, control, and telemetry packages
	$(GO) build $(PKG)
	cd control && $(GO) build ./...
	cd telemetry && $(GO) build ./...

.PHONY: build-examples
build-examples: build ## Build all example pipelines
	$(GO) build ./examples/...

# --- format & vet -------------------------------------------------------------

.PHONY: fmt
fmt: ## Run gofmt on all .go files (fixes in place)
	gofmt -w $(GOFILES)

.PHONY: fmt-check
fmt-check: ## Verify formatting (CI-friendly; fails on any unformatted files)
	@out=$$(gofmt -l $(GOFILES)); if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi; echo "fmt: ok"

.PHONY: vet
vet: ## Run go vet on root, control, and telemetry packages
	$(GO) vet $(PKG)
	cd control && $(GO) vet ./...
	cd telemetry && $(GO) vet ./...

.PHONY: vet-kubernetes
vet-kubernetes: ## Run go vet for Kubernetes-tagged controller packages
	cd control && $(GO) vet -tags kubernetes ./...

.PHONY: check-static
check-static: ## Check workflows, Dockerfiles, shell, YAML, and docs links
	./scripts/check-static.sh

# --- tests --------------------------------------------------------------------

.PHONY: test
test: ## Run all root, control, and telemetry tests
	$(GO) test ./...
	cd control && $(GO) test ./...
	cd telemetry && $(GO) test ./...

.PHONY: test-telemetry
test-telemetry: ## Run telemetry module tests
	cd telemetry && $(GO) test ./...

.PHONY: test-race
test-race: ## Run root and short control tests with the race detector
	$(GO) test -race ./...
	cd control && $(GO) test -short -race ./...
	cd telemetry && $(GO) test -race ./...

.PHONY: test-kubernetes
test-kubernetes: ## Run Kubernetes-tagged controller tests
	cd control && $(GO) test -short -race -tags kubernetes ./...

.PHONY: test-window
test-window: ## Run tests for windowing/watermark packages
	$(GO) test -race ./test/unit_tests/window/... ./test/unit_tests/watermark/...

.PHONY: test-integration
test-integration: ## Run offline integration tiers (fakes; live backends when env provides them)
	$(GO) test -count=1 ./test/integration/...

.PHONY: test-integration-live
test-integration-live: ## Run integration tiers against live backends (needs KAFKA_BROKERS/POSTGRES_DSN/etc.)
	WEIBO_LIVE=1 $(GO) test -count=1 -v ./test/integration/...

.PHONY: test-coverage
test-coverage: ## Run root, control, and telemetry coverage profiles
	$(GO) test -coverpkg=./... -coverprofile=$(COVER_FILE) -covermode=atomic ./...
	@$(GO) tool cover -func=$(COVER_FILE) | tail -1
	cd control && $(GO) test -short -coverprofile=control-coverage.out -covermode=atomic ./...
	cd control && $(GO) test -short -tags kubernetes -coverprofile=control-coverage-k8s.out -covermode=atomic ./backend/...
	@cd control && grep -v '^mode: ' control-coverage-k8s.out >> control-coverage.out && rm control-coverage-k8s.out && $(GO) tool cover -func=control-coverage.out | tail -1
	cd telemetry && $(GO) test -coverprofile=telemetry-coverage.out -covermode=atomic ./...
	@cd telemetry && $(GO) tool cover -func=telemetry-coverage.out | tail -1

.PHONY: coverage-gate
coverage-gate: test-coverage ## Enforce changed-package coverage minimums (roadmap #27)
	./scripts/check-coverage.sh --gate

.PHONY: coverage-report
coverage-report: test-coverage ## Print per-package coverage for root and control separately
	./scripts/check-coverage.sh

.PHONY: fuzz-smoke
fuzz-smoke: ## Run every fuzz target briefly (FUZZTIME=10s default, override FUZZTIME=30s)
	FUZZTIME=$(FUZZTIME) ./scripts/fuzz-smoke.sh

.PHONY: vuln
vuln: ## Run govulncheck on the root, control, and telemetry modules
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...
	cd control && $(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...
	cd telemetry && $(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: coverage-html
coverage-html: test-coverage ## Generate HTML coverage report
	@$(GO) tool cover -html=$(COVER_FILE) -o $(COVER_HTML)
	@echo "wrote $(COVER_HTML)"

.PHONY: clean-coverage
clean-coverage: ## Remove coverage artifacts
	rm -f $(COVER_FILE) $(COVER_HTML) control/control-coverage.out telemetry/telemetry-coverage.out

# --- integration --------------------------------------------------------------

.PHONY: kafka-test
kafka-test: build-examples ## Run the Kafka end-to-end test (requires local broker)
	./scripts/test-kafka.sh

.PHONY: prod-like-up
prod-like-up: ## Start the prod-like local stack (Kafka/Postgres/MinIO/Prometheus/Grafana/Caddy)
	docker compose -f deploy/compose/docker-compose.prod-like.yml up -d

.PHONY: prod-like-down
prod-like-down: ## Stop and remove the prod-like local stack, including volumes
	docker compose -f deploy/compose/docker-compose.prod-like.yml down -v

# --- composite targets --------------------------------------------------------

.PHONY: dashboard-e2e
dashboard-e2e: ## Boot the real dashboard, run the stream-demo job, assert the read path (needs docker + jq)
	./control/scripts/dashboard-stream-e2e.sh --ci

.PHONY: ci
ci: build fmt-check vet vet-kubernetes check-static test-race test-kubernetes test-coverage ## Run the stable local CI suite
	@echo "ci: all checks passed"

.PHONY: clean
clean: clean-coverage ## Remove build artifacts
	rm -f /tmp/weibo-*
