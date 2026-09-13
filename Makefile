# weibo Makefile
#
# Common development tasks. All targets use bash and assume the working
# directory is the repo root.
#
# Quick start:
#   make help        list available targets
#   make ci          run the same stable checks as hosted CI
#   make kafka-test  run the Kafka e2e test (requires local broker)

GO         ?= go
PKG        ?= ./...
COVER_FILE ?= coverage.out
COVER_HTML ?= coverage.html
GOFILES    := $(shell git ls-files '*.go' ':!:vendor/*')

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "weibo make targets:\n\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[1;34m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# --- build --------------------------------------------------------------------

.PHONY: build
build: ## Compile all root and control packages
	$(GO) build $(PKG)
	cd control && $(GO) build ./...

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
vet: ## Run go vet on root and control packages
	$(GO) vet $(PKG)
	cd control && $(GO) vet ./...

.PHONY: vet-kubernetes
vet-kubernetes: ## Run go vet for Kubernetes-tagged controller packages
	cd control && $(GO) vet -tags kubernetes ./...

# --- tests --------------------------------------------------------------------

.PHONY: test
test: ## Run all root and control tests
	$(GO) test ./...
	cd control && $(GO) test ./...

.PHONY: test-race
test-race: ## Run root and short control tests with the race detector
	$(GO) test -race ./...
	cd control && $(GO) test -short -race ./...

.PHONY: test-kubernetes
test-kubernetes: ## Run Kubernetes-tagged controller tests
	cd control && $(GO) test -short -race -tags kubernetes ./...

.PHONY: test-window
test-window: ## Run tests for windowing/watermark packages
	$(GO) test -race ./test/unit_tests/window/... ./test/unit_tests/watermark/...

.PHONY: test-coverage
test-coverage: ## Run root and control coverage profiles
	$(GO) test -coverpkg=./... -coverprofile=$(COVER_FILE) -covermode=atomic ./...
	@$(GO) tool cover -func=$(COVER_FILE) | tail -1
	cd control && $(GO) test -short -coverprofile=control-coverage.out -covermode=atomic ./...
	@cd control && $(GO) tool cover -func=control-coverage.out | tail -1

.PHONY: coverage-html
coverage-html: test-coverage ## Generate HTML coverage report
	@$(GO) tool cover -html=$(COVER_FILE) -o $(COVER_HTML)
	@echo "wrote $(COVER_HTML)"

.PHONY: clean-coverage
clean-coverage: ## Remove coverage artifacts
	rm -f $(COVER_FILE) $(COVER_HTML) control/control-coverage.out

# --- integration --------------------------------------------------------------

.PHONY: kafka-test
kafka-test: build-examples ## Run the Kafka end-to-end test (requires local broker)
	./scripts/test-kafka.sh

# --- composite targets --------------------------------------------------------

.PHONY: ci
ci: build fmt-check vet vet-kubernetes test-race test-kubernetes test-coverage ## Run the stable local CI suite
	@echo "ci: all checks passed"

.PHONY: clean
clean: clean-coverage ## Remove build artifacts
	rm -f /tmp/weibo-*
