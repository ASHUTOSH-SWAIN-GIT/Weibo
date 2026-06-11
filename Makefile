# mailer Makefile
#
# Common development tasks. All targets use bash and assume the working
# directory is the repo root.
#
# Quick start:
#   make help        list available targets
#   make ci          run everything CI runs (build, vet, test-race, fmt-check)
#   make kafka-test  run the Kafka e2e test (requires local broker)

GO         ?= go
PKG        ?= ./...
COVER_FILE ?= coverage.out
COVER_HTML ?= coverage.html

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "mailer make targets:\n\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[1;34m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# --- build --------------------------------------------------------------------

.PHONY: build
build: ## Compile all packages
	$(GO) build $(PKG)

.PHONY: build-examples
build-examples: build ## Build all example pipelines
	$(GO) build ./examples/...

# --- format & vet -------------------------------------------------------------

.PHONY: fmt
fmt: ## Run gofmt on all .go files (fixes in place)
	gofmt -w .

.PHONY: fmt-check
fmt-check: ## Verify formatting (CI-friendly; fails on any unformatted files)
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi; echo "fmt: ok"

.PHONY: vet
vet: ## Run go vet on all packages
	$(GO) vet $(PKG)

# --- tests --------------------------------------------------------------------

.PHONY: test
test: ## Run all unit tests
	$(GO) test ./test/unit_tests/...

.PHONY: test-race
test-race: ## Run tests with the race detector
	$(GO) test -race ./test/unit_tests/...

.PHONY: test-window
test-window: ## Run tests for windowing/watermark packages
	$(GO) test -race ./test/unit_tests/window/... ./test/unit_tests/watermark/...

.PHONY: test-coverage
test-coverage: ## Run all tests with coverage profile
	$(GO) test -coverpkg=./... -coverprofile=$(COVER_FILE) ./test/unit_tests/...
	@$(GO) tool cover -func=$(COVER_FILE) | tail -1

.PHONY: coverage-html
coverage-html: test-coverage ## Generate HTML coverage report
	@$(GO) tool cover -html=$(COVER_FILE) -o $(COVER_HTML)
	@echo "wrote $(COVER_HTML)"

.PHONY: clean-coverage
clean-coverage: ## Remove coverage artifacts
	rm -f $(COVER_FILE) $(COVER_HTML)

# --- integration --------------------------------------------------------------

.PHONY: kafka-test
kafka-test: build-examples ## Run the Kafka end-to-end test (requires local broker)
	./scripts/test-kafka.sh

# --- composite targets --------------------------------------------------------

.PHONY: ci
ci: build vet fmt-check test-race ## Run the full local CI suite
	@echo "ci: all checks passed"

.PHONY: clean
clean: clean-coverage ## Remove build artifacts
	rm -f /tmp/mailer-*
