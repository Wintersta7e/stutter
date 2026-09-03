GO ?= go
BIN := bin/stutter

.PHONY: all
all: fmt lint test build

.PHONY: fmt
fmt: ## Apply formatters (gci, gofumpt, golines)
	golangci-lint fmt

.PHONY: fmt-check
fmt-check: ## Fail if formatting is not clean
	golangci-lint fmt --diff

.PHONY: lint
lint: ## Run the full linter set
	golangci-lint run

.PHONY: test
test: ## Race-enabled, shuffled, with coverage
	$(GO) test -race -shuffle=on -covermode=atomic -coverprofile=coverage.out ./...

.PHONY: build
build: ## Build the CLI
	$(GO) build -trimpath -o $(BIN) ./cmd/stutter

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	govulncheck ./...

.PHONY: tidy
tidy: ## Fail if go.mod/go.sum are not tidy
	$(GO) mod tidy -diff

.PHONY: ci
ci: fmt-check lint tidy test vuln build ## Everything CI runs, locally

.PHONY: clean
clean:
	rm -rf bin dist coverage.out coverage.html
