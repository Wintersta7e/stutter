SHELL := bash
.SHELLFLAGS := -euo pipefail -c

GO ?= go
BIN ?= bin/stutter
PKG ?= ./cmd/stutter
# The build's cgo switch, deliberately a plain assignment: a CGO=1 on make's command line overrides
# it (a dynamic artifact a test asks for by name), one in the environment does not, so no shell can
# silently make the release dynamic.
CGO = 0
TESTGATE := $(GO) run ./tools/testgate

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
test: ## Race-enabled, shuffled, uncached, with coverage; every outcome counted, any skip fails
	$(GO) test -race -shuffle=on -count=1 -covermode=atomic -coverprofile=coverage.out -json ./... | $(TESTGATE) count

.PHONY: build
build: ## Build the CLI: the one build of the product, static unless CGO=1 is passed to make
	CGO_ENABLED=$(CGO) $(GO) build -trimpath -o $(BIN) $(PKG)

.PHONY: static-check
static-check: ## Fail unless $(BIN) was built without cgo and has no ELF interpreter
	@hdrs="$$(readelf -l $(BIN))"; \
	interp="$$(printf '%s\n' "$$hdrs" | grep -c INTERP || true)"; \
	cgo="$$($(GO) version -m $(BIN) | awk '$$2 ~ /^CGO_ENABLED=/ { sub(/^CGO_ENABLED=/, "", $$2); print $$2 }')"; \
	echo "artifact $(BIN): CGO_ENABLED=$${cgo:-unset} INTERP=$$interp"; \
	[ "$$cgo" = 0 ] && [ "$$interp" = 0 ]

.PHONY: buildscan
buildscan: ## Fail unless the Makefile's build recipe is the only build of the product
	git ls-files -z | $(TESTGATE) builddef $(BUILDSCAN_EXTRA)

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	govulncheck ./...

.PHONY: tidy
tidy: ## Fail if go.mod/go.sum are not tidy
	$(GO) mod tidy -diff

.PHONY: ci
ci: fmt-check lint buildscan tidy test vuln build static-check ## Everything CI runs, locally

.PHONY: clean
clean:
	rm -rf bin dist coverage.out coverage.html
