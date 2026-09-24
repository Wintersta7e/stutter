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
# The one pin of the unreachable-function check, shared by CI and the local gate; go run leaves go.mod
# untouched.
DEADCODE_VERSION := v0.50.0
# The oldest compose plugin Stutter supports, which the floor tests run against. The checksum is the
# one the release publishes for its docker-compose-linux-x86_64 asset.
COMPOSE_FLOOR_VERSION := 2.29.7
COMPOSE_FLOOR_SHA256 := 383ce6698cd5d5bbf958d2c8489ed75094e34a77d340404d9f32c4ae9e12baf0
COMPOSE_FLOOR_DIR ?= $(or $(XDG_CACHE_HOME),$(HOME)/.cache)/stutter/compose-floor-$(COMPOSE_FLOOR_VERSION)

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
	@if [ "$${STUTTER_TEST_DOCKER:-}" != skip ]; then \
		$(MAKE) --no-print-directory compose-floor DEST="$(COMPOSE_FLOOR_DIR)"; \
	fi
	STUTTER_TEST_COMPOSE_FLOOR="$(COMPOSE_FLOOR_DIR)/docker-compose" \
		$(GO) test -race -shuffle=on -count=1 -covermode=atomic -coverprofile=coverage.out -json ./... | \
		$(TESTGATE) count

.PHONY: compose-floor
compose-floor: ## Install the oldest supported compose plugin as $(DEST)/docker-compose, checksum verified
	@[ -n "$(DEST)" ] || { echo "compose-floor: name the directory with DEST=<dir>" >&2; exit 2; }
	@arch="$$(uname -m)"; [ "$$arch" = x86_64 ] || \
		{ echo "compose-floor: only the x86_64 plugin is pinned; this host is $$arch" >&2; exit 1; }
	@mkdir -p "$(DEST)"; \
	if ! echo "$(COMPOSE_FLOOR_SHA256)  $(DEST)/docker-compose" | sha256sum --check --status 2>/dev/null; then \
		curl -sSfL -o "$(DEST)/docker-compose.part" \
			"https://github.com/docker/compose/releases/download/v$(COMPOSE_FLOOR_VERSION)/docker-compose-linux-x86_64"; \
		echo "$(COMPOSE_FLOOR_SHA256)  $(DEST)/docker-compose.part" | sha256sum --check --status || \
			{ echo "compose-floor: the download does not match the pinned checksum" >&2; \
			  rm -f "$(DEST)/docker-compose.part"; exit 1; }; \
		chmod 0755 "$(DEST)/docker-compose.part"; \
		mv "$(DEST)/docker-compose.part" "$(DEST)/docker-compose"; \
	fi; \
	echo "compose-floor $(COMPOSE_FLOOR_VERSION): $(DEST)/docker-compose"

.PHONY: localproofs
localproofs: ## List the proofs CI never compiles, and fail on a production file hidden from it
	git ls-files -z '*.go' | $(TESTGATE) localproofs

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

.PHONY: deadcode
deadcode: ## Fail unless the functions unreachable from the CLI equal the committed baseline
	$(GO) run golang.org/x/tools/cmd/deadcode@$(DEADCODE_VERSION) -json ./cmd/stutter | \
		$(TESTGATE) deadcode tools/testgate/deadcode-baseline.txt

.PHONY: vuln
vuln: ## Scan dependencies for known vulnerabilities
	govulncheck ./...

.PHONY: tidy
tidy: ## Fail if go.mod/go.sum are not tidy
	$(GO) mod tidy -diff

.PHONY: ci
ci: fmt-check lint buildscan deadcode localproofs tidy test vuln build static-check ## Everything CI runs, locally

.PHONY: clean
clean:
	rm -rf bin dist coverage.out coverage.html
