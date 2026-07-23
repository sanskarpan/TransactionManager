.PHONY: build test race bench run lint fmt tidy vuln docker

# Build the server binary.
build:
	go build -o bin/server ./cmd/server

# Packages to test — excludes web/node_modules (vendored npm packages
# that ship stray .go files like flatted/golang are not part of this
# module's testable surface).
GO_PACKAGES := $(shell go list ./... | grep -v /web/node_modules)

# Run the unit test suite (no race detector; fast feedback).
test:
	go test $(GO_PACKAGES) -v -count=1 -timeout 300s

# Run the unit test suite with the race detector (CI parity).
race:
	go test $(GO_PACKAGES) -race -count=1 -timeout 300s

# Run benchmarks on the internal packages.
bench:
	go test ./internal/... -bench=. -benchmem -benchtime=5s

# Coverage report; prints total + per-function coverage.
coverage:
	go test $(GO_PACKAGES) -count=1 -coverprofile=coverage.out -coverpkg=$(shell echo $(GO_PACKAGES) | tr ' ' ',')
	go tool cover -func=coverage.out

# Run the server.
run:
	go run ./cmd/server

# go vet (zero-warning policy). L-INF-9: scoped to GO_PACKAGES so the
# vendored npm tree under web/node_modules (which ships stray .go files)
# does not break the lint target.
lint:
	go vet $(GO_PACKAGES)

# gofmt + goimports formatting check; fails if any file is not formatted.
fmt:
	@if ! gofmt -l . | grep -v node_modules | grep -q .; then \
		echo "files need gofmt:"; \
		gofmt -l . | grep -v node_modules; \
		exit 1; \
	else \
		echo "all files formatted"; \
	fi

# Verify go.mod is tidy; fails if `go mod tidy` would change anything.
tidy:
	go mod tidy
	@git diff --exit-code go.mod go.sum || { \
		echo "go.mod/go.sum not tidy; run 'go mod tidy' and commit"; \
		exit 1; }

# govulncheck (dependency CVE scan). Requires govulncheck installed.
vuln:
	@command -v govulncheck >/dev/null 2>&1 || { \
		echo "install govulncheck: go install golang.org/x/vuln/cmd/govulncheck@latest"; \
		exit 1; }
	govulncheck ./...

# Build the production container image.
docker:
	docker build -t txn-manager:dev .

# CI-parity target: run everything CI runs locally before pushing.
ci-local: tidy fmt lint race coverage
	@echo "CI-local checks passed; safe to push."
