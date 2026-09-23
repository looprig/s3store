.PHONY: test test-integration test-minio fmt fmt-check vet staticcheck gosec vuln secure check build

GO_DIRS := $(shell GOWORK=off go list -f '{{.Dir}}' ./...)
GO_FILES := $(foreach dir,$(GO_DIRS),$(wildcard $(dir)/*.go))

test:
	GOWORK=off go test -race ./...

test-integration:
	GOWORK=off go test -tags integration -race ./...

# Starts a disposable MinIO pinned by digest (Docker), runs the
# `integration && minio` tests against it, and removes the container.
test-minio:
	./scripts/minio-test.sh

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	@test -z "$$(gofmt -l $(GO_FILES))" || (gofmt -l $(GO_FILES) && exit 1)

vet:
	GOWORK=off go vet ./...
	GOWORK=off go vet -tags integration ./...
	GOWORK=off go vet -tags cloud ./...
	GOWORK=off go vet -tags 'integration minio' ./...

# Tagged files are compiled by no untagged analysis, so the integration and
# cloud tests would be unlinted by default. The cloud test is the one that never
# runs anywhere, which makes lint the only thing standing between it and rot.
staticcheck:
	GOWORK=off go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 ./...
	GOWORK=off go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 -tags integration ./...
	GOWORK=off go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 -tags cloud ./...
	GOWORK=off go run honnef.co/go/tools/cmd/staticcheck@v0.8.1 -tags 'integration minio' ./...

gosec:
	GOWORK=off go run github.com/securego/gosec/v2/cmd/gosec@v2.28.0 -quiet $(GO_DIRS)
	GOWORK=off go run github.com/securego/gosec/v2/cmd/gosec@v2.28.0 -quiet -tags integration $(GO_DIRS)
	GOWORK=off go run github.com/securego/gosec/v2/cmd/gosec@v2.28.0 -quiet -tags cloud $(GO_DIRS)
	GOWORK=off go run github.com/securego/gosec/v2/cmd/gosec@v2.28.0 -quiet -tags 'integration minio' $(GO_DIRS)

vuln:
	GOWORK=off go mod verify
	GOWORK=off go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...

secure: fmt-check vet staticcheck gosec vuln

build:
	GOWORK=off go build ./...

check: fmt-check vet staticcheck gosec vuln test build
