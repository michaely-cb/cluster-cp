# Set GOPATH to a local path, so that we don't accidentally pollute the
# default GOPATH that might be shared by platform repo.
export GITHASH ?= $(shell git rev-parse --short=10 HEAD)

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

PROTO_SRC_DIR := cerebras/pb/workflow/appliance

GITTOP ?= $(shell git rev-parse --show-toplevel)
export GOPATH := ${GITTOP}/.cluster_mgmt_go
export GOCACHE := ${GOPATH}/cache
export GOBIN := ${GOPATH}/bin
export GOFLAGS := -modcacherw
export GOENV ?= $(GITTOP)/flow/go.env

ifndef RELEASE_ID
  RELEASE_ID := 0.0.0
endif
CEREBRAS_VERSION ?= $(RELEASE_ID)+$(GITHASH)

.PHONY: all
all: build
.PHONY: build
build:
	@go fmt ./...
	@go vet ./...
	@env GOOS=$(GOOS) GOARCH=$(GOARCH) go build --ldflags="-X 'cerebras.net/kube-user-auth/pkg.CerebrasVersion=${CEREBRAS_VERSION}'" -o get-cerebras-token main.go

.PHONY: test
test:
	go test ./...

.PHONY: clean
clean:
	rm -f get-cerebras-token


# This else statement for a hardcoded version v1 is a temporary solution
# for when 'go' is either not available or too old of a version (outside of monolith env)
# TODO: Consolidate the version in the makefile and pass it to the binary
.PHONY: version
version:
	@$(MAKE) build > /dev/null 2>&1 && \
	    ./get-cerebras-token version 2>/dev/null | awk '{print $$2}' || \
	    echo "v1"
