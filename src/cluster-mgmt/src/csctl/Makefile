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

ifndef USER
  USER := $(shell id -un)
endif

ifndef TAG
  TAG:=$(USER)-$(GITHASH)
endif

ifndef RELEASE_ID
  RELEASE_ID := 0.0.0
endif

include $(GITTOP)/flow/appliance/version.mk

.PHONY: all
all: build docs

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Build

DOC_PLUGIN=github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc
.PHONY: deps
deps:  ## install dependencies
	$(MAKE) -C ../pb
	go mod tidy
	go vet
	go fmt
	test -f $(GOBIN)/protoc-gen-doc || \
	 { cd $$(mktemp -d) && go mod init temp && go get $(DOC_PLUGIN) && go install $(DOC_PLUGIN) ; }


.PHONY: build
build: deps ## build csctl
	env GOOS=$(GOOS) GOARCH=$(GOARCH) go build \
	  --ldflags="-X 'cerebras.com/cluster/csctl/pkg.CerebrasVersion=${CEREBRAS_VERSION}' \
	  -X 'cerebras.com/cluster/csctl/pkg.SemanticVersion=${CLUSTER_SEMANTIC_VERSION}'"


.PHONY: clean
clean:  ## clean
	rm -rf csctl 2>/dev/null
	rm -f csctl-reference.md
	$(MAKE) -C ../pb clean

.PHONY: test
test:  ## test
	go test ./...

.PHONY: docs
docs: deps  ## Make markdown reference docs.
	PATH=${GOBIN}:${PATH} protoc -I$(GITTOP)\
	 --doc_opt=csctl-reference.md.gotemplate,csctl-reference.md\
	 --doc_out=. $(GITTOP)/$(PROTO_SRC_DIR)/cluster_mgmt/csctl/v1/resources.proto

##@ Install

.PHONY: install
install: build uninstall  ## install csctl in GOBIN
	cp -f csctl $$(go env GOBIN)

.PHONY: uninstall
uninstall:  ## remove csctl from GOBIN
	rm -rf $$(go env GOBIN)/csctl 2>/dev/null
