SHELL := /bin/bash

GITTOP ?= $(shell git rev-parse --show-toplevel)

# Set GOPATH to a local path, so that we don't accidentally pollute the
# default GOPATH that might be shared by platform repo.
export GOPATH := $(GITTOP)/.cluster_mgmt_go/
export GOCACHE := ${GOPATH}/cache
export GOBIN := ${GOPATH}/bin
export GOENV ?= $(GITTOP)/flow/go.env

##@ General

.PHONY: all
all: build ## Same as build.

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Setup

.PHONY: clean
clean: ## Remove build resources
	rm -rf client/cluster_mgmt client/common client/hostio client/ws client/cerebras
	rm -rf tests/.pytest_cache
	rm -rf run_meta*.json
	rm -rf server/gen
	rm -rf venv


##@ Development
PROTO_SRC_DIR := cerebras/pb
.PHONY: clientpb
clientpb: ## Compile the client protos for local testing on mgmt client purpose
# ln -sf /Users/xxx/ws/monolith/src/framework/appliance_client/src/cerebras/appliance appliance
ifeq (x86_64,$(shell uname -m))
	$(SDK)/bin/protoc -I=$(GITTOP) \
	--python_out=appliance/cluster \
	--plugin=protoc-gen-grpc_python=$(SDK)/bin/grpc_python_plugin \
	--grpc_python_out=appliance/cluster \
	$(GITTOP)/$(PROTO_SRC_DIR)/hostio/br_node_config.proto \
	$(GITTOP)/$(PROTO_SRC_DIR)/ws/internal_precisions_table.proto \
	$(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/common/common_config.proto \
	$(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/cluster_mgmt/cluster.proto
else
	python3 -m venv venv
	. venv/bin/activate && pip install --upgrade "pip>=22.3.1"
	. venv/bin/activate && pip install -Ur requirements.txt
	. venv/bin/activate && python -m grpc_tools.protoc -I=$(GITTOP) \
	--python_out=appliance/cluster --grpc_python_out=appliance/cluster \
	$(GITTOP)/$(PROTO_SRC_DIR)/hostio/br_node_config.proto \
	$(GITTOP)/$(PROTO_SRC_DIR)/ws/internal_precisions_table.proto \
	$(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/common/common_config.proto \
	$(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/cluster_mgmt/cluster.proto
endif

.PHONY: server
server: ## Build the server.
	$(MAKE) -C server build

.PHONY: build
build: server ## Build client and server.

.PHONY: test
test:  ## Run go unit tests.
	$(MAKE) -C server test
