# Set GOPATH to a local path, so that we don't accidentally pollute the
# default GOPATH that might be shared by platform repo.
GITTOP ?= $(shell git rev-parse --show-toplevel)
export GOPATH := $(GITTOP)/.cluster_mgmt_go
export GOCACHE := ${GOPATH}/cache
export GOBIN := ${GOPATH}/bin
export GOFLAGS := -modcacherw
export GOENV ?= $(GITTOP)/flow/go.env

SHELL := /bin/bash

PROTO_SRC_DIR := cerebras/pb

##@ General

.PHONY: all
all: build

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Setup

.PHONY: prerequisites
prerequisites: ## Install all dependencies
	$(call install-go-tool,$(GOBIN),protoc-gen-go,google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1,v1.28.1)
	$(call install-go-tool,$(GOBIN),protoc-gen-go-grpc,google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0,1.2.0)

.PHONY: clean
clean: ## Remove build resources
	find . -type f -name '*.go' -exec rm -f {} \;
	rm -f docs.md

##@ Build

.PHONY: build
build: prerequisites ## build
	# Remove legacy protos/builds.
	# Can be removed in the future once all build monoliths are cleaned up.
	rm -f $(GITTOP)/$(PROTO_SRC_DIR)/cluster_mgmt/common_config.proto
	rm -f $(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/cluster_mgmt/csadm/api.proto
	rm -f ./workflow/appliance/cluster_mgmt/csadm/*go

	PATH=${GOPATH}/bin:${PATH} protoc\
	 --proto_path=$(GITTOP)\
	 --go_out=. --go_opt=module=$(PROTO_SRC_DIR)\
	 --go-grpc_out=. --go-grpc_opt=module=$(PROTO_SRC_DIR)\
	 $(GITTOP)/$(PROTO_SRC_DIR)/hostio/br_node_config.proto\
	 $(GITTOP)/$(PROTO_SRC_DIR)/ws/internal_precisions_table.proto\
	 $(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/common/*.proto\
	 $(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/cluster_mgmt/*.proto\
	 $(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/cluster_mgmt/csadm/*.proto\
	 $(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/cluster_mgmt/csctl/*.proto\
	 $(GITTOP)/$(PROTO_SRC_DIR)/workflow/appliance/cluster_mgmt/csctl/v1/*.proto


# install-go-tool installs a go executable in a temp directory and copies it back
# to the target directory. We go through this trouble because parallel makes can
# break if they run 'go install' at the same time. This gets around that problem
# by running in a temp directory and copying back the executables. A conflicting
# copy is ignored since realistically the only conflict will come from another
# make running the same command.
# 1: target directory
# 2: output executable
# 3: go pkg url @ version
# 4: target version output by $executable --version
define install-go-tool
@[ -f $(1)/$(2) ] && [[ $$($(1)/$(2) --version | cut -d' ' -f2) == $(4) ]] || { \
set -e ;\
mkdir -p $(1) ;\
TMP_DIR=$$(mktemp -d) ;\
cd $$TMP_DIR ;\
go mod init installer ;\
echo "Downloading $(3)" ;\
GOBIN=$$TMP_DIR/.bin go install $(3) ;\
if ! mv -f $$TMP_DIR/.bin/$(2) $(1) ; then echo 'file copy conflict detected, ignoring' ; fi ;\
rm -rf $$TMP_DIR ;\
}
endef
