ifndef GITHASH
export GITHASH := $(shell git rev-parse --short=10 HEAD)
endif
export DOCKER_REGISTRY ?= 171496337684.dkr.ecr.us-west-2.amazonaws.com

#
# USER is normally set in the environment via /etc/profile.
# if it is not set, use `id -un` (which is same as `whoami`).
#
ifndef USER
USER := $(shell id -un)
endif

ifndef TAG
TAG := $(USER)-$(GITHASH)
endif

ifndef RELEASE_ID
RELEASE_ID := 0.0.0
endif

include $(GITTOP)/flow/appliance/version.mk
DOCKER_BUILD_ARGS := --build-arg "CEREBRAS_VERSION=$(CEREBRAS_VERSION)" --build-arg "SEMANTIC_VERSION=$(CLUSTER_SEMANTIC_VERSION)"
DOCKER_BUILD_ARGS += --label "net.cerebras.client_version=$(PEP440_VERSION)"
DOCKER_BUILD_ARGS += --label "net.cerebras.cluster_semantic_version=$(CLUSTER_SEMANTIC_VERSION)"

GOLANG_IMG ?= golang:1.21.8
ECR_GOLANG=$(DOCKER_REGISTRY)/ecr-public/docker/library/$(GOLANG_IMG)
BASE_IMAGE:=$(shell docker pull $(ECR_GOLANG) > /dev/null 2>&1 && echo $(ECR_GOLANG) || echo $(GOLANG_IMG))
CLUSTER_CHARTS_ARCHIVE ?= cluster-server-charts-$(TAG).tar.gz
CLUSTER_DOCKER ?= cluster-server-$(TAG).docker
CLUSTER_IMG ?= cluster-server:$(TAG)
OPERATOR_CHARTS_ARCHIVE ?= job-operator-charts-$(TAG).tar.gz
OPERATOR_IMG ?= job-operator:$(TAG)
TOOLS_ARCHIVE ?= cluster-mgmt-tools-$(TAG).tar.gz
VALIDATION_BASE_IMAGE := python:3.7
VALIDATION_IMG ?= cluster-validation-service:$(TAG)

# Set GOPATH to a local path, so that we don't accidentally pollute the
# default GOPATH that might be shared by platform repo.
GITTOP ?= $(shell git rev-parse --show-toplevel)
export GOPATH := ${GITTOP}/.cluster_mgmt_go
export GOCACHE := ${GOPATH}/cache
export GOBIN := ${GOPATH}/bin
export GOENV ?= $(GITTOP)/flow/go.env
GO_BUILD_LOCAL := CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -modcacherw
##@ General

.PHONY: all
all: build

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf " \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: job-operator
job-operator:
	$(MAKE) -C $@

.PHONY: cluster
cluster: job-operator
	$(MAKE) -C $@

.PHONY: csctl
csctl:  ## Build cmd line tool csctl
	$(MAKE) -C $@

.PHONY: kube-user-auth
kube-user-auth: ## Build cmd line tool get-cerebras-token
	$(MAKE) -C $@

.PHONY: build
build: ## Build all binaries in serial order to prevent conflicts in GOPATH.
	$(MAKE) -C kube-user-auth
	$(MAKE) -C job-operator
	$(MAKE) -C cluster
	$(MAKE) -C csctl
	$(MAKE) -C cli
	$(MAKE) -C job-operator/images version
	$(MAKE) -C alert-router build version

.PHONY: clean
clean: ## Cleanup.
	$(MAKE) -C pb clean
	$(MAKE) -C job-operator clean
	$(MAKE) -C cluster clean
	$(MAKE) -C cli clean
	$(MAKE) -C csctl clean
	$(MAKE) -C system-maintenance clean
	rm -f $(TOOLS_ARCHIVE)
	rm -f cluster-server-charts-*.tar.gz job-operator-charts-*.tar.gz

.PHONY: test
test: ## Test.
	$(MAKE) -C job-operator test
	$(MAKE) -C cluster test
	$(MAKE) -C cli test
	$(MAKE) -C csctl test

.PHONY: e2e-test
e2e-test: ## Test e2e.
	$(MAKE) -C job-operator envtest
	$(MAKE) -C job-operator deploy
	$(MAKE) -C cluster/server deploy
	$(MAKE) -C job-operator run-e2e-test
	$(MAKE) -C cluster/server run-e2e-test
	@echo "complete all cluster mgmt e2e tests"

##@ Docker Images
platform ?= linux/amd64

.PHONY: all-builds
all-builds: all-docker-builds all-charts-builds ## Build all docker images and deployment charts.

.PHONY: all-save
all-save: all-docker-save all-charts-save ## Save all docker images and deployment charts.

# note: cluster-server is explictly built after operator since parallel go builds can cause conflicts in GOPATH.
.PHONY: all-docker-builds
all-docker-builds: ## Build all docker images.
	$(MAKE) operator-docker-build
	$(MAKE) cluster-docker-build

.PHONY: all-docker-push
all-docker-push: operator-docker-push cluster-docker-push

.PHONY: all-docker-save
all-docker-save: operator-docker-save cluster-docker-save

.PHONY: all-charts-builds
all-charts-builds: operator-charts-build  ## Build all deployment charts (no need for cluster-server).

.PHONY: all-charts-save
all-charts-save: operator-charts-save cluster-charts-save ## Save all deployment charts

.PHONY: operator-docker-push
operator-docker-push: operator-docker-build
	$(MAKE) -C job-operator docker-push

.PHONY: operator-docker-save
operator-docker-save: operator-docker-build
	$(MAKE) -C job-operator docker-save

.PHONY: operator-charts-build
operator-charts-build:
	$(MAKE) -C job-operator OPERATOR_IMG=custom_registry_url/$(OPERATOR_IMG) build-charts

.PHONY: operator-charts-save
operator-charts-save: operator-charts-build
	mv job-operator/build/config.tar.gz $(OPERATOR_CHARTS_ARCHIVE)

.PHONY: cluster-charts-save
cluster-charts-save:
	tar cfz $(CLUSTER_CHARTS_ARCHIVE) -C cluster/server charts

.PHONY: cluster-docker-save
cluster-docker-save: cluster-docker-build
	set -o pipefail; \
	docker save $(CLUSTER_IMG) | \
	gzip > $(CLUSTER_DOCKER)

.PHONY: operator-docker-build
operator-docker-build: ## Build docker image for job-operator. Optionally pass platform=<docker platform> to build against a particular archticture. Defaults to linux/amd-64.
	$(MAKE) job-operator SKIP_BUILD=true
	docker build -t ${OPERATOR_IMG} --platform ${platform} --build-arg BASE_IMAGE=${BASE_IMAGE} \
		${DOCKER_BUILD_ARGS} -f job-operator/Dockerfile .

.PHONY: operator-docker-build-local
operator-docker-build-local: ## Using golang cross build to reduce image build time for local development.
	$(MAKE) job-operator SKIP_BUILD=true
	cd job-operator && ${GO_BUILD_LOCAL} \
	    --ldflags="-X 'cerebras.com/job-operator/common.CerebrasVersion=${CEREBRAS_VERSION}' \
	    -X 'cerebras.com/job-operator/common.SemanticVersion=${CLUSTER_SEMANTIC_VERSION}'" \
	    -a -o manager-linux main.go
	docker build -t $(OPERATOR_IMG) --platform $(platform) --build-arg BASE_IMAGE=${BASE_IMAGE} \
		${DOCKER_BUILD_ARGS} -f job-operator/local.Dockerfile .
	cd job-operator && mv manager-linux bin

.PHONY: cluster-docker-build
cluster-docker-build: ## Build docker image for cluster server. Optionally pass platform=<docker platform> to build against a particular archticture. Defaults to linux/amd-64.
	$(MAKE) cluster SKIP_BUILD=true
	docker build -t ${CLUSTER_IMG} --platform ${platform} --build-arg BASE_IMAGE=${BASE_IMAGE} \
		${DOCKER_BUILD_ARGS} -f cluster/server/Dockerfile .

.PHONY: cluster-docker-build-local
cluster-docker-build-local: ##  Using golang cross build to reduce image build time for local development.
	$(MAKE) cluster SKIP_BUILD=true
	cd cluster/server && ${GO_BUILD_LOCAL} \
	    --ldflags="-X 'cerebras.com/cluster/server/pkg.CerebrasVersion=${CEREBRAS_VERSION}' \
	    -X 'cerebras.com/cluster/server/pkg/wsclient.SemanticVersion=${CLUSTER_SEMANTIC_VERSION}'" \
	    -a -o cluster_server-linux
	docker build -t ${CLUSTER_IMG} --platform ${platform} --build-arg BASE_IMAGE=${BASE_IMAGE} \
		${DOCKER_BUILD_ARGS} -f cluster/server/local.Dockerfile .
	cd cluster/server && mv cluster_server-linux bin

.PHONY: cluster-validation-service-docker-build
cluster-validation-service-docker-build:
	docker build -t ${VALIDATION_IMG} --platform ${platform} --build-arg BASE_IMAGE=${VALIDATION_BASE_IMAGE} cli/

.PHONY: cluster-docker-push
cluster-docker-push: cluster-docker-build ## Push docker images to registry.
	docker tag $(CLUSTER_IMG) $(DOCKER_REGISTRY)/$(CLUSTER_IMG)
	docker push $(DOCKER_REGISTRY)/$(CLUSTER_IMG)

.PHONY: cluster-docker-tag
cluster-docker-tag:
	docker tag $(CLUSTER_IMG) $(DOCKER_REGISTRY)/$(CLUSTER_IMG)

.PHONY: cluster-docker-pull
cluster-docker-pull:
	docker pull $(DOCKER_REGISTRY)/$(CLUSTER_IMG)
	docker tag $(DOCKER_REGISTRY)/$(CLUSTER_IMG) $(CLUSTER_IMG)

#
# multiple users can stomp on each other when pulling latest,
# so create a user-specific latest tag.
#
.PHONY: cluster-docker-pull-latest
cluster-docker-pull-latest: ## Pull latest docker image, re-tag as <user>-latest.
	docker pull $(DOCKER_REGISTRY)/cluster-server:latest
	docker tag $(DOCKER_REGISTRY)/cluster-server:latest cluster-server:${USER}-latest

.PHONY: cluster-mgmt-tools-archive
cluster-mgmt-tools-archive: csctl kube-user-auth ## Add executable tools for cluster-mgmt to a flat gzipped tarball.
	# csctl is also packaged in the cerebras_appliance wheel so might not be required here.
	tar -czh -C csctl csctl csctl.md csctl-reference.md \
	 -C ../../tools/cluster-admin cluster-volumes.sh cluster-volumes.md \
	 -C ../../src/kube-user-auth get-cerebras-token \
	 > $(TOOLS_ARCHIVE)

.PHONY: cluster-orchestration
cluster-orchestration: all-docker-save all-docker-push
	make -C cli cluster-orchestration
