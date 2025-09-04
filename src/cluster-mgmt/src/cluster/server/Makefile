export GITHASH ?= $(shell git rev-parse --short=10 HEAD)
export DOCKER_REGISTRY ?= 171496337684.dkr.ecr.us-west-2.amazonaws.com

# Set GOPATH to a local path, so that we don't accidentally pollute the
# default GOPATH that might be shared by platform repo.
GITTOP ?= $(shell git rev-parse --show-toplevel)
export GOPATH := $(GITTOP)/.cluster_mgmt_go/
export GOCACHE := ${GOPATH}/cache
export GOBIN := ${GOPATH}/bin
export GOFLAGS := -modcacherw
export GOENV ?= $(GITTOP)/flow/go.env

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

CLUSTER_REPO ?= cluster-server
CLUSTER_IMG = $(CLUSTER_REPO):$(TAG)
CBCORE_IMG ?= "fake"
KIND_CLUSTER_NAME ?= "cluster-mgmt-e2e"

.PHONY: all 
all: build ## Build all artifacts (alias to build).

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: protos
protos: ## Generate protobuf definitions.
	$(MAKE) -C ../../pb

build: $(shell find . -name "*.go") protos ## Build the cluster server binary.
	go mod tidy
	go fmt ./...
	go vet ./...
ifndef SKIP_BUILD
	go build -o bin/cluster-server --ldflags="\
	  -X 'cerebras.com/cluster/server/pkg.CerebrasVersion=${CEREBRAS_VERSION}' \
	  -X 'cerebras.com/cluster/server/pkg/wsclient.SemanticVersion=${CLUSTER_SEMANTIC_VERSION}'"
endif

platform ?= linux/amd64 ## Change to linux/arm64 if you want to deploy locally on Mac
.PHONY: check-platform
check-platform: ## Detect ARM64 architecture and update build platform if needed.
ifeq (arm64,$(shell uname -m))
	$(eval platform=linux/arm64)
endif

.PHONY: docker-build
docker-build: ## Build docker image for cluster-server. Optionally pass platform=<docker platform> to build against a particular architecture. Defaults to linux/amd-64.
ifndef SKIP_DOCKER_BUILD
	platform=$(platform) CLUSTER_IMG=$(CLUSTER_IMG) $(MAKE) -C ../.. cluster-docker-build
endif

.PHONY: kind-load-img
kind-load-img: ## Load built docker image into kind cluster.
	@echo "Loading image into kind"
	kind load docker-image ${CLUSTER_IMG} --name ${KIND_CLUSTER_NAME}

.PHONY: run
run: ## Run the cluster server locally with user auth disabled.
	DISABLE_USER_AUTH=true go run --ldflags="\
      -X 'cerebras.com/cluster/server/pkg.CerebrasVersion=${CEREBRAS_VERSION}' \
      -X 'cerebras.com/cluster/server/pkg/wsclient.SemanticVersion=${CLUSTER_SEMANTIC_VERSION}'" .

.PHONY: deploy
deploy: check-platform docker-build kind-load-img version ## Deploy the cluster server in kind and set up necessary volumes.
	@echo "Deploying cluster server in kind"
	helm --kubeconfig ../../job-operator/.kindconfig upgrade cluster-server charts --install --create-namespace --namespace job-operator \
	  --set wsjob.image=$(CBCORE_IMG) --set image.repository=$(CLUSTER_REPO) --set image.tag=$(TAG) \
	  --set extraEnvs.ALPINE_KUBECTL_TAG=${ALPINE_KUBECTL_VERSION} \
	  --set extraEnvs.ALPINE_CONTAINERD_TAG=${ALPINE_CONTAINERD_VERSION} \
	  --set extraEnvs.ALPINE_KUBE_USER_AUTH_TAG=${ALPINE_KUBE_USER_AUTH_VERSION} \
	  --set wsjob.localWorkdirRoot=/n1/wsjob/workdir \
	  --set wsjob.tensorStorageRoot=/n1/tensor-storage \
	  --set wsjob.workerCacheRoot=/n0/cache \
	  --set wsjob.workerSharedMemoryRoot=/dev/shm \
	  --set wsjob.cachedCompileRoot=/n1/wsjob/compile_root \
	  --set logExport.path=/n1/log-export \
	  --set debugviz.path=/n1/debug-artifact \
	  --set disableUserAuth=true --wait --timeout 1m \
	  --set skipSystemMaintenanceResourceLimits=true
	@echo "Creating workdir and cached compile and tensor storage volumes for jobs in kind"
	docker exec -t $$(kind get nodes --name ${KIND_CLUSTER_NAME} | grep control-plane) mkdir -p /n1/wsjob/workdir /n1/wsjob/compile_root /n1/log-export /n1/debug-artifact /n1/tensor-storage /n0/cluster-mgmt/workload-manager
	docker exec -t $$(kind get nodes --name ${KIND_CLUSTER_NAME} | grep control-plane) chmod 777 /n1/wsjob/workdir /n1/wsjob/compile_root 
	docker exec -t $$(kind get nodes --name ${KIND_CLUSTER_NAME} | grep control-plane) chmod a+w /n0/cluster-mgmt/workload-manager

ENVTEST_ASSETS_DIR=$(shell pwd)/../../job-operator/testbin
export KUBEBUILDER_ASSETS=$(ENVTEST_ASSETS_DIR)/bin
.PHONY: envtest
envtest: ## Run environment tests from job-operator.
	$(MAKE) -C ../../job-operator envtest

.PHONY: job-operator-deploy
job-operator-deploy:  ## Deploy job-operator in kind.
	$(MAKE) -C ../../job-operator deploy

.PHONY: version
version: alpine-kubectl-version alpine-containerd-version alpine-kube-user-auth-version ## Retrieve and display alpine image versions.

.PHONY: alpine-kubectl-version
alpine-kubectl-version:
	$(eval ALPINE_KUBECTL_VERSION=$(shell make -C ../../job-operator/images alpine-kubectl-version --no-print-directory))
	@echo $(ALPINE_KUBECTL_VERSION)

.PHONY: alpine-containerd-version
alpine-containerd-version:
	$(eval ALPINE_CONTAINERD_VERSION=$(shell make -C ../../job-operator/images alpine-containerd-version --no-print-directory))
	@echo $(ALPINE_CONTAINERD_VERSION)

.PHONY: alpine-kube-user-auth-version
alpine-kube-user-auth-version:
	$(eval ALPINE_KUBE_USER_AUTH_VERSION=$(shell $(MAKE) -C ../../kube-user-auth version --no-print-directory))
	@echo $(ALPINE_KUBE_USER_AUTH_VERSION)

.PHONY: test
test: version ## Run unit tests for cluster server and job operator.
	$(MAKE) -C ../../job-operator build SKIP_BUILD=true
	$(MAKE) build SKIP_BUILD=true
	XDG_CACHE_HOME="/tmp/.cluster-mgmt-test" \
	ALPINE_KUBECTL_TAG=$(ALPINE_KUBECTL_VERSION) \
	ALPINE_CONTAINERD_TAG=$(ALPINE_CONTAINERD_VERSION) \
	ALPINE_KUBE_USER_AUTH_TAG=$(ALPINE_KUBE_USER_AUTH_VERSION) \
	go test -timeout=10m -v ./...

.PHONY: e2e-test
e2e-test: envtest job-operator-deploy deploy ## Run end-to-end tests for cluster server.
# This is for e2e tests in CI.
# Both job operator and cluster server get deployed in KIND, though the cluster
# server deployment was more for sanity check on the build rather than actually
# being used in the e2e tests.
	$(MAKE) run-e2e-test

.PHONY: run-e2e-test
run-e2e-test: version  ## Run end-to-end tests without redeployment.
# This is for e2e tests without re-deploy.
# Before running an e2e-test for first time, run this: make envtest job-operator-deploy deploy
	XDG_CACHE_HOME="/tmp/.cluster-mgmt-test" \
	ALPINE_KUBECTL_TAG=$(ALPINE_KUBECTL_VERSION) \
	ALPINE_CONTAINERD_TAG=$(ALPINE_CONTAINERD_VERSION) \
	ALPINE_KUBE_USER_AUTH_TAG=$(ALPINE_KUBE_USER_AUTH_VERSION) \
	go test -v -parallel=1 -p=1 -tags=e2e ./... -run '$(TEST_NAME)' -timeout 30m -coverprofile cover.out
	@echo "complete cluster server e2e tests"
