# Set GOPATH to a local path, so that we don't accidentally pollute the
# default GOPATH that might be shared by platform repo.
GITTOP ?= $(shell git rev-parse --show-toplevel)
export GOPATH := $(GITTOP)/.cluster_mgmt_go
export GOCACHE := ${GOPATH}/cache
export GOBIN := ${GOPATH}/bin
export GOFLAGS := -modcacherw
export GOENV ?= $(GITTOP)/flow/go.env
PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))

# see flow/host.mk for explanation of these variables
ifeq ($(USER_CACHE_DIR),)
USER_CACHE_DIR := $(HOME)
endif

ifndef GITHASH
export GITHASH := $(shell git rev-parse --short=10 HEAD)
endif

DOCKER_REGISTRY ?= 171496337684.dkr.ecr.us-west-2.amazonaws.com

ifneq ($(VERBOSE),)
    override TEST_FLAG := -v
endif

#
# USER is normally set in the environment via /etc/profile.
# if it is not set, use `id -un` (which is same as `whoami`).
#
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

# Image URL to use all building/pushing image targets
OPERATOR_IMG ?= job-operator:$(TAG)
OPERATOR_DOCKER ?= job-operator-$(TAG).docker
KIND_CLUSTER_NAME ?= "cluster-mgmt-e2e"

# Base images to use against different architectures
OS_ARCH = $(shell go env GOOS)-$(shell go env GOARCH)
GOLANG_IMG ?= golang:1.21.8
ECR_GOLANG=$(DOCKER_REGISTRY)/ecr-public/docker/library/$(GOLANG_IMG)
BASE_IMAGE:=$(shell docker pull $(ECR_GOLANG) > /dev/null 2>&1 && echo $(ECR_GOLANG) || echo $(GOLANG_IMG))

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.30.0


# Setting SHELL to bash allows bash commands to be executed by recipes.
# This is a requirement for 'setup-envtest.sh' in the test target.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

ALPINE_KUBECTL_VERSION       ?= $(shell $(MAKE) -C images alpine-kubectl-version --no-print-directory)
ALPINE_CONTAINERD_VERSION    ?= $(shell $(MAKE) -C images alpine-containerd-version --no-print-directory)
ALPINE_KUBE_USER_AUTH_VERSION?= $(shell $(MAKE) -C ../kube-user-auth version --no-print-directory)

# NOTE: XDG_CACHE_HOME override to prevents ci tests from writing envtest
# state to NFS as it can crash otherwise
TEST_ENV := \
	XDG_CACHE_HOME="/tmp/.cluster-mgmt-test" \
	ALPINE_KUBECTL_TAG=$(ALPINE_KUBECTL_VERSION) \
	ALPINE_CONTAINERD_TAG=$(ALPINE_CONTAINERD_VERSION) \
	ALPINE_KUBE_USER_AUTH_TAG=$(ALPINE_KUBE_USER_AUTH_VERSION)

# TODO: We can parametrize the timeout as well as a followup.
# 10m should be more than enough after breaking up the tests into smaller pieces.
# We should continue to monitor and reduce the timeout where applicable.
# First argument is go build tags.
define go-test
	$(TEST_ENV) \
	go test $(TEST_FLAG) $(1) -timeout=10m ./... -coverprofile cover.out
endef

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: protos
protos:
	$(MAKE) -C ../pb

.PHONY: manifests
manifests: controller-gen protos ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects. Note kubeflow is an excluded folder, since we don't want those resources
	@# ensure deps downloaded as rw
	go install
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd:generateEmbeddedObjectMeta=true webhook paths="{., ./apis/..., ./common/..., ./config/..., ./controllers/..., ./server/...}" output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen protos ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	@# ensure deps downloaded as rw
	go install
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: tidy
tidy: protos ## Run go mod tidy against code.
	go mod tidy

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: protos ## Run go vet against code.
	go vet ./...

.PHONY: test
test: envtest ## Run tests in verbose mod.
	$(call go-test,-tags default)
	cd kubeflow/common; $(call go-test,)

.PHONY: cluster-controller-integ-test
cluster-controller-integ-test: envtest
	# The integration tag could be removed with some more refactor
	$(call go-test,-tags "integration cluster_controller")

.PHONY: resource-controller-integ-test
resource-controller-integ-test: envtest
	$(call go-test,-tags "integration resource_controller")

.PHONY: configmap-generation-integ-test
configmap-generation-integ-test: envtest
	$(call go-test,-tags "integration configmap_gen")

.PHONY: cluster-config-integ-test
cluster-config-integ-test: envtest
	$(call go-test,-tags "integration cluster_cfg")

.PHONY: lease-controller-integ-test
lease-controller-integ-test: envtest
	$(call go-test,-tags "integration lease_controller")

.PHONY: wsjob-controller-integ-test
wsjob-controller-integ-test: envtest
	$(call go-test,-tags "integration wsjob_controller")

.PHONY: monitoring-integ-test
monitoring-integ-test: envtest
	$(call go-test,-tags "integration monitoring")

.PHONY: job-termination-integ-test
job-termination-integ-test: envtest
	$(call go-test,-tags "integration job_termination")

.PHONY: workflow-integ-test
workflow-integ-test: envtest
	$(call go-test,-tags "integration workflow")

.PHONY: redundancy-sessions-integ-test
redundancy-sessions-integ-test: envtest
	$(call go-test,-tags "integration redundancy_sessions")

.PHONY: xlarge-job-integ-test
xlarge-job-integ-test: envtest
	$(call go-test,-tags "integration xlarge_jobs")

.PHONY: nvmf-scheduling-integ-test
nvmf-scheduling-integ-test: envtest
	$(call go-test,-tags "integration nvmf_scheduling")

.PHONY: ax-scheduling-integ-test
ax-scheduling-integ-test: envtest
	$(call go-test,-tags "integration ax_scheduling")

.PHONY: ix-scheduling-integ-test
ix-scheduling-integ-test: envtest
	$(call go-test,-tags "integration ix_scheduling")

.PHONY: scheduler-simulation-build-test
scheduler-simulation-build-test: envtest
	env CB_ISS_BUILD_ONLY=true $(call go-test,-tags "integration scheduler_simulation")

.PHONY: scalability-integ-test
scalability-integ-test: envtest
	$(call go-test,-tags "integration scalability")

.PHONY: server-integ-test
server-integ-test: envtest
	$(call go-test,-tags "integration server")

# When adding a new integration test add it to the list below and to
# monolith/tests/cluster_mgmt/test_002_cluster_mgmt_tests/test_run.py
.PHONY: integ-tests
integ-tests: \
	cluster-controller-integ-test \
	resource-controller-integ-test \
	configmap-generation-integ-test \
	cluster-config-integ-test \
	lease-controller-integ-test \
	wsjob-controller-integ-test \
	monitoring-integ-test \
	job-termination-integ-test \
	workflow-integ-test \
	redundancy-sessions-integ-test \
	xlarge-job-integ-test \
	nvmf-scheduling-integ-test \
	ax-scheduling-integ-test \
	ix-scheduling-integ-test \
	scheduler-simulation-build-test \
	scalability-integ-test \
	server-integ-test
	@echo "All integration tests completed."

# TODO: Add a Make target that exercises all tests in parallel using `make -j16`

.PHONY: e2e-test
e2e-test: test_deps deploy
	$(MAKE) run-e2e-test

.PHONY: run-e2e-test
run-e2e-test: version ## Runs e2e test. Assumes existing kind cluster with CRD deployed.
	$(call go-test,-tags e2e,./...)
	@echo "complete job operator e2e tests"

.PHONY: kind-start
kind-start: kind
	@if [ $(shell kind get clusters | grep ${KIND_CLUSTER_NAME} | wc -l | xargs) -eq 1 ]; then\
		echo "Cluster: ${KIND_CLUSTER_NAME} already exists, skip creation";\
	else\
		echo "Start creating cluster: ${KIND_CLUSTER_NAME}";\
		kind create cluster --name ${KIND_CLUSTER_NAME} --config kind.yaml --image kindest/node:v1.30.4;\
	fi
	kind get kubeconfig --name ${KIND_CLUSTER_NAME} > .kindconfig
	chmod go-r .kindconfig

.PHONY: kind-delete
kind-delete: kind
	@if [ $(shell kind get clusters | grep ${KIND_CLUSTER_NAME} | wc -l | xargs) -eq 1 ]; then\
		kind delete cluster --name ${KIND_CLUSTER_NAME};\
	fi
	rm -f .kindconfig

.PHONY: kind-load-img
kind-load-img: docker-build
	@echo "Loading image into kind"
	kind load docker-image ${OPERATOR_IMG} --name ${KIND_CLUSTER_NAME}

.PHONY: clean
clean:
	rm -rf bin/*
	rm -rf testbin/*
	rm -rf build/*
	rm -f *.out
	rm -f .kindconfig
	$(MAKE) -C ../pb clean

##@ Build

.PHONY: test_deps
test_deps: kubectl envtest
# NOTE: it's important to download test deps during build otherwise when tests
# run in parallel in ci, they can conflict during changes inside GOPATH

.PHONY: build
build: protos test_deps manifests generate fmt vet tidy ## Build manager binary.
	@# downloads kubeflow's modules to cache at build stage rather than test stage
	pushd kubeflow/common && go mod tidy && popd
	go build -o bin/manager --ldflags="\
      -X 'cerebras.com/job-operator/common.CerebrasVersion=${CEREBRAS_VERSION}' \
      -X 'cerebras.com/job-operator/common.SemanticVersion=${CLUSTER_SEMANTIC_VERSION}'" main.go

.PHONY: run
run: manifests generate fmt vet tidy install ## Run a controller from your host.
	go run --ldflags="-X 'cerebras.com/job-operator/common.CerebrasVersion=${CEREBRAS_VERSION}' \
                      -X 'cerebras.com/job-operator/common.SemanticVersion=${CLUSTER_SEMANTIC_VERSION}'" ./main.go --dev=true

.PHONY: docker-build
docker-build: ## Build docker image for job-operator. Optionally pass platform=<docker platform> to build against a particular archticture. Defaults to linux/amd-64.
ifndef SKIP_DOCKER_BUILD
	platform=$(platform) OPERATOR_IMG=$(OPERATOR_IMG) BASE_IMAGE=$(BASE_IMAGE) VERSION=$(TAG) $(MAKE) -C .. operator-docker-build
endif

.PHONY: docker-build-local
docker-build-local: ## Using golang cross build to reduce image build time for local development.
	platform=$(platform) OPERATOR_IMG=$(OPERATOR_IMG) BASE_IMAGE=$(BASE_IMAGE) $(MAKE) -C .. operator-docker-build-local

.PHONY: docker-push
docker-push: ## push images to registry
	docker tag $(OPERATOR_IMG) $(DOCKER_REGISTRY)/$(OPERATOR_IMG)
	docker push $(DOCKER_REGISTRY)/$(OPERATOR_IMG)

.PHONY: docker-tag
docker-tag:
	docker tag $(OPERATOR_IMG) $(DOCKER_REGISTRY)/$(OPERATOR_IMG)

.PHONY: docker-pull
docker-pull:
	docker pull $(DOCKER_REGISTRY)/$(OPERATOR_IMG)
	docker tag $(DOCKER_REGISTRY)/$(OPERATOR_IMG) $(OPERATOR_IMG)

#
# multiple users can stomp on each other when pulling latest,
# so create a user-specific latest tag.
#
.PHONY: docker-pull-latest
docker-pull-latest: ## Pull latest docker image, tag as $USER-latest
	docker pull $(DOCKER_REGISTRY)/job-operator:latest
	docker tag $(DOCKER_REGISTRY)/job-operator:latest job-operator:${USER}-latest

.PHONY: docker-save
docker-save:
	set -o pipefail; \
	docker save $(OPERATOR_IMG) | gzip > $(OPERATOR_DOCKER)

##@ Deployment

ignore-not-found ?= true

.PHONY: install
install: kind-start uninstall load-common-images update-image-tag ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUBECTL) create namespace job-operator --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) apply -k build/config/overlays/local-run --server-side --force-conflicts

.PHONY: uninstall
uninstall: manifests kubectl update-image-tag ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -k build/config/overlays/local-run

.PHONY: build-charts
build-charts:
	mkdir -p build && tar cfz build/config.tar.gz config

.PHONY: multibox-yaml
multibox-yaml: manifests kubectl update-image-tag ## Build job-operator.yaml for multibox cluster deploy
	$(KUBECTL) kustomize build/config/overlays/multibox > build/job-operator.yaml

.PHONY: update-image-tag
update-image-tag: manifests kustomize version ## Build job-operator.yaml for cluster deploy
	mkdir -p build && cp -rf config build/
	cd build/config/manager; \
	kustomize edit set annotation alpine-kubectl-tag:$(ALPINE_KUBECTL_VERSION) alpine-containerd-tag:$(ALPINE_CONTAINERD_VERSION) alpine-kube-user-auth-tag:$(ALPINE_KUBE_USER_AUTH_VERSION); \
	kustomize edit set image controller=${OPERATOR_IMG}

platform ?= linux/amd64 ## Change to linux/arm64 if you want to deploy locally on Mac
.PHONY: check-platform
check-platform:
ifeq (arm64,$(shell uname -m))
	$(eval platform=linux/arm64)
endif

.PHONY: deploy
deploy: check-platform docker-build kind-start kind-load-img undeploy load-common-images ## Deploy controller to the local kind cluster.
	@echo "deploying to local cluster"
	$(KUBECTL) create namespace job-operator &>/dev/null || true
	KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME} envsubst < build/config/samples/cluster.yaml > build/config/samples/.cluster.yaml
	$(KUBECTL) create cm -n job-operator cluster --from-file=clusterConfiguration.yaml=build/config/samples/.cluster.yaml --dry-run=client -oyaml | $(KUBECTL) apply -f-
	$(KUBECTL) apply -k build/config/overlays/local-deploy --server-side --force-conflicts

REGISTRY_URL=registry.local
.PHONY: load-common-images
load-common-images: version ## load common images
	$(MAKE) -C images build platform=$(platform)
	docker tag alpine-kubectl:$(ALPINE_KUBECTL_VERSION) $(REGISTRY_URL)/alpine-kubectl:$(ALPINE_KUBECTL_VERSION)
	kind load docker-image $(REGISTRY_URL)/alpine-kubectl:$(ALPINE_KUBECTL_VERSION) --name ${KIND_CLUSTER_NAME}
	docker tag alpine-containerd:$(ALPINE_CONTAINERD_VERSION) $(REGISTRY_URL)/alpine-containerd:$(ALPINE_CONTAINERD_VERSION)
	kind load docker-image $(REGISTRY_URL)/alpine-containerd:$(ALPINE_CONTAINERD_VERSION) --name ${KIND_CLUSTER_NAME}
	docker tag alpine-kube-user-auth:$(ALPINE_KUBE_USER_AUTH_VERSION) $(REGISTRY_URL)/alpine-kube-user-auth:$(ALPINE_KUBE_USER_AUTH_VERSION)
	kind load docker-image $(REGISTRY_URL)/alpine-kube-user-auth:$(ALPINE_KUBE_USER_AUTH_VERSION) --name ${KIND_CLUSTER_NAME}
	docker pull gcr.io/kubebuilder/kube-rbac-proxy:v0.8.0 --platform=linux/amd64 # this image's arm version has a bug marked as: "Architecture: amd"
	docker tag gcr.io/kubebuilder/kube-rbac-proxy:v0.8.0 $(REGISTRY_URL)/kubebuilder/kube-rbac-proxy:v0.8.0
	kind load docker-image $(REGISTRY_URL)/kubebuilder/kube-rbac-proxy:v0.8.0 --name ${KIND_CLUSTER_NAME}

.PHONY: version
version: alpine-kubectl-version alpine-containerd-version alpine-kube-user-auth-version

.PHONY: alpine-kubectl-version
alpine-kubectl-version:
	$(eval ALPINE_KUBECTL_VERSION=$(shell make -C images alpine-kubectl-version --no-print-directory))
	@echo $(ALPINE_KUBECTL_VERSION)

.PHONY: alpine-containerd-version
alpine-containerd-version:
	$(eval ALPINE_CONTAINERD_VERSION=$(shell make -C images alpine-containerd-version --no-print-directory))
	@echo $(ALPINE_CONTAINERD_VERSION)

.PHONY: alpine-kube-user-auth-version
alpine-kube-user-auth-version:
	$(eval ALPINE_KUBE_USER_AUTH_VERSION=$(shell $(MAKE) -C ../kube-user-auth version --no-print-directory))
	@echo $(ALPINE_KUBE_USER_AUTH_VERSION)

.PHONY: undeploy
undeploy: manifests kubectl update-image-tag ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -k build/config/overlays/local-deploy

CONTROLLER_GEN = $(shell pwd)/bin/controller-gen
.PHONY: controller-gen
controller-gen: ## Download controller-gen locally if necessary.
	$(call go-get-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen@v0.14.0)

# We should upgrade this to v5.0.4, as kubectl v1.30.4 uses v5.0.4 of kustomize. Can be installed locally, but devserver needs IT changes
# This is only used for kustomize edit for now which is low risk
.PHONY: kustomize
kustomize: ## Check kustomize is installed
	@which kustomize || ( \
	  echo "error: required dependency kustomize is not installed. " && \
	  echo "Load the kustomize module before proceeding." && \
	  echo "This should not happen in monolith on a devserver" && \
	  echo "Please install version 4.5.x if running on local machine." && \
	  echo "see: https://github.com/kubernetes-sigs/kustomize/releases/tag/kustomize%2Fv4.5.7 for binaries" &&\
	  false )

.PHONY: kind
kind: ## Check kind is installed
	@which kind || ( \
	  echo "error: required dependency kind is not installed. " && \
	  echo "Load the kind module before proceeding. " && \
	  echo "This should not happen in monolith on a devserver" && \
	  echo "Please install latest version or after 0.17 if running on local machine." && \
	  echo "There is a fix on 0.17 for Mac compatible: https://github.com/kubernetes-sigs/kind/pull/2957." && \
	  echo "see: https://github.com/kubernetes-sigs/kind/releases for install" && \
	  false )

# Set kubeconfig explicitly otherwise parallel runs will change the system
# kubeconfig and break the first test.
KUBECTL = $(shell pwd)/bin/kubectl --kubeconfig=$(shell pwd)/.kindconfig
KUBECTL_VERSION = v1.30.4
.PHONY: kubectl
kubectl: ## Download kubectl locally if necessary.
	@mkdir -p bin
	@if [ -x "bin/kubectl" ]; then \
		VERSION=$$(bin/kubectl version -ojson --client | jq -r '.clientVersion.gitVersion'); \
		if [ "$$VERSION" = "$(KUBECTL_VERSION)" ]; then \
			echo "kubectl is already installed, executable and running $(KUBECTL_VERSION)."; \
			exit 0; \
		fi; \
		rm -f bin/kubectl; \
	fi; \
	echo "Installing kubectl $(KUBECTL_VERSION)"; \
	[ $(shell uname) = Linux ] && curl --retry 5 -Lo bin/kubectl https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/linux/amd64/kubectl; \
	[ $(shell uname) = Darwin ] && curl --retry 5 -Lo bin/kubectl https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/darwin/amd64/kubectl; \
	chmod 755 bin/kubectl; \

ENVTEST = $(shell pwd)/bin/setup-envtest
ENVTEST_ASSETS_DIR=$(USER_CACHE_DIR)/.cache/envtest
export KUBEBUILDER_ASSETS=$(ENVTEST_ASSETS_DIR)/k8s/$(ENVTEST_K8S_VERSION)-$(OS_ARCH)
.PHONY: envtest
envtest: ## Download envtest-setup locally if necessary.
	$(call go-get-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.0.0-20230216140739-c98506dc3b8e)
	$(call cleanup-corrupted-download,$(KUBEBUILDER_ASSETS),etcd kube-apiserver kubectl)
	$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(ENVTEST_ASSETS_DIR) -p overview

# go-get-tool will 'go get' any package $2 and install it to $1.
# If the tool exists and version's match, it will not reinstall.
#
# NOTE:
# 1.18 changes the default behavior of `go get` - you must now use go install
# to create binaries. Additionally, golang.org/x/sys required a new patch
# version at the time of writing. Finally, go install does not work with modules
# that have replace directives (kustomize) so you need to strip the version and
# it will still install the correct version from the mod file.
# see: https://groups.google.com/g/golang-nuts/c/igwFOH-fWqI
define go-get-tool
MODULE_VERSION=$$(echo $(2) | cut -f2 -d'@'); \
[ -f $(1) ] && go version -m $(1) | awk '$$1=="mod"' | grep -wq "$$MODULE_VERSION" || { \
set -e ;\
TMP_DIR=$$(mktemp -d) ;\
cd $$TMP_DIR ;\
go mod init tmp ;\
echo "Downloading $(2)" ;\
go get $(2) ;\
GOBIN=$(PROJECT_DIR)/bin go install $$(echo $(2) | cut -f1 -d'@') ;\
rm -rf $$TMP_DIR;\
}
endef

# cleanup-corrupted-download will check if a folder $1 exists and all files of $2 exist in folder,
# and if not all files exist in folder, it will delete the folder to allow triggering a new download.
# Used for cases where downlaods don't check for existence of all files when folder exists.
# e.g. setup-envtest of  v0.0.0-20230216140739-c98506dc3b8e, which is resolved in [later versions](https://pkg.go.dev/sigs.k8s.io/controller-runtime/tools/setup-envtest?tab=versions) but require go 1.24+
define cleanup-corrupted-download
if [ -d "$(1)" ]; then \
  missing=0; \
  for file in $(2); do \
	if [ ! -f "$(1)/$$file" ]; then \
	  echo "Missing file: $(1)/$$file"; \
	  missing=1; \
	fi; \
  done; \
  if [ $$missing -eq 1 ]; then \
	echo "Add permission u+w and deleting $(1)"; \
	chmod u+w "$(1)"; \
	rm -rf "$(1)"; \
  fi; \
fi
endef