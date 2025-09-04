GITTOP ?= $(shell git rev-parse --show-toplevel)
CLITOP := $(GITTOP)/src/cluster_mgmt/src/cli
DOCKER_REGISTRY ?= 171496337684.dkr.ecr.us-west-2.amazonaws.com

# build scratch area
BUILDTMP := $(GITTOP)/build
LOCAL_BIN=bin

GITHASH ?= $(shell git rev-parse --short=10 HEAD)
USER ?= $(shell id -un)
TAG ?= $(USER)-$(GITHASH)
CLUSTER_PKGNAME ?= cluster-pkg-$(TAG)
CLUSTER_ARCHIVE ?= $(BUILDTMP)/$(CLUSTER_PKGNAME).tar.gz
KEEP_KIND ?= False
K8S_VERSION ?= 1.29.0
CLUSTER_ORCHESTRATION_PKGNAME ?= cluster-orchestration-$(TAG)
CLUSTER_ORCHESTRATION_ARCHIVE ?= $(BUILDTMP)/$(CLUSTER_ORCHESTRATION_PKGNAME).tar.gz

cluster-package: CLUSTER_PKGNAME = cluster-package-$(TAG)
cluster-package: CLUSTER_ARCHIVE = $(BUILDTMP)/$(CLUSTER_PKGNAME).tar.gz

export GOENV ?= $(GITTOP)/flow/go.env

# this can be overridden on `make` command line.
ifeq ($(CBCORE_URL),)
    override CBCORE_URL := "171496337684.dkr.ecr.us-west-2.amazonaws.com/cbcore:latest"
endif
ifneq ($(MANUAL_KIND_NAME),)
    override USE_CLUSTER := --use-cluster=$(MANUAL_KIND_NAME)
endif

##@ General

all: build

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf " \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

build: dep-kind ## Build only downloads dependencies
	$(MAKE) -C apps/prometheus/jsonnet

.PHONY: clean
clean: clean-dep-kind
	$(MAKE) -C apps/prometheus/jsonnet clean
	rm -rf cluster-pkg* cluster-package* cluster-orchestration*

.PHONY: clean-dep-kind
clean-dep-kind:
	find ./tests -type d -name .pytest_cache -exec rm -rf {} +
	find ./clusters -maxdepth 2 -type d -name build -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
	kind get clusters 2>/dev/null | grep pytest | xargs -I {} sh -c 'if [ -d "$(CLITOP)/clusters/$$1" ]; then kind delete cluster --name "$$1"; fi' -- {}
	docker ps -a | grep pytest | grep usernode | awk '{print $$1}' | xargs -r -n1 docker rm -f
	rm -rf ./clusters/pytest-*
	rm -f ~/.cs_cluster.conf .cs_cluster.conf
	rm -rf build/*
	rm -rf bin/*
	rm -rf cluster-pkg*
	rm -rf venv

##@ Tests

.PHONY: docker-kind-node-build
docker-kind-node-build: ## Build kind node image.
	docker build -t cerebras-kind-node:local -f apps/kind/kind-node.Dockerfile apps/kind --build-arg "K8S_VERSION=$(K8S_VERSION)"

.PHONY: docker-kind-node-push
docker-kind-node-push: docker-kind-node-build## Push kind node image to registry. Specify the version first, then change the tag version by incrementing the correct version number. This should only be run when there is a change to the kind node image!
	IMAGE=$(DOCKER_REGISTRY)/cerebras-kind-node:v1.29.0-5 &&\
	  docker tag cerebras-kind-node:local $$IMAGE &&\
	  docker push $$IMAGE &&\
	  echo "$$IMAGE"

.PHONY: test
test:  ## Run unit tests.
	python -m pytest tests --ignore=tests/integration --ignore=tests/installation --ignore=tests/cli
	$(MAKE) cli-test

.PHONY: cli-test
cli-test:
	@if [ ! -d $(VENV_DIR) ] ; then \
	  python3 -m venv $(VENV_DIR) ; \
	fi
	@. $(VENV_ACTIVATE) && pip install -r apps/csadm/csadm/requirements.txt
	@. $(VENV_ACTIVATE) && python -m pytest -vvv tests/cli/csadm

.PHONY: e2e-test
e2e-test: dep-kind  ## Run e2e tests. Set envvars CBCORE_IMAGE, CLUSTER_SERVER_IMAGE, JOB_OPERATOR_IMAGE to avoid building images at runtime.
	if [ $$(uname -m) = arm64 ]; then \
		docker build -t cerebras-kind-node:local -f apps/kind/kind-node.Dockerfile apps/kind --build-arg "K8S_VERSION=$(K8S_VERSION)" && \
		KIND_NODE_IMAGE=cerebras-kind-node:local pytest --capture=no --log-cli-level=INFO -vvv tests/integration --keep-cluster=$(KEEP_KIND) $(USE_CLUSTER);\
	else \
		pytest --capture=no --log-cli-level=INFO -vvv tests/integration --keep-cluster=$(KEEP_KIND) $(USE_CLUSTER);\
	fi

.PHONY: e2e-install-test
e2e-install-test: dep-kinder  ## Run e2e install tests.
	if [ $$(uname -m) = arm64 ]; then \
		docker build -t cerebras-kind-node:local -f apps/kind/kind-node.Dockerfile apps/kind --build-arg "K8S_VERSION=$(K8S_VERSION)" && \
		KIND_NODE_IMAGE=cerebras-kind-node:local pytest -vvv tests/installation/test_e2e_install.py --keep-cluster=$(KEEP_KIND) $(USE_CLUSTER);\
	else \
		pytest -vvv tests/installation/test_e2e_install.py --keep-cluster=$(KEEP_KIND) $(USE_CLUSTER);\
	fi

.PHONY: log-rotation-test
log-rotation-test:
	in_smoke_test=true bash apps/log-rotation/logs/main.sh
	in_smoke_test=true bash apps/log-rotation/registry/main.sh
	in_smoke_test=true bash apps/log-rotation/check-cleanup-history.sh --compile-dir cs_16697915076986806430 | grep -q delete
	in_smoke_test=true bash apps/log-rotation/check-cleanup-history.sh --cbcore-tag build-2146687ed4 | grep -q keep
	in_smoke_test=true bash apps/log-rotation/check-cleanup-history.sh --custom-worker-tag build-2146687ed4-af9c37bd1cf81728ec5d2cb6b87f8f7c | grep -q keep
	in_smoke_test=true bash apps/log-rotation/check-cleanup-history.sh --cluster-server-tag build-17d8f836fc | grep -q keep
	in_smoke_test=true bash apps/log-rotation/check-cleanup-history.sh --job-operator-tag build-9db030bca7 | grep -q keep

VENV_DIR := venv
VENV_ACTIVATE := $(VENV_DIR)/bin/activate

##@ Dependencies

.PHONY: dep-kinder
dep-kinder:
	if ! [ -f $(LOCAL_BIN)/kinder ] ; then \
	  echo "downloading kinder to $(LOCAL_BIN)/kinder";\
	  $(call go-get-tool,$(LOCAL_BIN)/kinder,k8s.io/kubeadm/kinder@v0.0.0-20240620113417-22a709af4203);\
	fi

.PHONY: dep-kind
dep-kind: ## Check kind is installed
ifeq (x86_64,$(shell uname -m))
	@which kind || ( \
	  echo "error: required dependency kind is not installed. " && \
	  echo "Load the kind module before proceeding. " && \
	  echo "This should not happen in monolith on a devserver" && \
	  echo "Please install version 14.x if running on local machine." && \
	  echo "see: https://github.com/kubernetes-sigs/kind/releases" && \
	  false )
else
	@if [ ! -d $(VENV_DIR) ] ; then \
	  python3 -m venv $(VENV_DIR) ; \
	fi
	@. $(VENV_ACTIVATE) && pip install --upgrade "pip>=22.3.1"
	@. $(VENV_ACTIVATE) && pip install -r requirements.txt
	@. $(VENV_ACTIVATE) && pip freeze > $(VENV_DIR)/.installed-requirements
	@echo "WARNING: again, venv is not required any more. Keeping this for local testing."
	if ! which kind 2>/dev/null && ! [ -f $(VENV_DIR)/bin/kind ] ; then \
	  echo "downloading kind to $(VENV_DIR)/bin/kind" ;\
	  [ $$(uname) = Darwin ] && [ $$(uname -m) = x86_64 ] && curl --retry 5 -Lo ./kind https://kind.sigs.k8s.io/dl/v0.14.0/kind-darwin-amd64 ;\
	  [ $$(uname) = Darwin ] && [ $$(uname -m) = arm64 ] && curl --retry 5 -Lo ./kind https://kind.sigs.k8s.io/dl/v0.14.0/kind-darwin-arm64 ;\
	  chmod +x ./kind ;\
	  mv ./kind $(VENV_DIR)/bin ;\
	fi
endif

PKGDEPS += stage-cilium
.PHONY: stage-cilium
stage-cilium:
	python3 cs_cluster.py --cluster=generic-multibox package cilium \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-ceph
.PHONY: stage-ceph
stage-ceph:
	python3 cs_cluster.py --cluster=generic-multibox package ceph \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-registry-sync
.PHONY: stage-registry-sync
stage-registry-sync:
	python3 cs_cluster.py --cluster=generic-multibox package registry-sync \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-cluster-server
.PHONY: stage-cluster-server
stage-cluster-server:
	python3 cs_cluster.py --cluster=generic-multibox package cluster-server \
		--pkg-path=$(CLUSTER_PKGNAME) \
		--cbcore $(GITTOP)/build/cbcore-$(TAG).docker \
		--image-file $(GITTOP)/src/cluster_mgmt/src/cluster-server-$(TAG).docker \
		$(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-debugviz-server
.PHONY: stage-debugviz-server
stage-debugviz-server:
	python3 cs_cluster.py --cluster=generic-multibox package debugviz-server \
		--pkg-path=$(CLUSTER_PKGNAME) \
		$(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-job-operator
.PHONY: stage-job-operator
stage-job-operator:
	python3 cs_cluster.py --cluster=generic-multibox package job-operator \
		--pkg-path=$(CLUSTER_PKGNAME) \
		--image-file $(GITTOP)/src/cluster_mgmt/src/job-operator/job-operator-$(TAG).docker \
		$(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-cluster-tools
.PHONY: stage-cluster-tools
stage-cluster-tools:
	python3 cs_cluster.py --cluster=generic-multibox package cluster-tools \
		--pkg-path=$(CLUSTER_PKGNAME) \
		--tools-tar-file $(GITTOP)/src/cluster_mgmt/src/cluster-mgmt-tools-$(TAG).tar.gz \
		$(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-common-images
stage-common-images:
	python3 cs_cluster.py --cluster=generic-multibox package common-images \
		--pkg-path=$(CLUSTER_PKGNAME) \
		$(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-binary-deps
.PHONY: stage-binary-deps
stage-binary-deps:
	python3 cs_cluster.py --cluster=generic-multibox package binary-deps \
		--pkg-path=$(CLUSTER_PKGNAME) \
		$(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-log-rotation
.PHONY: stage-log-rotation
stage-log-rotation:
	python3 cs_cluster.py --cluster=generic-multibox package log-rotation \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-multus
.PHONY: stage-multus
stage-multus:
	python3 cs_cluster.py --cluster=generic-multibox package multus \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-nginx
.PHONY: stage-nginx
stage-nginx:
	python3 cs_cluster.py --cluster=generic-multibox package nginx \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-nvme-of
.PHONY: stage-nvme-of
stage-nvme-of:
	python3 cs_cluster.py --cluster=generic-multibox package nvme-of \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-registry
.PHONY: stage-registry
stage-registry:
	python3 cs_cluster.py --cluster=generic-multibox package registry \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-kube-vip
.PHONY: stage-kube-vip
stage-kube-vip:
	python3 cs_cluster.py --cluster=generic-multibox package kube-vip \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-prometheus
.PHONY: stage-prometheus
stage-prometheus:
	python3 cs_cluster.py --cluster=generic-multibox package prometheus \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-cpingmesh
.PHONY: stage-cpingmesh
stage-cpingmesh:
	python3 cs_cluster.py --cluster=generic-multibox package cpingmesh \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-log-scraping
.PHONY: stage-log-scraping
stage-log-scraping:
	python3 cs_cluster.py --cluster=generic-multibox package log-scraping \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-alert-router
.PHONY: stage-alert-router
stage-alert-router:
	python3 cs_cluster.py --cluster=generic-multibox package alert-router \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

# SW-109779
# We will revive kafka when the adoption plan is clear
# PKGDEPS += stage-kafka
# .PHONY: stage-kafka
# stage-kafka:
# 	python3 cs_cluster.py --cluster=generic-multibox package kafka \
# 		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-kube-webhook
.PHONY: stage-kube-webhook
stage-kube-webhook:
	python3 cs_cluster.py --cluster=generic-multibox package kube-webhook \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-cbcore
.PHONY: stage-cbcore
stage-cbcore:
	python3 cs_cluster.py --cluster=generic-multibox package cbcore \
		--pkg-path=$(CLUSTER_PKGNAME) \
		--image-file ${GITTOP}/build/cbcore-$(TAG).docker \
		$(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-usernode
.PHONY: stage-usernode
stage-usernode:
	python3 cs_cluster.py --cluster=generic-multibox package usernode \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-k8s
.PHONY: stage-k8s
stage-k8s:
	python3 cs_cluster.py $(if $(CLUSTER),--cluster=$(CLUSTER),--cluster=generic-multibox) package k8s \
		$(if $(K8S_VERSION),--k8s-version=$(K8S_VERSION),) \
		$(if $(FROM_K8S_VERSION),--from-k8s-version=$(FROM_K8S_VERSION),) \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-cerebras-internal-setup
.PHONY: stage-cerebras-internal-setup
stage-cerebras-internal-setup:
	python3 cs_cluster.py --cluster=generic-multibox package cerebras-internal-setup \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

PKGDEPS += stage-rdma-device-plugin
.PHONY: stage-rdma-device-plugin
stage-rdma-device-plugin:
	python3 cs_cluster.py --cluster=generic-multibox package rdma-device-plugin \
		--pkg-path=$(CLUSTER_PKGNAME) $(if $(WITH_MANIFEST),,--without-manifest)

# PKGDEPS is to make use of the parallelism of make
# todo: update package all to run in parallel directly to avoid these manual setup
.PHONY: package
package: $(PKGDEPS)
	mkdir -p $(BUILDTMP)
	python3 cs_cluster.py --cluster=generic-multibox package all \
		--pkg-path=$(CLUSTER_PKGNAME) --pack-only
	tar cfz $(CLUSTER_ARCHIVE) $(CLUSTER_PKGNAME)

ORCHESTRATION_COMPONENTS += cluster-tools
ORCHESTRATION_PKGDEPS += stage-orchestration-cluster-tools
.PHONY: stage-orchestration-cluster-tools
stage-orchestration-cluster-tools:
	python3 cs_cluster.py --cluster=generic-multibox package cluster-tools \
		--pkg-path=$(CLUSTER_ORCHESTRATION_PKGNAME) \
		--without-manifest

ORCHESTRATION_COMPONENTS += common-images
ORCHESTRATION_PKGDEPS += stage-orchestration-common-images
stage-orchestration-common-images:
	python3 cs_cluster.py --cluster=generic-multibox package common-images \
		--pkg-path=$(CLUSTER_ORCHESTRATION_PKGNAME) \
		--without-manifest

ORCHESTRATION_COMPONENTS += cerebras-internal-setup
ORCHESTRATION_PKGDEPS += stage-orchestration-cerebras-internal-setup
.PHONY: stage-orchestration-cerebras-internal-setup
stage-orchestration-cerebras-internal-setup:
	python3 cs_cluster.py --cluster=generic-multibox package cerebras-internal-setup \
		--pkg-path=$(CLUSTER_ORCHESTRATION_PKGNAME) --without-manifest

ORCHESTRATION_COMPONENTS += cluster-server
ORCHESTRATION_PKGDEPS += stage-orchestration-cluster-server
.PHONY: stage-orchestration-cluster-server
stage-orchestration-cluster-server:
	python3 cs_cluster.py --cluster=generic-multibox package cluster-server \
		--pkg-path=$(CLUSTER_ORCHESTRATION_PKGNAME) \
		--image-file $(GITTOP)/src/cluster_mgmt/src/cluster-server-$(TAG).docker \
		--without-manifest

ORCHESTRATION_COMPONENTS += job-operator
ORCHESTRATION_PKGDEPS += stage-orchestration-job-operator
.PHONY: stage-orchestration-job-operator
stage-orchestration-job-operator:
	python3 cs_cluster.py --cluster=generic-multibox package job-operator \
		--pkg-path=$(CLUSTER_ORCHESTRATION_PKGNAME) \
		--image-file $(GITTOP)/src/cluster_mgmt/src/job-operator/job-operator-$(TAG).docker \
		--without-manifest
comma := ,
space := $(empty) $(empty)
empty :=

.PHONY: cluster-orchestration
cluster-orchestration: $(ORCHESTRATION_PKGDEPS)
	mkdir -p $(BUILDTMP)
	python3 cs_cluster.py --cluster=generic-multibox package all \
		--components=$(subst $(space),$(comma),$(strip $(ORCHESTRATION_COMPONENTS))) \
		--pkg-path=$(CLUSTER_ORCHESTRATION_PKGNAME) --pack-only
	tar cfz $(CLUSTER_ORCHESTRATION_ARCHIVE) $(CLUSTER_ORCHESTRATION_PKGNAME)


.PHONY: stage-package-cluster-server
stage-package-cluster-server:
	python3 cs_cluster.py --cluster=generic-multibox package cluster-server \
		--pkg-path=$(CLUSTER_PKGNAME) \
		--image-file $(GITTOP)/src/cluster_mgmt/src/cluster-server-$(TAG).docker \
		--without-manifest

PKGDEPS += stage-package-usernode
.PHONY: stage-package-usernode
stage-package-usernode:
	python3 cs_cluster.py --cluster=generic-multibox package usernode \
        --pkg-path=$(CLUSTER_PKGNAME) --config-only $(if $(WITH_MANIFEST),,--without-manifest)

# This is the new cluster-package make target for generating a package for cluster releases separating
# from the ML releases. We don't include cbcore in this package.
CLUSTER_PACKAGE_DEPS := $(filter-out stage-cbcore stage-cluster-server stage-usernode, $(PKGDEPS))
CLUSTER_PACKAGE_DEPS += stage-package-cluster-server
CLUSTER_PACKAGE_DEPS += stage-package-usernode
.PHONY: cluster-package
cluster-package: $(CLUSTER_PACKAGE_DEPS)
	mkdir -p $(BUILDTMP)
	python3 cs_cluster.py --cluster=generic-multibox package all --skip-components=cbcore \
		--pkg-path=$(CLUSTER_PKGNAME) --pack-only
	tar cfz $(CLUSTER_ARCHIVE) $(CLUSTER_PKGNAME)

# go-get-tool will 'go get' any package $2 and install it to $1.
#
# NOTE:
# 1.18 changes the default behavior of `go get` - you must now use go install
# to create binaries. Additionally, golang.org/x/sys required a new patch
# version at the time of writing. Finally, go install does not work with modules
# that have replace directives (kustomize) so you need to strip the version and
# it will still install the correct version from the mod file.
# see: https://groups.google.com/g/golang-nuts/c/igwFOH-fWqI
PROJECT_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
define go-get-tool
[ -f $(1) ] || { \
set -e ;\
TMP_DIR=$$(mktemp -d) ;\
cd $$TMP_DIR ;\
go mod init tmp ;\
echo "Downloading $(2)" ;\
go get $(2) ;\
GOBIN=$(PROJECT_DIR)/bin go install $$(echo $(2) | cut -f1 -d'@') ;\
rm -rf $$TMP_DIR ;\
}
endef
