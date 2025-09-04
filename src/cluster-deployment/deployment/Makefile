GITTOP ?= $(shell git rev-parse --show-toplevel)
CLITOP := $(GITTOP)/src/cluster_mgmt/src/cli
DOCKER_REGISTRY ?= 171496337684.dkr.ecr.us-west-2.amazonaws.com
GITHASH ?= $(shell git rev-parse --short=10 HEAD)
USER ?= $(shell id -un)
TAG ?= $(USER)-$(GITHASH)
KEEP_CLUSTER ?= False
ifneq ($(MANUAL_KIND_NAME),)
    override USE_CLUSTER := --use-cluster=$(MANUAL_KIND_NAME)
endif
export GOENV ?= $(GITTOP)/flow/go.env

venv: ../os-build/rocky8-base/roles/rocky8-base/templates/requirements.txt requirements.txt
	python3.11 -m venv venv
	. venv/bin/activate && pip install -r $$GITTOP/src/cluster_deployment/os-build/rocky8-base/roles/rocky8-base/templates/requirements.txt
	. venv/bin/activate && pip install -r $$GITTOP/src/cluster_deployment/deployment/requirements.txt
	. venv/bin/activate && pip install pytest docker jinja2 pytest-env
	touch venv

.PHONY: pytest
pytest: venv
	. venv/bin/activate ; pytest --verbose --ignore=deployment_manager/tests_pb3 -olog_cli=true -olog_cli_level=INFO $${FILE:-.}

.PHONY: unittest
unittest: venv
	# Django tests have to be run through manage_db.py to configure the DB
	. venv/bin/activate ; PYTHONPATH=$$(pwd -P) python3.11 deployment_manager/manage_db.py test --exclude-tag container_only deployment_manager/tests_container
	# pytests that pull in django deps
	. venv/bin/activate ; PYTHONPATH=$$(pwd -P) pytest deployment_manager/tests_container

.PHONY: test
test: pytest unittest
	@echo tests completed

.PHONY: pb3-test
pb3-test: dep-kinder  ## Run pb3 integration test
ifeq ($(SKIP_STAGE),)
	# clean up the old packages
	rm -rf $(CLITOP)/cluster-package-* $(CLITOP)/CS1-*.tar.gz $(CLITOP)/Cerebras-patches-*.tar.gz
	WITH_MANIFEST=true CLUSTER=kind CLUSTER_PKGNAME=cluster-package-$(TAG) FROM_K8S_VERSION=1.30.4 K8S_VERSION=1.31.10 $(MAKE) -C $(CLITOP) stage-k8s
	tar cfz $(CLITOP)/cluster-package-$(TAG).tar.gz -C $(CLITOP) ./cluster-package-$(TAG)
	# Create fake system image and security patch tarball
	touch $(CLITOP)/CS1-$(TAG).tar.gz
	touch $(CLITOP)/Cerebras-patches-$(TAG).tar.gz
endif
	PB3_PKG=cluster-package-$(TAG) pytest -olog_cli=true -olog_cli_level=INFO -v deployment_manager/tests_pb3 --keep-cluster=$(KEEP_CLUSTER) $(USE_CLUSTER)

LOCAL_BIN=bin
.PHONY: dep-kinder
dep-kinder:
	if ! [ -f $(LOCAL_BIN)/kinder ] ; then \
	  echo "downloading kinder to $(LOCAL_BIN)/kinder";\
	  $(call go-get-tool,$(LOCAL_BIN)/kinder,k8s.io/kubeadm/kinder@v0.0.0-20240620113417-22a709af4203);\
	fi

.PHONY: clean
clean:
	rm -rf venv
	rm -rf $(CLITOP)/cluster-package-*
	rm -f $(CLITOP)/CS1-*.tar.gz
	rm -f $(CLITOP)/Cerebras-patches-*.tar.gz
	$(MAKE) -C deployment_manager/tests/docker clean

.PHONY: migrations
migrations: venv
	# create django migrations for check-in
	. venv/bin/activate ; PYTHONPATH=$$(pwd -P) python3.11 deployment_manager/manage_db.py makemigrations -v3 --no-input db
	rm -f dm.db

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
