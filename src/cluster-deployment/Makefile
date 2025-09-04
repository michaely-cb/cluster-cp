GITTOP ?= $(shell git rev-parse --show-toplevel)
CLUSTER_DEPLOY_DIR := $(GITTOP)/src/cluster_deployment

# build scratch area
BUILDTMP := $(GITTOP)/build
CLUSTER_TMP_FILES := $(BUILDTMP)/cluster-deploy-files/files

GITHASH ?= $(shell git rev-parse --short=10 HEAD)
USER ?= $(shell id -un)
TAG ?= $(USER)-$(GITHASH)
PYTHON := "python3.11"

ifndef RELEASE_ID
RELEASE_ID := 0.0.0
endif

CLUSTER_PKGNAME ?= cluster-deploy-$(TAG)
CLUSTER_ARCHIVE ?= $(BUILDTMP)/$(CLUSTER_PKGNAME).tar.gz

DEPLOY_ARTIFACTS_SERVER ?= deploy-artifacts.cerebrassc.local


.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf " \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: config-tool-pkg
config-tool-pkg:  ## Download pip dependencies
	$(PYTHON) -m pip --disable-pip-version-check download -r $(CLUSTER_DEPLOY_DIR)/deployment/requirements.txt -d $(BUILDTMP)/$(CLUSTER_PKGNAME)/packages/deployment
	$(PYTHON) -m pip --disable-pip-version-check download -r $(CLUSTER_DEPLOY_DIR)/os-build/rocky8-base/roles/rocky8-base/templates/requirements.txt -d $(BUILDTMP)/$(CLUSTER_PKGNAME)/packages/os-build
	## We would like to install ansible as a executable in /usr/local/bin,
	## so not adding this package to deployment/requirements.txt
	$(PYTHON) -m pip --disable-pip-version-check download -r $(CLUSTER_DEPLOY_DIR)/deployment/requirements-ansible.txt -d $(BUILDTMP)/$(CLUSTER_PKGNAME)/packages/ansible
	$(PYTHON) -m venv $(BUILDTMP)/$(CLUSTER_PKGNAME)/venv
	$(BUILDTMP)/$(CLUSTER_PKGNAME)/venv/bin/python -m pip install -r $(CLUSTER_DEPLOY_DIR)/deployment/requirements.txt
	$(BUILDTMP)/$(CLUSTER_PKGNAME)/venv/bin/python -m pip install -r $(CLUSTER_DEPLOY_DIR)/os-build/rocky8-base/roles/rocky8-base/templates/requirements.txt

PKGDEPS += version
.PHONY: version
version:
	# generate version.json
	mkdir -p $(BUILDTMP)/$(CLUSTER_PKGNAME)/etc
	printf '{"version": "%s", "buildid": "%s", "githash": "%s"}\n' \
    	"${RELEASE_ID}" \
    	"${TAG}" \
    	"${GITHASH}" | \
	jq . > $(BUILDTMP)/$(CLUSTER_PKGNAME)/etc/version.json


PKGDEPS += installer
.PHONY: installer
installer: version  ## Build the installer
	mkdir -p $(BUILDTMP)/$(CLUSTER_PKGNAME)
	for src in deployment os-provision schema_migrator install.sh; do \
	    cp -r $(CLUSTER_DEPLOY_DIR)/$$src $(BUILDTMP)/$(CLUSTER_PKGNAME); \
	done
	cp -r $(CLUSTER_DEPLOY_DIR)/packages/python311 $(BUILDTMP)/$(CLUSTER_PKGNAME)/packages
	rm -rf $(BUILDTMP)/$(CLUSTER_PKGNAME)/deployment/deployment_manager/tests
	rm -rf $(BUILDTMP)/$(CLUSTER_PKGNAME)/deployment/deployment_manager/network_config/tests
	cd $(BUILDTMP) && tar cfz $(CLUSTER_ARCHIVE) $(CLUSTER_PKGNAME)


.PHONY: package
package: config-tool-pkg installer ## Build the cluster-deployment package tar ball with deploy-mgr installer and python deps

.PHONY: clean_package
clean_package:
	rm -rf $(BUILDTMP)/cluster-deploy-*


.PHONY: clean
clean: clean_package ## Clean cluster-deployment package and other python files
	find ./ -type f -name '*.pyc' -delete
	find ./ -type d -name '__pycache__' -delete


.PHONY: cluster-deploy-bundle-verify
cluster-deploy-bundle-verify: ## verify artifacts in cluster deployment manifest
	$(CLUSTER_DEPLOY_DIR)/artifacts/bundler.py -v verify \
	 $(CLUSTER_DEPLOY_DIR)/artifacts/manifest.yaml


.PHONY: cluster-deploy-bundle
cluster-deploy-bundle: ## bundle artifacts for deployment
	$(CLUSTER_DEPLOY_DIR)/artifacts/bundler.py -v bundle \
	 --release $(RELEASE_ID) \
	 --build $(GITHASH) \
	 --output $(BUILDTMP)/cluster-deploy-bundle.tar \
	 $(CLUSTER_DEPLOY_DIR)/artifacts/manifest.yaml


.PHONY: cluster-os-bundle-verify
cluster-os-bundle-verify: ## verify cluster os manifest
	$(CLUSTER_DEPLOY_DIR)/artifacts/bundler.py -v verify \
	 $(CLUSTER_DEPLOY_DIR)/os-build/manifest.yaml


.PHONY: cluster-os-bundle
cluster-os-bundle: ## bundle cluster os artifacts
	$(CLUSTER_DEPLOY_DIR)/artifacts/bundler.py -v bundle \
	 --release $(RELEASE_ID) \
	 --build $(GITHASH) \
	 --output $(BUILDTMP)/cluster-os-bundle.tar \
	 $(CLUSTER_DEPLOY_DIR)/os-build/manifest.yaml


.PHONY: deploy-files
deploy-files: ## Build file package needed in PB deployment. Deprecated, use deploy-artifacts-bundle
	$(CLUSTER_DEPLOY_DIR)/os-build/get-files.sh $(CLUSTER_TMP_FILES) $(RELEASE_ID) # Download files
	$(CLUSTER_DEPLOY_DIR)/os-build/create-os-tarball.sh # Download OS image
	tar -czf $(CLUSTER_DEPLOY_DIR)/files.tar.gz -C $(CLUSTER_TMP_FILES)/../ files
