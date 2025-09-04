ifndef GITHASH
export GITHASH := $(shell git rev-parse --short=10 HEAD)
endif

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

SERVER := sc-r9ra14-s9.cerebrassc.local
PATCHES_ARCHIVE ?= Cerebras-patches-$(TAG).tar.gz
ROCKY8_ARCHIVE ?= rocky8-base-$(TAG).tar.gz
BUNDLE_ARCHIVE ?= deploy-bundle-$(TAG).tar

GITTOP ?= $(shell git rev-parse --show-toplevel)
BUILDTMP ?= $(GITTOP)/build

.PHONY: clean
clean:
	rm -rf $(BUILDTMP)/Cerebras-patches-*
	# rm -rf $(BUILDTMP)/rocky8-base-*
	# rm -rf $(BUILDTMP)/deploy-bundle-*

.PHONY: cluster-patches
cluster-patches:
	mkdir -p $(BUILDTMP)
	curl -s http://$(SERVER)/releases/$(RELEASE_ID)/Cerebras-patches-rel_$(RELEASE_ID).tgz -o $(BUILDTMP)/$(PATCHES_ARCHIVE)

# .PHONY: os-image
# os-image: ## Copy OS image for cluster deployment
# 	mkdir -p $(BUILDTMP)
# 	curl -s http://$(SERVER)/releases/$(RELEASE_ID)/rocky8-base.tar.gz -o $(BUILDTMP)/$(ROCKY8_ARCHIVE)

# .PHONY: deploy-bundle
# deploy-bundle: ## Bundle NIC and switch firmwares needed for cluster deployment
# 	mkdir -p $(BUILDTMP)
# 	python3 $(GITTOP)/src/cluster_deployment/os-build/deploy-manifest/deploy_manifest.py --release $(RELEASE_ID) --output $(BUILDTMP)/$(BUNDLE_ARCHIVE)
