REGISTRY_URL=registry.local
ALPINE=alpine:3.16.2
ECR_ALPINE=$(DOCKER_REGISTRY)/ecr-public/docker/library/$(ALPINE)
ifndef COMMON_BASE
    COMMON_BASE:=$(shell docker pull $(ECR_ALPINE) > /dev/null 2>&1 && echo $(ECR_ALPINE) || echo $(ALPINE))
endif
platform ?= linux/amd64 ## Change to linux/arm64 if you want to deploy locally on Mac

.PHONY: version
version: alpine-kubectl-version alpine-containerd-version alpine-kube-user-auth-version

.PHONY: build
build: alpine-kubectl-build alpine-containerd-build alpine-kube-user-auth-build

.PHONY: alpine-kubectl-build
alpine-kubectl-build: alpine-kubectl-version
	docker build . -t alpine-kubectl:$(ALPINE_KUBECTL_VERSION) --platform ${platform} --build-arg BASE=$(COMMON_BASE) -f Dockerfile.kubectl
	docker tag alpine-kubectl:$(ALPINE_KUBECTL_VERSION) $(REGISTRY_URL)/alpine-kubectl:$(ALPINE_KUBECTL_VERSION)

.PHONY: alpine-containerd-build
alpine-containerd-build: alpine-containerd-version
	# https://github.com/Cerebras/monolith/pull/82295
	@TEMP_DIR=$$(mktemp -d -t dockerbuild-XXXXXX); \
	cp -LR kaniko-utils $$TEMP_DIR/; \
	cp -LR Dockerfile.containerd $$TEMP_DIR/; \
	trap 'rm -rf $$TEMP_DIR' EXIT; \
	docker build $$TEMP_DIR/ -t alpine-containerd:$(ALPINE_CONTAINERD_VERSION) --platform ${platform} --build-arg BASE=$(COMMON_BASE) -f $$TEMP_DIR/Dockerfile.containerd
	docker tag alpine-containerd:$(ALPINE_CONTAINERD_VERSION) $(REGISTRY_URL)/alpine-containerd:$(ALPINE_CONTAINERD_VERSION)

.PHONY: alpine-kube-user-auth-build
alpine-kube-user-auth-build: alpine-kube-user-auth-version
	@TEMP_DIR=$$(mktemp -d -t auth-build-XXXXXX); \
	trap 'rm -rf $$TEMP_DIR' EXIT; \
	$(MAKE) -C ../../ kube-user-auth && \
	cp ../../kube-user-auth/get-cerebras-token $$TEMP_DIR/ && \
	cp Dockerfile.kube-user-auth $$TEMP_DIR/ && \
	docker build $$TEMP_DIR -t alpine-kube-user-auth:$(ALPINE_KUBE_USER_AUTH_VERSION) --platform ${platform} \
	     --build-arg BASE=$(COMMON_BASE) -f $$TEMP_DIR/Dockerfile.kube-user-auth && \
	docker tag alpine-kube-user-auth:$(ALPINE_KUBE_USER_AUTH_VERSION) $(REGISTRY_URL)/alpine-kube-user-auth:$(ALPINE_KUBE_USER_AUTH_VERSION)

.PHONY: alpine-kubectl-version
alpine-kubectl-version:
	$(eval ALPINE_KUBECTL_VERSION=$(shell bash version.sh Dockerfile.kubectl alpine-kubectl))
	@echo $(ALPINE_KUBECTL_VERSION)

.PHONY: alpine-containerd-version
alpine-containerd-version:
	$(eval ALPINE_CONTAINERD_VERSION=$(shell bash version.sh . alpine-containerd $(shell echo kaniko-utils/*.sh) Dockerfile.containerd ))
	@echo $(ALPINE_CONTAINERD_VERSION)

.PHONY: alpine-kube-user-auth-version
alpine-kube-user-auth-version:
	$(eval ALPINE_KUBE_USER_AUTH_VERSION=$(shell $(MAKE) -C ../../kube-user-auth version --no-print-directory))
	@echo $(ALPINE_KUBE_USER_AUTH_VERSION)

.PHONY: test
test:
	bash test_version.sh
