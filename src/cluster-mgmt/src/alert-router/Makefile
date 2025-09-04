.PHONY: help
help: ## Show this help message
	@echo 'Usage:'
	@echo '  make <target>'
	@echo
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*##"; printf "\033[36m\033[0m"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

# Variables
DOCKER_CONFIG_PATH := $(shell [ -f ~/.docker/config.json ] && realpath ~/.docker/config.json || echo "")
DOCKER_REGISTRY ?= 171496337684.dkr.ecr.us-west-2.amazonaws.com
ECR_DIND=$(DOCKER_REGISTRY)/ecr-public/docker/library/docker:20.10-dind
IMAGE=alert-router
platform ?= linux/amd64
# Define the base image you want to use (here: golang:1.21-bullseye)
BASE_IMAGE ?= golang:1.21-bullseye
# Temporary build directory
BUILDTMP := $(GITTOP)/build

MOUNT_ARG := $(if $(DOCKER_CONFIG_PATH),-v $(DOCKER_CONFIG_PATH):/root/.docker/config.json:ro,)

# Get the git top level directory and the git hash
ifeq ($(GITTOP),)
    GITTOP := $(shell git rev-parse --show-toplevel)
endif
ifndef GITHASH
export GITHASH := $(shell git rev-parse --short=10 HEAD)
endif


.PHONY: build
build: ## Build the alert-router binary
	go mod tidy && go mod vendor
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/alert-router

.PHONY: tidy
tidy: ## Run go mod tidy
	go mod tidy

.PHONY: test
test: tidy ## Run tests
	go test ./...

version: ## Get the current version tag
	$(eval TAG=$(shell git log -1 --format=%H -- $(CURDIR) | cut -c1-10))
	@echo $(TAG)

.PHONY: docker-build docker-push

# The base target attempts to pull the desired BASE_IMAGE.
# If pulling directly fails, it uses a Docker-in-Docker container to pull, save, and load the image locally.
.PHONY: base
base:
	@set -e; \
	if ! docker pull $(BASE_IMAGE) --platform=$(platform); then \
	  RANDOM_STRING=$$(date +%s%3N); \
	  mkdir -p $(BUILDTMP)/$$RANDOM_STRING; \
	  docker run --privileged -d --rm --name build-dind-$$RANDOM_STRING \
	    $(MOUNT_ARG) \
	    --entrypoint /bin/sh $(ECR_DIND) -c \
	    "dockerd-entrypoint.sh && sleep 300"; \
	  sleep 5; \
	  docker exec -t build-dind-$$RANDOM_STRING docker pull $(BASE_IMAGE) --platform=$(platform); \
	  docker exec -t build-dind-$$RANDOM_STRING docker save $(BASE_IMAGE) -o /home/base.tar; \
	  docker cp build-dind-$$RANDOM_STRING:/home/base.tar $(BUILDTMP)/$$RANDOM_STRING/base.tar; \
	  docker load -i $(BUILDTMP)/$$RANDOM_STRING/base.tar; \
	  docker rm -f build-dind-$$RANDOM_STRING; \
	  rm -rf $(BUILDTMP)/$$RANDOM_STRING; \
	fi

# The docker-build target now depends on the 'base' target.
# It passes the BASE_IMAGE build-arg so the Dockerfile uses the pre-pulled image.
# We need to copy (and clean up) the dependency on (pb, job-operator) because we're using D-in-D
.PHONY: docker-build
docker-build: base version
	docker build \
		--platform ${platform} \
		-t ${IMAGE}:${TAG} \
		--build-arg TAG=${TAG} \
		--build-arg BASE_IMAGE=$(BASE_IMAGE) \
		-t ${DOCKER_REGISTRY}/${IMAGE}:${TAG} \
		-f Dockerfile ..

push: docker-build ## Push the Docker image to ECR
	docker push ${DOCKER_REGISTRY}/${IMAGE}:${TAG}

SEMANTIC_VERSION ?= 0.1.0
.PHONY: docker-build-local docker-push-local

docker-build-local: ## Build the Docker image locally
	docker build \
		-t registry.local/alert-router:${SEMANTIC_VERSION} .

docker-push-local: docker-build-local ## Push the Docker image to local registry
	docker push registry.local/alert-router:${SEMANTIC_VERSION}
