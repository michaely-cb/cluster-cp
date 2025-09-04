# Variables
DOCKER_REGISTRY := local
IMAGE_NAME := alert-router
# Generate TAG only once at the start of the make process
TAG := $(shell echo $${TAG:-$$(date +%Y%m%d-%H%M%S)})  # Use existing TAG or generate new one
FULL_IMAGE_NAME := $(IMAGE_NAME):$(TAG)

# Default target
.PHONY: help
help: ## Show this help message
	@echo 'Usage:'
	@echo '  make <target>'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*##"; printf "\033[36m"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-15s %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: all
all: build deploy ## Build and deploy alert-router (default)

# Build Docker image
.PHONY: build
build: ## Build the alert-router Docker image
	make -C .. build TAG=$(TAG)
	make -C .. docker-build

# Deploy alert-router to Kind cluster
.PHONY: deploy
deploy: build ## Deploy alert-router to Kind cluster
	kind load docker-image $(FULL_IMAGE_NAME)
	export ALERT_ROUTER_TAG=$(TAG) && ./deploy.sh

# Deploy mailhog
.PHONY: deploy-mailhog
deploy-mailhog: ## Deploy mailhog to the cluster
	kubectl apply -f ./mailhog.yaml

# Deploy mockserver
.PHONY: deploy-mockserver
deploy-mockserver: ## Deploy mockserver to the cluster
	kubectl apply -f ./mockserver.yaml

# Deploy everything
.PHONY: deploy-all
deploy-all: deploy deploy-mailhog deploy-mockserver ## Deploy all of alert-router, mailhog, and mockserver

# Clean up deployments
.PHONY: clean
clean: ## Remove all deployments
	kubectl delete -f ./test-kind-deploy.yaml || true
	kubectl delete -f ./mailhog.yaml || true
