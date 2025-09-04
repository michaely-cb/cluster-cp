IMAGE=registry.local/sflow-exporter
SFLOW_EXPORTER_TAG=sflow-exporter-20250406-0

DOCKER_REGISTRY ?= 171496337684.dkr.ecr.us-west-2.amazonaws.com
DOCKER_BIN ?= docker

all: format docker

.PHONY: format
format:
	go fmt ./...

.PHONY: docker
docker:
	$(DOCKER_BIN) build . -f Dockerfile -t ${IMAGE}:${SFLOW_EXPORTER_TAG}

.PHONY: push
push:
	@echo "Remember to update the image tag in the cli deployment code!"
	docker tag ${IMAGE}:${SFLOW_EXPORTER_TAG} ${DOCKER_REGISTRY}/sflow-exporter:${SFLOW_EXPORTER_TAG}
	docker push ${DOCKER_REGISTRY}/sflow-exporter:${SFLOW_EXPORTER_TAG}

.PHONY: test
test: docker
	$(DOCKER_BIN) build test \
		--build-arg FLOW_PROM_EXPORTER_IMAGE=${IMAGE} \
		--build-arg FLOW_PROM_EXPORTER_TAG=${SFLOW_EXPORTER_TAG} \
		-t test-sflow-exporter:${SFLOW_EXPORTER_TAG}
	$(DOCKER_BIN) run -it --rm test-sflow-exporter:${SFLOW_EXPORTER_TAG}

.PHONY: version
version:
	@echo $(SFLOW_EXPORTER_TAG)