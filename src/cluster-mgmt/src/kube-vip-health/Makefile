TAG=vip-20250509-0
DOCKER_REGISTRY ?= 171496337684.dkr.ecr.us-west-2.amazonaws.com

all:
	go fmt ./...
	go vet ./...
	mkdir -p bin
	go build -o bin/kube-vip

.PHONY: docker-build
docker-build:
	docker build . -t registry.local/kube-vip:${TAG}

.PHONY: docker-push
docker-push:
	@echo "Remember to update the image tag in the cli deployment code!"
	docker tag registry.local/kube-vip:${TAG} ${DOCKER_REGISTRY}/kube-vip:${TAG}
	docker push ${DOCKER_REGISTRY}/kube-vip:${TAG}

clean:
	rm -rf kube-vip-health
