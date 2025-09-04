IMAGE=kube-webhook:webhook-20250616-0
DOCKER_REGISTRY?=171496337684.dkr.ecr.us-west-2.amazonaws.com

GITTOP ?= $(shell git rev-parse --show-toplevel)
export GOPATH := $(GITTOP)/.cluster_mgmt_go
export GOCACHE := ${GOPATH}/cache
export GOBIN := ${GOPATH}/bin
export GOFLAGS := -modcacherw
export GOENV ?= $(GITTOP)/flow/go.env

platform ?= linux/amd64

all: docker-build

build:
	go fmt ./...
	go vet ./...
	mkdir -p bin
	go build -o bin/kube-webhook

.PHONY: docker-build
docker-build: build
	docker build . -t registry.local/$(IMAGE) --platform ${platform}
	docker tag registry.local/$(IMAGE) $(DOCKER_REGISTRY)/$(IMAGE)

.PHONY: docker-local-build
docker-local-build: build
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o bin/kube-webhook-linux main.go
	docker build -t registry.local/$(IMAGE) --platform $(platform) -f local.Dockerfile .
	docker tag registry.local/$(IMAGE) $(DOCKER_REGISTRY)/$(IMAGE)

.PHONY: push
push:
	docker push $(DOCKER_REGISTRY)/$(IMAGE)
