PROJECT_ROOT=$(GITTOP)/src/cluster_deployment

ECR_URL="171496337684.dkr.ecr.us-west-2.amazonaws.com"
IMAGE_NAME="rocky-dmgr"
IMAGE_TAG="8.9-7"

platform ?= linux/amd64
BASE=docker.io/rockylinux:8.9
BUILDTMP := $(GITTOP)/build

base_image:  ## Pull the rocky-8.9 image, possibly in a docker container if build machine client cannot
	if ! docker pull $(BASE) --platform=$(platform); then \
	  RANDOM_STRING=$(shell date +%s%3N) ;\
	  mkdir -p $(BUILDTMP)/$$RANDOM_STRING ;\
	  docker run --privileged -d --rm --name build-dind-$$RANDOM_STRING \
	  	-v $(DOCKER_CONFIG_PATH):/root/.docker/config.json:ro \
	  	--entrypoint /bin/sh docker.io/docker:20.10-dind -c \
	  	"dockerd-entrypoint.sh && sleep 300" ;\
	  sleep 5 ;\
	  docker exec -t build-dind-$$RANDOM_STRING docker pull $(BASE) --platform=$(platform) ;\
	  docker exec -t build-dind-$$RANDOM_STRING docker save $(BASE) -o /home/rocky.tar ;\
	  docker cp build-dind-$$RANDOM_STRING:/home/rocky.tar $(BUILDTMP)/$$RANDOM_STRING/rocky.tar ;\
	  docker load -i $(BUILDTMP)/$$RANDOM_STRING/rocky.tar ;\
	  docker rm -f build-dind-$$RANDOM_STRING ;\
	  rm -rf $(BUILDTMP)/$$RANDOM_STRING ;\
	fi

.PHONY: local
local: base_image
	cp $(PROJECT_ROOT)/os-build/rocky8-base/roles/rocky8-base/templates/requirements.txt requirements-osbuild.txt
	cp $(PROJECT_ROOT)/deployment/requirements.txt requirements.txt
	docker build . -t $(IMAGE_NAME):$(IMAGE_TAG)
	rm -f requirements*

.PHONY: push
push: local
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(ECR_URL)/$(IMAGE_NAME):$(IMAGE_TAG)
	docker push $(ECR_URL)/$(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: clean
clean:
	docker container ls --format='{{.ID}},{{ .Names }}' | grep dmtest | cut -d, -f1 | xargs -r -n1 docker container kill
	docker container ls -a --format='{{.ID}},{{ .Names }}' | grep dmtest | cut -d, -f1 | xargs -r -n1 docker container rm
	docker network ls --format='{{.ID}},{{ .Name }}' | grep dmtest | cut -d, -f1 | xargs -r -n1 docker network rm
