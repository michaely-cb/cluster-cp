ifeq ($(GITTOP),)
    GITTOP := $(shell git rev-parse --show-toplevel)
endif
ifndef GITHASH
export GITHASH := $(shell git rev-parse --short=10 HEAD)
endif

PYTHON_MAJOR ?= 3
PYTHON_MINOR ?= 11

DOCKER_CONFIG_PATH=$(shell realpath "$(HOME)/.docker/config.json")
DOCKER_REGISTRY ?= 171496337684.dkr.ecr.us-west-2.amazonaws.com
PYTHON=python:$(PYTHON_MAJOR).$(PYTHON_MINOR)-slim-bookworm
ECR_PYTHON=$(DOCKER_REGISTRY)/ecr-public/docker/library/$(PYTHON)
ECR_BUSYBOX=$(DOCKER_REGISTRY)/busybox
# check whether current host have ecr certs and go with ecr python if possible
ifndef BASE
    BASE:=$(shell docker pull $(ECR_BUSYBOX) > /dev/null 2>&1 && echo $(ECR_PYTHON) || echo $(PYTHON))
endif
platform ?= linux/amd64
BUILDTMP := $(GITTOP)/build
IMAGE=system-client
PLATFORM_SUBMODULE := cm/platform
PLATFORM_SUBMODULE_PATH := $(GITTOP)/$(PLATFORM_SUBMODULE)
PB_DIR = $(CURDIR)/pb
PROTO_DIR = $(CURDIR)/proto
SYSTEM_CLIENT_DIR = $(CURDIR)/system_client
EXPORTER_SYSTEM_CLIENT_DIR = $(GITTOP)/src/cluster_mgmt/src/cluster-mgmt-exporter/system_client
ECR_DIND=$(DOCKER_REGISTRY)/ecr-public/docker/library/docker:20.10-dind

docker-build: base version build-system-client
	docker build --platform ${platform} . -t ${IMAGE}:${TAG} --build-arg BASE=$(BASE) --build-arg TAG=${TAG}
	docker tag ${IMAGE}:${TAG} ${DOCKER_REGISTRY}/${IMAGE}:${TAG}

version: submodule-init
	$(eval PLATFORM_HASH=$(shell git -C $(PLATFORM_SUBMODULE_PATH) log -1 --format=%H -- idl | cut -c1-10))
	$(eval CURRENT_HASH=$(shell git log -1 --format=%H -- $(CURDIR) | cut -c1-10))
	$(eval TAG=$(PLATFORM_HASH)-$(CURRENT_HASH))
	@echo $(TAG)

# workaround with docker version issue
base:
	if ! docker pull $(BASE) --platform=$(platform); then \
	  RANDOM_STRING=$(shell date +%s%3N) ;\
	  mkdir -p $(BUILDTMP)/$$RANDOM_STRING ;\
	  docker run --privileged -d --rm --name build-dind-$$RANDOM_STRING \
        -v $(DOCKER_CONFIG_PATH):/root/.docker/config.json:ro \
        --entrypoint /bin/sh $(ECR_DIND) -c \
        "dockerd-entrypoint.sh && sleep 300" ;\
	  sleep 5 ;\
	  docker exec -t build-dind-$$RANDOM_STRING docker pull $(BASE) --platform=$(platform) ;\
	  docker exec -t build-dind-$$RANDOM_STRING docker save $(BASE) -o /home/python.tar ;\
	  docker cp build-dind-$$RANDOM_STRING:/home/python.tar $(BUILDTMP)/$$RANDOM_STRING/python.tar ;\
	  docker load -i $(BUILDTMP)/$$RANDOM_STRING/python.tar ;\
	  docker rm -f build-dind-$$RANDOM_STRING ;\
	  rm -rf $(BUILDTMP)/$$RANDOM_STRING ;\
	fi

push: docker-build
	docker push ${DOCKER_REGISTRY}/${IMAGE}:${TAG}

venv:
	@if [ ! -d ".venv" ]; then \
		echo "Creating virtual environment"; \
		python -m venv .venv; \
	else \
		echo "Virtual environment already exists"; \
	fi
	@echo "Activating virtual environment and installing dependencies"; \
	. .venv/bin/activate && pip install -q -r requirements.txt

venv-clean:
	@echo "Cleaning up virtual environment"
	@if [ -d ".venv" ]; then \
		rm -rf .venv; \
	fi

test: build-system-client
	@echo "Testing init scripts"
	PYTHONPATH=$(PB_DIR) .venv/bin/pytest -vvv .

submodule-init:
	@if [ -z "$(GITTOP)" ]; then \
		echo "GITTOP is not set. Please set GITTOP to the root of the git repository."; \
		exit 1; \
	fi
	@if git submodule status $(PLATFORM_SUBMODULE_PATH) | grep -q '^-'; then \
		git submodule update --init --remote --quiet $(PLATFORM_SUBMODULE_PATH); \
	fi

build-system-client: venv submodule-init
	@echo "Building protos, make sure you have grpc_tools installed"
	mkdir -p $(PB_DIR) $(PROTO_DIR)/{cb,cli/events,cli/system,schema,cli/diagnostics}
	cp $(GITTOP)/$(PLATFORM_SUBMODULE)/idl/cb/* $(PROTO_DIR)/cb
	cp $(GITTOP)/$(PLATFORM_SUBMODULE)/idl/cli/events/* $(PROTO_DIR)/cli/events
	cp $(GITTOP)/$(PLATFORM_SUBMODULE)/idl/cli/system/*_cs2.proto $(PROTO_DIR)/cli/system
	cp $(GITTOP)/$(PLATFORM_SUBMODULE)/idl/schema/TestResult.proto $(PROTO_DIR)/schema
	cp $(GITTOP)/$(PLATFORM_SUBMODULE)/idl/schema/DiagTest.proto $(PROTO_DIR)/schema
	cp $(GITTOP)/$(PLATFORM_SUBMODULE)/idl/cli/diagnostics/CliDiagnostics.proto $(PROTO_DIR)/cli/diagnostics

	@for file in $(PROTO_DIR)/cli/system/*.proto; do \
		sed -i 's|import "event/Event.proto";|import "cli/events/Event.proto";|g' "$$file"; \
		sed -i 's|\<event\.Event\>|cli.events.Event|g' "$$file"; \
	done

	@mkdir -p $(SYSTEM_CLIENT_DIR) $(SYSTEM_CLIENT_DIR)/proto
	cp $(EXPORTER_SYSTEM_CLIENT_DIR)/proto/* $(SYSTEM_CLIENT_DIR)/proto
	cp $(EXPORTER_SYSTEM_CLIENT_DIR)/port_channel_cache.py $(EXPORTER_SYSTEM_CLIENT_DIR)/tunnel_system_client.py $(SYSTEM_CLIENT_DIR)
	cp $(EXPORTER_SYSTEM_CLIENT_DIR)/../system_exporter_logger.py $(SYSTEM_CLIENT_DIR)
	cp $(CURDIR)/system_maintenance_client.py $(SYSTEM_CLIENT_DIR)/system_client.py
	touch $(SYSTEM_CLIENT_DIR)/__init__.py

	@# workaround by update component field in event proto from enum(10k+ entries) to int causing python grpc memory bloat
	cp system_client/proto/Event.proto $(PROTO_DIR)/cli/events
	cp system_client/proto/CliEvents.proto $(PROTO_DIR)/cli/events

	@mkdir -p $(PB_DIR)
	@. .venv/bin/activate && \
        python -m grpc_tools.protoc --python_out=$(PB_DIR) --mypy_out=$(PB_DIR) \
            --proto_path=$(PROTO_DIR) \
            $(PROTO_DIR)/cb/*.proto && \
        python -m grpc_tools.protoc --python_out=$(PB_DIR) --mypy_out=$(PB_DIR) \
            --grpc_python_out=$(PB_DIR) --mypy_grpc_out=$(PB_DIR) \
            --proto_path=$(PROTO_DIR) \
            $(PROTO_DIR)/cli/events/*.proto && \
        python -m grpc_tools.protoc --python_out=$(PB_DIR) --mypy_out=$(PB_DIR) \
            --grpc_python_out=$(PB_DIR) --mypy_grpc_out=$(PB_DIR) \
            --proto_path=$(PROTO_DIR) \
            $(PROTO_DIR)/cli/system/*.proto && \
        python -m grpc_tools.protoc --python_out=$(PB_DIR) --mypy_out=$(PB_DIR) \
            --grpc_python_out=$(PB_DIR) --mypy_grpc_out=$(PB_DIR) \
            --proto_path=$(PROTO_DIR) \
            $(PROTO_DIR)/schema/*.proto && \
        python -m grpc_tools.protoc --python_out=$(PB_DIR) --mypy_out=$(PB_DIR) \
            --grpc_python_out=$(PB_DIR) --mypy_grpc_out=$(PB_DIR) \
            --proto_path=$(PROTO_DIR) \
            $(PROTO_DIR)/cli/diagnostics/*.proto

clean-artifacts:
	@echo "Cleaning up generated artifacts"
	@rm -rf $(PB_DIR) $(PROTO_DIR) $(SYSTEM_CLIENT_DIR) wheels/

clean: venv-clean clean-artifacts