VENV_ACTIVATE := venv/bin/activate
PROJECT_ROOT=$(GITTOP)/src/cluster_deployment


.PHONY: help
help: ## Display this help.
	@echo "make build/clean"


$(VENV_ACTIVATE):
	python3 -m venv venv
	. $(VENV_ACTIVATE) && pip install --upgrade "pip>=22.3.1"
	. $(VENV_ACTIVATE) && pip install -r $(PROJECT_ROOT)/os-build/rocky8-base/roles/rocky8-base/templates/requirements.txt
	. $(VENV_ACTIVATE) && pip install pytest

.PHONY: build
build: $(VENV_ACTIVATE)


.PHONY: clean
clean:
	$(RM) -rf venv
	$(RM) -rf requirements.txt
	$(RM) -rf src/*.egg-info
	find ./ -type f -name '*.pyc' -delete
	find ./ -type d -name '__pycache__' -delete
