MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MAKEFILE_DIR := $(dir $(MAKEFILE_PATH))
MAKEFILE_DIR := $(shell echo $(MAKEFILE_DIR) | sed 's/\/$$//')
CBMON_VENV := $(MAKEFILE_DIR)/cbmon_venv
ADDITIONAL_PYTEST_OPTS ?=
SHELL := '/bin/bash'

# If MASTER_CONFIG_YAML env-var is not set, and /opt/cerebras/cluster/master-config.yaml exists, then set MASTER_CONFIG_YAML to it.
ifeq ($(strip $(MASTER_CONFIG_YAML)),)
    ifeq ($(shell test -e /opt/cerebras/cluster/master-config.yaml && echo exists),exists)
        export MASTER_CONFIG_YAML := /opt/cerebras/cluster/master-config.yaml
    endif
endif

.PHONY: help

help:
	@echo "Available targets:"
	@echo
	@echo "  venv            : Create the virtual environment"
	@echo "  source-venv     : Create a sub-shell with CBMON's Python virtual-environment activated"
	@echo "  cluster_summary : Show top-level details about the cluster"
	@echo "  run             : Run all the tests"
	@echo "  show_diff       : Do a diff between original reference-data, and re-goldenized if any"
	@echo "  help            : Display this help message"
	@echo
	@echo "Note: Set the MASTER_CONFIG_YAML environment variable before running the 'run' target"
	@echo "Additional Note: More options can be passed to 'pytest' using ADDITIONAL_PYTEST_OPTS env-variable"
	@echo "      Example: ADDITIONAL_PYTEST_OPTS='-k kernel' (And other pytest options like -k, -m etc.)"
	@echo ""

init:
# Keep a copy of original golden-reference-logs, can help figure the differences in the future
	@if [ ! -d "$(MAKEFILE_DIR)/tests/push_button_1/REFERENCE_DATA_ORIGINAL" ]; then \
		cp -rp $(MAKEFILE_DIR)/tests/push_button_1/REFERENCE_DATA $(MAKEFILE_DIR)/tests/push_button_1/REFERENCE_DATA_ORIGINAL; \
	fi

venv: init
	@if [ ! -e "$(CBMON_VENV)/venv.done" ]; then \
		if [ "$$(/bin/python3 --version 2>&1)" != "Python 3.6.8" ]; then \
			echo "[WARNING]: python3 installed locally (at /bin/python) is not Python 3.6.8."; \
			echo "Please check if the venv creation and package installation happen properly."; \
		fi; \
		/bin/python3 -m venv $(CBMON_VENV) && \
		( \
			. $(CBMON_VENV)/bin/activate && \
			cd $(MAKEFILE_DIR)/pkgs && \
			pip install --no-index --find-links . pip-21.3.1-py3-none-any.whl && \
			pip install --no-index --find-links . -r requirements.txt \
		) && \
		touch "$(CBMON_VENV)/venv.done"; \
	fi

source-venv: venv
	@echo "============================================================================="
	@echo "Creating a new sub-shell with python3 coming from CBMON's virtual-environment"
# Setting a PS1 value which is close to our current prompt, but removing dependency to our bash-startup routines
	@export PS1='\u@\h \[\033[32m\]\w\[\033[00m\] $$ ' && \
	source $(CBMON_VENV)/bin/activate && \
	echo -n "which python3: " && \
	which python3 && \
	echo "=============================================================================" && \
	$${SHELL} --norc --noprofile && \
	echo "=============================================================================" && \
	echo "Exited the sub-shell with CBMON's virtual-environment" && \
	echo "============================================================================="

basic_validation:
# Basic validation of MASTER_CONFIG_YAML
	@if [[ "$(MASTER_CONFIG_YAML)" == "" ]]; then \
		echo "[ERROR]: MASTER_CONFIG_YAML is not defined."; \
		echo "Please set it as the master_config.yaml and try again."; \
		echo "Example:"; \
		echo "export MASTER_CONFIG_YAML=/net/rushins-dev/srv/nfs/rushins-data/ws/cluster-deployment/validation-tool/cbmon_pytest/tmp/master_config.yaml"; \
		exit 1; \
	elif [[ ! -r "$(MASTER_CONFIG_YAML)" ]]; then \
		echo "[ERROR]: MASTER_CONFIG_YAML is not readable."; \
		echo "Please check the file path and permissions."; \
		exit 1; \
	fi

cluster_summary: venv basic_validation
	. $(CBMON_VENV)/bin/activate && \
	python3 $(MAKEFILE_DIR)/bin/cluster_summary.py

run: venv basic_validation cluster_summary clean_allure_dir
	EXIT_CODE=0; \
	. $(CBMON_VENV)/bin/activate && \
	pytest -v $(MAKEFILE_DIR)/tests/test_push_button_1.py --html=report.html --self-contained-html --alluredir=$(MAKEFILE_DIR)/allure_data $(ADDITIONAL_PYTEST_OPTS) || \
	EXIT_CODE=$$?; \
	tar -czf $(MAKEFILE_DIR)/allure_data.tar.gz -C $(MAKEFILE_DIR) allure_data; \
	#. $(CBMON_VENV)/bin/activate && \
	#$(MAKEFILE_DIR)/bin/show_webpage_link.sh; \
	exit $${EXIT_CODE}

show_diff:
	@if [ -d "$(MAKEFILE_DIR)/tests/push_button_1/REFERENCE_DATA_ORIGINAL" ]; then \
		(diff -r $(MAKEFILE_DIR)/tests/push_button_1/REFERENCE_DATA $(MAKEFILE_DIR)/tests/push_button_1/REFERENCE_DATA_ORIGINAL > /dev/null && echo "No differences w.r.t. reference-data") || \
		diff -r $(MAKEFILE_DIR)/tests/push_button_1/REFERENCE_DATA $(MAKEFILE_DIR)/tests/push_button_1/REFERENCE_DATA_ORIGINAL || true; \
	fi

clean_allure_dir:
	rm -rf $(MAKEFILE_DIR)/allure_data $(MAKEFILE_DIR)/allure_data.tar.gz

clean_venv:
	rm -rf $(CBMON_VENV)

clean: clean_venv clean_allure_dir
	rm -f $(MAKEFILE_DIR)/*.html
	rm -rf $(MAKEFILE_DIR)/__pycache__
	rm -rf $(MAKEFILE_DIR)/assets
