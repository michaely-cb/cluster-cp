.PHONY: all
.DEFAULT: all
all:


.PHONY: pylint
pylint:
	pylint network_config
	pylint tests/*.py

.PHONY: test
test:
	PYTHONPATH=$$(pwd -P) pytest network_config/tests -vvv


.PHONY: clean
clean:
	$(RM) -rf tests/__pycache__
	find ./ -type f -name '*.pyc' -delete
