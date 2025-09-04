GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

GITTOP ?= $(shell git rev-parse --show-toplevel)
export GOPATH := ${GITTOP}/.cluster_mgmt_go
export GOCACHE := ${GOPATH}/cache
export GOBIN := ${GOPATH}/bin
export GOFLAGS := -modcacherw
export GOENV ?= $(GITTOP)/flow/go.env

JSONNET_BIN_DIR ?= $(shell pwd)/bin
JSONNET_VENDOR=vendor
GRAFONNET_DASHBOARDS_DIR=grafonnet_dashboards
GRAFANA_DASHBOARD_LINTER_BIN=$(JSONNET_BIN_DIR)/dashboard-linter
JB_BIN=$(JSONNET_BIN_DIR)/jb
JSONNET_BIN=$(JSONNET_BIN_DIR)/jsonnet
JSONNETLINT_BIN=$(JSONNET_BIN_DIR)/jsonnet-lint
JSONNETFMT_BIN=$(JSONNET_BIN_DIR)/jsonnetfmt
PROMTOOL_BIN=$(JSONNET_BIN_DIR)/promtool
JSONNETFMT_ARGS=-n 2 --max-blank-lines 2 --string-style s --comment-style s
DASHBOARD_SRC_DIR ?= dashboards
DASHBOARD_OUT_DIR ?= dashboards_out
ALERTS_DIR ?= alerts
ALERTS_OUT_DIR ?= alerts_out

.NOTPARALLEL:

.PHONY: all
all: tooling fmt generate lint

.PHONY: generate
generate: $(DASHBOARD_OUT_DIR) $(ALERTS_OUT_DIR)

$(JSONNET_VENDOR): $(JB_BIN) jsonnetfile.json
	mkdir -p $(JSONNET_VENDOR)
	$(JB_BIN) install

.PHONY: fmt
fmt: $(JSONNETFMT_BIN)
	find . -name "$(JSONNET_VENDOR)" -prune -o -name '*.libsonnet' -print -o -name '*.jsonnet' -print | \
		xargs -n 1 -- $(JSONNETFMT_BIN) $(JSONNETFMT_ARGS) -i
	find . -name "$(JSONNET_VENDOR)" -prune -o -name '*.libsonnet' -print -o -name '*.jsonnet' -print | \
		xargs -n 1 -- $(JSONNETFMT_BIN) $(JSONNETFMT_ARGS) -i

$(DASHBOARD_OUT_DIR): $(JSONNET_BIN) $(JSONNET_VENDOR) dashboards.jsonnet $(DASHBOARD_SRC_DIR)/*.libsonnet $(GRAFONNET_DASHBOARDS_DIR)/*/*.libsonnet
	mkdir -p $(DASHBOARD_OUT_DIR)
	$(JSONNET_BIN) -J $(JSONNET_VENDOR) -m $(DASHBOARD_OUT_DIR) dashboards.jsonnet
	$(JSONNET_BIN) -J $(JSONNET_VENDOR) -m $(DASHBOARD_OUT_DIR) $(GRAFONNET_DASHBOARDS_DIR).jsonnet
	cp $(DASHBOARD_OUT_DIR)/wsjob-dashboard.json ../dashboards/ml-user
	cp $(DASHBOARD_OUT_DIR)/ml-admin-dashboard.json ../dashboards/ml-admin
	cp $(DASHBOARD_OUT_DIR)/cluster-control-plane-health.json ../dashboards/cluster-admin-unlocked
	cp $(DASHBOARD_OUT_DIR)/cp-summary.json ../dashboards/cluster-admin-unlocked

$(ALERTS_OUT_DIR): $(JSONNET_BIN) $(JSONNET_VENDOR) $(ALERTS_DIR)/*.libsonnet
	mkdir -p $(ALERTS_OUT_DIR)
	$(JSONNET_BIN) -J $(JSONNET_VENDOR) -S -e 'std.manifestYamlDoc((import "alerts/alert.libsonnet").prometheusAlerts)' -o $(ALERTS_OUT_DIR)/alert.yaml
	$(JSONNET_BIN) -J $(JSONNET_VENDOR) -S -e 'std.manifestYamlDoc((import "alerts/test_alert.libsonnet").prometheusAlerts)' -o $(ALERTS_OUT_DIR)/test.yaml

.PHONY: lint
lint: jsonnet-lint dashboards-lint

.PHONY: jsonnet-lint
jsonnet-lint: $(JSONNETLINT_BIN) $(JSONNET_VENDOR)
	@find . -name "*$(JSONNET_VENDOR)" -prune -o -name '*.libsonnet' -print -o -name '*.jsonnet' -print | \
		xargs -n 1 -- $(JSONNETLINT_BIN) -J $(JSONNET_VENDOR)

$(DASHBOARD_OUT_DIR)/.lint: $(DASHBOARD_OUT_DIR) ./.lint
	cp .lint $@

.PHONY: dashboards-lint
dashboards-lint: $(GRAFANA_DASHBOARD_LINTER_BIN) $(DASHBOARD_OUT_DIR)/.lint
	@find $(DASHBOARD_OUT_DIR) -name '*.json' -print0 | xargs -n 1 -0 $(GRAFANA_DASHBOARD_LINTER_BIN) lint --strict

.PHONY: clean
clean:
	# Remove all files and directories ignored by git.
	git clean -Xfd .

.PHONY: test
test: $(PROMTOOL_BIN)
	@$(PROMTOOL_BIN) test rules tests.yaml --enable-feature=promql-negative-offset

$(JSONNET_BIN_DIR):
	mkdir -p $(JSONNET_BIN_DIR)

$(JSONNET_BIN_DIR)/%:
	@if [ ! -f "$@" ]; then \
		$(MAKE) tooling; \
	fi

.PHONY: tooling
tooling: $(JSONNET_BIN_DIR)
	@echo "Installing tools from tools/tools.go" ;\
	cd tools && for tool in $$(go list -e --mod=mod -tags tools -f '{{ range .Imports }}{{ printf "%s\n" .}}{{end}}' ./); do \
		binary_path="$(JSONNET_BIN_DIR)/$$(basename $$tool)"; \
		if [ ! -f "$$binary_path" ]; then \
			echo "Building $$tool"; \
			go build -mod=mod -o "$$binary_path" $$tool; \
		fi; \
	done
