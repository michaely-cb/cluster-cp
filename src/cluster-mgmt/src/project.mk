.PHONY: cluster_mgmt_src
cluster_mgmt_src:
	$(MAKE) -C ${GITTOP}/src/cluster_mgmt/src CEREBRAS_VERSION=${CEREBRAS_VERSION} CLUSTER_SEMANTIC_VERSION=${CLUSTER_SEMANTIC_VERSION}

all: cluster_mgmt_src

cli:
