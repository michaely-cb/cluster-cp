ARG K8S_VERSION=""

# Copy Python 3.6 from a slim image to the Kind node image
FROM python:3.6.15-slim as python-build
FROM kindest/node:v${K8S_VERSION}

COPY --from=python-build /usr/local /usr/local
RUN rm -f /usr/local/bin/python3 && \
    ln -s /usr/local/bin/python3.6 /usr/local/bin/python3
# Make python3.6 site-packages also pointing to /usr/lib/python3/dist-packages
# so that it can pick up yaml package that will be installed there.
RUN mkdir -p /root/.local/lib/python3.6 && \
    ln -s /usr/lib/python3/dist-packages /root/.local/lib/python3.6/site-packages

# Update CNI plugins to support multus / macvlan plugin
RUN cd /opt/cni/bin && \
    curl https://github.com/containernetworking/plugins/releases/download/v1.4.0/cni-plugins-linux-amd64-v1.4.0.tgz -LO && \
    tar xzf cni-plugins-linux-amd64-v1.4.0.tgz && \
    rm -f cni-plugins-linux-amd64-v1.4.0.tgz

COPY kind-node-setup.sh kind-node-entrypoint.sh /kind/
RUN /kind/kind-node-setup.sh

RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["/usr/local/bin/entrypoint", "/sbin/init", "/kind/kind-node-entrypoint.sh"]
