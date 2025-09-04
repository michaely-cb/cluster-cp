ARG TAG="default"
ARG BASE=python:3.11-slim-bookworm
FROM ${BASE}

RUN apt-get update && apt-get install bash ethtool iputils-ping jq mstflint openssh-client pciutils sshpass systemd yq -y

ARG FRAMEWORK_DIR=/system-maintenance-framework
RUN mkdir -p ${FRAMEWORK_DIR}

COPY requirements.txt ${FRAMEWORK_DIR}/requirements.txt

# Build wheels for all dependencies
RUN mkdir -p ${FRAMEWORK_DIR}/wheels
RUN pip install --upgrade pip wheel
RUN pip wheel --wheel-dir=${FRAMEWORK_DIR}/wheels -r ${FRAMEWORK_DIR}/requirements.txt

# Also pip install requirements in the system-client image
RUN pip install --no-cache-dir -r ${FRAMEWORK_DIR}/requirements.txt

# system maintenance scripts
COPY system_maintenance*.py ${FRAMEWORK_DIR}/

# system client
COPY system_client ${FRAMEWORK_DIR}/system_client

# protos
COPY pb ${FRAMEWORK_DIR}/pb

ENV PYTHONPATH="${PYTHONPATH}:${FRAMEWORK_DIR}:${FRAMEWORK_DIR}/pb"