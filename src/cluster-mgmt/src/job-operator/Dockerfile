# Build the manager binary
ARG BASE_IMAGE=golang:1.21.8
ARG RUN_BASE=registry.k8s.io/build-image/go-runner:v2.3.1-go1.21.8-bullseye.0
ARG CEREBRAS_VERSION=""
ARG SEMANTIC_VERSION=""
FROM $BASE_IMAGE as builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY job-operator/go.mod job-operator/go.mod
COPY job-operator/go.sum job-operator/go.sum
COPY job-operator/kubeflow/ job-operator/kubeflow/
COPY pb/ pb/

WORKDIR /workspace/job-operator

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY job-operator/main.go main.go
COPY job-operator/apis/ apis/
COPY job-operator/client/ client/
COPY job-operator/common/ common/
COPY job-operator/controllers/ controllers/
COPY job-operator/server/ server/

ARG CEREBRAS_VERSION
ARG SEMANTIC_VERSION
# Build
RUN CGO_ENABLED=0 GOOS=linux go build -a -o manager \
    --ldflags="-X 'cerebras.com/job-operator/common.CerebrasVersion=${CEREBRAS_VERSION}' \
    -X 'cerebras.com/job-operator/common.SemanticVersion=${SEMANTIC_VERSION}'" main.go

# Use distroless + go-runer as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
# https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/component-base/logs/kube-log-runner
FROM $RUN_BASE
WORKDIR /
COPY --from=builder /workspace/job-operator/manager .
USER 65532:65532

ARG SEMANTIC_VERSION
LABEL semantic_version=${SEMANTIC_VERSION}

ENTRYPOINT ["/manager"]
