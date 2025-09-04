# Build the manager binary
ARG BASE_IMAGE=golang:1.21.8
ARG RUN_BASE=registry.k8s.io/build-image/go-runner:v2.3.1-go1.21.8-bullseye.0
ARG CEREBRAS_VERSION=""
ARG SEMANTIC_VERSION=""
FROM $BASE_IMAGE as builder

WORKDIR /workspace
# Copy the Go Modules manifests
COPY pb/ pb/
COPY cluster/server/go.mod cluster/server/go.mod
COPY cluster/server/go.sum cluster/server/go.sum
COPY job-operator/go.mod job-operator/go.mod
COPY job-operator/go.sum job-operator/go.sum
COPY kube-user-auth/go.mod kube-user-auth/go.mod
COPY kube-user-auth/go.sum kube-user-auth/go.sum

WORKDIR /workspace/job-operator
COPY job-operator/kubeflow/ kubeflow/

RUN go mod download

# Copy the go source
COPY job-operator/apis/ apis/
COPY job-operator/client/ client/
COPY job-operator/client-system/ client-system/
COPY job-operator/client-resourcelock/ client-resourcelock/
COPY job-operator/client-namespace/ client-namespace/
COPY job-operator/common/ common/
COPY job-operator/controllers/ controllers/

WORKDIR /workspace/kube-user-auth
COPY kube-user-auth/pkg/ pkg/

WORKDIR /workspace/cluster/server

# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY cluster/server/cluster cluster
COPY cluster/server/csadm csadm
COPY cluster/server/csctl csctl
COPY cluster/server/pkg pkg
COPY cluster/server/*.go ./

RUN go mod tidy

ARG CEREBRAS_VERSION
ARG SEMANTIC_VERSION
# Build
RUN CGO_ENABLED=0 GOOS=linux go build -a -o cluster_server \
    --ldflags="-X 'cerebras.com/cluster/server/pkg.CerebrasVersion=${CEREBRAS_VERSION}' \
    -X 'cerebras.com/cluster/server/pkg/wsclient.SemanticVersion=${SEMANTIC_VERSION}'"

# Use distroless + go-runer as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
# https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/component-base/logs/kube-log-runner
FROM $RUN_BASE
WORKDIR /
COPY --from=builder /workspace/cluster/server/cluster_server .
USER 65532:65532

ARG SEMANTIC_VERSION
LABEL semantic_version=${SEMANTIC_VERSION}

ENTRYPOINT ["/cluster_server"]
