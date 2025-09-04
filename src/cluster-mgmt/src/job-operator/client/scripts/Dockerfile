FROM golang:1.21.8

ENV GO111MODULE=off

# Note: the version should be pinned but pinning the version doesn't work
# with gomod disabled.
RUN go get k8s.io/code-generator
RUN go get k8s.io/apimachinery

ARG repo="${GOPATH}/src/pkg"

RUN mkdir -p $repo

WORKDIR $GOPATH/src/k8s.io/code-generator

VOLUME $repo
