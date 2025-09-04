# Build the manager binary
ARG BASE_IMAGE=golang:1.21.8
ARG RUN_BASE=registry.k8s.io/build-image/go-runner:v2.3.1-go1.21.8-bullseye.0
FROM $BASE_IMAGE as builder

WORKDIR /ws
COPY go.mod go.sum /ws/
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download
COPY main.go /ws/
COPY cmd/* /ws/cmd/
RUN CGO_ENABLED=0 GOOS=linux go build -a -o kube-webhook main.go

# Generate final distroless image
FROM $RUN_BASE
WORKDIR /
COPY --from=builder /ws/kube-webhook /
ENTRYPOINT ["./kube-webhook"]
