ARG BUILD_IMAGE=golang:1.21.8
ARG KUBE_VIP_IMAGE=171496337684.dkr.ecr.us-west-2.amazonaws.com/kube-vip:v0.8.9

# Build health watcher wrapper app
FROM ${BUILD_IMAGE} as builder
WORKDIR /app
COPY *.go *.mod *.sum /app/
RUN CGO_ENABLED=0 GOOS=linux go build

# Copy wrapper app to kube-vip image
FROM ${KUBE_VIP_IMAGE}
WORKDIR /
COPY --from=builder /app/kube-vip-health .
ENTRYPOINT ["/kube-vip-health"]
