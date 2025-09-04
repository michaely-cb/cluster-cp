# Use distroless + go-runner as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
# https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/component-base/logs/kube-log-runner
FROM registry.k8s.io/build-image/go-runner:v2.3.1-go1.21.8-bullseye.0
WORKDIR /
COPY bin/kube-webhook-linux kube-webhook

ENTRYPOINT ["./kube-webhook"]
