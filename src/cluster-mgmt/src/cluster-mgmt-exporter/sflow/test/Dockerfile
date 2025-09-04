ARG FLOW_PROM_EXPORTER_IMAGE="sflow-exporter"
ARG FLOW_PROM_EXPORTER_TAG="latest"

FROM golang:alpine3.21 AS test-builder
RUN mkdir /test
WORKDIR /test
COPY goflow2_prom_exporter_test.go go.mod go.sum ./
COPY expected_metrics.prom test_input_flow_samples_proto.bin ./
RUN CGO_ENABLED=0 go test -c -o goflow2-prom-exporter-test goflow2_prom_exporter_test.go

FROM ${FLOW_PROM_EXPORTER_IMAGE}:${FLOW_PROM_EXPORTER_TAG}
COPY --from=test-builder /test /test
WORKDIR /test

# Uncomment to include debugging tools:
# COPY --from=busybox:1.35.0-uclibc /bin/sh /bin/sh
# COPY --from=busybox:1.35.0-uclibc /bin/cat /bin/cat
# COPY --from=busybox:1.35.0-uclibc /bin/ls /bin/ls

ENTRYPOINT ["/test/goflow2-prom-exporter-test"]