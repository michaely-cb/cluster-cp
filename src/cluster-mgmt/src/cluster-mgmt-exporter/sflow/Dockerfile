FROM golang:alpine3.21 AS builder
ARG LDFLAGS=""
ARG GOFLOW2_VERSION="v2.2.1"

RUN apk --update --no-cache add git build-base gcc

RUN git clone --depth 1 --branch "${GOFLOW2_VERSION}" https://github.com/netsampler/goflow2 /build
COPY ./goflow2_prom_exporter.go /build/
WORKDIR /build

RUN CGO_ENABLED=0 go build -ldflags "${LDFLAGS}" -o goflow2 cmd/goflow2/main.go
RUN CGO_ENABLED=0 go build -ldflags "${LDFLAGS}" -o goflow2-prom-exporter goflow2_prom_exporter.go

FROM gcr.io/distroless/static-debian12:latest
ARG src_dir
ARG VERSION=""
ARG CREATED=""
ARG DESCRIPTION=""
ARG NAME=""
ARG MAINTAINER=""
ARG URL=""
ARG LICENSE=""
ARG REV=""

ENV GOFLOW2_SFLOW_PORT=6343
ENV GOFLOW2_HTTP_PORT=8008
ENV GOFLOW2_SOCKETS=4
ENV GOFLOW2_WORKERS=8
ENV GOFLOW2_BLOCKING=false
ENV FLOW_PROM_EXPORTER_HTTP_PORT=8080
ENV FLOW_PROM_EXPORTER_FLOW_TIMEOUT=10m

LABEL org.opencontainers.image.created="${CREATED}"
LABEL org.opencontainers.image.authors="${MAINTAINER}"
LABEL org.opencontainers.image.url="${URL}"
LABEL org.opencontainers.image.title="${NAME}"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.description="${DESCRIPTION}"
LABEL org.opencontainers.image.licenses="${LICENSE}"
LABEL org.opencontainers.image.revision="${REV}"

COPY --from=builder /build/goflow2-prom-exporter /
COPY --from=builder /build/goflow2 /
# We need sh to execute entrypoint.sh
COPY --from=busybox:1.35.0-uclibc /bin/sh /bin/sh
COPY ./entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]