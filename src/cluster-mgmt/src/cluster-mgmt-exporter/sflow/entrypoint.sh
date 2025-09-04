#!/bin/sh
/goflow2 \
  -listen "sflow://:${GOFLOW2_SFLOW_PORT}?count=${GOFLOW2_SOCKETS}&workers=${GOFLOW2_WORKERS}&blocking=${GOFLOW2_BLOCKING}" \
  -addr=":${GOFLOW2_HTTP_PORT}" -transport.file.sep= -format=bin | \
    /goflow2-prom-exporter  -addr=":${FLOW_PROM_EXPORTER_HTTP_PORT}" \
      -flowtimeout ${FLOW_PROM_EXPORTER_FLOW_TIMEOUT} \
      -noout