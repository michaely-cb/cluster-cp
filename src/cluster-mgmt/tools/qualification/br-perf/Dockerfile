FROM alpine:latest

RUN apk update && \
    apk add iperf3

RUN mv /usr/bin/iperf3 /home/iperf3 && \
    rm -rf /var/cache/apk/* && \
    rm -rf /usr/bin/*

RUN adduser -D -u 1000 iperf3
USER iperf3

ENTRYPOINT [ "/home/iperf3" ]
