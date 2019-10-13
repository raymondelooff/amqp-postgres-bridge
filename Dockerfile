FROM golang:1.13-alpine

LABEL maintainer="raymondelooff"

RUN mkdir -p /go/src/github.com/raymondelooff/amqp-postgres-bridge

ADD . /go/src/github.com/raymondelooff/amqp-postgres-bridge

RUN cd /go/src/github.com/raymondelooff/amqp-postgres-bridge && \
    apk add --no-cache --virtual .build-deps git dep && \
    dep ensure && \
    go build -o /go/bin/amqp-postgres-bridge /go/src/github.com/raymondelooff/amqp-postgres-bridge/cmd/bridge/main.go && \
    rm -rf /go/src/* && \
    apk del .build-deps

VOLUME /config

ENTRYPOINT ["/go/bin/amqp-postgres-bridge"]
