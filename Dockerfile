FROM golang:1.13-alpine

LABEL maintainer="raymondelooff"

WORKDIR /go/src/app

ADD . .

RUN mkdir -p /app

RUN go build -o /app/amqp-postgres-bridge cmd/bridge/main.go && \
    rm -rf /go

VOLUME /config

ENTRYPOINT ["/app/amqp-postgres-bridge"]
