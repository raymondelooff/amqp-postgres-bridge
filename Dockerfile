FROM golang:1.20-alpine AS builder

LABEL maintainer="raymondelooff"

WORKDIR /go/src/app

ADD . .

RUN go mod vendor

RUN CGO_ENABLED=0 GOOS=linux go build -o /go/bin/amqp-postgres-bridge

FROM gcr.io/distroless/base

WORKDIR /bin

COPY --from=builder /go/bin/amqp-postgres-bridge .

VOLUME /config

ENTRYPOINT ["/bin/amqp-postgres-bridge"]
