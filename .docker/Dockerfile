FROM golang:1.23-alpine AS builder

WORKDIR /build
RUN apk update --no-cache && apk add --no-cache tzdata
COPY ../ .

RUN go mod tidy

RUN go build -o order_created_consumer cmd/order_created/consumer/main.go
RUN go build -o order_created_daemons cmd/order_created/daemons/main.go
RUN go build -o transaction_outbox cmd/transaction_outbox/main.go

FROM alpine:3.14 AS production
RUN apk update --no-cache && apk add --no-cache ca-certificates

COPY --from=builder /usr/share/zoneinfo/Europe/Moscow /usr/share/zoneinfo/Europe/Moscow
ENV TZ=Europe/Moscow
WORKDIR /build
COPY --from=builder build/order_created_consumer /build/order_created_consumer
COPY --from=builder build/order_created_daemons /build/order_created_daemons
COPY --from=builder build/transaction_outbox /build/transaction_outbox


