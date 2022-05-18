FROM golang:1.17-alpine as builder

COPY . /build/
WORKDIR /build

RUN GOPROXY=https://goproxy.cn go build .


FROM alpine:3.13

COPY --from=builder /build/prometheus-remote-storage /prometheus-remote-storage

CMD ["/prometheus-remote-storage", "--clickhouse.url=clickhouse://default:C1ickh0use@clickhouse-tkeel-core:9000", "--clickhouse.database=core", "--clickhouse.table=timeseries", "--web.listen-address=:9202"]
