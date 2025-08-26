FROM golang:1.24.4-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/server   ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/producer ./cmd/producer
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/migrator ./cmd/migrator

FROM alpine:3.20
WORKDIR /app
RUN adduser -D -u 10001 appuser
COPY --from=builder /out/* /app/
COPY migrations /migrations
USER appuser

ENV HTTP_ADDR=:8081
EXPOSE 8081
CMD ["/app/server"]
