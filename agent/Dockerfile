FROM golang:1.24-alpine3.21 AS builder
WORKDIR /app
COPY main.go .
COPY agent ./agent
RUN go mod init agent && \
    go mod tidy && \
    go build -o out/main

FROM alpine:3.21
WORKDIR /app
COPY --from=builder /app/out/main main
ENTRYPOINT ["/app/main"]