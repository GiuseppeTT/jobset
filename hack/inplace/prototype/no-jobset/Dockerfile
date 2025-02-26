FROM golang:1.24rc3-alpine3.21 AS builder
WORKDIR /app
COPY main.go .
COPY agent ./agent
RUN go mod init inplace && \
    go mod tidy && \
    go build -o out/main

FROM alpine:3.21.2
WORKDIR /app
COPY --from=builder /app/out/main main
CMD ["./main"]