FROM golang:1.24rc3-alpine3.21 AS builder
WORKDIR /app
COPY main.go .
RUN go mod init agent && \
    go mod tidy && \
    go build -o /app/out/main

FROM alpine:3.21.2
WORKDIR /app
COPY --from=builder /app/out/main /app/main
CMD ["./main"]