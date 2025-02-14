# Build
FROM golang:1.24rc3-alpine3.21 as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY agent.go ./
RUN go build -o /app/out/agent .

# Run
FROM alpine:3.21.2

WORKDIR /app

COPY --from=builder /app/out/agent ./

CMD ["./agent"]