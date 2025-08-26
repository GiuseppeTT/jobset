ARG BUILDER_IMAGE=golang:1.24
ARG BASE_IMAGE=alpine:latest

# Build the worker binary
FROM --platform=${BUILDPLATFORM} ${BUILDER_IMAGE} AS builder

ARG CGO_ENABLED
ARG TARGETARCH

WORKDIR /workspace

# Copy go mod and sum files
COPY ../go.mod ../go.sum ./

# Download dependencies
RUN go mod download

# Copy the go source for the worker
COPY main.go .

# Build
RUN CGO_ENABLED=${CGO_ENABLED} GOOS=linux GOARCH=${TARGETARCH} go build -o /worker main.go

# Final image
FROM --platform=${BUILDPLATFORM} ${BASE_IMAGE}
WORKDIR /
COPY --from=builder /worker .

ENTRYPOINT ["/worker"]