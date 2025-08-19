ARG BUILDER_IMAGE=golang:1.24
ARG BASE_IMAGE=alpine:latest

# Build the barrier binary
FROM --platform=${BUILDPLATFORM} ${BUILDER_IMAGE} AS builder

ARG CGO_ENABLED
ARG TARGETARCH

WORKDIR /workspace

# Copy go mod and sum files
COPY ../go.mod ../go.sum ./

# Download dependencies
RUN go mod download

# Copy the go source for the barrier
COPY main.go .

# Build
RUN CGO_ENABLED=${CGO_ENABLED} GOOS=linux GOARCH=${TARGETARCH} go build -o /barrier main.go

# Final image
FROM --platform=${BUILDPLATFORM} ${BASE_IMAGE}
WORKDIR /
COPY --from=builder /barrier .

ENTRYPOINT ["/barrier"]
