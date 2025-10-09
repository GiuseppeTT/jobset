ARG BUILDER_IMAGE=golang:1.24
ARG BASE_IMAGE=alpine:latest

# Build the agent-worker binary
FROM --platform=${BUILDPLATFORM} ${BUILDER_IMAGE} AS builder

ARG CGO_ENABLED
ARG TARGETARCH

WORKDIR /workspace

COPY .. .

WORKDIR /workspace/agent-worker

RUN go mod download

RUN CGO_ENABLED=${CGO_ENABLED} GOOS=linux GOARCH=${TARGETARCH} go build -o /agent-worker main.go

# Final image
FROM --platform=${BUILDPLATFORM} ${BASE_IMAGE}
WORKDIR /

# Install bash
RUN apk add --no-cache bash

# Copy the exit-on-demand script and make it executable
COPY --from=builder /workspace/agent-worker/exit-on-demand.sh .
RUN chmod +x exit-on-demand.sh

# Copy the agent-worker binary
COPY --from=builder /agent-worker .

ENTRYPOINT ["/agent-worker"]