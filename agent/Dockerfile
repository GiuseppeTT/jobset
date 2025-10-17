ARG BUILDER_IMAGE=golang:1.24
ARG BASE_IMAGE=alpine:latest

###############################################################################
# Build the agent binary
FROM --platform=${BUILDPLATFORM} ${BUILDER_IMAGE} AS builder

ARG CGO_ENABLED
ARG TARGETARCH

WORKDIR /workspace

COPY .. .

WORKDIR /workspace/agent

RUN go mod download

RUN CGO_ENABLED=${CGO_ENABLED} GOOS=linux GOARCH=${TARGETARCH} go build -o /agent main.go

###############################################################################
# Final image
FROM --platform=${BUILDPLATFORM} ${BASE_IMAGE}
WORKDIR /

RUN apk add --no-cache bash

# Copy the exit-on-demand script and make it executable
COPY --from=builder /workspace/agent/exit-on-demand.sh .
RUN chmod +x exit-on-demand.sh

# Copy the agent binary
COPY --from=builder /agent .

ENTRYPOINT ["/agent"]