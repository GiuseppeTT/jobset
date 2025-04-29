ARG BUILDER_IMAGE=golang:1.24
ARG BASE_IMAGE=gcr.io/distroless/static:nonroot

# Build the orchestrator binary
FROM --platform=${BUILDPLATFORM} ${BUILDER_IMAGE} AS builder

ARG CGO_ENABLED=0  
ARG TARGETARCH     

WORKDIR /workspace

# Copy the go source
COPY . .

# Build
RUN CGO_ENABLED=${CGO_ENABLED} GOOS=linux GOARCH=${TARGETARCH} go build -o bin/orchestrator main.go

FROM --platform=${BUILDPLATFORM} ${BASE_IMAGE}
WORKDIR /
COPY --from=builder /workspace/bin/orchestrator .
USER 65532:65532

ENTRYPOINT ["/orchestrator"]