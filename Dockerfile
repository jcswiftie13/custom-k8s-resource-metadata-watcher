# syntax=docker/dockerfile:1.6

FROM golang:1.25-bookworm AS build

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOFLAGS=-buildvcs=false

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/metadata-exporter ./cmd

FROM busybox:1.38

# World-readable/executable so non-root can run and `cp` the binary into a volume.
COPY --from=build --chmod=0555 /out/metadata-exporter /usr/local/bin/metadata-exporter

# Numeric UID required when Kubernetes runAsNonRoot is set without runAsUser.
# 65532 matches the previous distroless nonroot user and integration manifests.
USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/metadata-exporter"]
