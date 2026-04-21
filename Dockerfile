# syntax=docker/dockerfile:1.6

FROM golang:1.22-bookworm AS build

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

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/metadata-exporter /usr/local/bin/metadata-exporter

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/metadata-exporter"]
