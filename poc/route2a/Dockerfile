# syntax=docker/dockerfile:1.6

# This image is intended to run `make bench-worst` inside a Linux container.
# Runtime only needs to connect to ClickHouse; it does not include a docker
# daemon/CLI, and tests use the pre-bundled native `router_check_tool`.

FROM envoyproxy/envoy:tools-v1.34-latest AS tools

FROM golang:1.25-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    make ca-certificates bash \
  && rm -rf /var/lib/apt/lists/*

ENV CGO_ENABLED=0

WORKDIR /workspaces/poc/route2a

# Preload module deps for faster container builds.
COPY go.mod go.sum ./
RUN go mod download

# Copy the PoC code.
COPY . ./

# Bundle router_check_tool so matchcheck.Detect() uses the native path
# (POC_ROUTERCHECK_BIN) instead of falling back to `docker run`.
COPY --from=tools /usr/local/bin/router_check_tool /usr/local/bin/router_check_tool
RUN chmod +x /usr/local/bin/router_check_tool
ENV POC_ROUTERCHECK_BIN=/usr/local/bin/router_check_tool

# Keep the container alive so you can docker exec in and run make targets.
CMD ["bash", "-lc", "sleep infinity"]

