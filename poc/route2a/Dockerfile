# syntax=docker/dockerfile:1.6

# This image is intended to run `make bench-worst` inside a Linux container.
# Runtime only needs to connect to ClickHouse; it does not include a docker
# daemon/CLI, and tests use the pre-bundled native `router_check_tool`.
#
# POC_CH_ADDR points the runtime at a reachable ClickHouse instance.
#
# Package `.` tests are precompiled (`go test -c`) so runtime make bench-*
# skips compiling istio.io/istio (high peak RAM; often OOM-killed otherwise).

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

# Precompile package `.` test binary. -p 1 / GOMAXPROCS=2 lower compile peak RAM.
# Makefile picks this up via POC_TEST_BIN and runs the binary instead of `go test`.
RUN GOMAXPROCS=2 go test -c -o /usr/local/bin/route2a.test -p 1 .
ENV POC_TEST_BIN=/usr/local/bin/route2a.test

# Keep the container alive so you can docker exec in and run make targets.
CMD ["bash", "-lc", "sleep infinity"]

