# POC route-resolution 2A: scoped on-demand istiod translation + Envoy's
# router_check_tool as the sole host+path -> service resolution engine.
#
# Scale is env-configurable: POC_GATEWAYS (default 600), POC_VS (default 100).
# Ramp with e.g. `make bench-warm POC_GATEWAYS=100`.
#
# router_check_tool runs as a NATIVE binary when available (POC_ROUTERCHECK_BIN
# or on PATH) — the only way to get meaningful throughput numbers — otherwise via
# docker on the prebuilt tools image (correctness still holds; per-batch container
# startup dominates timing, so don't cite perf numbers from a docker run).

ROUTERCHECK_IMAGE ?= envoyproxy/envoy:tools-v1.34-latest

export POC_ROUTERCHECK_IMAGE = $(ROUTERCHECK_IMAGE)

.PHONY: gen gwresolve spike bench-worst bench-warm bench-routercheck verify clean \
        ch-up ch-down ch-load ch-flow ch-verify ch-demo

# Generate + spot-check the corpus (600 gw / 60k VS) WITHOUT materializing it all.
gen:
	go test -run TestScaleGen -v ./internal/scalegen/

# Gateway host->gateway resolution correctness (most-specific disambiguation).
gwresolve:
	go test -run TestResolveMostSpecific -v ./internal/gwresolve/

# Validate the scoped config-generator translation path.
spike:
	go test -run TestRoutesForScopedSpike -v ./internal/translate/

# SINGLE-WORST: real single-request latency — full pipeline per one host+path
# (batch=1, cold). This is the honest online-serving number (sampled).
bench-worst:
	go test -run TestResolveSingleWorst -v -timeout 40m .

# WARM: full corpus, dependency-epoch cache warm — bulk/steady-state throughput
# (the scenario where rccache pays off), asserts full-corpus 0 mismatches.
bench-warm:
	go test -run TestResolveWarm -v -timeout 40m .

# Offline oracle cross-validation of the resolver (agreement, not throughput).
bench-routercheck:
	go test -run TestMatchRouterCheckScale -v -timeout 40m .

# End-to-end: generate, resolver correctness, warm (full-corpus 0 mismatches),
# single-request worst-case latency. Report in out/report.md.
verify: clean
	go test -run TestScaleGen ./internal/scalegen/
	go test -run TestResolveMostSpecific ./internal/gwresolve/
	go test -run 'TestResolveWarm|TestResolveSingleWorst' -v -timeout 60m .

clean:
	rm -rf out

# ---- Ingress IP->Gateway over ClickHouse (cmd/ipflow) ----
# Full traffic path: DNS/IP -> ClickHouse 3-hop (IP->Gateway) -> gwresolve ->
# translate(from ClickHouse) -> router_check_tool -> cluster.
# Test data is program-generated (cmd/ipflow -mode=load), never hand-written SQL.
# Scale/version knobs (dev: shrink these): POC_GATEWAYS POC_VS
#   POC_VER_DEPLOY POC_VER_SVC POC_VER_GW POC_VER_VS POC_VER_KSVC
# e.g. make ch-load POC_GATEWAYS=20 POC_VS=5 POC_VER_KSVC=5

CH_IMAGE ?= clickhouse/clickhouse-server:24.8
CH_NAME  ?= route2a-ch
CH_ADDR  ?= 127.0.0.1:9000
HOST     ?= svc00.gw000.example.com
REQPATH  ?= /healthz

# Start a ClickHouse container and wait until it answers queries. The users.d
# override opens the password-less `default` user to any network so host-side
# clients can reach the container (the image otherwise locks it to localhost).
ch-up:
	docker run -d --rm --name $(CH_NAME) -p 9000:9000 -p 8123:8123 \
	  --ulimit nofile=262144:262144 \
	  -v "$(CURDIR)/testdata/clickhouse-open-users.xml:/etc/clickhouse-server/users.d/zzz-open-networks.xml:ro" \
	  $(CH_IMAGE)
	@echo "waiting for ClickHouse to be ready..."
	@for i in $$(seq 1 60); do \
	  if docker exec $(CH_NAME) clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then \
	    echo "ClickHouse ready"; exit 0; fi; \
	  sleep 1; \
	done; echo "timeout waiting for ClickHouse"; exit 1

ch-down:
	-docker rm -f $(CH_NAME)

# Generate + stream the multi-version corpus into ClickHouse.
ch-load:
	POC_CH_ADDR=$(CH_ADDR) go run ./cmd/ipflow -mode=load

# Resolve one request end-to-end (IP derived from HOST via simulated DNS).
ch-flow:
	POC_CH_ADDR=$(CH_ADDR) go run ./cmd/ipflow -mode=query -host=$(HOST) -path=$(REQPATH)

# 3-hop oracle sampling + multi-version AsOf(T) selection (no router_check_tool needed).
ch-verify:
	POC_CH_ADDR=$(CH_ADDR) go run ./cmd/ipflow -mode=verify

# One-shot demo: up -> load -> flow.
ch-demo: ch-up
	$(MAKE) ch-load
	$(MAKE) ch-flow
