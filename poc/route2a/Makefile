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
        ch-up ch-down

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

# ---- ClickHouse container lifecycle ----
# bench-warm / bench-worst run the whole pipeline over ClickHouse (IP 3-hop ->
# gwresolve -> translate-from-ClickHouse -> router_check_tool). They auto-load the
# corpus on first run (untimed) and skip the load when it is already present, so
# the flow is just: `make ch-up` then `make bench-worst` (or bench-warm).
# Correctness lives in `go test` (TestIPFlowClickHouse) and cmd/ipflow (manual CLI).
# Scale/version knobs: POC_GATEWAYS POC_VS
#   POC_VER_DEPLOY POC_VER_SVC POC_VER_GW POC_VER_VS POC_VER_KSVC (default 1 each)

CH_IMAGE ?= clickhouse/clickhouse-server:24.8
CH_NAME  ?= route2a-ch
CH_ADDR  ?= 127.0.0.1:9000

# Start a ClickHouse container and wait until it answers queries. The users.d
# override opens the password-less `default` user to any network so host-side
# clients can reach the container (the image otherwise locks it to localhost).
ch-up:
	docker run -d --rm --name $(CH_NAME) -p 9000:9000 -p 8123:8123 \
	  --ulimit nofile=262144:262144 \
	  -v "$(CURDIR)/testdata/clickhouse-open-users.xml:/etc/clickhouse-server/users.d/zzz-open-networks.xml:ro" \
	  $(CH_IMAGE)
	@echo "waiting for ClickHouse to accept host connections on the mapped ports..."
	@for i in $$(seq 1 90); do \
	  if curl -fsS "http://127.0.0.1:8123/ping" >/dev/null 2>&1; then \
	    echo "ClickHouse ready"; exit 0; fi; \
	  sleep 1; \
	done; echo "timeout waiting for ClickHouse (check: docker logs $(CH_NAME))"; exit 1

ch-down:
	-docker rm -f $(CH_NAME)
