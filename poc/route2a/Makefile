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

.PHONY: gen gwresolve spike bench-worst bench-warm bench-routercheck verify clean

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
