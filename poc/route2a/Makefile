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
ROUTERCHECK_BIN   := $(CURDIR)/bin/router_check_tool

export POC_ROUTERCHECK_IMAGE = $(ROUTERCHECK_IMAGE)
# Only export when the binary actually runs on this OS (Linux ELF on macOS must not win).
_ROUTERCHECK_RUNS := $(shell test -x '$(ROUTERCHECK_BIN)' && '$(ROUTERCHECK_BIN)' --version >/dev/null 2>&1 && echo yes)
ifeq ($(_ROUTERCHECK_RUNS),yes)
export POC_ROUTERCHECK_BIN = $(ROUTERCHECK_BIN)
else
unexport POC_ROUTERCHECK_BIN
endif

# DB selects the config-store backend the bench/test pipeline runs against:
# clickhouse (default) | postgres | mariadb. `make bench-warm DB=postgres` etc.
# POC_DB is exported so `go test` (and the ipflow CLI) pick the same backend; the
# per-DB connection string falls back to storeopen defaults (127.0.0.1 containers),
# overridable with POC_CH_ADDR / POC_PG_ADDR / POC_MARIA_ADDR.
DB ?= clickhouse
export POC_DB = $(DB)

.PHONY: gen gwresolve spike bench-worst bench-warm bench-routercheck routercheck-bin verify clean \
        ch-up ch-down ch-reset pg-up pg-down pg-reset maria-up maria-down maria-reset \
        db-up db-down db-reset

# Extract router_check_tool from the official Envoy tools image into bin/ (gitignored).
routercheck-bin:
	mkdir -p bin
	docker pull $(ROUTERCHECK_IMAGE)
	cid=$$(docker create $(ROUTERCHECK_IMAGE)); \
	docker cp $$cid:/usr/local/bin/router_check_tool $(ROUTERCHECK_BIN); \
	docker rm $$cid; \
	chmod +x $(ROUTERCHECK_BIN)
	@$(ROUTERCHECK_BIN) --version

# Generate + spot-check the corpus (600 gw / 60k VS) WITHOUT materializing it all.
gen:
	go test -run TestScaleGen -v ./internal/scalegen/

# Gateway host->gateway resolution correctness (most-specific disambiguation).
gwresolve:
	go test -run TestResolveMostSpecific -v ./internal/gwresolve/

# Validate the scoped config-generator translation path.
spike:
	go test -run TestRoutesForScopedSpike -v ./internal/translate/

# When POC_TEST_BIN is set (container image prebuilds `go test -c` for package
# `.`), run that binary instead of `go test` to avoid compile-time OOM.
ifneq ($(strip $(POC_TEST_BIN)),)
define run_pkg_test
	$(POC_TEST_BIN) -test.run='$(1)' -test.v -test.timeout=$(2)
endef
else
define run_pkg_test
	go test -run '$(1)' -v -timeout $(2) .
endef
endif

# SINGLE-WORST: real single-request latency — full pipeline per one host+path
# (batch=1, cold). This is the honest online-serving number (sampled).
bench-worst:
	$(call run_pkg_test,TestResolveSingleWorst,40m)

# WARM: full corpus, dependency-epoch cache warm — bulk/steady-state throughput
# (the scenario where rccache pays off), asserts full-corpus 0 mismatches.
bench-warm:
	$(call run_pkg_test,TestResolveWarm,80m)

# Offline oracle cross-validation of the resolver (agreement, not throughput).
bench-routercheck:
	$(call run_pkg_test,TestMatchRouterCheckScale,40m)

# End-to-end: generate, resolver correctness, warm (full-corpus 0 mismatches),
# single-request worst-case latency. Report in out/report.md.
verify: clean
	go test -run TestScaleGen ./internal/scalegen/
	go test -run TestResolveMostSpecific ./internal/gwresolve/
	$(call run_pkg_test,TestResolveWarm|TestResolveSingleWorst,60m)

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

CH_IMAGE ?= clickhouse/clickhouse-server:25.5
CH_NAME  ?= route2a-ch
CH_ADDR  ?= 127.0.0.1:9000

PG_IMAGE    ?= postgres:17
PG_NAME     ?= route2a-pg
MARIA_IMAGE ?= mariadb:11
MARIA_NAME  ?= route2a-maria

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

# ch-reset drops the corpus tables (keeping the container up) so the next bench-*
# run reloads them and recreates the schema. Use this after a DDL/index change:
# ensureLoaded only reloads on a row-count mismatch, so an unchanged corpus would
# otherwise keep the OLD schema (and miss new indexes). Dropping the tables makes
# gw_versions absent, which forces ingload.Load -> CreateSchema to rebuild fresh.
ch-reset:
	docker exec $(CH_NAME) clickhouse-client --multiquery --query "\
	DROP TABLE IF EXISTS service_versions; \
	DROP TABLE IF EXISTS deploy_versions; \
	DROP TABLE IF EXISTS gw_versions; \
	DROP TABLE IF EXISTS vs_versions;"
	@echo "corpus tables dropped; next bench-* / test run reloads with the current schema"

# ---- PostgreSQL container lifecycle (matches storeopen's default POC_PG_ADDR:
# postgres://postgres:postgres@127.0.0.1:5432/route2a) ----
pg-up:
	docker run -d --rm --name $(PG_NAME) -p 5432:5432 \
	  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=route2a \
	  $(PG_IMAGE)
	@echo "waiting for PostgreSQL to accept connections..."
	@for i in $$(seq 1 90); do \
	  if docker exec $(PG_NAME) pg_isready -U postgres -d route2a >/dev/null 2>&1; then \
	    echo "PostgreSQL ready"; exit 0; fi; \
	  sleep 1; \
	done; echo "timeout waiting for PostgreSQL (check: docker logs $(PG_NAME))"; exit 1

pg-down:
	-docker rm -f $(PG_NAME)

# pg-reset drops the corpus tables so the next run reloads with the current schema
# (ingload.Load -> CreateSchema rebuilds all tables + GIN indexes fresh).
pg-reset:
	docker exec $(PG_NAME) psql -U postgres -d route2a -c "\
	DROP TABLE IF EXISTS service_versions, deploy_versions, gw_versions, vs_versions CASCADE;"
	@echo "corpus tables dropped; next bench-* / test run reloads with the current schema"

# ---- MariaDB container lifecycle (matches storeopen's default POC_MARIA_ADDR:
# root:root@tcp(127.0.0.1:3306)/route2a) ----
maria-up:
	docker run -d --rm --name $(MARIA_NAME) -p 3306:3306 \
	  -e MARIADB_ROOT_PASSWORD=root -e MARIADB_DATABASE=route2a \
	  $(MARIA_IMAGE)
	@echo "waiting for MariaDB to accept connections..."
	@for i in $$(seq 1 90); do \
	  if docker exec $(MARIA_NAME) mariadb-admin ping -uroot -proot --silent >/dev/null 2>&1; then \
	    echo "MariaDB ready"; exit 0; fi; \
	  sleep 1; \
	done; echo "timeout waiting for MariaDB (check: docker logs $(MARIA_NAME))"; exit 1

maria-down:
	-docker rm -f $(MARIA_NAME)

# maria-reset drops the main version tables (CreateSchema drops child tables too).
maria-reset:
	docker exec $(MARIA_NAME) mariadb -uroot -proot route2a -e "\
	DROP TABLE IF EXISTS service_versions, deploy_versions, gw_versions, vs_versions;"
	@echo "corpus tables dropped; next bench-* / test run reloads with the current schema"

# ---- backend-agnostic dispatchers: `make db-up DB=postgres`, etc. ----
db-up:
	@case "$(DB)" in \
	  clickhouse) $(MAKE) ch-up ;; \
	  postgres)   $(MAKE) pg-up ;; \
	  mariadb)    $(MAKE) maria-up ;; \
	  *) echo "unknown DB=$(DB) (want clickhouse|postgres|mariadb)"; exit 1 ;; \
	esac

db-down:
	@case "$(DB)" in \
	  clickhouse) $(MAKE) ch-down ;; \
	  postgres)   $(MAKE) pg-down ;; \
	  mariadb)    $(MAKE) maria-down ;; \
	  *) echo "unknown DB=$(DB) (want clickhouse|postgres|mariadb)"; exit 1 ;; \
	esac

db-reset:
	@case "$(DB)" in \
	  clickhouse) $(MAKE) ch-reset ;; \
	  postgres)   $(MAKE) pg-reset ;; \
	  mariadb)    $(MAKE) maria-reset ;; \
	  *) echo "unknown DB=$(DB) (want clickhouse|postgres|mariadb)"; exit 1 ;; \
	esac
