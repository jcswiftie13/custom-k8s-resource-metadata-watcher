.PHONY: build test vet e2e bench-collect routesim-build routercheck-bin e2e-routing

build:
	go build ./...

test:
	go test ./... -race -count=1

vet:
	go vet ./...

# e2e runs the full Kind-based integration suite. See
# docs/INTEGRATION_TESTS.md for what each scenario covers.
e2e:
	./test/integration/run.sh

# e2e-routing runs just the routing-history resolution scenario (exporter
# history ingest -> ClickHouse -> routesim range resolution). Reuse a kept
# cluster with SKIP_KIND_CREATE=1 for fast iteration.
e2e-routing:
	GOTEST_FLAGS="-run TestHistory_RoutingResolution" ./test/integration/run.sh

ROUTESIM_DIR   := test/integration/routesim
ROUTESIM_IMAGE ?= envoyproxy/envoy:tools-v1.34-latest

# routesim-build cross-compiles the routing-resolution e2e test binary
# (test/integration/routesim, a separate module — its istio.io/istio dependency
# stays out of the main go.mod). The binary runs INSIDE the Envoy tools image
# (linux) next to the native router_check_tool, so GOOS is always linux;
# GOARCH defaults to the host arch and must match the platform the tools image
# is pulled for. run.sh invokes this automatically.
routesim-build:
	cd $(ROUTESIM_DIR) && GOOS=linux GOARCH=$${ROUTESIM_GOARCH:-$$(go env GOARCH)} CGO_ENABLED=0 \
	  go test -c -o bin/routesim.test .

# routercheck-bin extracts the native router_check_tool (linux ELF) from the
# Envoy tools image — for linux hosts that want to run routesim.test directly
# instead of inside the container. Mirrors poc/route2a's target of the same name.
routercheck-bin:
	mkdir -p $(ROUTESIM_DIR)/bin
	docker pull $(ROUTESIM_IMAGE)
	cid=$$(docker create $(ROUTESIM_IMAGE)); \
	docker cp $$cid:/usr/local/bin/router_check_tool $(ROUTESIM_DIR)/bin/router_check_tool; \
	docker rm $$cid; \
	chmod +x $(ROUTESIM_DIR)/bin/router_check_tool

# bench-collect quantifies scrape-time cost as a function of N anchors and
# K dynamic keys. Use this to validate that a config change (especially
# adding expandLabels) hasn't blown up Collect() latency or allocation.
# Adjust -benchtime to taste; 3x is enough to spot >10% regressions.
bench-collect:
	go test -run NONE -bench BenchmarkCollect -benchmem -benchtime 3x ./pkg/collector
