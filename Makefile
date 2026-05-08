.PHONY: build test vet e2e bench-collect

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

# bench-collect quantifies scrape-time cost as a function of N anchors and
# K dynamic keys. Use this to validate that a config change (especially
# adding expandLabels) hasn't blown up Collect() latency or allocation.
# Adjust -benchtime to taste; 3x is enough to spot >10% regressions.
bench-collect:
	go test -run NONE -bench BenchmarkCollect -benchmem -benchtime 3x ./pkg/collector
