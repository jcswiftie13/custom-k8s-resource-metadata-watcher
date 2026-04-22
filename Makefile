.PHONY: build test vet e2e

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
