.PHONY: test test-race test-integration test-compatibility lint coverage coverage-html clean

test:
	go test -v -count=1 ./pkg/kubemq/...

test-race:
	go test -v -race -count=1 ./pkg/kubemq/...

test-integration:
	go test -v -race -count=1 -tags=integration -timeout=5m ./pkg/kubemq/...

test-compatibility:
	go test -v -race -count=1 -tags=compatibility -timeout=10m ./pkg/kubemq/...

lint:
	golangci-lint run ./...

coverage:
	go test -v -race -count=1 -coverprofile=coverage.out ./pkg/kubemq/...
	go tool cover -func=coverage.out | tail -1

coverage-html:
	go test -v -race -count=1 -coverprofile=coverage.out ./pkg/kubemq/...
	go tool cover -html=coverage.out -o coverage.html

clean:
	rm -f coverage.out coverage.html
