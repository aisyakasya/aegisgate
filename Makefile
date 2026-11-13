.PHONY: all build build-ui build-go test race bench selftest run clean

BINARY_NAME := aegisgate

all: build-ui build-go

build-ui:
	cd web && npm install && npm run build

build-go:
	go build -trimpath -ldflags="-s -w" -o bin/$(BINARY_NAME) ./cmd/aegisgate

build: all

test:
	go test -v ./...

race:
	go test -race ./...

bench:
	go test -bench=. -benchmem ./internal/...

selftest:
	go run ./cmd/aegisgate selftest

run: build
	./bin/$(BINARY_NAME) serve --listen :8080 --control :9090 --mode RECORD

clean:
	rm -rf bin/ .aegisgate/ web/dist/ coverage.out
