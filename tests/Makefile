ROOT_DIR := $(shell git rev-parse --show-toplevel)
BIN_DIR  := $(ROOT_DIR)/bin
BIN_FILE := $(BIN_DIR)/loadtest

$(BIN_DIR):
	mkdir -p $@

.PHONY: build
build: $(BIN_DIR)
	go build -o $(BIN_FILE) loadtest.go

.PHONY: test
test:
	go test -count=1 ./...

.PHONY: clean
clean:
	rm $(BIN_FILE) || true
	rmdir $(BIN_DIR) || true
