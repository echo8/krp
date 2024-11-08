ROOT_DIR  := $(shell git rev-parse --show-toplevel)
BIN_DIR   := $(ROOT_DIR)/bin
SRC_DIR   := $(ROOT_DIR)/src
TESTS_DIR := $(ROOT_DIR)/tests
LOCAL_DIR := $(ROOT_DIR)/local
LOADTEST  := $(BIN_DIR)/loadtest

PHONY: build-krp
build-krp:
	make -C $(SRC_DIR) build

PHONY: build-loadtest
build-loadtest:
	make -C $(TESTS_DIR) build

PHONY: build
build: build-krp

PHONY: test
test:
	make -C $(SRC_DIR) test

PHONY: test-e2e
test-e2e: build-krp
	make -C $(TESTS_DIR) test

PHONY: run-loadtest
run-loadtest: build-krp build-loadtest
	$(LOADTEST)

PHONY: run-local
run-local: build-krp
	docker compose -f $(LOCAL_DIR)/docker-compose.yaml up

PHONY: clean
clean:
	make -C $(SRC_DIR) clean
	make -C $(TESTS_DIR) clean
