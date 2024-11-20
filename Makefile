ROOT_DIR             := $(shell git rev-parse --show-toplevel)
BIN_DIR              := $(ROOT_DIR)/bin
DOCS_DIR             := $(ROOT_DIR)/docs
SRC_DIR              := $(ROOT_DIR)/src
TESTS_DIR            := $(ROOT_DIR)/tests
LOCAL_DIR            := $(ROOT_DIR)/local
LOCAL_DOCKER_COMPOSE := $(LOCAL_DIR)/docker-compose.yaml
LOADTEST             := $(BIN_DIR)/loadtest
GO_RELEASER_CONF     := $(ROOT_DIR)/src/.goreleaser.yaml

.PHONY: build-krp
build-krp:
	make -C $(SRC_DIR) build

.PHONY: build-loadtest
build-loadtest:
	make -C $(TESTS_DIR) build

.PHONY: build-docs
build-docs:
	make -C $(DOCS_DIR) build

.PHONY: build
build: build-krp

.PHONY: test
test:
	make -C $(SRC_DIR) test

.PHONY: test-e2e
test-e2e: build-krp
	make -C $(TESTS_DIR) test

.PHONY: run-loadtest
run-loadtest: build-krp build-loadtest
	$(LOADTEST)

.PHONY: run-local
run-local: build-krp
	docker compose -f $(LOCAL_DOCKER_COMPOSE) build krp
	docker compose -f $(LOCAL_DOCKER_COMPOSE) up

.PHONY: serve-docs
serve-docs:
	make -C $(DOCS_DIR) serve

.PHONY: release-snapshot
release-snapshot:
	make -C $(SRC_DIR) release-snapshot

.PHONY: release
release:
	make -C $(SRC_DIR) release

.PHONY: clean
clean:
	make -C $(SRC_DIR) clean
	make -C $(TESTS_DIR) clean
	make -C $(DOCS_DIR) clean
