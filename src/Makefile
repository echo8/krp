ROOT_DIR    := $(shell git rev-parse --show-toplevel)
BIN_DIR     := $(ROOT_DIR)/bin
BIN_FILE    := $(BIN_DIR)/krp
DIST_DIR    := $(ROOT_DIR)/src/dist
LOCAL_CONF  := $(ROOT_DIR)/local/config.yaml
SCRIPTS_DIR := $(ROOT_DIR)/scripts

$(BIN_DIR):
	mkdir -p $@

.PHONY: build
build: $(BIN_DIR)
	go build -o $(BIN_FILE) main.go

.PHONY: test
test:
	go test -count=1 ./...

.PHONY: release-snapshot
release-snapshot:
	cp $(LOCAL_CONF) config.yaml
	docker run \
		-v $(ROOT_DIR):/tmp/krp \
		-i \
		--entrypoint '' \
		--privileged \
		techknowlogick/xgo:latest /bin/bash < $(SCRIPTS_DIR)/release-snapshot.sh
	rm config.yaml
	sudo chown -R $(shell id -u):$(shell id -g) $(DIST_DIR)

.PHONY: clean
clean:
	rm -rf $(BIN_DIR) || true
	rm -rf $(DIST_DIR) || true
