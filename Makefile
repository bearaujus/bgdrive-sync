BIN_NAME := bgdrive-sync

.PHONY: build-server
build-server:
	GOOS=linux GOARCH=amd64 go build -o bin/$(BIN_NAME)_server cmd/*.go

.PHONY: build
build:
	go build -o bin/$(BIN_NAME) cmd/*.go

.PHONY: run
run: build
	bin/$(BIN_NAME)
