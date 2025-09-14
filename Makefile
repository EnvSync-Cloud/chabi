SHELL := /bin/bash
.PHONY: all build test lint fmt clippy run-server tester bench clean help

# Default ports (can be overridden)
REDIS_PORT ?= 6379
HTTP_PORT ?= 8080

all: build

build:
	cargo build --workspace

fmt:
	cargo fmt --all

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

lint: fmt clippy

# Run the Redis/HTTP server (foreground)
dev:
	REDIS_PORT=$(REDIS_PORT) HTTP_PORT=$(HTTP_PORT) cargo run -p chabi-server

# Convenience: run the Rust integration tester against a running server
# Starts the server, waits for ports, runs tests, then stops the server
# Usage: make test REDIS_PORT=6380 HTTP_PORT=8081
# or simply: make test
test: tester

tester:
	@set -e; \
	REDIS_PORT=$(REDIS_PORT) HTTP_PORT=$(HTTP_PORT) cargo run -p chabi-server & \
	SERVER_PID=$$!; \
	echo "Starting chabi-server (pid=$$SERVER_PID) on ports $$REDIS_PORT/$$HTTP_PORT..."; \
	for i in `seq 1 50`; do \
	  (echo > /dev/tcp/127.0.0.1/$$REDIS_PORT) >/dev/null 2>&1 && break; \
	  sleep 0.2; \
	done; \
	for i in `seq 1 50`; do \
	  (echo > /dev/tcp/127.0.0.1/$$HTTP_PORT) >/dev/null 2>&1 && break; \
	  sleep 0.2; \
	done; \
	cargo run -p chabi-tester; \
	STATUS=$$?; \
	kill $$SERVER_PID || true; \
	wait $$SERVER_PID || true; \
	exit $$STATUS

# Quick local benchmark using Go membench (optional)
# Requires Go toolchain installed
bench:
	@which go >/dev/null 2>&1 || { echo "Go not found. Install Go to run membench."; exit 1; }; \
	REDIS_PORT=$(REDIS_PORT) HTTP_PORT=$(HTTP_PORT) cargo run -p chabi-server & \
	SERVER_PID=$$!; \
	echo "Starting chabi-server for membench (pid=$$SERVER_PID) on port $$REDIS_PORT..."; \
	for i in `seq 1 50`; do \
	  (echo > /dev/tcp/127.0.0.1/$$REDIS_PORT) >/dev/null 2>&1 && break; \
	  sleep 0.2; \
	done; \
	cd apps/membench && make run-redis; \
	STATUS=$$?; \
	kill $$SERVER_PID || true; \
	wait $$SERVER_PID || true; \
	exit $$STATUS

clean:
	cargo clean

help:
	@echo "Chabi Makefile"; \
	echo; \
	echo "Targets:"; \
	echo "  build          - cargo build --workspace"; \
	echo "  fmt            - cargo fmt --all"; \
	echo "  clippy         - cargo clippy (deny warnings)"; \
	echo "  lint           - run fmt + clippy"; \
	echo "  dev     	   - run chabi-server (use REDIS_PORT/HTTP_PORT vars)"; \
	echo "  test           - start server, run Rust integration tester, stop server"; \
	echo "  bench          - start server, run Go membench quick-run, stop server"; \
	echo "  clean          - cargo clean"; \
	echo; \
	echo "Variables:"; \
	echo "  REDIS_PORT     - Redis port (default 6379)"; \
	echo "  HTTP_PORT      - HTTP port (default 8080)";