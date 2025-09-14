# Chabi - Alternative to Redis / DiceDB

Chabi is a Rust-based reimplementation of core Redis functionality with a focus on correctness, async safety, and approachable code. It ships with a Redis-compatible TCP server, a small HTTP API, an integration tester.

Status: early development, but end-to-end Redis/HTTP tests currently pass.

## Features
- Redis protocol server (RESP) with async command handlers
- Implemented commands:
  - Connection: PING, ECHO
  - Strings: SET, GET, DEL, EXISTS, APPEND, STRLEN
  - Lists: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN
  - Sets: SADD, SMEMBERS, SISMEMBER, SCARD, SREM
  - Hashes: HSET, HGET, HGETALL, HEXISTS, HDEL, HLEN, HKEYS, HVALS
  - Keys: KEYS, TTL, EXPIRE, RENAME, TYPE
  - Server: INFO, SAVE
  - Docs: DOCS, COMMAND
  - Pub/Sub: PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PUBSUB (basic)
- HTTP server with minimal key-value endpoints (for demos)
- Integration tester exercises Redis and HTTP paths

## Project Structure
- packages/
  - chabi-core: RESP, command implementations, shared types
  - chabi-server: runs Redis + HTTP servers, wires command registry
  - chabi-redis-handler: reusable RESP server implementation (experimental)
  - chabi-http-handler: reusable HTTP handler (experimental)
- apps/
  - chabi-tester: end-to-end tests for commands via Redis and HTTP
  - membench: lightweight Go benchmark harness

## Quickstart
Prerequisites: Rust (stable), optionally Go (for membench)

- Build everything:
  make build

- Run the server (defaults to ports 6379 and 8080):
  make run-server
  # or set custom ports
  REDIS_PORT=6380 HTTP_PORT=8081 make run-server

- Run integration tests (starts server, runs tests, shuts down):
  make test

- Run quick benchmark (requires Go):
  make bench

## Configuration
The server reads ports from environment variables (with defaults):
- REDIS_PORT (default 6379)
- HTTP_PORT (default 8080)

## Benchmarks (illustrative)

We use [membench](https://github.com/DiceDB/membench) developed by DiceDB Team for benchmarking Chabi.

On a Hetzner CCX23 machine (4 vCPU, 16GB RAM):

| Metric               | DiceDB   | Redis    | Chabi    |
| -------------------- | -------- | -------- | -------- |
| Throughput (ops/sec) | 15655    | 12267    | 35633    |
| GET p50 (ms)         | 0.227327 | 0.270335 | 0.071167 |
| GET p90 (ms)         | 0.337919 | 0.329727 | 0.108543 |
| SET p50 (ms)         | 0.230399 | 0.272383 | 0.072703 |
| SET p90 (ms)         | 0.339967 | 0.331775 | 0.111103 |

Run the scheduled/dispatchable benchmark in CI under the Benchmarks workflow or locally via make bench.

## Development
- Format: cargo fmt --all
- Lint: cargo clippy --workspace --all-targets -- -D warnings
- Workspace members are defined in the root Cargo.toml.

## Continuous Integration
We provide GitHub Actions workflows:
- CI: builds, formats, lints, launches server, runs tester
- Benchmarks: builds a release server and runs membench

## Notes
- TTL/EXPIRE semantics are implemented with an async expirations map.
- Pub/Sub is basic and suitable for functional tests.
- This repository is evolving; APIs and internals may change.