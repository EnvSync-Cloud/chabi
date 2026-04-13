# Chabi

Chabi is a Rust Redis-compatible server with snapshot persistence, a minimal HTTP operational surface, and an in-repo docs site.

Docs:

- Deployed docs: <https://envsync-cloud.github.io/chabi/>
- Local docs app: [`apps/docs`](./apps/docs)

## Run Chabi

Build the workspace:

```bash
make build
```

Start the server:

```bash
make dev
```

Run integration tests:

```bash
make test
```

Run the local benchmark harness:

```bash
make bench
```

## Benchmarks

Published benchmark details live in the docs:

- Methodology: <https://envsync-cloud.github.io/chabi/docs/benchmarks/methodology/>
- Results: <https://envsync-cloud.github.io/chabi/docs/benchmarks/results/>

On a Hetzner CCX23 machine (4 vCPU, 16GB RAM):

| Metric | DiceDB | Redis | Chabi |
| --- | ---: | ---: | ---: |
| Throughput (ops/sec) | 15655 | 12267 | 35633 |
| GET p50 (ms) | 0.227327 | 0.270335 | 0.071167 |
| GET p90 (ms) | 0.337919 | 0.329727 | 0.108543 |
| SET p50 (ms) | 0.230399 | 0.272383 | 0.072703 |
| SET p90 (ms) | 0.339967 | 0.331775 | 0.111103 |

## Docs Development

Install workspace dependencies:

```bash
pnpm install
```

Run the docs app locally:

```bash
pnpm docs:dev
```

Type-check the docs app:

```bash
pnpm docs:check
```

Build the static export:

```bash
pnpm docs:build
```

## Project Layout

- `packages/chabi-core`: RESP types, command implementations, storage
- `packages/chabi-server`: Redis server, HTTP server, snapshots, metrics
- `packages/chabi-redis-handler`: experimental reusable RESP handler
- `packages/chabi-http-handler`: experimental reusable HTTP handler
- `apps/chabi-tester`: integration tests
- `apps/membench`: Go benchmark harness
- `apps/docs`: Next.js + Fumadocs static docs site
