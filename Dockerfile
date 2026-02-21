# ---- Builder stage ----
FROM rust:latest AS builder

WORKDIR /app

# Copy workspace manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY packages/chabi-core/Cargo.toml packages/chabi-core/Cargo.toml
COPY packages/chabi-server/Cargo.toml packages/chabi-server/Cargo.toml
COPY packages/chabi-redis-handler/Cargo.toml packages/chabi-redis-handler/Cargo.toml
COPY packages/chabi-http-handler/Cargo.toml packages/chabi-http-handler/Cargo.toml
COPY apps/chabi-tester/Cargo.toml apps/chabi-tester/Cargo.toml

# Create dummy src files to cache dependency compilation
RUN mkdir -p packages/chabi-core/src && echo "pub fn _dummy() {}" > packages/chabi-core/src/lib.rs && \
    mkdir -p packages/chabi-server/src && echo "fn main() {}" > packages/chabi-server/src/main.rs && \
    mkdir -p packages/chabi-redis-handler/src && echo "pub fn _dummy() {}" > packages/chabi-redis-handler/src/lib.rs && \
    mkdir -p packages/chabi-http-handler/src && echo "pub fn _dummy() {}" > packages/chabi-http-handler/src/lib.rs && \
    mkdir -p apps/chabi-tester/src && echo "fn main() {}" > apps/chabi-tester/src/main.rs

RUN cargo build --release -p chabi-server 2>/dev/null || true

# Copy actual source code
COPY packages/ packages/
COPY apps/ apps/

# Touch source files to invalidate cached builds of our code (not deps)
RUN touch packages/chabi-core/src/lib.rs && \
    touch packages/chabi-server/src/main.rs && \
    touch packages/chabi-redis-handler/src/lib.rs && \
    touch packages/chabi-http-handler/src/lib.rs

RUN cargo build --release -p chabi-server

# ---- Runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/chabi-server /usr/local/bin/chabi-server

ENV REDIS_PORT=6379
ENV HTTP_PORT=8080
ENV BIND_HOST=0.0.0.0
ENV SNAPSHOT_PATH=/data

RUN mkdir -p /data

EXPOSE 6379 8080

ENTRYPOINT ["chabi-server"]
