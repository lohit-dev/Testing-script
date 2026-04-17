FROM rust:1.88-bookworm AS builder

WORKDIR /app

# Native build deps commonly needed by crypto/network crates.
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build both binaries so runtime can choose either.
RUN cargo build --release --bin testing_script --bin flood_test


FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/testing_script /usr/local/bin/testing_script
COPY --from=builder /app/target/release/flood_test /usr/local/bin/flood_test
COPY run-forever.sh /usr/local/bin/run-forever.sh
RUN chmod +x /usr/local/bin/run-forever.sh

# Runtime data dir used by STORAGE_DIR in .env (default: ./tmp/spark_test_sdk)
RUN mkdir -p /app/tmp/spark_test_sdk
RUN mkdir -p /app/reports

# Loop runner for Coolify long-running deployments.
# RUN_MODE: testing_script | flood_test
# RESTART_DELAY_SECONDS: delay before re-running after completion
ENTRYPOINT ["/usr/local/bin/run-forever.sh"]
