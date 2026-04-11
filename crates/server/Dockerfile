FROM rust:1.94.1-slim AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY crates/server/Cargo.toml crates/server/Cargo.toml

RUN mkdir -p src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs && \
    mkdir -p crates/server/src && echo "fn main() {}" > crates/server/src/main.rs && \
    cargo build --release -p narrowdb-server && \
    rm -rf src crates/server/src

COPY src/ src/
COPY crates/server/src/ crates/server/src/

RUN touch src/lib.rs src/main.rs crates/server/src/main.rs && \
    cargo build --release -p narrowdb-server

FROM debian:bookworm-slim

COPY --from=builder /build/target/release/narrowdb-server /usr/local/bin/narrowdb-server

EXPOSE 5433

ENV NARROWDB_PATH=/data/narrowdb.db
ENV NARROWDB_LISTEN=0.0.0.0:5433

ENTRYPOINT ["narrowdb-server"]
