# Stage 1: Build
FROM rust:1.85-slim AS builder

RUN apt-get update && apt-get install -y pkg-config && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY benches/ benches/
COPY build.rs ./

RUN cargo build --release

# Copy man pages to a known location for the runtime stage
RUN mkdir -p /build/man && cp /build/target/release/build/innodb-utils-*/out/man/*.1 /build/man/

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/inno /usr/local/bin/inno
COPY --from=builder /build/man/ /usr/local/share/man/man1/

ENTRYPOINT ["inno"]
CMD ["--help"]
