# syntax=docker/dockerfile:1.7

FROM rust:1.91-bookworm AS chef

RUN cargo install --locked cargo-chef --version 0.1.78

WORKDIR /app

FROM chef AS planner

COPY . .
RUN cargo chef prepare --bin thanos-v1-reader --recipe-path recipe.json

FROM chef AS builder

ARG TARGETARCH

RUN apt-get update \
    && apt-get install --no-install-recommends -y libclang-dev libprotobuf-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

ENV PROTOC_INCLUDE=/usr/include

WORKDIR /app

# Keep the cooked dependency artifacts in the image layer. Unlike a target cache
# mount, this layer is exported by the GitHub Actions Buildx cache and can be reused
# by fresh CI runners whenever the Cargo recipe is unchanged.
COPY --from=planner /app/recipe.json recipe.json
RUN case "${TARGETARCH}" in \
        amd64) export CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v4" ;; \
        arm64) export CARGO_BUILD_TARGET=aarch64-unknown-linux-gnu ;; \
        *) echo "unsupported target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && cargo chef cook --release --locked --bin thanos-v1-reader --target "${CARGO_BUILD_TARGET}" --recipe-path recipe.json

COPY . .
RUN case "${TARGETARCH}" in \
        amd64) export CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v4" ;; \
        arm64) export CARGO_BUILD_TARGET=aarch64-unknown-linux-gnu ;; \
        *) echo "unsupported target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && cargo build --release --locked --bin thanos-v1-reader --target "${CARGO_BUILD_TARGET}" \
    && cp "target/${CARGO_BUILD_TARGET}/release/thanos-v1-reader" /app/thanos-v1-reader

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin reader
WORKDIR /app
COPY --from=builder /app/thanos-v1-reader /usr/local/bin/thanos-v1-reader

USER reader
ENTRYPOINT ["/usr/local/bin/thanos-v1-reader"]
