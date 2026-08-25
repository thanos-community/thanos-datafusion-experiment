# syntax=docker/dockerfile:1.7

FROM rust:1.91-bookworm AS builder

ARG TARGETARCH

RUN apt-get update \
    && apt-get install --no-install-recommends -y libprotobuf-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

ENV PROTOC_INCLUDE=/usr/include

WORKDIR /app

# Build all dependency crates before adding the application sources. The build script
# and vendored schemas are included because generating the protobuf bindings needs protoc.
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,id=thanos-v1-reader-target-${TARGETARCH},target=/app/target \
    mkdir src \
    && printf 'fn main() {}\n' > src/main.rs \
    && case "${TARGETARCH}" in \
        amd64) export CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v4" ;; \
        arm64) export CARGO_BUILD_TARGET=aarch64-unknown-linux-gnu ;; \
        *) echo "unsupported target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,id=thanos-v1-reader-target-${TARGETARCH},target=/app/target \
    case "${TARGETARCH}" in \
        amd64) export CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-cpu=x86-64-v4" ;; \
        arm64) export CARGO_BUILD_TARGET=aarch64-unknown-linux-gnu ;; \
        *) echo "unsupported target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && cargo build --release --locked \
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
