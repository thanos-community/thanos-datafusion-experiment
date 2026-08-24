.PHONY: reader-dev reader-watch

OTEL_SDK_DISABLED ?= true
RUST_LOG ?= debug,opentelemetry_sdk=warn

export OTEL_SDK_DISABLED RUST_LOG

reader-dev:
	cd thanos-v1-reader && cargo run

reader-watch:
	cd thanos-v1-reader && cargo watch -w src -w Cargo.toml -w dev.toml -x run
