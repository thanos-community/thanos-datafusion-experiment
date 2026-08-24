.PHONY: clean e2e-test gen-dev-series reader-dev reader-watch test

OTEL_SDK_DISABLED ?= true
RUST_LOG ?= debug,opentelemetry_sdk=warn,datafusion=info

export OTEL_SDK_DISABLED RUST_LOG

clean:
	rm -rf thanos-v1-reader/target
	rm -rf thanos-block-gen/target/index_cache

gen-dev-series:
	cd thanos-block-gen && go run . 

reader-dev:
	cd thanos-v1-reader && cargo run

reader-watch:
	cd thanos-v1-reader && cargo watch -w src -w Cargo.toml -w dev.toml -x run

e2e-test:
	cd thanos-v1-reader && cargo test --test e2e

test:
	cd thanos-v1-reader && cargo test
	cd thanos-block-gen && go test ./...
