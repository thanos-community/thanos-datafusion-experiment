.PHONY: clean e2e-test gen-dev-series reader-dev reader-watch store-api-conformance-test test

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
	cd thanos-v1-reader && cargo test --test e2e --test native_histogram_parity --test raw_scalar_parity --test scalar_downsample_parity --test native_histogram_downsample_parity --test native_histogram_query_parity

store-api-conformance-test:
	cd thanos-v1-reader && cargo build --bin thanos-v1-reader
	cd thanos-block-gen && THANOS_V1_READER_BIN="../thanos-v1-reader/target/debug/thanos-v1-reader" go test -run '^TestThanosV1ReaderStoreAPIConformance$$' -count=1

test:
	cd thanos-v1-reader && cargo test
	cd thanos-block-gen && go test ./...
