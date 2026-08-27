#!/usr/bin/env bash
set -euo pipefail

# Generate a two-hour scalar TSDB block near 10 MiB and compare it with Vortex.
# PODS can be overridden when calibrating a different platform (default: 250).
repo=$(cd "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d "${TMPDIR:-/tmp}/thanos-vortex-benchmark.XXXXXX")
trap 'rm -rf "$work"' EXIT
pods=${PODS:-250}

(cd "$repo/thanos-block-gen" && go run . \
  --output "$work/blocks" --clean \
  --mint 1700000000000 --maxt 1700007200000 --samples 480 \
  --instances 50 --pods "$pods" --routes 5 \
  --native-histograms=false --downsample-5m=false)
block=$(find "$work/blocks" -mindepth 1 -maxdepth 1 -type d | head -n 1)
(cd "$repo/thanos-v1-reader" && CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run --quiet --bin convert -- \
  --report "file://$block" "file://$work/block.vortex" > "$work/vortex-storage.csv")
"$repo/thanos-v1-reader/benches/block-storage.sh" "$block" "$work/block.vortex" "$work/vortex-storage.csv"
