#!/usr/bin/env bash
set -euo pipefail

# Usage: block-storage.sh <tsdb-block-directory> <converted-parquet-file>
# Reports the source TSDB index/chunks footprint against the flattened Parquet file.
block=$1
parquet=$2
index_bytes=$(stat -f %z "$block/index")
chunks_bytes=$(find "$block/chunks" -type f -exec stat -f %z {} + | awk '{total += $1} END {print total + 0}')
parquet_bytes=$(stat -f %z "$parquet")
printf 'format,index_bytes,sample_bytes,total_bytes\n'
printf 'tsdb,%s,%s,%s\n' "$index_bytes" "$chunks_bytes" "$((index_bytes + chunks_bytes))"
printf 'parquet,0,%s,%s\n' "$parquet_bytes" "$parquet_bytes"
