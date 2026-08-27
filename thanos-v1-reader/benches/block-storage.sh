#!/usr/bin/env bash
set -euo pipefail

# Usage: block-storage.sh <tsdb-block-directory> <converted-vortex-file> [vortex-report.csv]
# Reports the source TSDB index/chunks footprint against the flattened Vortex file.
block=$1
vortex=$2
report=${3:-}
index_bytes=$(stat -f %z "$block/index")
chunks_bytes=$(find "$block/chunks" -type f -exec stat -f %z {} + | awk '{total += $1} END {print total + 0}')
vortex_bytes=$(stat -f %z "$vortex")
printf 'format,index_bytes,hll_metadata_bytes,data_bytes,total_bytes\n'
printf 'tsdb,%s,0,%s,%s\n' "$index_bytes" "$chunks_bytes" "$((index_bytes + chunks_bytes))"
if [[ -n "$report" ]]; then
  awk 'NR == 2 { print; found = 1 } END { exit !found }' "$report"
else
  printf 'vortex,0,0,%s,%s\n' "$vortex_bytes" "$vortex_bytes"
fi
