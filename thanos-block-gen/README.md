# Thanos block generator

`thanos-block-gen` writes local, Thanos-compatible Prometheus TSDB fixture blocks.
It creates dummy counter, gauge, classic histogram, integer native histogram, and
float native histogram series. By default, it creates a one-hour block ending now,
with 240 samples per series and 5,700 series across instances, pods, routes, and
histogram dimensions. Every metric family includes 100 distinct `pod` label values.
It also creates a 5-minute downsampled block.

Each block contains practical Thanos metadata: external labels, source, downsample
resolution, compaction lineage, index statistics, upload time, and SHA-256 hashes
with sizes for every block data file. The CLI prints the resolved configuration as
an `info` log line before generation, followed by every generated metric name, its
metric type, series count, and cardinality for each label in an aligned terminal
table.

## Run

```bash
go run . --output ./target --clean
```

The target directory receives one raw block and one block whose `meta.json` reports
a Thanos downsample resolution of `300000` milliseconds.

## Options

```bash
go run . \
  --output ./target \
  --clean \
  --samples 120 \
  --instances 20 \
  --pods 100 \
  --routes 5 \
  --native-series 10 \
  --external-label cluster=dev \
  --external-label replica=0 \
  --downsample-5m=true
```

Use `--mint` and `--maxt` to set the inclusive/exclusive sample range as Unix
milliseconds. The range must be at least five minutes when generating 5m blocks.
