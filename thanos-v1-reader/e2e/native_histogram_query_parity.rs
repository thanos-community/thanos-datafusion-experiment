use std::collections::BTreeMap;

use arrow::array::{
    Array, Float64Array, Int32Array, Int64Array, ListArray, StringArray, StructArray,
    TimestampMillisecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use serde::Deserialize;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::ThanosRepositoryConfig,
    index_context,
};

const RESOLUTION_5M: i64 = 5 * 60 * 1000;
const RESOLUTION_1H: i64 = 60 * 60 * 1000;

#[derive(Debug, Deserialize)]
struct OracleSeries {
    labels: BTreeMap<String, String>,
    chunks: Vec<OracleAggrChunk>,
}

#[derive(Debug, Deserialize)]
struct OracleAggrChunk {
    #[serde(default)]
    histograms: Vec<OracleHistogram>,
    count: Option<OracleEncodedChunk>,
    sum: Option<OracleEncodedChunk>,
    counter: Option<OracleEncodedChunk>,
}

#[derive(Debug, Deserialize)]
struct OracleEncodedChunk {
    #[serde(default)]
    samples: Vec<OracleSample>,
    #[serde(default)]
    histograms: Vec<OracleHistogram>,
}

#[derive(Debug, Deserialize)]
struct OracleSample {
    timestamp: i64,
    value_bits: u64,
}

#[derive(Debug, Deserialize)]
struct OracleHistogram {
    timestamp: i64,
    kind: String,
    counter_reset_hint: u8,
    schema: i32,
    count: u64,
    sum_bits: u64,
    zero_threshold_bits: u64,
    zero_count: u64,
    positive_spans: Vec<OracleSpan>,
    #[serde(default)]
    positive_buckets: Vec<i64>,
    #[serde(default)]
    positive_bucket_bits: Vec<u64>,
    negative_spans: Vec<OracleSpan>,
    #[serde(default)]
    negative_buckets: Vec<i64>,
    #[serde(default)]
    negative_bucket_bits: Vec<u64>,
    #[serde(default)]
    custom_value_bits: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct OracleSpan {
    offset: i32,
    length: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct HistogramRow {
    timestamp: i64,
    aggregate: String,
    resolution: i64,
    labels: BTreeMap<String, String>,
    kind: String,
    schema: i32,
    count_int: Option<u64>,
    count_float_bits: Option<u64>,
    sum_bits: u64,
    zero_threshold_bits: u64,
    zero_count_int: Option<u64>,
    zero_count_float_bits: Option<u64>,
    reset_hint: u8,
    positive_spans: Vec<OracleSpan>,
    negative_spans: Vec<OracleSpan>,
    positive_buckets_int: Option<Vec<i64>>,
    negative_buckets_int: Option<Vec<i64>>,
    positive_bucket_bits: Option<Vec<u64>>,
    negative_bucket_bits: Option<Vec<u64>>,
    custom_value_bits: Vec<u64>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ScalarRow {
    timestamp: i64,
    value_bits: u64,
    resolution: i64,
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Copy)]
enum OracleSlot {
    Raw,
    Sum,
    Counter,
}

#[tokio::test]
async fn datafusion_native_histograms_match_go_decoded_chunks() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-native-query-e2e-{}",
        std::process::id()
    ));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator_directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");
    let status = std::process::Command::new("go")
        .args([
            "run",
            ".",
            "--output",
            blocks.to_str().unwrap(),
            "--clean",
            "--mint",
            "1700000000000",
            "--maxt",
            "1700010800000",
            "--samples",
            "720",
            "--instances",
            "1",
            "--pods",
            "1",
            "--routes",
            "1",
            "--native-series",
            "1",
            "--downsample-5m=true",
            "--downsample-1h=true",
        ])
        .current_dir(&generator_directory)
        .status()
        .unwrap();
    assert!(status.success());

    let repository = ThanosRepositoryConfig {
        name: "e2e".to_owned(),
        uri: format!("file://{}", blocks.display()),
    };
    let schemas = build_block_index(std::slice::from_ref(&repository), cache.to_str().unwrap())
        .await
        .unwrap();
    let context = index_context(
        &block_index_file_path(cache.to_str().unwrap()),
        &chunk_index_directory_path(cache.to_str().unwrap()),
        &schemas,
        std::slice::from_ref(&repository),
    )
    .await
    .unwrap();

    for metric in ["dummy_native_histogram", "dummy_float_native_histogram"] {
        let raw_oracle = go_bucket_store_series(&generator_directory, &blocks, metric, "raw", 0);
        let expected_raw = oracle_rows(&raw_oracle, OracleSlot::Raw, 0);
        let actual_raw = query_rows(&context, metric, 0, None).await;
        assert_eq!(actual_raw, expected_raw, "raw SQL parity for {metric}");

        for resolution in [RESOLUTION_5M, RESOLUTION_1H] {
            let oracle = go_bucket_store_series(
                &generator_directory,
                &blocks,
                metric,
                "count,sum,counter",
                resolution,
            );
            let mut expected_count = oracle_scalar_rows(&oracle, resolution);
            let mut actual_count = query_scalar_rows(&context, metric, resolution).await;
            expected_count.sort();
            actual_count.sort();
            assert_eq!(
                actual_count, expected_count,
                "COUNT SQL parity for {metric} at {resolution}"
            );
            let expected_counter = oracle_rows(&oracle, OracleSlot::Counter, resolution);
            let actual_counter = query_rows(&context, metric, resolution, None).await;
            assert_eq!(
                actual_counter, expected_counter,
                "default COUNTER SQL parity for {metric} at {resolution}"
            );

            let expected_sum = oracle_rows(&oracle, OracleSlot::Sum, resolution);
            let actual_sum = query_rows(&context, metric, resolution, Some("sum")).await;
            assert_eq!(
                actual_sum, expected_sum,
                "SUM SQL parity for {metric} at {resolution}"
            );
            assert!(
                query_rows(&context, metric, resolution, Some("min"))
                    .await
                    .is_empty()
            );
            assert!(
                query_rows(&context, metric, resolution, Some("max"))
                    .await
                    .is_empty()
            );
        }
    }

    std::fs::remove_dir_all(root).unwrap();
}

async fn query_scalar_rows(
    context: &datafusion::prelude::SessionContext,
    metric: &str,
    resolution: i64,
) -> Vec<ScalarRow> {
    let batches = context
        .sql(&format!(
            "SELECT timestamp, value, histogram, downsample_resolution, \"cluster\", job, pod, replica, series \
             FROM metrics.{metric} WHERE downsample_resolution = {resolution} AND aggregate_kind = 'count'"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let timestamps = column::<TimestampMillisecondArray>(&batch, "timestamp");
        let values = column::<Float64Array>(&batch, "value");
        let histograms = column::<StructArray>(&batch, "histogram");
        let resolutions = column::<Int64Array>(&batch, "downsample_resolution");
        let cluster = column::<StringArray>(&batch, "cluster");
        let job = column::<StringArray>(&batch, "job");
        let pod = column::<StringArray>(&batch, "pod");
        let replica = column::<StringArray>(&batch, "replica");
        let series = column::<StringArray>(&batch, "series");
        for index in 0..batch.num_rows() {
            assert!(!values.is_null(index));
            assert!(histograms.is_null(index));
            rows.push(ScalarRow {
                timestamp: timestamps.value(index),
                value_bits: values.value(index).to_bits(),
                resolution: resolutions.value(index),
                labels: BTreeMap::from([
                    ("cluster".to_owned(), cluster.value(index).to_owned()),
                    ("job".to_owned(), job.value(index).to_owned()),
                    ("pod".to_owned(), pod.value(index).to_owned()),
                    ("replica".to_owned(), replica.value(index).to_owned()),
                    ("series".to_owned(), series.value(index).to_owned()),
                ]),
            });
        }
    }
    rows
}

fn oracle_scalar_rows(series: &[OracleSeries], resolution: i64) -> Vec<ScalarRow> {
    series
        .iter()
        .flat_map(|series| {
            series.chunks.iter().flat_map(move |chunk| {
                chunk
                    .count
                    .as_ref()
                    .expect("COUNT slot")
                    .samples
                    .iter()
                    .map(move |sample| ScalarRow {
                        timestamp: sample.timestamp,
                        value_bits: sample.value_bits,
                        resolution,
                        labels: series
                            .labels
                            .iter()
                            .filter(|(name, _)| name.as_str() != "__name__")
                            .map(|(name, value)| (name.clone(), value.clone()))
                            .collect(),
                    })
            })
        })
        .collect()
}

async fn query_rows(
    context: &datafusion::prelude::SessionContext,
    metric: &str,
    resolution: i64,
    aggregate: Option<&str>,
) -> Vec<HistogramRow> {
    let aggregate_filter = aggregate
        .map(|aggregate| format!(" AND aggregate_kind = '{aggregate}'"))
        .unwrap_or_default();
    let batches = context
        .sql(&format!(
            "SELECT timestamp, value, histogram, downsample_resolution, aggregate_kind, \"cluster\", job, pod, replica, series \
             FROM metrics.{metric} WHERE downsample_resolution = {resolution}{aggregate_filter}"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let timestamps = column::<TimestampMillisecondArray>(&batch, "timestamp");
        let values = column::<Float64Array>(&batch, "value");
        let histograms = column::<StructArray>(&batch, "histogram");
        let resolutions = column::<Int64Array>(&batch, "downsample_resolution");
        let aggregates = column::<StringArray>(&batch, "aggregate_kind");
        let cluster = column::<StringArray>(&batch, "cluster");
        let job = column::<StringArray>(&batch, "job");
        let pod = column::<StringArray>(&batch, "pod");
        let replica = column::<StringArray>(&batch, "replica");
        let series = column::<StringArray>(&batch, "series");
        for index in 0..batch.num_rows() {
            assert!(values.is_null(index));
            assert!(!histograms.is_null(index));
            rows.push(HistogramRow {
                timestamp: timestamps.value(index),
                aggregate: aggregates.value(index).to_owned(),
                resolution: resolutions.value(index),
                labels: BTreeMap::from([
                    ("cluster".to_owned(), cluster.value(index).to_owned()),
                    ("job".to_owned(), job.value(index).to_owned()),
                    ("pod".to_owned(), pod.value(index).to_owned()),
                    ("replica".to_owned(), replica.value(index).to_owned()),
                    ("series".to_owned(), series.value(index).to_owned()),
                ]),
                kind: struct_string(histograms, "kind", index),
                schema: struct_column::<Int32Array>(histograms, "schema").value(index),
                count_int: nullable_u64(histograms, "count_int", index),
                count_float_bits: nullable_f64_bits(histograms, "count_float", index),
                sum_bits: struct_column::<Float64Array>(histograms, "sum")
                    .value(index)
                    .to_bits(),
                zero_threshold_bits: struct_column::<Float64Array>(histograms, "zero_threshold")
                    .value(index)
                    .to_bits(),
                zero_count_int: nullable_u64(histograms, "zero_count_int", index),
                zero_count_float_bits: nullable_f64_bits(histograms, "zero_count_float", index),
                reset_hint: struct_column::<UInt8Array>(histograms, "reset_hint").value(index),
                positive_spans: span_values(histograms, "positive_spans", index),
                negative_spans: span_values(histograms, "negative_spans", index),
                positive_buckets_int: i64_list(histograms, "positive_buckets_int", index),
                negative_buckets_int: i64_list(histograms, "negative_buckets_int", index),
                positive_bucket_bits: f64_list_bits(histograms, "positive_buckets_float", index),
                negative_bucket_bits: f64_list_bits(histograms, "negative_buckets_float", index),
                custom_value_bits: f64_list_bits(histograms, "custom_values", index)
                    .unwrap_or_default(),
            });
        }
    }
    sort_rows(&mut rows);
    rows
}

fn oracle_rows(series: &[OracleSeries], slot: OracleSlot, resolution: i64) -> Vec<HistogramRow> {
    let mut rows = series
        .iter()
        .flat_map(|series| {
            series.chunks.iter().flat_map(move |chunk| {
                let (aggregate, histograms) = match slot {
                    OracleSlot::Raw => ("raw", chunk.histograms.as_slice()),
                    OracleSlot::Sum => (
                        "sum",
                        chunk.sum.as_ref().expect("SUM slot").histograms.as_slice(),
                    ),
                    OracleSlot::Counter => (
                        "counter",
                        chunk
                            .counter
                            .as_ref()
                            .expect("COUNTER slot")
                            .histograms
                            .as_slice(),
                    ),
                };
                histograms.iter().map(move |histogram| HistogramRow {
                    timestamp: histogram.timestamp,
                    aggregate: aggregate.to_owned(),
                    resolution,
                    labels: series
                        .labels
                        .iter()
                        .filter(|(name, _)| name.as_str() != "__name__")
                        .map(|(name, value)| (name.clone(), value.clone()))
                        .collect(),
                    kind: histogram.kind.clone(),
                    schema: histogram.schema,
                    count_int: (histogram.kind == "histogram").then_some(histogram.count),
                    count_float_bits: (histogram.kind == "float_histogram")
                        .then_some(histogram.count),
                    sum_bits: histogram.sum_bits,
                    zero_threshold_bits: histogram.zero_threshold_bits,
                    zero_count_int: (histogram.kind == "histogram").then_some(histogram.zero_count),
                    zero_count_float_bits: (histogram.kind == "float_histogram")
                        .then_some(histogram.zero_count),
                    reset_hint: histogram.counter_reset_hint,
                    positive_spans: histogram.positive_spans.clone(),
                    negative_spans: histogram.negative_spans.clone(),
                    positive_buckets_int: (histogram.kind == "histogram")
                        .then(|| histogram.positive_buckets.clone()),
                    negative_buckets_int: (histogram.kind == "histogram")
                        .then(|| histogram.negative_buckets.clone()),
                    positive_bucket_bits: (histogram.kind == "float_histogram")
                        .then(|| histogram.positive_bucket_bits.clone()),
                    negative_bucket_bits: (histogram.kind == "float_histogram")
                        .then(|| histogram.negative_bucket_bits.clone()),
                    custom_value_bits: histogram.custom_value_bits.clone(),
                })
            })
        })
        .collect::<Vec<_>>();
    sort_rows(&mut rows);
    rows
}

fn sort_rows(rows: &mut [HistogramRow]) {
    rows.sort_by(|left, right| {
        (&left.labels, left.timestamp, &left.aggregate, left.sum_bits).cmp(&(
            &right.labels,
            right.timestamp,
            &right.aggregate,
            right.sum_bits,
        ))
    });
}

fn column<'a, T: 'static>(batch: &'a arrow::record_batch::RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<T>()
        .unwrap()
}

fn struct_column<'a, T: 'static>(array: &'a StructArray, name: &str) -> &'a T {
    array
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<T>()
        .unwrap()
}

fn struct_string(array: &StructArray, name: &str, index: usize) -> String {
    struct_column::<StringArray>(array, name)
        .value(index)
        .to_owned()
}

fn nullable_u64(array: &StructArray, name: &str, index: usize) -> Option<u64> {
    let values = struct_column::<UInt64Array>(array, name);
    (!values.is_null(index)).then(|| values.value(index))
}

fn nullable_f64_bits(array: &StructArray, name: &str, index: usize) -> Option<u64> {
    let values = struct_column::<Float64Array>(array, name);
    (!values.is_null(index)).then(|| values.value(index).to_bits())
}

fn span_values(array: &StructArray, name: &str, index: usize) -> Vec<OracleSpan> {
    let values = struct_column::<ListArray>(array, name).value(index);
    let spans = values.as_any().downcast_ref::<StructArray>().unwrap();
    let offsets = struct_column::<Int32Array>(spans, "offset");
    let lengths = struct_column::<UInt32Array>(spans, "length");
    (0..spans.len())
        .map(|index| OracleSpan {
            offset: offsets.value(index),
            length: lengths.value(index),
        })
        .collect()
}

fn i64_list(array: &StructArray, name: &str, index: usize) -> Option<Vec<i64>> {
    let lists = struct_column::<ListArray>(array, name);
    if lists.is_null(index) {
        return None;
    }
    let values = lists.value(index);
    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
    Some(values.values().to_vec())
}

fn f64_list_bits(array: &StructArray, name: &str, index: usize) -> Option<Vec<u64>> {
    let lists = struct_column::<ListArray>(array, name);
    if lists.is_null(index) {
        return None;
    }
    let values = lists.value(index);
    let values = values.as_any().downcast_ref::<Float64Array>().unwrap();
    Some(
        values
            .values()
            .iter()
            .map(|value| value.to_bits())
            .collect(),
    )
}

fn go_bucket_store_series(
    generator_directory: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
    aggregates: &str,
    max_resolution: i64,
) -> Vec<OracleSeries> {
    let output = std::process::Command::new("go")
        .args([
            "run",
            "-tags=slicelabels",
            "./cmd/store-oracle",
            "--bucket",
            blocks.to_str().unwrap(),
            "--metric",
            metric,
            "--aggregates",
            aggregates,
            "--max-resolution",
            &max_resolution.to_string(),
        ])
        .current_dir(generator_directory)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Go oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
