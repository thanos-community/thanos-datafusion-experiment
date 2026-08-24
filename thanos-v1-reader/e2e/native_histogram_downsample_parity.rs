use std::collections::BTreeMap;

use futures::StreamExt;
use serde::Deserialize;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::ThanosRepositoryConfig,
    index_context,
    store_service::ThanosStoreService,
    thanos_proto::thanos::{
        self, Aggr, LabelMatcher, PartialResponseStrategy, store_server::Store,
    },
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
    min_time: i64,
    max_time: i64,
    encoding: i32,
    data: String,
    hash: u64,
    #[serde(default)]
    histograms: Vec<OracleHistogramSample>,
    count: Option<OracleEncodedChunk>,
    sum: Option<OracleEncodedChunk>,
    min: Option<OracleEncodedChunk>,
    max: Option<OracleEncodedChunk>,
    counter: Option<OracleEncodedChunk>,
}

#[derive(Debug, Deserialize)]
struct OracleEncodedChunk {
    encoding: i32,
    data: String,
    hash: u64,
    #[serde(default)]
    samples: Vec<OracleFloatSample>,
    #[serde(default)]
    histograms: Vec<OracleHistogramSample>,
}

#[derive(Debug, Deserialize)]
struct OracleFloatSample {
    timestamp: i64,
    value_bits: u64,
}

#[derive(Debug, Deserialize)]
struct OracleHistogramSample {
    timestamp: i64,
    kind: String,
    counter_reset_hint: i32,
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
}

#[derive(Debug, Deserialize)]
struct OracleSpan {
    offset: i32,
    length: u32,
}

#[derive(Debug)]
struct ReaderSeries {
    labels: BTreeMap<String, String>,
    chunks: Vec<ReaderAggrChunk>,
}

#[derive(Debug)]
struct ReaderAggrChunk {
    min_time: i64,
    max_time: i64,
    raw: Option<ReaderChunk>,
    count: Option<ReaderChunk>,
    sum: Option<ReaderChunk>,
    min: Option<ReaderChunk>,
    max: Option<ReaderChunk>,
    counter: Option<ReaderChunk>,
}

#[derive(Debug)]
struct ReaderChunk {
    encoding: i32,
    data: String,
    hash: u64,
}

#[tokio::test]
async fn native_histogram_downsample_chunks_match_go_bucket_store() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-native-downsample-e2e-{}",
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
    let service = ThanosStoreService::new(context, std::slice::from_ref(&repository))
        .await
        .unwrap();
    let aggregates = [Aggr::Count, Aggr::Sum, Aggr::Counter];

    for metric in ["dummy_native_histogram", "dummy_float_native_histogram"] {
        for resolution in [RESOLUTION_5M, RESOLUTION_1H] {
            let expected = go_bucket_store_series(
                &generator_directory,
                &blocks,
                metric,
                "count,sum,counter",
                resolution,
            );
            let actual = reader_series(&service, metric, &aggregates, resolution)
                .await
                .unwrap();
            assert_store_parity(&actual, &expected);
            assert_native_histogram_semantics(&expected);
        }

        let expected_raw =
            go_bucket_store_series(&generator_directory, &blocks, metric, "count", 0);
        let actual_raw = reader_series(&service, metric, &[Aggr::Count], 0)
            .await
            .unwrap();
        assert_store_parity(&actual_raw, &expected_raw);
        let (raw_encoding, raw_kind) = if metric == "dummy_native_histogram" {
            (1, "histogram")
        } else {
            (2, "float_histogram")
        };
        assert!(
            expected_raw
                .iter()
                .flat_map(|series| &series.chunks)
                .all(|chunk| {
                    chunk.encoding == raw_encoding
                        && !chunk.data.is_empty()
                        && !chunk.histograms.is_empty()
                        && chunk.histograms.iter().all(|histogram| {
                            histogram.kind == raw_kind
                                && !histogram.positive_spans.is_empty()
                                && !histogram.negative_spans.is_empty()
                                && if raw_kind == "histogram" {
                                    !histogram.positive_buckets.is_empty()
                                        && !histogram.negative_buckets.is_empty()
                                } else {
                                    !histogram.positive_bucket_bits.is_empty()
                                        && !histogram.negative_bucket_bits.is_empty()
                                }
                        })
                        && chunk.count.is_none()
                })
        );
    }

    let go_error = go_bucket_store_error(
        &generator_directory,
        &blocks,
        "dummy_native_histogram",
        "min",
        RESOLUTION_1H,
    );
    assert!(go_error.contains("aggregate min does not exist"));
    let rust_error = reader_series(
        &service,
        "dummy_native_histogram",
        &[Aggr::Min],
        RESOLUTION_1H,
    )
    .await
    .unwrap_err();
    assert!(
        rust_error
            .message()
            .contains("aggregate min does not exist")
    );

    std::fs::remove_dir_all(root).unwrap();
}

fn assert_store_parity(actual: &[ReaderSeries], expected: &[OracleSeries]) {
    assert_eq!(actual.len(), expected.len());
    for (actual_series, expected_series) in actual.iter().zip(expected) {
        assert_eq!(actual_series.labels, expected_series.labels);
        assert_eq!(actual_series.chunks.len(), expected_series.chunks.len());
        for (actual_chunk, expected_chunk) in
            actual_series.chunks.iter().zip(&expected_series.chunks)
        {
            assert_eq!(actual_chunk.min_time, expected_chunk.min_time);
            assert_eq!(actual_chunk.max_time, expected_chunk.max_time);
            assert_chunk(
                actual_chunk.raw.as_ref(),
                (!expected_chunk.data.is_empty()).then_some((
                    expected_chunk.encoding,
                    expected_chunk.data.as_str(),
                    expected_chunk.hash,
                )),
            );
            assert_chunk(
                actual_chunk.count.as_ref(),
                expected_chunk.count.as_ref().map(chunk_key),
            );
            assert_chunk(
                actual_chunk.sum.as_ref(),
                expected_chunk.sum.as_ref().map(chunk_key),
            );
            assert_chunk(
                actual_chunk.min.as_ref(),
                expected_chunk.min.as_ref().map(chunk_key),
            );
            assert_chunk(
                actual_chunk.max.as_ref(),
                expected_chunk.max.as_ref().map(chunk_key),
            );
            assert_chunk(
                actual_chunk.counter.as_ref(),
                expected_chunk.counter.as_ref().map(chunk_key),
            );
        }
    }
}

fn assert_chunk(actual: Option<&ReaderChunk>, expected: Option<(i32, &str, u64)>) {
    match (actual, expected) {
        (Some(actual), Some((encoding, data, hash))) => {
            assert_eq!(actual.encoding, encoding);
            assert_eq!(actual.data, data);
            assert_eq!(actual.hash, hash);
        }
        (None, None) => {}
        values => panic!("chunk placement mismatch: {values:?}"),
    }
}

fn chunk_key(chunk: &OracleEncodedChunk) -> (i32, &str, u64) {
    (chunk.encoding, &chunk.data, chunk.hash)
}

fn assert_native_histogram_semantics(series: &[OracleSeries]) {
    for chunk in series.iter().flat_map(|series| &series.chunks) {
        let count = chunk.count.as_ref().expect("COUNT slot");
        let sum = chunk.sum.as_ref().expect("SUM slot");
        let counter = chunk.counter.as_ref().expect("COUNTER slot");
        assert_eq!(count.encoding, 0);
        assert_eq!(sum.encoding, 2);
        assert_eq!(counter.encoding, 2);
        assert!(!count.samples.is_empty());
        assert!(
            count
                .samples
                .windows(2)
                .all(|pair| pair[0].timestamp <= pair[1].timestamp)
        );
        assert!(count.samples.iter().all(|sample| sample.value_bits != 0));
        assert!(!sum.histograms.is_empty());
        assert!(!counter.histograms.is_empty());
        assert!(
            sum.histograms
                .windows(2)
                .all(|pair| pair[0].timestamp <= pair[1].timestamp)
        );
        assert!(
            counter
                .histograms
                .windows(2)
                .all(|pair| pair[0].timestamp <= pair[1].timestamp)
        );
        for histogram in sum.histograms.iter().chain(&counter.histograms) {
            assert_eq!(histogram.kind, "float_histogram");
            assert_eq!(histogram.schema, 0);
            assert!(histogram.count != 0);
            assert!(histogram.sum_bits != 0);
            assert!(histogram.zero_threshold_bits != 0);
            assert!(histogram.zero_count != 0);
            assert!(
                histogram
                    .positive_spans
                    .iter()
                    .all(|span| span.length > 0 && (-100..100).contains(&span.offset))
            );
            assert!(
                histogram
                    .negative_spans
                    .iter()
                    .all(|span| span.length > 0 && (-100..100).contains(&span.offset))
            );
            assert!(!histogram.positive_bucket_bits.is_empty());
            assert!(!histogram.negative_bucket_bits.is_empty());
            assert!(histogram.positive_buckets.is_empty());
            assert!(histogram.negative_buckets.is_empty());
        }
        assert!(
            sum.histograms
                .iter()
                .all(|histogram| histogram.counter_reset_hint == 3)
        );
        assert!(
            counter
                .histograms
                .iter()
                .all(|histogram| matches!(histogram.counter_reset_hint, 0 | 2))
        );
    }
}

fn go_bucket_store_series(
    generator_directory: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
    aggregates: &str,
    max_resolution: i64,
) -> Vec<OracleSeries> {
    let output = go_bucket_store(
        generator_directory,
        blocks,
        metric,
        aggregates,
        max_resolution,
    );
    assert!(
        output.status.success(),
        "Go BucketStore oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn go_bucket_store_error(
    generator_directory: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
    aggregates: &str,
    max_resolution: i64,
) -> String {
    let output = go_bucket_store(
        generator_directory,
        blocks,
        metric,
        aggregates,
        max_resolution,
    );
    assert!(!output.status.success());
    String::from_utf8(output.stderr).unwrap()
}

fn go_bucket_store(
    generator_directory: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
    aggregates: &str,
    max_resolution: i64,
) -> std::process::Output {
    std::process::Command::new("go")
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
        .unwrap()
}

async fn reader_series(
    service: &ThanosStoreService,
    metric: &str,
    aggregates: &[Aggr],
    max_resolution: i64,
) -> Result<Vec<ReaderSeries>, tonic::Status> {
    let request = thanos::SeriesRequest {
        min_time: i64::MIN,
        max_time: i64::MAX,
        matchers: vec![LabelMatcher {
            r#type: thanos::label_matcher::Type::Eq as i32,
            name: "__name__".to_owned(),
            value: metric.to_owned(),
        }],
        aggregates: aggregates
            .iter()
            .map(|aggregate| *aggregate as i32)
            .collect(),
        max_resolution_window: max_resolution,
        partial_response_strategy: PartialResponseStrategy::Abort as i32,
        ..Default::default()
    };
    let mut stream = service
        .series(tonic::Request::new(request))
        .await?
        .into_inner();
    let mut result = Vec::new();
    while let Some(response) = stream.next().await {
        let response = response?;
        let Some(response_result) = response.result else {
            panic!("reader returned an empty response");
        };
        let series = match response_result {
            thanos::series_response::Result::Series(series) => series,
            thanos::series_response::Result::Hints(_) => continue,
            _ => panic!("reader returned an unexpected response"),
        };
        result.push(ReaderSeries {
            labels: series
                .labels
                .into_iter()
                .map(|label| (label.name, label.value))
                .collect(),
            chunks: series.chunks.into_iter().map(convert_aggr_chunk).collect(),
        });
    }
    Ok(result)
}

fn convert_aggr_chunk(chunk: thanos::AggrChunk) -> ReaderAggrChunk {
    ReaderAggrChunk {
        min_time: chunk.min_time,
        max_time: chunk.max_time,
        raw: chunk.raw.map(convert_chunk),
        count: chunk.count.map(convert_chunk),
        sum: chunk.sum.map(convert_chunk),
        min: chunk.min.map(convert_chunk),
        max: chunk.max.map(convert_chunk),
        counter: chunk.counter.map(convert_chunk),
    }
}

fn convert_chunk(chunk: thanos::Chunk) -> ReaderChunk {
    ReaderChunk {
        encoding: chunk.r#type,
        data: encode_hex(&chunk.data),
        hash: chunk.hash,
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[usize::from(byte >> 4)] as char);
        result.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    result
}
