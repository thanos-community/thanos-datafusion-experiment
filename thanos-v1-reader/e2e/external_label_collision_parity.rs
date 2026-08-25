use std::collections::{BTreeMap, BTreeSet};

use arrow::{
    array::{Float64Array, StringArray, StringViewArray, StructArray, TimestampMillisecondArray},
    record_batch::RecordBatch,
};
use arrow_flight::{
    Ticket,
    decode::FlightRecordBatchStream,
    sql::{TicketStatementQuery, server::FlightSqlService},
};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use serde::Deserialize;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::ThanosRepositoryConfig,
    flight_service::DataFusionFlightService,
    index_context,
    store_service::ThanosStoreService,
    thanos_proto::{
        hintspb,
        thanos::{
            self, Aggr, LabelMatcher, PartialResponseStrategy, ShardInfo, store_server::Store,
        },
    },
};

const MINT: i64 = 1_700_000_000_000;
const MAXT: i64 = MINT + 10 * 60 * 1000;
const EXTERNAL_JOB: &str = "external-job";
const EXTERNAL_POD: &str = "external-pod";
const EXTERNAL_REPLICA: &str = "external-replica";
const EXTERNAL_SERIES: &str = "external-series";
const INTERNAL_REPLICA: &str = "internal-replica";

#[derive(Clone, Debug, Default)]
struct SeriesCase {
    matchers: Vec<Matcher>,
    shard: Option<Shard>,
    without: Vec<String>,
}

#[derive(Clone, Debug)]
struct Matcher {
    kind: &'static str,
    name: &'static str,
    value: &'static str,
}

#[derive(Clone, Debug)]
struct Shard {
    index: i64,
    total: i64,
    by: bool,
    labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct OracleSeries {
    labels: BTreeMap<String, String>,
    chunks: Vec<OracleChunk>,
}

#[derive(Debug, Deserialize)]
struct OracleChunk {
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
    sum_bits: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ScalarRow {
    timestamp: i64,
    value_bits: u64,
    instance: String,
    job: String,
    method: String,
    pod: String,
    replica: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct HistogramRow {
    timestamp: i64,
    sum_bits: u64,
    job: String,
    pod: String,
    replica: String,
    series: String,
}

#[tokio::test]
async fn external_labels_overwrite_internal_labels_everywhere_go_does() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-external-label-collision-e2e-{}",
        std::process::id()
    ));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");
    let block_id = generate_collision_block(&generator, &blocks);

    let repository = ThanosRepositoryConfig {
        name: "external-label-collision".to_owned(),
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
    let service = ThanosStoreService::new(context.clone(), std::slice::from_ref(&repository))
        .await
        .unwrap();

    for (metric, visible_series) in [("dummy_requests_total", 2), ("dummy_native_histogram", 1)] {
        let baseline = exact_series(
            &service,
            &generator,
            &blocks,
            metric,
            &SeriesCase::default(),
        )
        .await;
        assert_eq!(baseline.len(), visible_series);
        assert_external_labels_only(&baseline);

        for case in [
            SeriesCase {
                matchers: vec![matcher("eq", "job", EXTERNAL_JOB)],
                ..Default::default()
            },
            SeriesCase {
                matchers: vec![matcher("neq", "job", "internal-job")],
                ..Default::default()
            },
            SeriesCase {
                matchers: vec![matcher("re", "job", "^external-.*$")],
                ..Default::default()
            },
            SeriesCase {
                matchers: vec![matcher("nre", "job", "^internal-.*$")],
                ..Default::default()
            },
            SeriesCase {
                matchers: vec![matcher("eq", "missing", "")],
                ..Default::default()
            },
        ] {
            let result = exact_series(&service, &generator, &blocks, metric, &case).await;
            assert_eq!(result, baseline);
        }

        for case in [
            SeriesCase {
                matchers: vec![matcher("eq", "job", "internal-job")],
                ..Default::default()
            },
            SeriesCase {
                matchers: vec![matcher("neq", "job", EXTERNAL_JOB)],
                ..Default::default()
            },
            SeriesCase {
                matchers: vec![matcher("re", "job", "^internal-.*$")],
                ..Default::default()
            },
            SeriesCase {
                matchers: vec![matcher("nre", "job", "^external-.*$")],
                ..Default::default()
            },
            SeriesCase {
                matchers: vec![matcher("neq", "missing", "")],
                ..Default::default()
            },
        ] {
            assert!(
                exact_series(&service, &generator, &blocks, metric, &case)
                    .await
                    .is_empty()
            );
        }

        let removed = exact_series(
            &service,
            &generator,
            &blocks,
            metric,
            &SeriesCase {
                without: vec!["replica".to_owned(), "pod".to_owned()],
                ..Default::default()
            },
        )
        .await;
        assert!(decode_series(&removed).iter().all(|series| {
            series
                .labels
                .iter()
                .all(|label| label.name != "replica" && label.name != "pod")
        }));

        let shard_labels = vec![
            "job".to_owned(),
            "pod".to_owned(),
            "replica".to_owned(),
            "series".to_owned(),
            "instance".to_owned(),
        ];
        let mut union = BTreeSet::new();
        for index in 0..3 {
            let shard = exact_series(
                &service,
                &generator,
                &blocks,
                metric,
                &SeriesCase {
                    shard: Some(Shard {
                        index,
                        total: 3,
                        by: true,
                        labels: shard_labels.clone(),
                    }),
                    ..Default::default()
                },
            )
            .await;
            for series in shard {
                assert!(union.insert(series), "series appeared in multiple shards");
            }
        }
        assert_eq!(union, baseline.into_iter().collect());
    }

    exact_series_hints(&service, &generator, &blocks, &block_id).await;
    exact_label_endpoints(&service, &generator, &blocks, &block_id).await;

    let scalar_oracle = go_series_json(
        &generator,
        &blocks,
        "dummy_requests_total",
        &SeriesCase::default(),
    );
    let expected_scalar = scalar_rows_from_oracle(&scalar_oracle);
    let scalar_sql = "SELECT timestamp, value, instance, job, method, pod, replica \
                      FROM metrics.dummy_requests_total \
                      WHERE downsample_resolution = 0";
    let actual_scalar = scalar_rows(
        context
            .sql(scalar_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(actual_scalar, expected_scalar);
    assert_eq!(
        scalar_rows(flight_batches(&context, scalar_sql).await),
        expected_scalar
    );

    let histogram_oracle = go_series_json(
        &generator,
        &blocks,
        "dummy_native_histogram",
        &SeriesCase::default(),
    );
    let expected_histograms = histogram_rows_from_oracle(&histogram_oracle);
    let histogram_sql = "SELECT timestamp, histogram, job, pod, replica, series \
                         FROM metrics.dummy_native_histogram \
                         WHERE downsample_resolution = 0";
    let actual_histograms = histogram_rows(
        context
            .sql(histogram_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    );
    assert_eq!(actual_histograms.len(), 12 * 2 * 4);
    assert_eq!(
        actual_histograms.iter().cloned().collect::<BTreeSet<_>>(),
        expected_histograms.iter().cloned().collect::<BTreeSet<_>>()
    );
    assert_eq!(
        histogram_rows(flight_batches(&context, histogram_sql).await),
        actual_histograms
    );

    std::fs::remove_dir_all(root).unwrap();
}

fn matcher(kind: &'static str, name: &'static str, value: &'static str) -> Matcher {
    Matcher { kind, name, value }
}

fn generate_collision_block(generator: &std::path::Path, blocks: &std::path::Path) -> String {
    let output = std::process::Command::new("go")
        .args([
            "run",
            ".",
            "--output",
            blocks.to_str().unwrap(),
            "--clean",
            "--mint",
            &MINT.to_string(),
            "--maxt",
            &MAXT.to_string(),
            "--samples",
            "12",
            "--instances",
            "2",
            "--pods",
            "4",
            "--routes",
            "1",
            "--native-series",
            "2",
            "--downsample-5m=false",
            "--internal-label",
            &format!("replica={INTERNAL_REPLICA}"),
            "--internal-label",
            "job=internal-job",
            "--external-label",
            &format!("job={EXTERNAL_JOB}"),
            "--external-label",
            &format!("pod={EXTERNAL_POD}"),
            "--external-label",
            &format!("replica={EXTERNAL_REPLICA}"),
            "--external-label",
            &format!("series={EXTERNAL_SERIES}"),
        ])
        .current_dir(generator)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture generation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("raw block: ").map(str::trim))
        .unwrap()
        .to_owned()
}

async fn exact_series(
    service: &ThanosStoreService,
    generator: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
    case: &SeriesCase,
) -> Vec<String> {
    let expected = go_series_wire(generator, blocks, metric, case);
    let actual = reader_series_wire(service, metric, case).await;
    assert_eq!(actual, expected, "Series mismatch for {metric}: {case:?}");
    actual
}

fn go_series_wire(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
    case: &SeriesCase,
) -> Vec<String> {
    let mut command = oracle_command(generator, blocks);
    command.args(["--metric", metric, "--aggregates", "raw", "--wire-format"]);
    add_series_case_args(&mut command, case);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "Go Series oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn go_series_json(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
    case: &SeriesCase,
) -> Vec<OracleSeries> {
    let mut command = oracle_command(generator, blocks);
    command.args(["--metric", metric, "--aggregates", "raw"]);
    add_series_case_args(&mut command, case);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "Go Series oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn add_series_case_args(command: &mut std::process::Command, case: &SeriesCase) {
    for matcher in &case.matchers {
        command.args(["--series-matcher", &matcher_arg(matcher)]);
    }
    if !case.without.is_empty() {
        command.arg(format!(
            "--without-replica-labels={}",
            case.without.join(",")
        ));
    }
    if let Some(shard) = &case.shard {
        command.args([
            "--shard-enabled",
            "--shard-index",
            &shard.index.to_string(),
            "--shard-total",
            &shard.total.to_string(),
            &format!("--shard-by={}", shard.by),
            "--shard-labels",
            &shard.labels.join(","),
        ]);
    }
}

async fn reader_series_wire(
    service: &ThanosStoreService,
    metric: &str,
    case: &SeriesCase,
) -> Vec<String> {
    let mut matchers = vec![LabelMatcher {
        r#type: thanos::label_matcher::Type::Eq as i32,
        name: "__name__".to_owned(),
        value: metric.to_owned(),
    }];
    matchers.extend(case.matchers.iter().map(matcher_proto));
    let mut stream = service
        .series(tonic::Request::new(thanos::SeriesRequest {
            min_time: MINT,
            max_time: MAXT,
            matchers,
            aggregates: vec![Aggr::Raw as i32],
            partial_response_strategy: PartialResponseStrategy::Abort as i32,
            shard_info: case.shard.as_ref().map(|shard| ShardInfo {
                shard_index: shard.index,
                total_shards: shard.total,
                by: shard.by,
                labels: shard.labels.clone(),
            }),
            without_replica_labels: case.without.clone(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut result = Vec::new();
    while let Some(response) = stream.next().await {
        match response.unwrap().result.unwrap() {
            thanos::series_response::Result::Series(series) => {
                result.push(encode_hex(&series.encode_to_vec()));
            }
            thanos::series_response::Result::Hints(_) => {}
            result => panic!("unexpected Series result: {result:?}"),
        }
    }
    result
}

async fn exact_series_hints(
    service: &ThanosStoreService,
    generator: &std::path::Path,
    blocks: &std::path::Path,
    block_id: &str,
) {
    let block_matcher = format!("eq:__block_id:{block_id}");
    let output = oracle_command(generator, blocks)
        .args([
            "--metric",
            "dummy_requests_total",
            "--aggregates",
            "raw",
            "--stream-wire-format",
            "--hints-type",
            "request",
            "--block-matcher",
            &block_matcher,
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Go hints oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();

    let hints = hintspb::SeriesRequestHints {
        block_matchers: vec![LabelMatcher {
            r#type: thanos::label_matcher::Type::Eq as i32,
            name: "__block_id".to_owned(),
            value: block_id.to_owned(),
        }],
        enable_query_stats: false,
    };
    let mut stream = service
        .series(tonic::Request::new(thanos::SeriesRequest {
            min_time: MINT,
            max_time: MAXT,
            matchers: vec![LabelMatcher {
                r#type: thanos::label_matcher::Type::Eq as i32,
                name: "__name__".to_owned(),
                value: "dummy_requests_total".to_owned(),
            }],
            aggregates: vec![Aggr::Raw as i32],
            partial_response_strategy: PartialResponseStrategy::Abort as i32,
            hints: Some(prost_types::Any {
                type_url: "type.googleapis.com/hintspb.SeriesRequestHints".to_owned(),
                value: hints.encode_to_vec(),
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut actual = Vec::new();
    while let Some(response) = stream.next().await {
        actual.push(encode_hex(&response.unwrap().encode_to_vec()));
    }
    assert_eq!(actual, expected);
}

async fn exact_label_endpoints(
    service: &ThanosStoreService,
    generator: &std::path::Path,
    blocks: &std::path::Path,
    block_id: &str,
) {
    let matcher = matcher("eq", "job", EXTERNAL_JOB);
    let block_matcher = format!("eq:__block_id:{block_id}");
    let request_hint = |type_url: &str, value: Vec<u8>| prost_types::Any {
        type_url: type_url.to_owned(),
        value,
    };

    let names_output = oracle_command(generator, blocks)
        .args([
            "--endpoint",
            "label-names",
            "--series-matcher",
            &matcher_arg(&matcher),
            "--without-replica-labels=replica",
            "--hints-type",
            "request",
            "--block-matcher",
            &block_matcher,
        ])
        .output()
        .unwrap();
    assert!(names_output.status.success());
    let names_hex: String = serde_json::from_slice(&names_output.stdout).unwrap();
    let expected_names =
        thanos::LabelNamesResponse::decode(decode_hex(&names_hex).as_slice()).unwrap();
    let actual_names = service
        .label_names(tonic::Request::new(thanos::LabelNamesRequest {
            start: MINT,
            end: MAXT,
            matchers: vec![matcher_proto(&matcher)],
            without_replica_labels: vec!["replica".to_owned()],
            hints: Some(request_hint(
                "type.googleapis.com/hintspb.LabelNamesRequestHints",
                hintspb::LabelNamesRequestHints {
                    block_matchers: vec![LabelMatcher {
                        r#type: thanos::label_matcher::Type::Eq as i32,
                        name: "__block_id".to_owned(),
                        value: block_id.to_owned(),
                    }],
                }
                .encode_to_vec(),
            )),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(actual_names.encode_to_vec(), expected_names.encode_to_vec());

    for label in ["job", "pod", "replica"] {
        let values_output = oracle_command(generator, blocks)
            .args([
                "--endpoint",
                "label-values",
                "--label",
                label,
                "--series-matcher",
                &matcher_arg(&matcher),
                "--hints-type",
                "request",
                "--block-matcher",
                &block_matcher,
            ])
            .output()
            .unwrap();
        assert!(values_output.status.success());
        let values_hex: String = serde_json::from_slice(&values_output.stdout).unwrap();
        let expected =
            thanos::LabelValuesResponse::decode(decode_hex(&values_hex).as_slice()).unwrap();
        let actual = service
            .label_values(tonic::Request::new(thanos::LabelValuesRequest {
                label: label.to_owned(),
                start: MINT,
                end: MAXT,
                matchers: vec![matcher_proto(&matcher)],
                hints: Some(request_hint(
                    "type.googleapis.com/hintspb.LabelValuesRequestHints",
                    hintspb::LabelValuesRequestHints {
                        block_matchers: vec![LabelMatcher {
                            r#type: thanos::label_matcher::Type::Eq as i32,
                            name: "__block_id".to_owned(),
                            value: block_id.to_owned(),
                        }],
                    }
                    .encode_to_vec(),
                )),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(actual.encode_to_vec(), expected.encode_to_vec());
    }
}

fn scalar_rows_from_oracle(series: &[OracleSeries]) -> Vec<ScalarRow> {
    let mut rows = series
        .iter()
        .flat_map(|series| {
            series.chunks.iter().flat_map(|chunk| {
                chunk.samples.iter().map(|sample| ScalarRow {
                    timestamp: sample.timestamp,
                    value_bits: sample.value_bits,
                    instance: series.labels["instance"].clone(),
                    job: series.labels["job"].clone(),
                    method: series.labels["method"].clone(),
                    pod: series.labels["pod"].clone(),
                    replica: series.labels["replica"].clone(),
                })
            })
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn histogram_rows_from_oracle(series: &[OracleSeries]) -> Vec<HistogramRow> {
    let mut rows = series
        .iter()
        .flat_map(|series| {
            series.chunks.iter().flat_map(|chunk| {
                chunk.histograms.iter().map(|histogram| HistogramRow {
                    timestamp: histogram.timestamp,
                    sum_bits: histogram.sum_bits,
                    job: series.labels["job"].clone(),
                    pod: series.labels["pod"].clone(),
                    replica: series.labels["replica"].clone(),
                    series: series.labels["series"].clone(),
                })
            })
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn scalar_rows(batches: Vec<RecordBatch>) -> Vec<ScalarRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let timestamp = column::<TimestampMillisecondArray>(&batch, "timestamp");
        let value = column::<Float64Array>(&batch, "value");
        for index in 0..batch.num_rows() {
            rows.push(ScalarRow {
                timestamp: timestamp.value(index),
                value_bits: value.value(index).to_bits(),
                instance: string_value(&batch, "instance", index),
                job: string_value(&batch, "job", index),
                method: string_value(&batch, "method", index),
                pod: string_value(&batch, "pod", index),
                replica: string_value(&batch, "replica", index),
            });
        }
    }
    rows.sort();
    rows
}

fn histogram_rows(batches: Vec<RecordBatch>) -> Vec<HistogramRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let timestamp = column::<TimestampMillisecondArray>(&batch, "timestamp");
        let histogram = column::<StructArray>(&batch, "histogram");
        let sum = histogram
            .column_by_name("sum")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push(HistogramRow {
                timestamp: timestamp.value(index),
                sum_bits: sum.value(index).to_bits(),
                job: string_value(&batch, "job", index),
                pod: string_value(&batch, "pod", index),
                replica: string_value(&batch, "replica", index),
                series: string_value(&batch, "series", index),
            });
        }
    }
    rows.sort();
    rows
}

async fn flight_batches(
    context: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Vec<RecordBatch> {
    let service = DataFusionFlightService::new(context.clone(), "grpc://127.0.0.1:50051");
    let response = service
        .do_get_statement(
            TicketStatementQuery {
                statement_handle: sql.as_bytes().to_vec().into(),
            },
            tonic::Request::new(Ticket::new(Vec::new())),
        )
        .await
        .unwrap();
    FlightRecordBatchStream::new_from_flight_data(
        response
            .into_inner()
            .map_err(arrow_flight::error::FlightError::from),
    )
    .try_collect()
    .await
    .unwrap()
}

fn assert_external_labels_only(encoded: &[String]) {
    for series in decode_series(encoded) {
        let labels = series
            .labels
            .into_iter()
            .map(|label| (label.name, label.value))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(labels.get("job").map(String::as_str), Some(EXTERNAL_JOB));
        assert_eq!(labels.get("pod").map(String::as_str), Some(EXTERNAL_POD));
        assert_eq!(
            labels.get("replica").map(String::as_str),
            Some(EXTERNAL_REPLICA)
        );
        assert!(!labels.values().any(|value| value == INTERNAL_REPLICA));
    }
}

fn decode_series(encoded: &[String]) -> Vec<thanos::Series> {
    encoded
        .iter()
        .map(|value| thanos::Series::decode(decode_hex(value).as_slice()).unwrap())
        .collect()
}

fn matcher_arg(matcher: &Matcher) -> String {
    format!("{}:{}:{}", matcher.kind, matcher.name, matcher.value)
}

fn matcher_proto(matcher: &Matcher) -> LabelMatcher {
    let kind = match matcher.kind {
        "eq" => thanos::label_matcher::Type::Eq,
        "neq" => thanos::label_matcher::Type::Neq,
        "re" => thanos::label_matcher::Type::Re,
        "nre" => thanos::label_matcher::Type::Nre,
        _ => unreachable!(),
    };
    LabelMatcher {
        r#type: kind as i32,
        name: matcher.name.to_owned(),
        value: matcher.value.to_owned(),
    }
}

fn oracle_command(generator: &std::path::Path, blocks: &std::path::Path) -> std::process::Command {
    let mut command = std::process::Command::new("go");
    command
        .args([
            "run",
            "-tags=slicelabels",
            "./cmd/store-oracle",
            "--bucket",
            blocks.to_str().unwrap(),
        ])
        .current_dir(generator);
    command
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<T>()
        .unwrap()
}

fn string_value(batch: &RecordBatch, name: &str, index: usize) -> String {
    let column = batch.column_by_name(name).unwrap();
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        values.value(index).to_owned()
    } else {
        column
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(index)
            .to_owned()
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

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).unwrap();
            let low = (pair[1] as char).to_digit(16).unwrap();
            ((high << 4) | low) as u8
        })
        .collect()
}
