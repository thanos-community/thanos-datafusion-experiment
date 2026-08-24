use std::collections::BTreeSet;

use futures::StreamExt;
use prost::Message;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::ThanosRepositoryConfig,
    index_context,
    store_service::ThanosStoreService,
    thanos_proto::thanos::{
        self, Aggr, LabelMatcher, PartialResponseStrategy, ShardInfo, store_server::Store,
    },
};

const MINT: i64 = 1_700_000_000_000;
const MAXT: i64 = MINT + 60 * 60 * 1000;

#[derive(Clone, Debug)]
struct ShardCase {
    index: i64,
    total: i64,
    by: bool,
    labels: Vec<String>,
}

#[derive(Clone, Default)]
struct RequestCase {
    without: Option<Vec<String>>,
    shard: Option<ShardCase>,
    limit: i64,
    match_replica: Option<String>,
}

#[tokio::test]
async fn replica_label_removal_matches_go_before_sharding_and_merging() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-replica-label-e2e-{}",
        std::process::id()
    ));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");
    for replica in 0..3 {
        generate_replica(&generator, &blocks, replica, replica == 0);
    }

    let repository = ThanosRepositoryConfig {
        name: "replicas".to_owned(),
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
    let metrics = [
        "dummy_requests_total",
        "dummy_request_duration_seconds_bucket",
        "dummy_native_histogram",
        "dummy_float_native_histogram",
    ];

    for metric in metrics {
        let unremoved = exact_case(
            &service,
            &generator,
            &blocks,
            metric,
            &RequestCase::default(),
        )
        .await;
        assert!(
            decode_series(&unremoved)
                .iter()
                .all(|series| has_label(series, "replica"))
        );

        let removed_case = RequestCase {
            without: Some(vec!["replica".to_owned()]),
            ..Default::default()
        };
        let removed = exact_case(&service, &generator, &blocks, metric, &removed_case).await;
        assert_eq!(
            removed.len() * 3,
            unremoved.len(),
            "three replicas collapse for {metric}"
        );
        assert!(
            decode_series(&removed)
                .iter()
                .all(|series| !has_label(series, "replica"))
        );

        let duplicate_and_absent = RequestCase {
            without: Some(vec![
                "replica".to_owned(),
                "".to_owned(),
                "absent".to_owned(),
                "replica".to_owned(),
            ]),
            ..Default::default()
        };
        assert_eq!(
            exact_case(&service, &generator, &blocks, metric, &duplicate_and_absent).await,
            removed
        );

        let matched_replica = RequestCase {
            without: Some(vec!["replica".to_owned()]),
            match_replica: Some("1".to_owned()),
            ..Default::default()
        };
        let matched = exact_case(&service, &generator, &blocks, metric, &matched_replica).await;
        assert_eq!(matched, removed, "matching happens before label removal");
    }

    let limited = RequestCase {
        without: Some(vec!["replica".to_owned()]),
        limit: 2,
        ..Default::default()
    };
    assert_eq!(
        exact_case(&service, &generator, &blocks, metrics[0], &limited)
            .await
            .len(),
        2,
        "limit is applied per replica block before merged output"
    );

    for metric in metrics {
        let unsharded = exact_case(
            &service,
            &generator,
            &blocks,
            metric,
            &RequestCase {
                without: Some(vec!["replica".to_owned()]),
                ..Default::default()
            },
        )
        .await;
        let mut union = BTreeSet::new();
        for index in 0..3 {
            let case = RequestCase {
                without: Some(vec!["replica".to_owned()]),
                shard: Some(ShardCase {
                    index,
                    total: 3,
                    by: true,
                    labels: vec![
                        "replica".to_owned(),
                        "series".to_owned(),
                        "route".to_owned(),
                        "pod".to_owned(),
                        "le".to_owned(),
                        "instance".to_owned(),
                    ],
                }),
                ..Default::default()
            };
            for series in exact_case(&service, &generator, &blocks, metric, &case).await {
                assert!(union.insert(series), "shards overlap for {metric}");
            }
        }
        assert_eq!(
            union,
            unsharded.into_iter().collect(),
            "shard union after replica removal for {metric}"
        );
    }

    let unremoved = exact_case(
        &service,
        &generator,
        &blocks,
        metrics[0],
        &RequestCase::default(),
    )
    .await;
    let mut unremoved_union = BTreeSet::new();
    for index in 0..3 {
        let case = RequestCase {
            shard: Some(ShardCase {
                index,
                total: 3,
                by: true,
                labels: vec!["replica".to_owned()],
            }),
            ..Default::default()
        };
        for series in exact_case(&service, &generator, &blocks, metrics[0], &case).await {
            assert!(unremoved_union.insert(series));
        }
    }
    assert_eq!(unremoved_union, unremoved.into_iter().collect());

    let removed = RequestCase {
        without: Some(vec!["replica".to_owned()]),
        ..Default::default()
    };
    let removed_unsharded = exact_case(&service, &generator, &blocks, metrics[0], &removed).await;
    let mut non_empty_shards = 0;
    let mut removed_union = BTreeSet::new();
    for index in 0..3 {
        let case = RequestCase {
            without: removed.without.clone(),
            shard: Some(ShardCase {
                index,
                total: 3,
                by: true,
                labels: vec!["replica".to_owned()],
            }),
            ..Default::default()
        };
        let shard = exact_case(&service, &generator, &blocks, metrics[0], &case).await;
        non_empty_shards += usize::from(!shard.is_empty());
        removed_union.extend(shard);
    }
    assert_eq!(non_empty_shards, 1, "removed hash label is not retained");
    assert_eq!(removed_union, removed_unsharded.into_iter().collect());

    let remove_ordinary_label = RequestCase {
        without: Some(vec!["pod".to_owned()]),
        ..Default::default()
    };
    for metric in metrics {
        let result = exact_case(
            &service,
            &generator,
            &blocks,
            metric,
            &remove_ordinary_label,
        )
        .await;
        assert!(
            decode_series(&result)
                .iter()
                .all(|series| !has_label(series, "pod")),
            "Go removes any requested label name"
        );
    }

    std::fs::remove_dir_all(root).unwrap();
}

async fn exact_case(
    service: &ThanosStoreService,
    generator: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
    case: &RequestCase,
) -> Vec<String> {
    let actual = reader_wire(service, metric, case).await;
    let expected = go_wire(generator, blocks, metric, case);
    if actual != expected {
        let first_difference = actual
            .iter()
            .zip(&expected)
            .position(|(actual, expected)| actual != expected)
            .unwrap_or(actual.len().min(expected.len()));
        panic!(
            "replica-label parity for {metric}: reader={} Go={} first_difference={first_difference}\nreader={:?}\nGo={:?}",
            actual.len(),
            expected.len(),
            actual
                .get(first_difference)
                .map(|series| series_fingerprint(series)),
            expected
                .get(first_difference)
                .map(|series| series_fingerprint(series))
        );
    }
    actual
}

fn generate_replica(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    replica: usize,
    clean: bool,
) {
    let mut command = std::process::Command::new("go");
    command.args([
        "run",
        ".",
        "--output",
        blocks.to_str().unwrap(),
        "--mint",
        &MINT.to_string(),
        "--maxt",
        &MAXT.to_string(),
        "--samples",
        "30",
        "--instances",
        "8",
        "--pods",
        "4",
        "--routes",
        "3",
        "--native-series",
        "2",
        "--downsample-5m=false",
        "--external-label",
        &format!("replica={replica}"),
    ]);
    if clean {
        command.arg("--clean");
    }
    let output = command.current_dir(generator).output().unwrap();
    assert!(
        output.status.success(),
        "fixture generation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn go_wire(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
    case: &RequestCase,
) -> Vec<String> {
    let mut command = std::process::Command::new("go");
    command
        .args([
            "run",
            "-tags=slicelabels",
            "./cmd/store-oracle",
            "--bucket",
            blocks.to_str().unwrap(),
            "--metric",
            metric,
            "--aggregates",
            "raw",
            "--wire-format",
            "--limit",
            &case.limit.to_string(),
        ])
        .current_dir(generator);
    if let Some(without) = &case.without {
        command.arg(format!("--without-replica-labels={}", without.join(",")));
    }
    if let Some(replica) = &case.match_replica {
        command.args(["--match-label", &format!("replica={replica}")]);
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
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "Go oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

async fn reader_wire(
    service: &ThanosStoreService,
    metric: &str,
    case: &RequestCase,
) -> Vec<String> {
    let mut matchers = vec![LabelMatcher {
        r#type: thanos::label_matcher::Type::Eq as i32,
        name: "__name__".to_owned(),
        value: metric.to_owned(),
    }];
    if let Some(replica) = &case.match_replica {
        matchers.push(LabelMatcher {
            r#type: thanos::label_matcher::Type::Eq as i32,
            name: "replica".to_owned(),
            value: replica.clone(),
        });
    }
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
            without_replica_labels: case.without.clone().unwrap_or_default(),
            limit: case.limit,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut result = Vec::new();
    while let Some(response) = stream.next().await {
        let response = response.unwrap();
        let Some(response_result) = response.result else {
            panic!("reader returned an empty response");
        };
        let series = match response_result {
            thanos::series_response::Result::Series(series) => series,
            thanos::series_response::Result::Hints(_) => continue,
            _ => panic!("reader returned an unexpected response"),
        };
        for chunk in &series.chunks {
            let raw = chunk.raw.as_ref().expect("raw chunk");
            assert!(
                !thanos_v1_reader::chunk_reader::decode_record(&framed_record(raw), false)
                    .unwrap()
                    .is_empty()
            );
        }
        result.push(encode_hex(&series.encode_to_vec()));
    }
    result
}

fn decode_series(encoded: &[String]) -> Vec<thanos::Series> {
    encoded
        .iter()
        .map(|value| thanos::Series::decode(decode_hex(value).as_slice()).unwrap())
        .collect()
}

fn has_label(series: &thanos::Series, name: &str) -> bool {
    series.labels.iter().any(|label| label.name == name)
}

fn series_fingerprint(encoded: &str) -> (Vec<String>, Vec<(i64, i64, u64)>) {
    let series = thanos::Series::decode(decode_hex(encoded).as_slice()).unwrap();
    (
        series
            .labels
            .iter()
            .map(|label| format!("{}={}", label.name, label.value))
            .collect(),
        series
            .chunks
            .iter()
            .map(|chunk| {
                let raw = chunk.raw.as_ref().unwrap();
                (
                    chunk.min_time,
                    chunk.max_time,
                    xxhash_rust::xxh64::xxh64(&raw.data, 0),
                )
            })
            .collect(),
    )
}

fn framed_record(chunk: &thanos::Chunk) -> Vec<u8> {
    let mut record = encode_uvarint(chunk.data.len());
    record.push(chunk.r#type as u8 + 1);
    record.extend_from_slice(&chunk.data);
    let checksum = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI)
        .checksum(&record[record.len() - chunk.data.len() - 1..]);
    record.extend_from_slice(&checksum.to_be_bytes());
    record
}

fn encode_uvarint(mut value: usize) -> Vec<u8> {
    let mut encoded = Vec::new();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        encoded.push(byte);
        if value == 0 {
            return encoded;
        }
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
