use futures::StreamExt;
use prost::Message;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::ThanosRepositoryConfig,
    index_context,
    store_service::ThanosStoreService,
    thanos_proto::{
        hintspb,
        thanos::{
            self, Aggr, LabelMatcher, PartialResponseStrategy, ShardInfo, store_server::Store,
        },
    },
};
use tonic::Code;

const START: i64 = 1_700_000_000_000;
const HOUR: i64 = 60 * 60 * 1000;
const REQUEST_HINTS_TYPE_URL: &str = "type.googleapis.com/hintspb.SeriesRequestHints";

#[derive(Clone, Debug)]
struct BlockIds {
    raw: String,
    five_minutes: Option<String>,
    one_hour: Option<String>,
}

#[derive(Clone, Debug)]
struct BlockMatcher {
    kind: &'static str,
    name: &'static str,
    value: String,
}

#[derive(Clone, Debug)]
struct QueryCase {
    name: &'static str,
    hints: bool,
    block_matchers: Vec<BlockMatcher>,
    min_time: i64,
    max_time: i64,
    max_resolution: i64,
    aggregates: Vec<Aggr>,
    skip_chunks: bool,
    shard: Option<(i64, i64)>,
    without_replica: bool,
    hints_type_url: Option<&'static str>,
}

#[tokio::test]
async fn series_block_hints_and_queried_blocks_match_go() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-series-hints-e2e-{}",
        std::process::id()
    ));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");

    let base = generate_blocks(
        &generator,
        &blocks,
        START,
        START + 3 * HOUR,
        180,
        true,
        true,
    );
    let tail = generate_blocks(
        &generator,
        &blocks,
        START + 3 * HOUR,
        START + 4 * HOUR,
        60,
        false,
        false,
    );
    let superseded = generate_blocks(
        &generator,
        &blocks,
        START + 5 * HOUR / 2,
        START + 7 * HOUR / 2,
        60,
        false,
        false,
    );
    let replacement = generate_blocks(
        &generator,
        &blocks,
        START + 5 * HOUR / 2,
        START + 7 * HOUR / 2,
        60,
        false,
        false,
    );
    make_compaction_replacement(&blocks, &superseded.raw, &replacement.raw);

    let repository = ThanosRepositoryConfig {
        name: "hints".to_owned(),
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

    let full = QueryCase {
        name: "no-request-hints",
        hints: false,
        block_matchers: vec![],
        min_time: START,
        max_time: START + 4 * HOUR - 1,
        max_resolution: 0,
        aggregates: vec![Aggr::Raw],
        skip_chunks: false,
        shard: None,
        without_replica: false,
        hints_type_url: None,
    };
    let no_hints = exact_stream(&service, &generator, &blocks, &full).await;
    assert!(is_response_hints(no_hints.last().unwrap()));

    let mut empty_hints = full.clone();
    empty_hints.name = "empty-request-hints";
    empty_hints.hints = true;
    assert_eq!(
        exact_stream(&service, &generator, &blocks, &empty_hints).await,
        no_hints
    );
    let mut alternate_type_url = empty_hints.clone();
    alternate_type_url.name = "alternate-request-hints-type-url-prefix";
    alternate_type_url.hints_type_url = Some("custom.example/hintspb.SeriesRequestHints");
    assert_eq!(
        exact_stream(&service, &generator, &blocks, &alternate_type_url).await,
        no_hints
    );

    let cases = [
        case_with_matcher(
            "exact-block-id",
            "eq",
            "__block_id",
            replacement.raw.clone(),
        ),
        QueryCase {
            name: "mixed-regex-block-ids",
            hints: true,
            block_matchers: vec![BlockMatcher {
                kind: "re",
                name: "__block_id",
                value: format!("{}|{}", base.raw, replacement.raw),
            }],
            ..full.clone()
        },
        case_with_matcher("exclude-one-block", "neq", "__block_id", base.raw.clone()),
        QueryCase {
            name: "ordered-conjunction",
            hints: true,
            block_matchers: vec![
                BlockMatcher {
                    kind: "re",
                    name: "__block_id",
                    value: ".+".to_owned(),
                },
                BlockMatcher {
                    kind: "nre",
                    name: "__block_id",
                    value: format!("{}|{}", tail.raw, superseded.raw),
                },
            ],
            ..full.clone()
        },
        case_with_matcher(
            "no-matching-block",
            "eq",
            "__block_id",
            "01AAAAAAAAAAAAAAAAAAAAAAAA".to_owned(),
        ),
        case_with_matcher("external-label-match", "eq", "cluster", "dummy".to_owned()),
        case_with_matcher(
            "external-label-mismatch",
            "neq",
            "cluster",
            "dummy".to_owned(),
        ),
        QueryCase {
            name: "one-hour-block",
            hints: true,
            block_matchers: vec![BlockMatcher {
                kind: "eq",
                name: "__block_id",
                value: base.one_hour.clone().unwrap(),
            }],
            min_time: START,
            max_time: START + 3 * HOUR - 1,
            max_resolution: HOUR,
            aggregates: vec![Aggr::Count],
            ..full.clone()
        },
        QueryCase {
            name: "filtered-coarse-block-does-not-fallback",
            hints: true,
            block_matchers: vec![BlockMatcher {
                kind: "eq",
                name: "__block_id",
                value: base.raw.clone(),
            }],
            min_time: START,
            max_time: START + 3 * HOUR - 1,
            max_resolution: HOUR,
            aggregates: vec![Aggr::Count],
            ..full.clone()
        },
        QueryCase {
            name: "five-minute-block",
            hints: true,
            block_matchers: vec![BlockMatcher {
                kind: "eq",
                name: "__block_id",
                value: base.five_minutes.clone().unwrap(),
            }],
            min_time: START,
            max_time: START + 3 * HOUR - 1,
            max_resolution: 5 * 60 * 1000,
            aggregates: vec![Aggr::Count],
            ..full.clone()
        },
        QueryCase {
            name: "skip-chunks-still-reports-blocks",
            hints: true,
            skip_chunks: true,
            ..full.clone()
        },
        QueryCase {
            name: "empty-shard-still-reports-blocks",
            hints: true,
            shard: Some((100, 100)),
            ..full.clone()
        },
        QueryCase {
            name: "replica-removal-does-not-change-block-hints",
            hints: true,
            without_replica: true,
            ..full.clone()
        },
        QueryCase {
            name: "outside-time-range",
            hints: true,
            min_time: START + 10 * HOUR,
            max_time: START + 11 * HOUR,
            ..full.clone()
        },
    ];
    for case in cases {
        let stream = exact_stream(&service, &generator, &blocks, &case).await;
        assert!(
            is_response_hints(stream.last().unwrap()),
            "hints must be the final frame for {}",
            case.name
        );
        assert_eq!(
            stream
                .iter()
                .filter(|response| is_response_hints(response))
                .count(),
            1,
            "exactly one hints frame for {}",
            case.name
        );
        let hints = response_hints(stream.last().unwrap());
        assert!(
            hints
                .queried_blocks
                .iter()
                .all(|block| block.id != superseded.raw),
            "deduplicated source must not be reported"
        );
        if matches!(
            case.name,
            "no-matching-block"
                | "external-label-mismatch"
                | "filtered-coarse-block-does-not-fallback"
                | "outside-time-range"
        ) {
            assert!(hints.queried_blocks.is_empty(), "{}", case.name);
        }
        if case.name == "skip-chunks-still-reports-blocks" {
            assert!(!hints.queried_blocks.is_empty());
            assert!(
                stream[..stream.len() - 1]
                    .iter()
                    .all(|response| response_has_no_chunks(response))
            );
        }
        if case.name == "empty-shard-still-reports-blocks" {
            assert!(!hints.queried_blocks.is_empty());
            assert_eq!(stream.len(), 1);
        }
    }

    for hints_type in ["response", "unknown", "invalid-url", "malformed"] {
        let go_error = go_error(&generator, &blocks, hints_type);
        assert!(go_error.contains("InvalidArgument"));
        let rust_error = reader_error(&service, hints_type).await;
        assert_eq!(rust_error.code(), Code::InvalidArgument);
        assert!(
            rust_error
                .message()
                .contains("unmarshal series request hints")
        );
    }
    for matcher in [
        BlockMatcher {
            kind: "invalid",
            name: "__block_id",
            value: base.raw.clone(),
        },
        BlockMatcher {
            kind: "re",
            name: "__block_id",
            value: "[".to_owned(),
        },
    ] {
        assert!(go_matcher_error(&generator, &blocks, &matcher).contains("InvalidArgument"));
        assert_eq!(
            reader_matcher_error(&service, &matcher).await.code(),
            Code::InvalidArgument
        );
    }

    std::fs::remove_dir_all(root).unwrap();
}

fn case_with_matcher(
    name: &'static str,
    kind: &'static str,
    matcher_name: &'static str,
    value: String,
) -> QueryCase {
    QueryCase {
        name,
        hints: true,
        block_matchers: vec![BlockMatcher {
            kind,
            name: matcher_name,
            value,
        }],
        min_time: START,
        max_time: START + 4 * HOUR - 1,
        max_resolution: 0,
        aggregates: vec![Aggr::Raw],
        skip_chunks: false,
        shard: None,
        without_replica: false,
        hints_type_url: None,
    }
}

async fn exact_stream(
    service: &ThanosStoreService,
    generator: &std::path::Path,
    blocks: &std::path::Path,
    case: &QueryCase,
) -> Vec<String> {
    let expected = go_stream(generator, blocks, case);
    let actual = reader_stream(service, case).await;
    assert_eq!(actual, expected, "Series stream mismatch for {}", case.name);
    actual
}

fn go_stream(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    case: &QueryCase,
) -> Vec<String> {
    let aggregates = case
        .aggregates
        .iter()
        .map(|aggregate| aggregate.as_str_name().to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(",");
    let mut command = std::process::Command::new("go");
    command
        .args([
            "run",
            "-tags=slicelabels",
            "./cmd/store-oracle",
            "--bucket",
            blocks.to_str().unwrap(),
            "--metric",
            "dummy_requests_total",
            "--aggregates",
            &aggregates,
            "--min-time",
            &case.min_time.to_string(),
            "--max-time",
            &case.max_time.to_string(),
            "--max-resolution",
            &case.max_resolution.to_string(),
            "--stream-wire-format",
        ])
        .current_dir(generator);
    if case.hints {
        command.args(["--hints-type", "request"]);
    }
    if let Some(type_url) = case.hints_type_url {
        command.args(["--hints-type-url", type_url]);
    }
    for matcher in &case.block_matchers {
        command.args([
            "--block-matcher",
            &format!("{}:{}:{}", matcher.kind, matcher.name, matcher.value),
        ]);
    }
    if case.skip_chunks {
        command.arg("--skip-chunks");
    }
    if let Some((index, total)) = case.shard {
        command.args([
            "--shard-enabled",
            "--shard-index",
            &index.to_string(),
            "--shard-total",
            &total.to_string(),
        ]);
    }
    if case.without_replica {
        command.arg("--without-replica-labels=replica");
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "Go oracle failed for {}: {}",
        case.name,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

async fn reader_stream(service: &ThanosStoreService, case: &QueryCase) -> Vec<String> {
    let hints = case.hints.then(|| prost_types::Any {
        type_url: case
            .hints_type_url
            .unwrap_or(REQUEST_HINTS_TYPE_URL)
            .to_owned(),
        value: hintspb::SeriesRequestHints {
            block_matchers: case
                .block_matchers
                .iter()
                .map(block_matcher_proto)
                .collect(),
            enable_query_stats: false,
        }
        .encode_to_vec(),
    });
    let mut stream = service
        .series(tonic::Request::new(thanos::SeriesRequest {
            min_time: case.min_time,
            max_time: case.max_time,
            matchers: vec![LabelMatcher {
                r#type: thanos::label_matcher::Type::Eq as i32,
                name: "__name__".to_owned(),
                value: "dummy_requests_total".to_owned(),
            }],
            max_resolution_window: case.max_resolution,
            aggregates: case
                .aggregates
                .iter()
                .map(|aggregate| *aggregate as i32)
                .collect(),
            partial_response_strategy: PartialResponseStrategy::Abort as i32,
            skip_chunks: case.skip_chunks,
            hints,
            shard_info: case.shard.map(|(index, total)| ShardInfo {
                shard_index: index,
                total_shards: total,
                by: false,
                labels: vec![],
            }),
            without_replica_labels: case
                .without_replica
                .then(|| vec!["replica".to_owned()])
                .unwrap_or_default(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut result = Vec::new();
    while let Some(response) = stream.next().await {
        result.push(encode_hex(&response.unwrap().encode_to_vec()));
    }
    result
}

fn block_matcher_proto(matcher: &BlockMatcher) -> LabelMatcher {
    let kind = match matcher.kind {
        "eq" => thanos::label_matcher::Type::Eq,
        "neq" => thanos::label_matcher::Type::Neq,
        "re" => thanos::label_matcher::Type::Re,
        "nre" => thanos::label_matcher::Type::Nre,
        "invalid" => {
            return LabelMatcher {
                r#type: 99,
                name: matcher.name.to_owned(),
                value: matcher.value.clone(),
            };
        }
        _ => unreachable!(),
    };
    LabelMatcher {
        r#type: kind as i32,
        name: matcher.name.to_owned(),
        value: matcher.value.clone(),
    }
}

fn go_matcher_error(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    matcher: &BlockMatcher,
) -> String {
    let output = std::process::Command::new("go")
        .args([
            "run",
            "-tags=slicelabels",
            "./cmd/store-oracle",
            "--bucket",
            blocks.to_str().unwrap(),
            "--metric",
            "dummy_requests_total",
            "--hints-type",
            "request",
            "--block-matcher",
            &format!("{}:{}:{}", matcher.kind, matcher.name, matcher.value),
        ])
        .current_dir(generator)
        .output()
        .unwrap();
    assert!(!output.status.success());
    String::from_utf8_lossy(&output.stderr).into_owned()
}

async fn reader_matcher_error(
    service: &ThanosStoreService,
    matcher: &BlockMatcher,
) -> tonic::Status {
    let result = service
        .series(tonic::Request::new(thanos::SeriesRequest {
            min_time: START,
            max_time: START + HOUR,
            matchers: vec![LabelMatcher {
                r#type: thanos::label_matcher::Type::Eq as i32,
                name: "__name__".to_owned(),
                value: "dummy_requests_total".to_owned(),
            }],
            hints: Some(prost_types::Any {
                type_url: REQUEST_HINTS_TYPE_URL.to_owned(),
                value: hintspb::SeriesRequestHints {
                    block_matchers: vec![block_matcher_proto(matcher)],
                    enable_query_stats: false,
                }
                .encode_to_vec(),
            }),
            ..Default::default()
        }))
        .await;
    match result {
        Ok(_) => panic!("invalid block matcher unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn go_error(generator: &std::path::Path, blocks: &std::path::Path, hints_type: &str) -> String {
    let output = std::process::Command::new("go")
        .args([
            "run",
            "-tags=slicelabels",
            "./cmd/store-oracle",
            "--bucket",
            blocks.to_str().unwrap(),
            "--metric",
            "dummy_requests_total",
            "--hints-type",
            hints_type,
        ])
        .current_dir(generator)
        .output()
        .unwrap();
    assert!(!output.status.success());
    String::from_utf8_lossy(&output.stderr).into_owned()
}

async fn reader_error(service: &ThanosStoreService, hints_type: &str) -> tonic::Status {
    let hints = match hints_type {
        "response" => prost_types::Any {
            type_url: "type.googleapis.com/hintspb.SeriesResponseHints".to_owned(),
            value: hintspb::SeriesResponseHints::default().encode_to_vec(),
        },
        "unknown" => prost_types::Any {
            type_url: "type.googleapis.com/unknown.SeriesRequestHints".to_owned(),
            value: vec![],
        },
        "invalid-url" => prost_types::Any {
            type_url: "hintspb.SeriesRequestHints".to_owned(),
            value: vec![],
        },
        "malformed" => prost_types::Any {
            type_url: REQUEST_HINTS_TYPE_URL.to_owned(),
            value: vec![0xff],
        },
        _ => unreachable!(),
    };
    match service
        .series(tonic::Request::new(thanos::SeriesRequest {
            min_time: START,
            max_time: START + HOUR,
            matchers: vec![LabelMatcher {
                r#type: thanos::label_matcher::Type::Eq as i32,
                name: "__name__".to_owned(),
                value: "dummy_requests_total".to_owned(),
            }],
            hints: Some(hints),
            ..Default::default()
        }))
        .await
    {
        Ok(_) => panic!("malformed hints unexpectedly succeeded"),
        Err(error) => error,
    }
}

fn is_response_hints(encoded: &str) -> bool {
    let response = thanos::SeriesResponse::decode(decode_hex(encoded).as_slice()).unwrap();
    matches!(
        response.result,
        Some(thanos::series_response::Result::Hints(_))
    )
}

fn response_hints(encoded: &str) -> hintspb::SeriesResponseHints {
    let response = thanos::SeriesResponse::decode(decode_hex(encoded).as_slice()).unwrap();
    let Some(thanos::series_response::Result::Hints(hints)) = response.result else {
        panic!("expected response hints");
    };
    assert_eq!(
        hints.type_url,
        "type.googleapis.com/hintspb.SeriesResponseHints"
    );
    hintspb::SeriesResponseHints::decode(hints.value.as_slice()).unwrap()
}

fn response_has_no_chunks(encoded: &str) -> bool {
    let response = thanos::SeriesResponse::decode(decode_hex(encoded).as_slice()).unwrap();
    match response.result {
        Some(thanos::series_response::Result::Series(series)) => series.chunks.is_empty(),
        Some(thanos::series_response::Result::Batch(batch)) => {
            batch.series.iter().all(|series| series.chunks.is_empty())
        }
        _ => false,
    }
}

fn generate_blocks(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    mint: i64,
    maxt: i64,
    samples: usize,
    downsample_5m: bool,
    downsample_1h: bool,
) -> BlockIds {
    let output = std::process::Command::new("go")
        .args([
            "run",
            ".",
            "--output",
            blocks.to_str().unwrap(),
            "--mint",
            &mint.to_string(),
            "--maxt",
            &maxt.to_string(),
            "--samples",
            &samples.to_string(),
            "--instances",
            "1",
            "--pods",
            "1",
            "--routes",
            "1",
            "--native-series",
            "1",
            &format!("--downsample-5m={downsample_5m}"),
            &format!("--downsample-1h={downsample_1h}"),
        ])
        .current_dir(generator)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "block generation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = String::from_utf8(output.stdout).unwrap();
    let id = |prefix: &str| {
        output
            .lines()
            .find_map(|line| line.strip_prefix(prefix).map(str::trim))
            .map(str::to_owned)
    };
    BlockIds {
        raw: id("raw block: ").unwrap(),
        five_minutes: id("5m block:  "),
        one_hour: id("1h block:  "),
    }
}

fn make_compaction_replacement(blocks: &std::path::Path, source: &str, replacement: &str) {
    let path = blocks.join(replacement).join("meta.json");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    meta["compaction"]["level"] = 2.into();
    meta["compaction"]["sources"] = serde_json::json!([source, replacement]);
    meta["thanos"]["source"] = "compactor".into();
    std::fs::write(path, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();
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
