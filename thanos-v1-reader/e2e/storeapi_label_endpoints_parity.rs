use prost::Message;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::ThanosRepositoryConfig,
    index_context,
    store_service::ThanosStoreService,
    thanos_proto::{
        hintspb,
        thanos::{self, LabelMatcher, store_server::Store},
    },
};
use tonic::Code;

const START: i64 = 1_700_000_000_000;
const HOUR: i64 = 60 * 60 * 1000;

#[derive(Clone, Copy, Debug)]
enum Endpoint {
    Names,
    Values,
}

impl Endpoint {
    fn oracle_name(self) -> &'static str {
        match self {
            Self::Names => "label-names",
            Self::Values => "label-values",
        }
    }

    fn request_type_url(self) -> &'static str {
        match self {
            Self::Names => "type.googleapis.com/hintspb.LabelNamesRequestHints",
            Self::Values => "type.googleapis.com/hintspb.LabelValuesRequestHints",
        }
    }
}

#[derive(Clone, Debug)]
struct Matcher {
    kind: &'static str,
    name: &'static str,
    value: String,
}

#[derive(Clone, Debug)]
struct Case {
    name: &'static str,
    endpoint: Endpoint,
    label: String,
    start: i64,
    end: i64,
    matchers: Vec<Matcher>,
    without: Vec<String>,
    limit: i64,
    hints: bool,
    block_matchers: Vec<Matcher>,
    hints_type_url: Option<&'static str>,
}

impl Case {
    fn names(name: &'static str) -> Self {
        Self {
            name,
            endpoint: Endpoint::Names,
            label: String::new(),
            start: START,
            end: START + 4 * HOUR - 1,
            matchers: vec![],
            without: vec![],
            limit: 0,
            hints: false,
            block_matchers: vec![],
            hints_type_url: None,
        }
    }

    fn values(name: &'static str, label: &str) -> Self {
        Self {
            endpoint: Endpoint::Values,
            label: label.to_owned(),
            ..Self::names(name)
        }
    }
}

#[tokio::test]
async fn label_names_and_values_match_go_bucket_store() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-label-endpoints-e2e-{}",
        std::process::id()
    ));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");

    let base = generate(
        &generator,
        &blocks,
        START,
        START + 3 * HOUR,
        "0",
        true,
        true,
        true,
    );
    let replica = generate(
        &generator,
        &blocks,
        START,
        START + 3 * HOUR,
        "1",
        false,
        false,
        false,
    );
    let superseded = generate(
        &generator,
        &blocks,
        START + 5 * HOUR / 2,
        START + 7 * HOUR / 2,
        "0",
        false,
        false,
        false,
    );
    let replacement = generate(
        &generator,
        &blocks,
        START + 5 * HOUR / 2,
        START + 7 * HOUR / 2,
        "0",
        false,
        false,
        false,
    );
    make_compaction_replacement(&blocks, &superseded[0], &replacement[0]);

    let repository = ThanosRepositoryConfig {
        name: "labels".to_owned(),
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

    let metric = |value: &str| Matcher {
        kind: "eq",
        name: "__name__",
        value: value.to_owned(),
    };
    let mut cases = vec![
        Case::names("all-label-names"),
        Case {
            name: "names-scalar-matcher",
            matchers: vec![metric("dummy_requests_total")],
            ..Case::names("unused")
        },
        Case {
            name: "names-classic-histogram-matcher",
            matchers: vec![metric("dummy_request_duration_seconds_bucket")],
            ..Case::names("unused")
        },
        Case {
            name: "names-native-histogram-matcher",
            matchers: vec![metric("dummy_native_histogram")],
            ..Case::names("unused")
        },
        Case {
            name: "names-external-label-matcher",
            matchers: vec![Matcher {
                kind: "eq",
                name: "replica",
                value: "1".to_owned(),
            }],
            ..Case::names("unused")
        },
        Case {
            name: "names-remove-replica",
            without: vec!["replica".to_owned()],
            ..Case::names("unused")
        },
        Case {
            name: "names-remove-ordinary-label",
            without: vec!["pod".to_owned(), "pod".to_owned(), "absent".to_owned()],
            ..Case::names("unused")
        },
        Case {
            name: "names-matched-series-remove-ordinary-label",
            matchers: vec![metric("dummy_requests_total")],
            without: vec!["pod".to_owned()],
            ..Case::names("unused")
        },
        Case {
            name: "names-empty-matcher-name-matches-missing-label",
            matchers: vec![Matcher {
                kind: "eq",
                name: "",
                value: String::new(),
            }],
            ..Case::names("unused")
        },
        Case {
            name: "names-limit-one",
            limit: 1,
            ..Case::names("unused")
        },
        Case {
            name: "names-negative-limit-is-unlimited",
            limit: -1,
            ..Case::names("unused")
        },
        Case {
            name: "names-outside-time-range",
            start: START + 10 * HOUR,
            end: START + 11 * HOUR,
            ..Case::names("unused")
        },
        Case {
            name: "names-exact-block-hint",
            hints: true,
            block_matchers: vec![Matcher {
                kind: "eq",
                name: "__block_id",
                value: base[0].clone(),
            }],
            ..Case::names("unused")
        },
        Case {
            name: "names-alternate-any-prefix",
            hints: true,
            hints_type_url: Some("custom.example/hintspb.LabelNamesRequestHints"),
            ..Case::names("unused")
        },
        Case {
            name: "names-mixed-block-hints",
            hints: true,
            block_matchers: vec![
                Matcher {
                    kind: "re",
                    name: "__block_id",
                    value: format!("{}|{}", base[0], replica[0]),
                },
                Matcher {
                    kind: "neq",
                    name: "__block_id",
                    value: replica[0].clone(),
                },
            ],
            ..Case::names("unused")
        },
        Case {
            name: "names-nonmatching-block-hint",
            hints: true,
            block_matchers: vec![Matcher {
                kind: "eq",
                name: "__block_id",
                value: "01AAAAAAAAAAAAAAAAAAAAAAAA".to_owned(),
            }],
            ..Case::names("unused")
        },
        Case::values("replica-values", "replica"),
        Case::values("pod-values", "pod"),
        Case::values("classic-histogram-le-values", "le"),
        Case::values("missing-label-values", "absent"),
        Case::values("empty-label-name-is-valid", ""),
        Case {
            name: "values-scalar-matcher",
            matchers: vec![metric("dummy_requests_total")],
            ..Case::values("unused", "pod")
        },
        Case {
            name: "values-classic-matcher",
            matchers: vec![metric("dummy_request_duration_seconds_bucket")],
            ..Case::values("unused", "route")
        },
        Case {
            name: "values-native-matcher",
            matchers: vec![metric("dummy_native_histogram")],
            ..Case::values("unused", "series")
        },
        Case {
            name: "values-limit-two",
            limit: 2,
            ..Case::values("unused", "pod")
        },
        Case {
            name: "values-negative-limit-is-unlimited",
            limit: -7,
            ..Case::values("unused", "pod")
        },
        Case {
            name: "values-target-replica-suppressed",
            without: vec!["replica".to_owned()],
            ..Case::values("unused", "replica")
        },
        Case {
            name: "values-unrelated-replica-suppression",
            without: vec!["replica".to_owned()],
            ..Case::values("unused", "pod")
        },
        Case {
            name: "values-exact-block-hint",
            hints: true,
            block_matchers: vec![Matcher {
                kind: "eq",
                name: "__block_id",
                value: base[1].clone(),
            }],
            ..Case::values("unused", "pod")
        },
        Case {
            name: "values-outside-time-range",
            start: START + 10 * HOUR,
            end: START + 11 * HOUR,
            ..Case::values("unused", "pod")
        },
    ];
    for case in &cases {
        exact_case(&service, &generator, &blocks, case).await;
    }

    let suppression = cases
        .iter()
        .find(|case| case.name == "values-target-replica-suppressed")
        .unwrap();
    let suppressed = reader_response(&service, suppression).await.unwrap();
    assert_eq!(
        suppressed,
        EndpointResponse::Values(thanos::LabelValuesResponse::default())
    );
    let go_suppressed = go_suppressed_with_bad_hints(&generator, &blocks);
    assert_eq!(go_suppressed, thanos::LabelValuesResponse::default());
    let rust_suppressed = service
        .label_values(tonic::Request::new(thanos::LabelValuesRequest {
            label: "replica".to_owned(),
            hints: Some(prost_types::Any {
                type_url: "type.googleapis.com/hintspb.LabelNamesResponseHints".to_owned(),
                value: vec![],
            }),
            without_replica_labels: vec!["replica".to_owned()],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rust_suppressed, thanos::LabelValuesResponse::default());

    for endpoint in [Endpoint::Names, Endpoint::Values] {
        for hints_type in ["response", "unknown", "invalid-url", "malformed"] {
            let go_error = go_error(&generator, &blocks, endpoint, hints_type);
            assert!(go_error.contains("InvalidArgument"), "{go_error}");
            let rust_error = reader_error(&service, endpoint, hints_type).await;
            assert_eq!(rust_error.code(), Code::InvalidArgument);
        }
        for matcher in [
            Matcher {
                kind: "invalid",
                name: "pod",
                value: "pod-0".to_owned(),
            },
            Matcher {
                kind: "re",
                name: "pod",
                value: "[".to_owned(),
            },
        ] {
            let go_error = go_matcher_error(&generator, &blocks, endpoint, &matcher);
            assert!(go_error.contains("InvalidArgument"), "{go_error}");
            assert_eq!(
                reader_matcher_error(&service, endpoint, &matcher)
                    .await
                    .code(),
                Code::InvalidArgument
            );
        }
    }

    cases.clear();
    std::fs::remove_dir_all(root).unwrap();
}

async fn exact_case(
    service: &ThanosStoreService,
    generator: &std::path::Path,
    blocks: &std::path::Path,
    case: &Case,
) {
    let expected = go_response(generator, blocks, case);
    let actual = reader_response(service, case).await.unwrap();
    match (expected, actual) {
        (EndpointResponse::Names(mut expected), EndpointResponse::Names(mut actual)) => {
            canonicalize_names_hints(&mut expected);
            canonicalize_names_hints(&mut actual);
            assert_eq!(actual, expected, "{}", case.name);
            assert!(actual.warnings.is_empty());
            if case.name == "names-exact-block-hint" {
                assert_eq!(actual.encode_to_vec(), expected.encode_to_vec());
            }
        }
        (EndpointResponse::Values(mut expected), EndpointResponse::Values(mut actual)) => {
            canonicalize_values_hints(&mut expected);
            canonicalize_values_hints(&mut actual);
            assert_eq!(actual, expected, "{}", case.name);
            assert!(actual.warnings.is_empty());
            if case.name == "values-exact-block-hint" {
                assert_eq!(actual.encode_to_vec(), expected.encode_to_vec());
            }
        }
        _ => panic!("endpoint response mismatch"),
    }
}

#[derive(Debug, PartialEq)]
enum EndpointResponse {
    Names(thanos::LabelNamesResponse),
    Values(thanos::LabelValuesResponse),
}

fn go_response(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    case: &Case,
) -> EndpointResponse {
    let mut command = oracle_command(generator, blocks, case.endpoint);
    command.args([
        "--min-time",
        &case.start.to_string(),
        "--max-time",
        &case.end.to_string(),
        "--limit",
        &case.limit.to_string(),
    ]);
    if matches!(case.endpoint, Endpoint::Values) {
        command.args(["--label", &case.label]);
    }
    if !case.without.is_empty() {
        command.arg(format!(
            "--without-replica-labels={}",
            case.without.join(",")
        ));
    }
    if case.hints {
        command.args(["--hints-type", "request"]);
    }
    if let Some(type_url) = case.hints_type_url {
        command.args(["--hints-type-url", type_url]);
    }
    for matcher in &case.matchers {
        command.args(["--series-matcher", &matcher_arg(matcher)]);
    }
    for matcher in &case.block_matchers {
        command.args(["--block-matcher", &matcher_arg(matcher)]);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "Go oracle failed for {}: {}",
        case.name,
        String::from_utf8_lossy(&output.stderr)
    );
    let encoded: String = serde_json::from_slice(&output.stdout).unwrap();
    let bytes = decode_hex(&encoded);
    match case.endpoint {
        Endpoint::Names => {
            EndpointResponse::Names(thanos::LabelNamesResponse::decode(bytes.as_slice()).unwrap())
        }
        Endpoint::Values => {
            EndpointResponse::Values(thanos::LabelValuesResponse::decode(bytes.as_slice()).unwrap())
        }
    }
}

async fn reader_response(
    service: &ThanosStoreService,
    case: &Case,
) -> Result<EndpointResponse, tonic::Status> {
    let hints = request_hints(
        case.endpoint,
        case.hints,
        case.hints_type_url,
        &case.block_matchers,
    );
    let matchers = case.matchers.iter().map(matcher_proto).collect();
    match case.endpoint {
        Endpoint::Names => service
            .label_names(tonic::Request::new(thanos::LabelNamesRequest {
                start: case.start,
                end: case.end,
                hints,
                matchers,
                without_replica_labels: case.without.clone(),
                limit: case.limit,
                ..Default::default()
            }))
            .await
            .map(|response| EndpointResponse::Names(response.into_inner())),
        Endpoint::Values => service
            .label_values(tonic::Request::new(thanos::LabelValuesRequest {
                label: case.label.clone(),
                start: case.start,
                end: case.end,
                hints,
                matchers,
                without_replica_labels: case.without.clone(),
                limit: case.limit,
                ..Default::default()
            }))
            .await
            .map(|response| EndpointResponse::Values(response.into_inner())),
    }
}

fn request_hints(
    endpoint: Endpoint,
    enabled: bool,
    type_url: Option<&str>,
    matchers: &[Matcher],
) -> Option<prost_types::Any> {
    enabled.then(|| {
        let block_matchers = matchers.iter().map(matcher_proto).collect();
        let value = match endpoint {
            Endpoint::Names => hintspb::LabelNamesRequestHints { block_matchers }.encode_to_vec(),
            Endpoint::Values => hintspb::LabelValuesRequestHints { block_matchers }.encode_to_vec(),
        };
        prost_types::Any {
            type_url: type_url.unwrap_or(endpoint.request_type_url()).to_owned(),
            value,
        }
    })
}

fn canonicalize_names_hints(response: &mut thanos::LabelNamesResponse) {
    let Some(hints) = &mut response.hints else {
        return;
    };
    assert_eq!(
        hints.type_url,
        "type.googleapis.com/hintspb.LabelNamesResponseHints"
    );
    let mut decoded = hintspb::LabelNamesResponseHints::decode(hints.value.as_slice()).unwrap();
    decoded
        .queried_blocks
        .sort_by(|left, right| left.id.cmp(&right.id));
    hints.value = decoded.encode_to_vec();
}

fn canonicalize_values_hints(response: &mut thanos::LabelValuesResponse) {
    let Some(hints) = &mut response.hints else {
        return;
    };
    assert_eq!(
        hints.type_url,
        "type.googleapis.com/hintspb.LabelValuesResponseHints"
    );
    let mut decoded = hintspb::LabelValuesResponseHints::decode(hints.value.as_slice()).unwrap();
    decoded
        .queried_blocks
        .sort_by(|left, right| left.id.cmp(&right.id));
    hints.value = decoded.encode_to_vec();
}

fn oracle_command(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    endpoint: Endpoint,
) -> std::process::Command {
    let mut command = std::process::Command::new("go");
    command
        .args([
            "run",
            "-tags=slicelabels",
            "./cmd/store-oracle",
            "--bucket",
            blocks.to_str().unwrap(),
            "--endpoint",
            endpoint.oracle_name(),
        ])
        .current_dir(generator);
    command
}

fn go_error(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    endpoint: Endpoint,
    hints_type: &str,
) -> String {
    let output = oracle_command(generator, blocks, endpoint)
        .args(["--hints-type", hints_type])
        .output()
        .unwrap();
    assert!(!output.status.success());
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn go_suppressed_with_bad_hints(
    generator: &std::path::Path,
    blocks: &std::path::Path,
) -> thanos::LabelValuesResponse {
    let output = oracle_command(generator, blocks, Endpoint::Values)
        .args([
            "--label",
            "replica",
            "--without-replica-labels=replica",
            "--hints-type",
            "response",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let encoded: String = serde_json::from_slice(&output.stdout).unwrap();
    thanos::LabelValuesResponse::decode(decode_hex(&encoded).as_slice()).unwrap()
}

async fn reader_error(
    service: &ThanosStoreService,
    endpoint: Endpoint,
    hints_type: &str,
) -> tonic::Status {
    let hints = match hints_type {
        "response" => prost_types::Any {
            type_url: match endpoint {
                Endpoint::Names => "type.googleapis.com/hintspb.LabelNamesResponseHints".to_owned(),
                Endpoint::Values => {
                    "type.googleapis.com/hintspb.LabelValuesResponseHints".to_owned()
                }
            },
            value: vec![],
        },
        "unknown" => prost_types::Any {
            type_url: "type.googleapis.com/unknown.RequestHints".to_owned(),
            value: vec![],
        },
        "invalid-url" => prost_types::Any {
            type_url: "hintspb.RequestHints".to_owned(),
            value: vec![],
        },
        "malformed" => prost_types::Any {
            type_url: endpoint.request_type_url().to_owned(),
            value: vec![0xff],
        },
        _ => unreachable!(),
    };
    let case = Case {
        hints: false,
        ..match endpoint {
            Endpoint::Names => Case::names("error"),
            Endpoint::Values => Case::values("error", "pod"),
        }
    };
    let result = match endpoint {
        Endpoint::Names => service
            .label_names(tonic::Request::new(thanos::LabelNamesRequest {
                hints: Some(hints),
                ..Default::default()
            }))
            .await
            .map(|_| ()),
        Endpoint::Values => service
            .label_values(tonic::Request::new(thanos::LabelValuesRequest {
                label: case.label,
                hints: Some(hints),
                ..Default::default()
            }))
            .await
            .map(|_| ()),
    };
    result.unwrap_err()
}

fn go_matcher_error(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    endpoint: Endpoint,
    matcher: &Matcher,
) -> String {
    let output = oracle_command(generator, blocks, endpoint)
        .args(["--series-matcher", &matcher_arg(matcher)])
        .output()
        .unwrap();
    assert!(!output.status.success());
    String::from_utf8_lossy(&output.stderr).into_owned()
}

async fn reader_matcher_error(
    service: &ThanosStoreService,
    endpoint: Endpoint,
    matcher: &Matcher,
) -> tonic::Status {
    let case = Case {
        matchers: vec![matcher.clone()],
        ..match endpoint {
            Endpoint::Names => Case::names("matcher-error"),
            Endpoint::Values => Case::values("matcher-error", "pod"),
        }
    };
    reader_response(service, &case).await.unwrap_err()
}

fn matcher_arg(matcher: &Matcher) -> String {
    format!("{}:{}:{}", matcher.kind, matcher.name, matcher.value)
}

fn matcher_proto(matcher: &Matcher) -> LabelMatcher {
    let kind = match matcher.kind {
        "eq" => thanos::label_matcher::Type::Eq as i32,
        "neq" => thanos::label_matcher::Type::Neq as i32,
        "re" => thanos::label_matcher::Type::Re as i32,
        "nre" => thanos::label_matcher::Type::Nre as i32,
        "invalid" => 99,
        _ => unreachable!(),
    };
    LabelMatcher {
        r#type: kind,
        name: matcher.name.to_owned(),
        value: matcher.value.clone(),
    }
}

fn generate(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    mint: i64,
    maxt: i64,
    replica: &str,
    downsample_5m: bool,
    downsample_1h: bool,
    clean: bool,
) -> Vec<String> {
    let mut command = std::process::Command::new("go");
    command.args([
        "run",
        ".",
        "--output",
        blocks.to_str().unwrap(),
        "--mint",
        &mint.to_string(),
        "--maxt",
        &maxt.to_string(),
        "--samples",
        "180",
        "--instances",
        "2",
        "--pods",
        "2",
        "--routes",
        "2",
        "--native-series",
        "2",
        &format!("--downsample-5m={downsample_5m}"),
        &format!("--downsample-1h={downsample_1h}"),
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
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| {
            ["raw block: ", "5m block:  ", "1h block:  "]
                .into_iter()
                .find_map(|prefix| line.strip_prefix(prefix).map(str::trim))
                .map(str::to_owned)
        })
        .collect()
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
