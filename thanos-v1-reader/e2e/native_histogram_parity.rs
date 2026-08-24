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

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct OracleSeries {
    labels: BTreeMap<String, String>,
    chunks: Vec<OracleChunk>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct OracleChunk {
    min_time: i64,
    max_time: i64,
    encoding: i32,
    data: String,
    hash: u64,
}

#[tokio::test]
async fn native_histogram_chunks_match_go_bucket_store() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-native-e2e-{}",
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
            "1700000600000",
            "--samples",
            "10",
            "--instances",
            "1",
            "--pods",
            "1",
            "--routes",
            "1",
            "--native-series",
            "1",
            "--downsample-5m=false",
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

    for (metric, expected_encoding) in [
        ("dummy_native_histogram", 1),
        ("dummy_float_native_histogram", 2),
    ] {
        let expected = go_bucket_store_series(&generator_directory, &blocks, metric);
        let actual = reader_series(&service, metric).await;
        assert_eq!(actual, expected, "StoreAPI mismatch for {metric}");
        assert_eq!(actual.len(), 1);
        assert!(
            actual[0]
                .chunks
                .iter()
                .all(|chunk| chunk.encoding == expected_encoding)
        );
    }

    std::fs::remove_dir_all(root).unwrap();
}

fn go_bucket_store_series(
    generator_directory: &std::path::Path,
    blocks: &std::path::Path,
    metric: &str,
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
        ])
        .current_dir(generator_directory)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Go BucketStore oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

async fn reader_series(service: &ThanosStoreService, metric: &str) -> Vec<OracleSeries> {
    let request = thanos::SeriesRequest {
        min_time: i64::MIN,
        max_time: i64::MAX,
        matchers: vec![LabelMatcher {
            r#type: thanos::label_matcher::Type::Eq as i32,
            name: "__name__".to_owned(),
            value: metric.to_owned(),
        }],
        aggregates: vec![Aggr::Raw as i32],
        partial_response_strategy: PartialResponseStrategy::Abort as i32,
        ..Default::default()
    };
    let mut stream = service
        .series(tonic::Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let mut result = Vec::new();
    while let Some(response) = stream.next().await {
        let response = response.unwrap();
        let Some(thanos::series_response::Result::Series(series)) = response.result else {
            panic!("reader returned a non-series response");
        };
        result.push(OracleSeries {
            labels: series
                .labels
                .into_iter()
                .map(|label| (label.name, label.value))
                .collect(),
            chunks: series
                .chunks
                .into_iter()
                .map(|chunk| {
                    let raw = chunk.raw.expect("raw query must return a raw chunk");
                    OracleChunk {
                        min_time: chunk.min_time,
                        max_time: chunk.max_time,
                        encoding: raw.r#type,
                        data: encode_hex(&raw.data),
                        hash: raw.hash,
                    }
                })
                .collect(),
        });
    }
    result
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
