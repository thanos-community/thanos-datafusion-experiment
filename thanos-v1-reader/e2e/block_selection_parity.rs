use std::collections::BTreeMap;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::ThanosRepositoryConfig,
    index_context,
    store_service::ThanosStoreService,
    thanos_proto::thanos::{
        self, Aggr, LabelMatcher, PartialResponseStrategy, store_server::Store,
    },
};

const START: i64 = 1_700_000_000_000;
const HOUR: i64 = 60 * 60 * 1000;
const RESOLUTION_5M: i64 = 5 * 60 * 1000;
const RESOLUTION_1H: i64 = HOUR;

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct OracleSeries {
    labels: BTreeMap<String, String>,
    chunks: Vec<OracleAggrChunk>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct OracleAggrChunk {
    min_time: i64,
    max_time: i64,
    encoding: i32,
    data: String,
    hash: u64,
    #[serde(default)]
    samples: Vec<OracleSample>,
    count: Option<OracleEncodedChunk>,
    sum: Option<OracleEncodedChunk>,
    min: Option<OracleEncodedChunk>,
    max: Option<OracleEncodedChunk>,
    counter: Option<OracleEncodedChunk>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct OracleEncodedChunk {
    encoding: i32,
    data: String,
    hash: u64,
    samples: Vec<OracleSample>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct OracleSample {
    timestamp: i64,
    value_bits: u64,
}

#[derive(Clone, Copy)]
struct QueryCase {
    name: &'static str,
    min_time: i64,
    max_time: i64,
    max_resolution: i64,
    raw: bool,
}

#[tokio::test]
async fn mixed_block_resolution_and_overlap_selection_matches_go() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-block-selection-e2e-{}",
        std::process::id()
    ));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");

    let _base = generate_blocks(
        &generator,
        &blocks,
        START,
        START + 3 * HOUR,
        180,
        true,
        true,
    );
    let _tail = generate_blocks(
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
    make_compaction_replacement(&blocks, &superseded, &replacement);

    let repository = ThanosRepositoryConfig {
        name: "selection".to_owned(),
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

    for case in [
        QueryCase {
            name: "raw-full-range",
            min_time: START,
            max_time: START + 4 * HOUR - 1,
            max_resolution: 0,
            raw: true,
        },
        QueryCase {
            name: "five-minute-with-raw-gap-fallback",
            min_time: START,
            max_time: START + 4 * HOUR - 1,
            max_resolution: RESOLUTION_5M,
            raw: false,
        },
        QueryCase {
            name: "one-hour-with-overlapping-raw-fallback",
            min_time: START,
            max_time: START + 4 * HOUR - 1,
            max_resolution: RESOLUTION_1H,
            raw: false,
        },
        QueryCase {
            name: "partial-tail-window",
            min_time: START + 13 * HOUR / 4,
            max_time: START + 15 * HOUR / 4,
            max_resolution: RESOLUTION_1H,
            raw: false,
        },
        QueryCase {
            name: "overlap-window",
            min_time: START + 11 * HOUR / 4,
            max_time: START + 13 * HOUR / 4,
            max_resolution: RESOLUTION_1H,
            raw: false,
        },
    ] {
        let aggregate_names = if case.raw {
            "raw"
        } else {
            "count,sum,min,max,counter"
        };
        let aggregates = if case.raw {
            vec![Aggr::Raw]
        } else {
            vec![Aggr::Count, Aggr::Sum, Aggr::Min, Aggr::Max, Aggr::Counter]
        };
        let expected = go_series(&generator, &blocks, aggregate_names, case);
        let actual = reader_series(&service, &aggregates, case).await;
        assert_eq!(actual, expected, "selection mismatch for {}", case.name);
        assert!(actual.iter().flat_map(|series| &series.chunks).count() > 0);
    }

    std::fs::remove_dir_all(root).unwrap();
}

fn generate_blocks(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    mint: i64,
    maxt: i64,
    samples: usize,
    downsample_5m: bool,
    downsample_1h: bool,
) -> String {
    let downsample_5m = format!("--downsample-5m={downsample_5m}");
    let downsample_1h = format!("--downsample-1h={downsample_1h}");
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
            &downsample_5m,
            &downsample_1h,
        ])
        .current_dir(generator)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "block generation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("raw block: ").map(str::trim))
        .expect("raw block ID")
        .to_owned()
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

fn go_series(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    aggregates: &str,
    case: QueryCase,
) -> Vec<OracleSeries> {
    let output = std::process::Command::new("go")
        .args([
            "run",
            "-tags=slicelabels",
            "./cmd/store-oracle",
            "--bucket",
            blocks.to_str().unwrap(),
            "--metric",
            "dummy_requests_total",
            "--aggregates",
            aggregates,
            "--min-time",
            &case.min_time.to_string(),
            "--max-time",
            &case.max_time.to_string(),
            "--max-resolution",
            &case.max_resolution.to_string(),
        ])
        .current_dir(generator)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Go oracle failed for {}: {}",
        case.name,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

async fn reader_series(
    service: &ThanosStoreService,
    aggregates: &[Aggr],
    case: QueryCase,
) -> Vec<OracleSeries> {
    let mut stream = service
        .series(tonic::Request::new(thanos::SeriesRequest {
            min_time: case.min_time,
            max_time: case.max_time,
            matchers: vec![LabelMatcher {
                r#type: thanos::label_matcher::Type::Eq as i32,
                name: "__name__".to_owned(),
                value: "dummy_requests_total".to_owned(),
            }],
            aggregates: aggregates
                .iter()
                .map(|aggregate| *aggregate as i32)
                .collect(),
            max_resolution_window: case.max_resolution,
            partial_response_strategy: PartialResponseStrategy::Abort as i32,
            ..Default::default()
        }))
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
            chunks: series.chunks.into_iter().map(convert_aggr_chunk).collect(),
        });
    }
    result
}

fn convert_aggr_chunk(chunk: thanos::AggrChunk) -> OracleAggrChunk {
    let raw = chunk.raw.map(convert_chunk);
    OracleAggrChunk {
        min_time: chunk.min_time,
        max_time: chunk.max_time,
        encoding: raw.as_ref().map(|chunk| chunk.encoding).unwrap_or(0),
        data: raw
            .as_ref()
            .map(|chunk| chunk.data.clone())
            .unwrap_or_default(),
        hash: raw.as_ref().map(|chunk| chunk.hash).unwrap_or(0),
        samples: raw.map(|chunk| chunk.samples).unwrap_or_default(),
        count: chunk.count.map(convert_chunk),
        sum: chunk.sum.map(convert_chunk),
        min: chunk.min.map(convert_chunk),
        max: chunk.max.map(convert_chunk),
        counter: chunk.counter.map(convert_chunk),
    }
}

fn convert_chunk(chunk: thanos::Chunk) -> OracleEncodedChunk {
    let samples = thanos_v1_reader::chunk_reader::decode_record(&framed_record(&chunk), false)
        .unwrap()
        .into_iter()
        .map(|sample| OracleSample {
            timestamp: sample.timestamp,
            value_bits: sample.value.to_bits(),
        })
        .collect();
    OracleEncodedChunk {
        encoding: chunk.r#type,
        data: encode_hex(&chunk.data),
        hash: chunk.hash,
        samples,
    }
}

fn framed_record(chunk: &thanos::Chunk) -> Vec<u8> {
    let mut record = encode_uvarint(chunk.data.len());
    record.push(1);
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
