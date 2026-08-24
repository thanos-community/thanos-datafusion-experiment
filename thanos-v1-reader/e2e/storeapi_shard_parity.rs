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

#[tokio::test]
async fn storeapi_shards_match_go_and_partition_all_sample_types() {
    let root =
        std::env::temp_dir().join(format!("thanos-v1-reader-shard-e2e-{}", std::process::id()));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");
    generate_fixture(&generator, &blocks);

    let repository = ThanosRepositoryConfig {
        name: "shards".to_owned(),
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
    let labels = vec![
        "series".to_owned(),
        "route".to_owned(),
        "pod".to_owned(),
        "le".to_owned(),
        "instance".to_owned(),
    ];
    for metric in metrics {
        let unsharded = reader_wire(&service, metric, None).await;
        assert_eq!(unsharded, go_wire(&generator, &blocks, metric, None));
        assert!(!unsharded.is_empty(), "fixture series for {metric}");

        let mut union = BTreeSet::new();
        for index in 0..3 {
            let shard = ShardCase {
                index,
                total: 3,
                by: true,
                labels: labels.clone(),
            };
            let actual = reader_wire(&service, metric, Some(&shard)).await;
            assert_eq!(
                actual,
                go_wire(&generator, &blocks, metric, Some(&shard)),
                "Go shard mismatch for {metric}, shard {index}/3"
            );
            for series in actual {
                assert!(
                    union.insert(series),
                    "series overlapped shards for {metric}"
                );
            }
        }
        assert_eq!(
            union,
            unsharded.into_iter().collect(),
            "shard union for {metric}"
        );
    }

    for index in [0, 2, 4] {
        let shard = ShardCase {
            index,
            total: 5,
            by: false,
            labels: vec![
                "__name__".to_owned(),
                "cluster".to_owned(),
                "replica".to_owned(),
            ],
        };
        assert_eq!(
            reader_wire(&service, metrics[0], Some(&shard)).await,
            go_wire(&generator, &blocks, metrics[0], Some(&shard))
        );
    }

    for index in 0..2 {
        let shard = ShardCase {
            index,
            total: 2,
            by: true,
            labels: vec!["cluster".to_owned()],
        };
        assert_eq!(
            reader_wire(&service, metrics[0], Some(&shard)).await,
            go_wire(&generator, &blocks, metrics[0], Some(&shard)),
            "external labels participate in hashing"
        );
    }

    let unsharded = reader_wire(&service, metrics[0], None).await;
    for shard in [
        ShardCase {
            index: 99,
            total: 0,
            by: true,
            labels: vec![],
        },
        ShardCase {
            index: -1,
            total: -2,
            by: false,
            labels: vec!["pod".to_owned()],
        },
    ] {
        let actual = reader_wire(&service, metrics[0], Some(&shard)).await;
        assert_eq!(actual, unsharded, "non-positive totals disable sharding");
        assert_eq!(
            actual,
            go_wire(&generator, &blocks, metrics[0], Some(&shard))
        );
    }
    for shard in [
        ShardCase {
            index: 3,
            total: 3,
            by: false,
            labels: vec![],
        },
        ShardCase {
            index: -1,
            total: 3,
            by: true,
            labels: vec!["instance".to_owned()],
        },
    ] {
        let actual = reader_wire(&service, metrics[0], Some(&shard)).await;
        assert!(actual.is_empty(), "out-of-range indices select no series");
        assert_eq!(
            actual,
            go_wire(&generator, &blocks, metrics[0], Some(&shard))
        );
    }

    let mut empty = None;
    for index in 0..17 {
        let shard = ShardCase {
            index,
            total: 17,
            by: true,
            labels: vec!["instance".to_owned()],
        };
        if reader_wire(&service, metrics[0], Some(&shard))
            .await
            .is_empty()
        {
            empty = Some(shard);
            break;
        }
    }
    let empty = empty.expect("a natural empty shard");
    assert!(go_wire(&generator, &blocks, metrics[0], Some(&empty)).is_empty());

    std::fs::remove_dir_all(root).unwrap();
}

fn generate_fixture(generator: &std::path::Path, blocks: &std::path::Path) {
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
            "30",
            "--instances",
            "12",
            "--pods",
            "5",
            "--routes",
            "4",
            "--native-series",
            "3",
        ])
        .current_dir(generator)
        .output()
        .unwrap();
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
    shard: Option<&ShardCase>,
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
        ])
        .current_dir(generator);
    if let Some(shard) = shard {
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
        "Go shard oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

async fn reader_wire(
    service: &ThanosStoreService,
    metric: &str,
    shard: Option<&ShardCase>,
) -> Vec<String> {
    let mut stream = service
        .series(tonic::Request::new(thanos::SeriesRequest {
            min_time: MINT,
            max_time: MAXT,
            matchers: vec![LabelMatcher {
                r#type: thanos::label_matcher::Type::Eq as i32,
                name: "__name__".to_owned(),
                value: metric.to_owned(),
            }],
            aggregates: vec![Aggr::Raw as i32],
            partial_response_strategy: PartialResponseStrategy::Abort as i32,
            shard_info: shard.map(|shard| ShardInfo {
                shard_index: shard.index,
                total_shards: shard.total,
                by: shard.by,
                labels: shard.labels.clone(),
            }),
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
        assert!(!series.chunks.is_empty());
        for chunk in &series.chunks {
            let raw = chunk.raw.as_ref().expect("raw chunk");
            assert!(
                !thanos_v1_reader::chunk_reader::decode_record(&framed_record(raw), false)
                    .unwrap()
                    .is_empty(),
                "selected chunks expose decoded samples"
            );
        }
        result.push(encode_hex(&series.encode_to_vec()));
    }
    result
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
