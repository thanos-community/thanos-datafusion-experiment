use prost::Message;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::ThanosRepositoryConfig,
    index_context,
    store_service::ThanosStoreService,
    thanos_proto::thanos::info::{self, info_server::Info},
};

const START: i64 = 1_700_000_000_000;
const HOUR: i64 = 60 * 60 * 1000;

#[tokio::test]
async fn static_loaded_block_info_matches_go() {
    let root =
        std::env::temp_dir().join(format!("thanos-v1-reader-info-e2e-{}", std::process::id()));
    let generator = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");

    let empty_blocks = root.join("empty-blocks");
    let empty_cache = root.join("empty-cache");
    std::fs::create_dir_all(&empty_blocks).unwrap();
    let empty_service = service(&empty_blocks, &empty_cache).await;
    let empty_go = go_info(&generator, &empty_blocks);
    let empty_rust = reader_info(&empty_service).await;
    assert_eq!(empty_rust.encode_to_vec(), empty_go.encode_to_vec());
    let empty_store = empty_rust.store.as_ref().unwrap();
    assert_eq!(empty_store.min_time, i64::MAX);
    assert_eq!(empty_store.max_time, i64::MIN);
    assert!(empty_rust.label_sets.is_empty());
    assert!(empty_store.tsdb_infos.is_empty());

    let single_blocks = root.join("single-block");
    let single_cache = root.join("single-cache");
    generate(
        &generator,
        &single_blocks,
        START,
        START + HOUR,
        "0",
        false,
        false,
        true,
    );
    let single_service = service(&single_blocks, &single_cache).await;
    let single_go = go_info(&generator, &single_blocks);
    let single_rust = reader_info(&single_service).await;
    assert_eq!(single_rust.encode_to_vec(), single_go.encode_to_vec());
    assert_capabilities(&single_rust);

    let blocks = root.join("multi-blocks");
    let cache = root.join("multi-cache");
    generate(
        &generator,
        &blocks,
        START,
        START + 3 * HOUR,
        "0",
        true,
        true,
        true,
    );
    generate(
        &generator,
        &blocks,
        START + 3 * HOUR,
        START + 4 * HOUR,
        "0",
        false,
        false,
        false,
    );
    generate(
        &generator,
        &blocks,
        START + 5 * HOUR,
        START + 6 * HOUR,
        "0",
        false,
        false,
        false,
    );
    generate(
        &generator,
        &blocks,
        START,
        START + 2 * HOUR,
        "1",
        false,
        false,
        false,
    );
    let source = generate(
        &generator,
        &blocks,
        START + 5 * HOUR,
        START + 6 * HOUR,
        "2",
        false,
        false,
        false,
    );
    let replacement = generate(
        &generator,
        &blocks,
        START + 7 * HOUR,
        START + 8 * HOUR,
        "2",
        false,
        false,
        false,
    );
    make_compaction_replacement(&blocks, &source, &replacement);

    let multi_service = service(&blocks, &cache).await;
    let mut expected = go_info(&generator, &blocks);
    let mut actual = reader_info(&multi_service).await;
    canonicalize_tsdb_infos(&mut expected);
    canonicalize_tsdb_infos(&mut actual);
    assert_eq!(actual, expected);
    assert_capabilities(&actual);

    let store = actual.store.as_ref().unwrap();
    assert_eq!(store.min_time, START);
    assert_eq!(store.max_time, START + 8 * HOUR);
    assert_eq!(actual.label_sets.len(), 3);
    assert_eq!(store.tsdb_infos.len(), 4);

    let replica_zero = store
        .tsdb_infos
        .iter()
        .filter(|info| label_value(info, "replica") == Some("0"))
        .collect::<Vec<_>>();
    assert_eq!(replica_zero.len(), 2);
    assert_eq!(
        replica_zero
            .iter()
            .map(|info| (info.min_time, info.max_time))
            .collect::<Vec<_>>(),
        vec![
            (START, START + 4 * HOUR),
            (START + 5 * HOUR, START + 6 * HOUR)
        ]
    );
    let replica_two = store
        .tsdb_infos
        .iter()
        .find(|info| label_value(info, "replica") == Some("2"))
        .unwrap();
    assert_eq!(
        (replica_two.min_time, replica_two.max_time),
        (START + 7 * HOUR, START + 8 * HOUR)
    );

    std::fs::remove_dir_all(root).unwrap();
}

async fn service(blocks: &std::path::Path, cache: &std::path::Path) -> ThanosStoreService {
    let repository = ThanosRepositoryConfig {
        name: "info".to_owned(),
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
    ThanosStoreService::new(context, std::slice::from_ref(&repository))
        .await
        .unwrap()
}

async fn reader_info(service: &ThanosStoreService) -> info::InfoResponse {
    service
        .info(tonic::Request::new(info::InfoRequest {}))
        .await
        .unwrap()
        .into_inner()
}

fn go_info(generator: &std::path::Path, blocks: &std::path::Path) -> info::InfoResponse {
    let output = std::process::Command::new("go")
        .args([
            "run",
            "-tags=slicelabels",
            "./cmd/store-oracle",
            "--bucket",
            blocks.to_str().unwrap(),
            "--endpoint",
            "info",
        ])
        .current_dir(generator)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Go Info oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let encoded: String = serde_json::from_slice(&output.stdout).unwrap();
    info::InfoResponse::decode(decode_hex(&encoded).as_slice()).unwrap()
}

fn canonicalize_tsdb_infos(response: &mut info::InfoResponse) {
    response
        .store
        .as_mut()
        .unwrap()
        .tsdb_infos
        .sort_by(|left, right| {
            labels_key(left)
                .cmp(&labels_key(right))
                .then_with(|| left.min_time.cmp(&right.min_time))
                .then_with(|| left.max_time.cmp(&right.max_time))
        });
}

fn labels_key(info: &info::TsdbInfo) -> Vec<(String, String)> {
    info.labels
        .as_ref()
        .map(|labels| {
            labels
                .labels
                .iter()
                .map(|label| (label.name.clone(), label.value.clone()))
                .collect()
        })
        .unwrap_or_default()
}

fn label_value<'a>(info: &'a info::TsdbInfo, name: &str) -> Option<&'a str> {
    info.labels
        .as_ref()?
        .labels
        .iter()
        .find(|label| label.name == name)
        .map(|label| label.value.as_str())
}

fn assert_capabilities(response: &info::InfoResponse) {
    assert_eq!(response.component_type, "store");
    let store = response.store.as_ref().unwrap();
    assert!(store.supports_sharding);
    assert!(store.supports_without_replica_labels);
    assert!(response.rules.is_none());
    assert!(response.metric_metadata.is_none());
    assert!(response.targets.is_none());
    assert!(response.exemplars.is_none());
    assert!(response.query.is_none());
    assert!(response.status.is_none());
}

#[allow(clippy::too_many_arguments)]
fn generate(
    generator: &std::path::Path,
    blocks: &std::path::Path,
    mint: i64,
    maxt: i64,
    replica: &str,
    downsample_5m: bool,
    downsample_1h: bool,
    clean: bool,
) -> String {
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
        "120",
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
        .find_map(|line| line.strip_prefix("raw block: ").map(str::trim))
        .unwrap()
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
