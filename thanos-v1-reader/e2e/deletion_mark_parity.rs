use std::{
    collections::BTreeSet,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::array::{Array, StringArray, StringViewArray};
use futures::StreamExt;
use prost::Message;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index_at, chunk_index_directory_path},
    block_sync::{BlockRefresher, Clock},
    config::ThanosRepositoryConfig,
    index_context,
    store_service::{SharedReaderState, ThanosStoreService},
    thanos_proto::{
        hintspb,
        thanos::{
            self, LabelMatcher, SeriesResponse,
            info::{self, info_server::Info},
            store_server::Store,
        },
    },
};

const START: i64 = 1_700_000_000_000;
const HOUR_MS: i64 = 60 * 60 * 1000;
const DELAY_SECONDS: u64 = 2 * 60 * 60;

#[derive(Debug)]
struct TestClock {
    unix_seconds: AtomicU64,
}

impl TestClock {
    fn new(unix_seconds: u64) -> Self {
        Self {
            unix_seconds: AtomicU64::new(unix_seconds),
        }
    }

    fn set(&self, unix_seconds: u64) {
        self.unix_seconds.store(unix_seconds, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(self.unix_seconds.load(Ordering::SeqCst))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletion_mark_grace_and_refresh_match_go() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-deletion-e2e-{}",
        std::process::id()
    ));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator = Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");
    let target = generate(&generator, &blocks, START, START + HOUR_MS, "0", true);
    let survivor = generate(
        &generator,
        &blocks,
        START + HOUR_MS,
        START + 2 * HOUR_MS,
        "1",
        false,
    );
    let repository = ThanosRepositoryConfig {
        name: "deletion".to_owned(),
        uri: format!("file://{}", blocks.display()),
    };
    let base_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let clock = Arc::new(TestClock::new(base_seconds));
    let service = initial_service(
        &repository,
        &cache,
        Duration::from_secs(DELAY_SECONDS),
        SystemTime::now(),
    )
    .await;
    let state = service.shared_state();
    let refresher = BlockRefresher::new_with_clock(
        state.clone(),
        std::slice::from_ref(&repository),
        &cache,
        Duration::from_secs(DELAY_SECONDS),
        Duration::ZERO,
        clock.clone(),
    );

    assert_go_view(&generator, &blocks, &service, &state, "2h").await;

    write_mark(&blocks, &target, &target, 1, base_seconds as i64);
    clock.set(base_seconds + DELAY_SECONDS - 1);
    refresher.refresh().await.unwrap();
    assert!(sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "2h").await;

    // Go uses a strict greater-than comparison, so the exact boundary remains visible.
    clock.set(base_seconds + DELAY_SECONDS);
    refresher.refresh().await.unwrap();
    assert!(sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "2h").await;

    clock.set(base_seconds + DELAY_SECONDS + 1);
    refresher.refresh().await.unwrap();
    assert!(!sql_block_ids(&state).await.contains(&target));
    assert!(sql_block_ids(&state).await.contains(&survivor));
    assert_go_view(&generator, &blocks, &service, &state, "0s").await;

    // Removing the marker makes the block eligible again on the next complete refresh.
    std::fs::remove_file(mark_path(&blocks, &target)).unwrap();
    refresher.refresh().await.unwrap();
    assert!(sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "2h").await;

    // Future timestamps have negative age and remain visible even with a zero delay.
    write_mark(
        &blocks,
        &target,
        &target,
        1,
        (base_seconds + 10 * 60 * 60) as i64,
    );
    refresher.refresh().await.unwrap();
    assert!(sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "0s").await;

    // Invalid marker ULIDs are unmarshal errors even though the marker ID is not matched to its path.
    write_mark(&blocks, &target, "not-a-ulid", 1, 0);
    refresher.refresh().await.unwrap();
    assert!(sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "0s").await;

    // Partial JSON is warned about and ignored by the current Go filter.
    std::fs::write(mark_path(&blocks, &target), b"{not-json").unwrap();
    refresher.refresh().await.unwrap();
    assert!(sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "0s").await;

    // Go defaults a missing deletion_time to Unix zero, and re-reads valid updates each refresh.
    std::fs::write(
        mark_path(&blocks, &target),
        serde_json::to_vec(&serde_json::json!({"id": target, "version": 1})).unwrap(),
    )
    .unwrap();
    refresher.refresh().await.unwrap();
    assert!(!sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "0s").await;

    std::fs::remove_file(mark_path(&blocks, &target)).unwrap();
    refresher.refresh().await.unwrap();
    let healthy = canonical_info(reader_info(&service).await);

    // Unsupported versions fail the staged refresh and retain the healthy published snapshot.
    write_mark(&blocks, &target, &target, 2, 0);
    assert!(refresher.refresh().await.is_err());
    assert_eq!(canonical_info(reader_info(&service).await), healthy);
    assert!(!go_succeeds(&generator, &blocks, "0s"));
    std::fs::remove_file(mark_path(&blocks, &target)).unwrap();
    refresher.refresh().await.unwrap();

    // A non-NotFound marker read failure also aborts publication in both stacks.
    let healthy = canonical_info(reader_info(&service).await);
    std::fs::create_dir(mark_path(&blocks, &target)).unwrap();
    assert!(refresher.refresh().await.is_err());
    assert_eq!(canonical_info(reader_info(&service).await), healthy);
    assert!(!go_succeeds(&generator, &blocks, "0s"));
    std::fs::remove_dir(mark_path(&blocks, &target)).unwrap();
    refresher.refresh().await.unwrap();

    // Deletion filtering runs before compaction-source deduplication.
    let source = generate(
        &generator,
        &blocks,
        START + 2 * HOUR_MS,
        START + 3 * HOUR_MS,
        "2",
        false,
    );
    let replacement = generate(
        &generator,
        &blocks,
        START + 3 * HOUR_MS,
        START + 4 * HOUR_MS,
        "2",
        false,
    );
    make_compaction_replacement(&blocks, &source, &replacement);
    write_mark(&blocks, &replacement, &replacement, 1, 0);
    refresher.refresh().await.unwrap();
    let ids = sql_block_ids(&state).await;
    assert!(ids.contains(&source));
    assert!(!ids.contains(&replacement));
    assert_go_view(&generator, &blocks, &service, &state, "0s").await;

    std::fs::remove_file(mark_path(&blocks, &replacement)).unwrap();
    write_mark(&blocks, &source, &source, 1, 0);
    refresher.refresh().await.unwrap();
    let ids = sql_block_ids(&state).await;
    assert!(!ids.contains(&source));
    assert!(ids.contains(&replacement));
    assert_go_view(&generator, &blocks, &service, &state, "0s").await;

    drop(refresher);
    drop(service);
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}

async fn initial_service(
    repository: &ThanosRepositoryConfig,
    cache: &Path,
    delay: Duration,
    now: SystemTime,
) -> ThanosStoreService {
    let cache = cache.to_str().unwrap();
    let schemas = build_block_index_at(
        std::slice::from_ref(repository),
        cache,
        delay,
        Duration::ZERO,
        now,
    )
    .await
    .unwrap();
    let context = index_context(
        &block_index_file_path(cache),
        &chunk_index_directory_path(cache),
        &schemas,
        std::slice::from_ref(repository),
    )
    .await
    .unwrap();
    ThanosStoreService::new(context, std::slice::from_ref(repository))
        .await
        .unwrap()
}

async fn assert_go_view(
    generator: &Path,
    blocks: &Path,
    service: &ThanosStoreService,
    state: &SharedReaderState,
    delay: &str,
) {
    assert_eq!(
        canonical_info(reader_info(service).await),
        canonical_info(go_info(generator, blocks, delay))
    );
    assert_eq!(
        canonical_names(reader_names(service).await),
        canonical_names(go_names(generator, blocks, delay))
    );
    assert_eq!(
        canonical_values(reader_values(service).await),
        canonical_values(go_values(generator, blocks, delay))
    );
    assert_eq!(
        canonical_series(reader_series(service).await),
        canonical_series(go_series(generator, blocks, delay))
    );
    let replicas = sql_replicas(state).await;
    assert_eq!(replicas, reader_values(service).await.values);
}

async fn reader_info(service: &ThanosStoreService) -> info::InfoResponse {
    service
        .info(tonic::Request::new(info::InfoRequest {}))
        .await
        .unwrap()
        .into_inner()
}

async fn reader_names(service: &ThanosStoreService) -> thanos::LabelNamesResponse {
    service
        .label_names(tonic::Request::new(thanos::LabelNamesRequest {
            start: START,
            end: START + 10 * HOUR_MS,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn reader_values(service: &ThanosStoreService) -> thanos::LabelValuesResponse {
    service
        .label_values(tonic::Request::new(thanos::LabelValuesRequest {
            label: "replica".to_owned(),
            start: START,
            end: START + 10 * HOUR_MS,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn reader_series(service: &ThanosStoreService) -> Vec<SeriesResponse> {
    service
        .series(tonic::Request::new(series_request()))
        .await
        .unwrap()
        .into_inner()
        .map(|response| response.unwrap())
        .collect()
        .await
}

fn series_request() -> thanos::SeriesRequest {
    thanos::SeriesRequest {
        min_time: START,
        max_time: START + 10 * HOUR_MS,
        matchers: vec![LabelMatcher {
            r#type: thanos::label_matcher::Type::Eq as i32,
            name: "__name__".to_owned(),
            value: "dummy_requests_total".to_owned(),
        }],
        partial_response_strategy: thanos::PartialResponseStrategy::Abort as i32,
        ..Default::default()
    }
}

async fn sql_block_ids(state: &SharedReaderState) -> BTreeSet<String> {
    sql_strings(state, "SELECT DISTINCT block_ulid FROM chunks").await
}

async fn sql_replicas(state: &SharedReaderState) -> Vec<String> {
    sql_strings(
        state,
        "SELECT DISTINCT replica FROM metrics.dummy_requests_total ORDER BY replica",
    )
    .await
    .into_iter()
    .collect()
}

async fn sql_strings(state: &SharedReaderState, query: &str) -> BTreeSet<String> {
    let snapshot = state.query_snapshot().await;
    let batches = snapshot
        .context()
        .sql(query)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column(0);
            (0..column.len())
                .map(|index| {
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
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn go_info(generator: &Path, blocks: &Path, delay: &str) -> info::InfoResponse {
    decode_single(go(generator, blocks, delay, &["--endpoint", "info"]).stdout)
}

fn go_names(generator: &Path, blocks: &Path, delay: &str) -> thanos::LabelNamesResponse {
    decode_single(
        go(
            generator,
            blocks,
            delay,
            &[
                "--endpoint",
                "label-names",
                "--min-time",
                &START.to_string(),
                "--max-time",
                &(START + 10 * HOUR_MS).to_string(),
            ],
        )
        .stdout,
    )
}

fn go_values(generator: &Path, blocks: &Path, delay: &str) -> thanos::LabelValuesResponse {
    decode_single(
        go(
            generator,
            blocks,
            delay,
            &[
                "--endpoint",
                "label-values",
                "--label",
                "replica",
                "--min-time",
                &START.to_string(),
                "--max-time",
                &(START + 10 * HOUR_MS).to_string(),
            ],
        )
        .stdout,
    )
}

fn go_series(generator: &Path, blocks: &Path, delay: &str) -> Vec<SeriesResponse> {
    let output = go(
        generator,
        blocks,
        delay,
        &[
            "--metric",
            "dummy_requests_total",
            "--min-time",
            &START.to_string(),
            "--max-time",
            &(START + 10 * HOUR_MS).to_string(),
            "--stream-wire-format",
        ],
    );
    assert!(
        output.status.success(),
        "Go oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let values: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();
    values
        .into_iter()
        .map(|value| SeriesResponse::decode(decode_hex(&value).as_slice()).unwrap())
        .collect()
}

fn go_succeeds(generator: &Path, blocks: &Path, delay: &str) -> bool {
    go(generator, blocks, delay, &["--endpoint", "info"])
        .status
        .success()
}

fn go(generator: &Path, blocks: &Path, delay: &str, args: &[&str]) -> std::process::Output {
    let output = std::process::Command::new("go")
        .args(["run", "-tags=slicelabels", "./cmd/store-oracle", "--bucket"])
        .arg(blocks)
        .args(["--deletion-mark-delay", delay])
        .args(args)
        .current_dir(generator)
        .output()
        .unwrap();
    if !args.contains(&"--endpoint") || !args.contains(&"info") {
        assert!(
            output.status.success(),
            "Go oracle failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    output
}

fn decode_single<T: Message + Default>(output: Vec<u8>) -> T {
    let encoded: String = serde_json::from_slice(&output).unwrap();
    T::decode(decode_hex(&encoded).as_slice()).unwrap()
}

fn canonical_info(mut response: info::InfoResponse) -> Vec<u8> {
    response
        .store
        .as_mut()
        .unwrap()
        .tsdb_infos
        .sort_by_key(|info| {
            (
                info.labels.as_ref().unwrap().encode_to_vec(),
                info.min_time,
                info.max_time,
            )
        });
    response.encode_to_vec()
}

fn canonical_names(mut response: thanos::LabelNamesResponse) -> Vec<u8> {
    if let Some(hints) = &mut response.hints {
        let mut value = hintspb::LabelNamesResponseHints::decode(hints.value.as_slice()).unwrap();
        value
            .queried_blocks
            .sort_by(|left, right| left.id.cmp(&right.id));
        hints.value = value.encode_to_vec();
    }
    response.encode_to_vec()
}

fn canonical_values(mut response: thanos::LabelValuesResponse) -> Vec<u8> {
    if let Some(hints) = &mut response.hints {
        let mut value = hintspb::LabelValuesResponseHints::decode(hints.value.as_slice()).unwrap();
        value
            .queried_blocks
            .sort_by(|left, right| left.id.cmp(&right.id));
        hints.value = value.encode_to_vec();
    }
    response.encode_to_vec()
}

fn canonical_series(mut responses: Vec<SeriesResponse>) -> Vec<Vec<u8>> {
    for response in &mut responses {
        if let Some(thanos::series_response::Result::Hints(hints)) = &mut response.result {
            let mut value = hintspb::SeriesResponseHints::decode(hints.value.as_slice()).unwrap();
            value
                .queried_blocks
                .sort_by(|left, right| left.id.cmp(&right.id));
            hints.value = value.encode_to_vec();
        }
    }
    let mut encoded = responses
        .into_iter()
        .map(|response| response.encode_to_vec())
        .collect::<Vec<_>>();
    encoded.sort();
    encoded
}

fn write_mark(blocks: &Path, block: &str, mark_id: &str, version: i32, deletion_time: i64) {
    std::fs::write(
        mark_path(blocks, block),
        serde_json::to_vec_pretty(&serde_json::json!({
            "id": mark_id,
            "version": version,
            "details": "parity fixture",
            "deletion_time": deletion_time,
        }))
        .unwrap(),
    )
    .unwrap();
}

fn mark_path(blocks: &Path, block: &str) -> std::path::PathBuf {
    blocks.join(block).join("deletion-mark.json")
}

fn generate(
    generator: &Path,
    blocks: &Path,
    mint: i64,
    maxt: i64,
    replica: &str,
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
        "12",
        "--instances",
        "1",
        "--pods",
        "1",
        "--routes",
        "1",
        "--native-series",
        "1",
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

fn make_compaction_replacement(blocks: &Path, source: &str, replacement: &str) {
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
