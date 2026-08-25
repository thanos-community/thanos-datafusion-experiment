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
use chrono::{DateTime, SecondsFormat, Utc};
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
const BASE_SECONDS: u64 = 1_735_689_600;
const DELAY_SECONDS: u64 = 2 * 60 * 60;
const DELETION_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

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
async fn consistency_delay_refresh_matches_go() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-consistency-e2e-{}",
        std::process::id()
    ));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator = Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");

    let target = generate(&generator, &blocks, START, START + HOUR_MS, "0", true);
    set_meta_fields(&blocks, &target, Some(BASE_SECONDS as i64), Some("sidecar"));
    let survivor = generate(
        &generator,
        &blocks,
        START + HOUR_MS,
        START + 2 * HOUR_MS,
        "1",
        false,
    );
    set_meta_fields(
        &blocks,
        &survivor,
        Some((BASE_SECONDS - DELAY_SECONDS) as i64),
        Some("sidecar"),
    );

    let repository = ThanosRepositoryConfig {
        name: "consistency".to_owned(),
        uri: format!("file://{}", blocks.display()),
    };
    let clock = Arc::new(TestClock::new(BASE_SECONDS));
    let service = initial_service(
        &repository,
        &cache,
        Duration::from_secs(DELAY_SECONDS),
        clock.now(),
    )
    .await;
    let state = service.shared_state();
    let refresher = BlockRefresher::new_with_clock(
        state.clone(),
        std::slice::from_ref(&repository),
        &cache,
        DELETION_DELAY,
        Duration::from_secs(DELAY_SECONDS),
        clock.clone(),
    );

    let ids = sql_block_ids(&state).await;
    assert!(!ids.contains(&target));
    assert!(ids.contains(&survivor));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;

    clock.set(BASE_SECONDS + DELAY_SECONDS - 1);
    refresher.refresh().await.unwrap();
    assert!(!sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;

    // The Go filter hides age < delay, so exact equality is mature.
    clock.set(BASE_SECONDS + DELAY_SECONDS);
    refresher.refresh().await.unwrap();
    assert!(sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;

    clock.set(BASE_SECONDS + DELAY_SECONDS + 1);
    refresher.refresh().await.unwrap();
    assert!(sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;

    // Without upload_time, the filter uses the directory ULID timestamp. Go's unsigned
    // subtraction makes a future ULID visible rather than too fresh.
    let future_original = generate(
        &generator,
        &blocks,
        START + 2 * HOUR_MS,
        START + 3 * HOUR_MS,
        "2",
        false,
    );
    let future = rename_with_timestamp(
        &blocks,
        &future_original,
        (BASE_SECONDS + 10 * 60 * 60) * 1000,
    );
    set_meta_fields(&blocks, &future, None, Some("sidecar"));
    refresher.refresh().await.unwrap();
    assert!(sql_block_ids(&state).await.contains(&future));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;

    // Zero is the Store Gateway default. It admits upload_time at equality and still preserves
    // the future-ULID fallback behavior.
    let zero_cache = root.join("zero-cache");
    let zero_service = initial_service(&repository, &zero_cache, Duration::ZERO, clock.now()).await;
    let zero_state = zero_service.shared_state();
    assert!(sql_block_ids(&zero_state).await.contains(&target));
    assert!(sql_block_ids(&zero_state).await.contains(&future));
    assert_go_view(
        &generator,
        &blocks,
        &zero_service,
        &zero_state,
        "0s",
        clock.now(),
    )
    .await;
    drop(zero_service);
    drop(zero_state);

    // A future explicit upload_time has negative age and is too fresh; unlike the future-ULID
    // fallback, this path uses signed time arithmetic.
    set_meta_fields(
        &blocks,
        &target,
        Some((BASE_SECONDS + DELAY_SECONDS + 10 * 60 * 60) as i64),
        Some("sidecar"),
    );
    refresher.refresh().await.unwrap();
    assert!(!sql_block_ids(&state).await.contains(&target));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;
    set_meta_fields(&blocks, &target, Some(BASE_SECONDS as i64), Some("sidecar"));
    refresher.refresh().await.unwrap();

    // Consistency runs before deletion filtering and deduplication. A fresh compactor replacement
    // bypasses consistency; its deletion mark removes it before dedup, retaining the source.
    let source = generate(
        &generator,
        &blocks,
        START + 3 * HOUR_MS,
        START + 4 * HOUR_MS,
        "3",
        false,
    );
    set_meta_fields(
        &blocks,
        &source,
        Some((BASE_SECONDS - DELAY_SECONDS) as i64),
        Some("sidecar"),
    );
    let replacement = generate(
        &generator,
        &blocks,
        START + 4 * HOUR_MS,
        START + 5 * HOUR_MS,
        "3",
        false,
    );
    make_compaction_replacement(&blocks, &source, &replacement);
    set_meta_fields(
        &blocks,
        &replacement,
        Some((BASE_SECONDS + DELAY_SECONDS + 1) as i64),
        Some("compactor"),
    );
    write_mark(&blocks, &replacement);
    refresher.refresh().await.unwrap();
    let ids = sql_block_ids(&state).await;
    assert!(ids.contains(&source));
    assert!(!ids.contains(&replacement));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;

    std::fs::remove_file(mark_path(&blocks, &replacement)).unwrap();
    refresher.refresh().await.unwrap();
    let ids = sql_block_ids(&state).await;
    assert!(!ids.contains(&source));
    assert!(ids.contains(&replacement));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;

    // A fresh non-compactor replacement is removed before dedup, leaving its source active.
    set_meta_fields(
        &blocks,
        &replacement,
        Some((BASE_SECONDS + DELAY_SECONDS + 1) as i64),
        Some("sidecar"),
    );
    refresher.refresh().await.unwrap();
    let ids = sql_block_ids(&state).await;
    assert!(ids.contains(&source));
    assert!(!ids.contains(&replacement));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;

    // Non-ULID directories are not blocks in the Go lister and are ignored.
    let invalid = generate(
        &generator,
        &blocks,
        START + 5 * HOUR_MS,
        START + 6 * HOUR_MS,
        "4",
        false,
    );
    std::fs::rename(blocks.join(&invalid), blocks.join("not-a-block")).unwrap();
    refresher.refresh().await.unwrap();
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;

    // A malformed upload_time is a corrupt partial meta. Go skips it without failing the sync;
    // Rust publishes the same valid remainder atomically.
    set_raw_upload_time(&blocks, &target, serde_json::json!("not-a-time"));
    refresher.refresh().await.unwrap();
    assert!(!sql_block_ids(&state).await.contains(&target));
    assert!(go_succeeds(&generator, &blocks, "2h", clock.now()));
    assert_go_view(&generator, &blocks, &service, &state, "2h", clock.now()).await;
    set_meta_fields(&blocks, &target, Some(BASE_SECONDS as i64), Some("sidecar"));
    refresher.refresh().await.unwrap();

    drop(refresher);
    drop(service);
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}

async fn initial_service(
    repository: &ThanosRepositoryConfig,
    cache: &Path,
    consistency_delay: Duration,
    now: SystemTime,
) -> ThanosStoreService {
    let cache = cache.to_str().unwrap();
    let schemas = build_block_index_at(
        std::slice::from_ref(repository),
        cache,
        DELETION_DELAY,
        consistency_delay,
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
    now: SystemTime,
) {
    assert_eq!(
        canonical_info(reader_info(service).await),
        canonical_info(go_info(generator, blocks, delay, now))
    );
    assert_eq!(
        canonical_names(reader_names(service).await),
        canonical_names(go_names(generator, blocks, delay, now))
    );
    assert_eq!(
        canonical_values(reader_values(service).await),
        canonical_values(go_values(generator, blocks, delay, now))
    );
    assert_eq!(
        canonical_series(reader_series(service).await),
        canonical_series(go_series(generator, blocks, delay, now))
    );
    assert_eq!(
        sql_replicas(state).await,
        reader_values(service).await.values
    );
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
    let batches = state
        .query_snapshot()
        .await
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

fn go_info(generator: &Path, blocks: &Path, delay: &str, now: SystemTime) -> info::InfoResponse {
    decode_single(go(generator, blocks, delay, now, &["--endpoint", "info"]).stdout)
}

fn go_names(
    generator: &Path,
    blocks: &Path,
    delay: &str,
    now: SystemTime,
) -> thanos::LabelNamesResponse {
    decode_single(
        go(
            generator,
            blocks,
            delay,
            now,
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

fn go_values(
    generator: &Path,
    blocks: &Path,
    delay: &str,
    now: SystemTime,
) -> thanos::LabelValuesResponse {
    decode_single(
        go(
            generator,
            blocks,
            delay,
            now,
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

fn go_series(generator: &Path, blocks: &Path, delay: &str, now: SystemTime) -> Vec<SeriesResponse> {
    let output = go(
        generator,
        blocks,
        delay,
        now,
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
    let values: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();
    values
        .into_iter()
        .map(|value| SeriesResponse::decode(decode_hex(&value).as_slice()).unwrap())
        .collect()
}

fn go_succeeds(generator: &Path, blocks: &Path, delay: &str, now: SystemTime) -> bool {
    go(generator, blocks, delay, now, &["--endpoint", "info"])
        .status
        .success()
}

fn go(
    generator: &Path,
    blocks: &Path,
    delay: &str,
    now: SystemTime,
    args: &[&str],
) -> std::process::Output {
    let reference = rfc3339(system_seconds(now) as i64);
    let output = std::process::Command::new("go")
        .args(["run", "-tags=slicelabels", "./cmd/store-oracle", "--bucket"])
        .arg(blocks)
        .args(["--consistency-delay", delay])
        .args(["--consistency-reference-time", &reference])
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

fn set_meta_fields(blocks: &Path, block: &str, upload_seconds: Option<i64>, source: Option<&str>) {
    let path = blocks.join(block).join("meta.json");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    match upload_seconds {
        Some(seconds) => meta["thanos"]["upload_time"] = rfc3339(seconds).into(),
        None => {
            meta["thanos"]
                .as_object_mut()
                .unwrap()
                .remove("upload_time");
        }
    }
    if let Some(source) = source {
        meta["thanos"]["source"] = source.into();
    }
    std::fs::write(path, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();
}

fn set_raw_upload_time(blocks: &Path, block: &str, value: serde_json::Value) {
    let path = blocks.join(block).join("meta.json");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    meta["thanos"]["upload_time"] = value;
    std::fs::write(path, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();
}

fn rename_with_timestamp(blocks: &Path, original: &str, timestamp_ms: u64) -> String {
    const ENCODING: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let mut timestamp = timestamp_ms;
    let mut prefix = [b'0'; 10];
    for value in prefix.iter_mut().rev() {
        *value = ENCODING[(timestamp & 31) as usize];
        timestamp >>= 5;
    }
    let renamed = format!(
        "{}{}",
        std::str::from_utf8(&prefix).unwrap(),
        &original[10..]
    );
    std::fs::rename(blocks.join(original), blocks.join(&renamed)).unwrap();
    let path = blocks.join(&renamed).join("meta.json");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    meta["ulid"] = renamed.clone().into();
    std::fs::write(path, serde_json::to_vec_pretty(&meta).unwrap()).unwrap();
    renamed
}

fn write_mark(blocks: &Path, block: &str) {
    std::fs::write(
        mark_path(blocks, block),
        serde_json::to_vec_pretty(&serde_json::json!({
            "id": block,
            "version": 1,
            "deletion_time": 0,
        }))
        .unwrap(),
    )
    .unwrap();
}

fn mark_path(blocks: &Path, block: &str) -> std::path::PathBuf {
    blocks.join(block).join("deletion-mark.json")
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
    String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("raw block: ").map(str::trim))
        .unwrap()
        .to_owned()
}

fn rfc3339(seconds: i64) -> String {
    DateTime::<Utc>::from_timestamp(seconds, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn system_seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_secs()
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
