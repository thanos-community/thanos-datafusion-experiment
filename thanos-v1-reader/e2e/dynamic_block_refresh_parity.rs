use std::{collections::BTreeSet, path::Path, sync::Arc};

use arrow::array::{Array, StringArray, StringViewArray};
use futures::StreamExt;
use prost::Message;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    block_sync::BlockRefresher,
    config::ThanosRepositoryConfig,
    index_context,
    store_service::ThanosStoreService,
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
const HOUR: i64 = 60 * 60 * 1000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refreshes_publish_coherent_go_compatible_snapshots() {
    let root = std::env::temp_dir().join(format!(
        "thanos-v1-reader-refresh-e2e-{}",
        std::process::id()
    ));
    let blocks = root.join("blocks");
    let cache = root.join("cache");
    let generator = Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");

    let first = generate(&generator, &blocks, START, START + HOUR, "0", true);
    let repository = ThanosRepositoryConfig {
        name: "refresh".to_owned(),
        uri: format!("file://{}", blocks.display()),
    };
    let service = initial_service(&repository, &cache).await;
    let state = service.shared_state();
    let refresher = Arc::new(BlockRefresher::new(
        state.clone(),
        std::slice::from_ref(&repository),
        &cache,
    ));

    assert_all_paths(&generator, &blocks, &service, &state).await;

    generate(
        &generator,
        &blocks,
        START + HOUR,
        START + 2 * HOUR,
        "1",
        false,
    );
    refresher.refresh().await.unwrap();
    assert_all_paths(&generator, &blocks, &service, &state).await;

    std::fs::remove_dir_all(blocks.join(&first)).unwrap();
    refresher.refresh().await.unwrap();
    assert_all_paths(&generator, &blocks, &service, &state).await;

    let in_flight_query = state.query_snapshot().await;
    let old_query_replicas = sql_replicas(in_flight_query.context()).await;
    let source = generate(
        &generator,
        &blocks,
        START + 2 * HOUR,
        START + 3 * HOUR,
        "2",
        false,
    );
    refresher.refresh().await.unwrap();
    assert_eq!(
        sql_replicas(in_flight_query.context()).await,
        old_query_replicas,
        "an in-flight query changed snapshots during refresh"
    );
    drop(in_flight_query);
    assert_all_paths(&generator, &blocks, &service, &state).await;
    let before_replacement = state.query_snapshot().await;
    assert!(
        sql_block_ids(before_replacement.context())
            .await
            .contains(&source)
    );
    drop(before_replacement);

    let replacement = generate(
        &generator,
        &blocks,
        START + 3 * HOUR,
        START + 4 * HOUR,
        "2",
        false,
    );
    make_compaction_replacement(&blocks, &source, &replacement);
    refresher.refresh().await.unwrap();
    // The source is superseded in StoreAPI and DataFusion state, not merely query-time selected.
    assert_all_paths(&generator, &blocks, &service, &state).await;
    let after_replacement = state.query_snapshot().await;
    let block_ids = sql_block_ids(after_replacement.context()).await;
    assert!(!block_ids.contains(&source));
    assert!(block_ids.contains(&replacement));
    drop(after_replacement);

    let corrupt = generate(
        &generator,
        &blocks,
        START + 4 * HOUR,
        START + 5 * HOUR,
        "3",
        false,
    );
    let corrupt_index = blocks.join(&corrupt).join("index");
    let valid_index = std::fs::read(&corrupt_index).unwrap();
    std::fs::remove_file(&corrupt_index).unwrap();
    let before_failure = canonical_info(reader_info(&service).await);
    let before_failure_query = state.query_snapshot().await;
    let before_failure_replicas = sql_replicas(before_failure_query.context()).await;
    drop(before_failure_query);
    assert!(refresher.refresh().await.is_err());
    assert_eq!(
        canonical_info(reader_info(&service).await),
        before_failure,
        "failed refresh changed the published state"
    );
    let query = state.query_snapshot().await;
    assert_eq!(sql_replicas(query.context()).await, before_failure_replicas);

    std::fs::write(&corrupt_index, valid_index).unwrap();
    refresher.refresh().await.unwrap();
    assert_all_paths(&generator, &blocks, &service, &state).await;

    // Loading two blocks in one refresh must expose the old or final set, never one addition.
    let old_info = canonical_info(reader_info(&service).await);
    generate(
        &generator,
        &blocks,
        START + 5 * HOUR,
        START + 6 * HOUR,
        "4",
        false,
    );
    generate(
        &generator,
        &blocks,
        START + 6 * HOUR,
        START + 7 * HOUR,
        "5",
        false,
    );
    let running_refresh = {
        let refresher = refresher.clone();
        tokio::spawn(async move { refresher.refresh().await.unwrap() })
    };
    let mut observed = vec![old_info.clone()];
    while !running_refresh.is_finished() {
        observed.push(canonical_info(reader_info(&service).await));
        tokio::task::yield_now().await;
    }
    running_refresh.await.unwrap();
    let new_info = canonical_info(reader_info(&service).await);
    assert_ne!(old_info, new_info);
    assert!(
        observed
            .iter()
            .all(|response| response == &old_info || response == &new_info),
        "a request observed a partially built block set"
    );
    assert_all_paths(&generator, &blocks, &service, &state).await;

    drop(refresher);
    drop(service);
    drop(state);
    std::fs::remove_dir_all(root).unwrap();
}

async fn assert_all_paths(
    generator: &Path,
    blocks: &Path,
    service: &ThanosStoreService,
    state: &thanos_v1_reader::store_service::SharedReaderState,
) {
    assert_eq!(
        canonical_info(reader_info(service).await),
        canonical_info(go_info(generator, blocks))
    );
    assert_eq!(
        canonical_label_names(reader_label_names(service).await),
        canonical_label_names(go_label_names(generator, blocks))
    );
    let reader_label_values = reader_label_values(service).await;
    assert_eq!(
        canonical_label_values(reader_label_values.clone()),
        canonical_label_values(go_label_values(generator, blocks))
    );
    let reader_series = reader_series(service).await;
    assert_eq!(
        canonical_series(reader_series.clone()),
        canonical_series(go_series(generator, blocks))
    );
    let query = state.query_snapshot().await;
    assert_eq!(
        sql_replicas(query.context()).await,
        reader_label_values.values
    );
}

async fn initial_service(repository: &ThanosRepositoryConfig, cache: &Path) -> ThanosStoreService {
    let cache = cache.to_str().unwrap();
    let schemas = build_block_index(std::slice::from_ref(repository), cache)
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

async fn reader_info(service: &ThanosStoreService) -> info::InfoResponse {
    service
        .info(tonic::Request::new(info::InfoRequest {}))
        .await
        .unwrap()
        .into_inner()
}

async fn reader_label_names(service: &ThanosStoreService) -> thanos::LabelNamesResponse {
    service
        .label_names(tonic::Request::new(thanos::LabelNamesRequest {
            start: START,
            end: START + 10 * HOUR,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn reader_label_values(service: &ThanosStoreService) -> thanos::LabelValuesResponse {
    service
        .label_values(tonic::Request::new(thanos::LabelValuesRequest {
            label: "replica".to_owned(),
            start: START,
            end: START + 10 * HOUR,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
}

async fn reader_series(service: &ThanosStoreService) -> Vec<SeriesResponse> {
    let response = service
        .series(tonic::Request::new(series_request()))
        .await
        .unwrap();
    response
        .into_inner()
        .map(|response| response.unwrap())
        .collect()
        .await
}

fn series_request() -> thanos::SeriesRequest {
    thanos::SeriesRequest {
        min_time: START,
        max_time: START + 10 * HOUR,
        matchers: vec![LabelMatcher {
            r#type: thanos::label_matcher::Type::Eq as i32,
            name: "__name__".to_owned(),
            value: "dummy_requests_total".to_owned(),
        }],
        partial_response_strategy: thanos::PartialResponseStrategy::Abort as i32,
        ..Default::default()
    }
}

async fn sql_replicas(context: &datafusion::prelude::SessionContext) -> Vec<String> {
    let batches = context
        .sql(
            "SELECT DISTINCT replica \
             FROM metrics.dummy_requests_total \
             ORDER BY replica",
        )
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

async fn sql_block_ids(context: &datafusion::prelude::SessionContext) -> BTreeSet<String> {
    let batches = context
        .sql("SELECT DISTINCT block_ulid FROM chunks")
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

fn go_info(generator: &Path, blocks: &Path) -> info::InfoResponse {
    decode_single(go(generator, blocks, &["--endpoint", "info"]))
}

fn go_label_names(generator: &Path, blocks: &Path) -> thanos::LabelNamesResponse {
    decode_single(go(
        generator,
        blocks,
        &[
            "--endpoint",
            "label-names",
            "--min-time",
            &START.to_string(),
            "--max-time",
            &(START + 10 * HOUR).to_string(),
        ],
    ))
}

fn go_label_values(generator: &Path, blocks: &Path) -> thanos::LabelValuesResponse {
    decode_single(go(
        generator,
        blocks,
        &[
            "--endpoint",
            "label-values",
            "--label",
            "replica",
            "--min-time",
            &START.to_string(),
            "--max-time",
            &(START + 10 * HOUR).to_string(),
        ],
    ))
}

fn go_series(generator: &Path, blocks: &Path) -> Vec<SeriesResponse> {
    let output = go(
        generator,
        blocks,
        &[
            "--metric",
            "dummy_requests_total",
            "--min-time",
            &START.to_string(),
            "--max-time",
            &(START + 10 * HOUR).to_string(),
            "--stream-wire-format",
        ],
    );
    let values: Vec<String> = serde_json::from_slice(&output).unwrap();
    values
        .into_iter()
        .map(|value| SeriesResponse::decode(decode_hex(&value).as_slice()).unwrap())
        .collect()
}

fn go(generator: &Path, blocks: &Path, args: &[&str]) -> Vec<u8> {
    let output = std::process::Command::new("go")
        .args(["run", "-tags=slicelabels", "./cmd/store-oracle", "--bucket"])
        .arg(blocks)
        .args(args)
        .current_dir(generator)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Go oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
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

fn canonical_label_names(mut response: thanos::LabelNamesResponse) -> Vec<u8> {
    canonicalize_label_hints(response.hints.as_mut(), true);
    response.encode_to_vec()
}

fn canonical_label_values(mut response: thanos::LabelValuesResponse) -> Vec<u8> {
    canonicalize_label_hints(response.hints.as_mut(), false);
    response.encode_to_vec()
}

fn canonicalize_label_hints(hints: Option<&mut prost_types::Any>, names: bool) {
    let Some(hints) = hints else {
        return;
    };
    if names {
        let mut decoded = hintspb::LabelNamesResponseHints::decode(hints.value.as_slice()).unwrap();
        decoded
            .queried_blocks
            .sort_by(|left, right| left.id.cmp(&right.id));
        hints.value = decoded.encode_to_vec();
    } else {
        let mut decoded =
            hintspb::LabelValuesResponseHints::decode(hints.value.as_slice()).unwrap();
        decoded
            .queried_blocks
            .sort_by(|left, right| left.id.cmp(&right.id));
        hints.value = decoded.encode_to_vec();
    }
}

fn canonical_series(mut responses: Vec<SeriesResponse>) -> Vec<Vec<u8>> {
    for response in &mut responses {
        if let Some(thanos::series_response::Result::Hints(hints)) = &mut response.result {
            let mut decoded = hintspb::SeriesResponseHints::decode(hints.value.as_slice()).unwrap();
            decoded
                .queried_blocks
                .sort_by(|left, right| left.id.cmp(&right.id));
            hints.value = decoded.encode_to_vec();
        }
    }
    let mut encoded = responses
        .into_iter()
        .map(|response| response.encode_to_vec())
        .collect::<Vec<_>>();
    encoded.sort();
    encoded
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
