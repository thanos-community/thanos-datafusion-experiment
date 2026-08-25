use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::{ReaderConfig, StorageConfig, ThanosRepositoryConfig},
    index_context,
    storage::RepositoryRegistry,
};

#[tokio::test]
async fn counter_samples_match_generated_block_values() {
    let root = std::env::temp_dir().join(format!("thanos-v1-reader-e2e-{}", std::process::id()));
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
            "2",
            "--pods",
            "2",
            "--routes",
            "1",
            "--native-series",
            "1",
            "--downsample-5m=false",
        ])
        .current_dir(generator_directory)
        .status()
        .unwrap();
    assert!(status.success());

    let repository = ThanosRepositoryConfig {
        name: "e2e".to_owned(),
        uri: format!("file://{}", blocks.display()),
        s3: None,
        gcs: None,
    };
    let repositories = vec![repository];
    let storage = RepositoryRegistry::new(&ReaderConfig {
        listen_addr: "127.0.0.1:1".to_owned(),
        metrics_listen_addr: "127.0.0.1:2".to_owned(),
        index_cache_location: cache.display().to_string(),
        repositories: repositories.clone(),
        storage: StorageConfig::default(),
    })
    .unwrap();
    let schemas = build_block_index(&repositories, cache.to_str().unwrap(), &storage)
        .await
        .unwrap();
    let context = index_context(
        &block_index_file_path(cache.to_str().unwrap()),
        &chunk_index_directory_path(cache.to_str().unwrap()),
        &schemas,
        &repositories,
        storage,
    )
    .await
    .unwrap();

    let batches = context
        .sql(
            "SELECT timestamp, value \
             FROM metrics.dummy_requests_total \
             WHERE downsample_resolution = 0 \
             ORDER BY timestamp, pod",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let actual = batches
        .iter()
        .flat_map(|batch| {
            let timestamps = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .unwrap();
            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            (0..batch.num_rows()).map(move |index| (timestamps.value(index), values.value(index)))
        })
        .collect::<Vec<_>>();

    let mint = 1_700_000_000_000_i64;
    let step = 60_000_i64;
    let expected = (0..10)
        .flat_map(|sample| {
            (0..2).map(move |pod| {
                (
                    mint + sample * step,
                    (1_000 + pod * 100 + sample * 7) as f64,
                )
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    std::fs::remove_dir_all(root).unwrap();
}
