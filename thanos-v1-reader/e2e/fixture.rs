use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use tempfile::TempDir;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::{ReaderConfig, StorageConfig, ThanosRepositoryConfig},
    index_context,
    storage::RepositoryRegistry,
    store_service::ThanosStoreService,
};

pub const MINT: i64 = 1_700_000_000_000;
pub const MAXT: i64 = 1_700_003_600_000;
pub const SAMPLE_COUNT: usize = 240;
pub const POD_COUNT: usize = 2;
pub const RESOLUTION_5M: i64 = 5 * 60 * 1000;

pub struct GeneratedFixture {
    root: Option<TempDir>,
    blocks: PathBuf,
    generator_directory: PathBuf,
}

pub fn generated_fixture() -> GeneratedFixture {
    let root = tempfile::Builder::new()
        .prefix("thanos-v1-reader-e2e-")
        .tempdir()
        .expect("create e2e fixture directory");
    let blocks = root.path().join("blocks");
    let generator_directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen");
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
            &SAMPLE_COUNT.to_string(),
            "--instances",
            "2",
            "--pods",
            &POD_COUNT.to_string(),
            "--routes",
            "2",
            "--native-series",
            "1",
            "--scalar-edge-cases",
            "--downsample-5m=true",
        ])
        .current_dir(&generator_directory)
        .output()
        .expect("run deterministic Thanos fixture generator");
    assert!(
        output.status.success(),
        "Thanos fixture generator failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    GeneratedFixture {
        root: Some(root),
        blocks,
        generator_directory,
    }
}

impl GeneratedFixture {
    fn root(&self) -> &Path {
        self.root
            .as_ref()
            .expect("e2e fixture root is available")
            .path()
    }

    pub fn blocks(&self) -> &Path {
        &self.blocks
    }

    pub fn generator_directory(&self) -> &Path {
        &self.generator_directory
    }

    fn repository(&self) -> ThanosRepositoryConfig {
        ThanosRepositoryConfig {
            name: "e2e".to_owned(),
            uri: format!("file://{}", self.blocks.display()),
            s3: None,
            gcs: None,
        }
    }

    fn cache(&self, name: &str) -> PathBuf {
        self.root().join(name)
    }
}

impl Drop for GeneratedFixture {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(root) = self.root.take() {
                let path = root.keep();
                eprintln!("retained failed e2e fixture at {}", path.display());
            }
        }
    }
}

pub async fn indexed_context(
    fixture: &GeneratedFixture,
    cache_name: &str,
) -> datafusion::prelude::SessionContext {
    let cache = fixture.cache(cache_name);
    let repository = fixture.repository();
    let storage = RepositoryRegistry::new(&ReaderConfig {
        listen_addr: "127.0.0.1:1".to_owned(),
        metrics_listen_addr: "127.0.0.1:2".to_owned(),
        index_cache_location: cache.display().to_string(),
        repositories: vec![repository.clone()],
        storage: StorageConfig::default(),
    })
    .unwrap();
    let schemas = build_block_index(
        std::slice::from_ref(&repository),
        cache.to_str().unwrap(),
        &storage,
    )
    .await
    .unwrap();
    index_context(
        &block_index_file_path(cache.to_str().unwrap()),
        &chunk_index_directory_path(cache.to_str().unwrap()),
        &schemas,
        std::slice::from_ref(&repository),
        storage,
    )
    .await
    .unwrap()
}

pub async fn store_service(
    fixture: &GeneratedFixture,
    cache_name: &str,
) -> (datafusion::prelude::SessionContext, ThanosStoreService) {
    let repository = fixture.repository();
    let context = indexed_context(fixture, cache_name).await;
    let storage = RepositoryRegistry::new(&ReaderConfig {
        listen_addr: "127.0.0.1:1".to_owned(),
        metrics_listen_addr: "127.0.0.1:2".to_owned(),
        index_cache_location: fixture.cache(cache_name).display().to_string(),
        repositories: vec![repository.clone()],
        storage: StorageConfig::default(),
    })
    .unwrap();
    let service =
        ThanosStoreService::new(context.clone(), std::slice::from_ref(&repository), storage)
            .await
            .unwrap();
    (context, service)
}

pub fn go_bucket_store_series<T: DeserializeOwned>(
    fixture: &GeneratedFixture,
    metric: &str,
    aggregates: Option<&str>,
    max_resolution: Option<i64>,
) -> Vec<T> {
    let mut command = std::process::Command::new("go");
    command.args([
        "run",
        "-tags=slicelabels",
        "./cmd/store-oracle",
        "--bucket",
        fixture.blocks().to_str().unwrap(),
        "--metric",
        metric,
    ]);
    if let Some(aggregates) = aggregates {
        command.args(["--aggregates", aggregates]);
    }
    if let Some(max_resolution) = max_resolution {
        command.args(["--max-resolution", &max_resolution.to_string()]);
    }
    let output = command
        .current_dir(fixture.generator_directory())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "Go BucketStore oracle failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
