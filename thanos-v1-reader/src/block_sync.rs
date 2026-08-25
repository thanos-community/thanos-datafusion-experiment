use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use tokio::sync::Mutex;

use crate::{
    block_index::{block_index_file_path, build_block_index_at, chunk_index_directory_path},
    config::{DEFAULT_CONSISTENCY_DELAY, DEFAULT_DELETION_MARK_DELAY, ThanosRepositoryConfig},
    index_context,
    store_service::SharedReaderState,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

static GENERATION: AtomicU64 = AtomicU64::new(0);

pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

#[derive(Debug)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Rebuilds the complete reader view and publishes it with one atomic snapshot swap.
#[derive(Clone)]
pub struct BlockRefresher {
    state: SharedReaderState,
    repositories: Arc<Vec<ThanosRepositoryConfig>>,
    cache_root: PathBuf,
    refresh_lock: Arc<Mutex<()>>,
    deletion_mark_delay: Duration,
    consistency_delay: Duration,
    clock: Arc<dyn Clock>,
}

impl BlockRefresher {
    pub fn new(
        state: SharedReaderState,
        repositories: &[ThanosRepositoryConfig],
        cache_root: impl Into<PathBuf>,
    ) -> Self {
        Self::new_with_clock(
            state,
            repositories,
            cache_root,
            DEFAULT_DELETION_MARK_DELAY,
            DEFAULT_CONSISTENCY_DELAY,
            Arc::new(SystemClock),
        )
    }

    pub fn new_with_policy(
        state: SharedReaderState,
        repositories: &[ThanosRepositoryConfig],
        cache_root: impl Into<PathBuf>,
        deletion_mark_delay: Duration,
        consistency_delay: Duration,
    ) -> Self {
        Self::new_with_clock(
            state,
            repositories,
            cache_root,
            deletion_mark_delay,
            consistency_delay,
            Arc::new(SystemClock),
        )
    }

    pub fn new_with_clock(
        state: SharedReaderState,
        repositories: &[ThanosRepositoryConfig],
        cache_root: impl Into<PathBuf>,
        deletion_mark_delay: Duration,
        consistency_delay: Duration,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            state,
            repositories: Arc::new(repositories.to_vec()),
            cache_root: cache_root.into(),
            refresh_lock: Arc::new(Mutex::new(())),
            deletion_mark_delay,
            consistency_delay,
            clock,
        }
    }

    /// Discover, validate, and index all currently visible blocks before publishing them.
    ///
    /// A failed build leaves the previously published snapshot untouched. Requests that started
    /// before publication retain their old snapshot and cache files until they complete.
    pub async fn refresh(&self) -> Result<(), BoxError> {
        let _guard = self.refresh_lock.lock().await;
        let generation = self.generation_path();
        let generation_string = generation.to_string_lossy().into_owned();
        let result = async {
            let schemas = build_block_index_at(
                &self.repositories,
                &generation_string,
                self.deletion_mark_delay,
                self.consistency_delay,
                self.clock.now(),
            )
            .await?;
            let context = index_context(
                &block_index_file_path(&generation_string),
                &chunk_index_directory_path(&generation_string),
                &schemas,
                &self.repositories,
            )
            .await?;
            self.state
                .replace(context, &self.repositories, generation.clone())
                .await
        }
        .await;

        if result.is_err() {
            let _ = std::fs::remove_dir_all(&generation);
        }
        result
    }

    fn generation_path(&self) -> PathBuf {
        let root = strip_file_scheme(&self.cache_root);
        let sequence = GENERATION.fetch_add(1, Ordering::Relaxed);
        root.join("refresh")
            .join(format!("{}-{sequence}", std::process::id()))
    }
}

fn strip_file_scheme(path: &Path) -> PathBuf {
    let value = path.to_string_lossy();
    value
        .strip_prefix("file://")
        .map(PathBuf::from)
        .unwrap_or_else(|| path.to_owned())
}
