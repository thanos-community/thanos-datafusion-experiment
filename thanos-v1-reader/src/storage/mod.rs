use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use dashmap::DashMap;
use futures::{FutureExt, future::BoxFuture};
use opendal::{
    Operator,
    layers::{ConcurrentLimitLayer, MetricsLayer, OtelTraceLayer, RetryLayer, TimeoutLayer},
    services::{Fs, Gcs, S3},
};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::config::{CachePolicy, ChunkCacheConfig, ReaderConfig, StorageConfig, ThanosRepositoryConfig};

pub type BoxError = Box<dyn std::error::Error>;

/// The common byte-range interface used by TSDB index and chunk readers.
#[async_trait]
pub trait RangeReader: Send + Sync {
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Vec<u8>, io::Error>;
}

#[derive(Clone)]
pub struct Repository {
    name: Arc<str>,
    operator: Operator,
    cache: Option<Arc<RangeCache>>,
    bulk_chunk_size: u64,
    bulk_concurrency: usize,
}

impl Repository {
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, io::Error> {
        let reader = self
            .operator
            .reader_with(path)
            .chunk(usize::try_from(self.bulk_chunk_size).map_err(|_| io::Error::other("bulk chunk size overflows usize"))?)
            .concurrent(self.bulk_concurrency)
            .await
            .map_err(io_error)?;
        Ok(reader.read(..).await.map_err(io_error)?.to_bytes().to_vec())
    }

    pub fn operator(&self) -> &Operator {
        &self.operator
    }
}

#[async_trait]
impl RangeReader for Repository {
    async fn read_range(&self, path: &str, range: Range<u64>) -> Result<Vec<u8>, io::Error> {
        if range.start >= range.end {
            return Ok(Vec::new());
        }
        match &self.cache {
            Some(cache) => cache.read_range(&self.operator, self.name.clone(), path, range).await,
            None => self
                .operator
                .read_with(path)
                .range(range)
                .await
                .map_err(io_error)
                .map(|buffer| buffer.to_bytes().to_vec()),
        }
    }
}

#[derive(Clone)]
pub struct RepositoryRegistry {
    by_uri: Arc<BTreeMap<String, Arc<Repository>>>,
    max_concurrent_chunk_reads: usize,
}

impl std::fmt::Debug for RepositoryRegistry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RepositoryRegistry")
            .field("repositories", &self.by_uri.keys().collect::<Vec<_>>())
            .field("max_concurrent_chunk_reads", &self.max_concurrent_chunk_reads)
            .finish()
    }
}

impl RepositoryRegistry {
    pub fn empty() -> Self {
        Self {
            by_uri: Arc::new(BTreeMap::new()),
            max_concurrent_chunk_reads: 1,
        }
    }

    pub fn new(config: &ReaderConfig) -> Result<Self, BoxError> {
        let cache = config
            .storage
            .chunk_cache
            .as_ref()
            .map(RangeCache::open)
            .transpose()?
            .map(Arc::new);
        let mut by_uri = BTreeMap::new();
        for repository in &config.repositories {
            let operator = repository_operator(repository, &config.storage)?;
            by_uri.insert(
                repository.uri.clone(),
                Arc::new(Repository {
                    name: Arc::from(repository.name.as_str()),
                    operator,
                    cache: cache.clone(),
                    bulk_chunk_size: config.storage.bulk_read_chunk_size_bytes()?,
                    bulk_concurrency: config.storage.bulk_read_concurrency,
                }),
            );
        }
        Ok(Self {
            by_uri: Arc::new(by_uri),
            max_concurrent_chunk_reads: config.storage.max_concurrent_chunk_reads,
        })
    }

    pub fn get(&self, uri: &str) -> Option<Arc<Repository>> {
        self.by_uri.get(uri).cloned()
    }

    pub fn require(&self, uri: &str) -> Result<Arc<Repository>, BoxError> {
        self.get(uri)
            .ok_or_else(|| format!("no storage repository for URI {uri:?}").into())
    }

    pub fn max_concurrent_chunk_reads(&self) -> usize {
        self.max_concurrent_chunk_reads
    }
}

pub fn repository_operator(
    repository: &ThanosRepositoryConfig,
    storage: &StorageConfig,
) -> Result<Operator, BoxError> {
    let (scheme, location) = repository
        .uri
        .split_once("://")
        .ok_or_else(|| format!("repository URI {:?} must include a scheme", repository.uri))?;
    let operator = match scheme {
        "file" => Operator::new(Fs::default().root(location))?,
        "s3" => {
            let (bucket, root) = bucket_and_root(location)?;
            let mut builder = S3::default().bucket(bucket).root(root);
            if let Some(options) = &repository.s3 {
                if let Some(region) = &options.region {
                    builder = builder.region(region);
                }
                if let Some(endpoint) = &options.endpoint {
                    builder = builder.endpoint(endpoint);
                }
                if options.virtual_host_style {
                    builder = builder.enable_virtual_host_style();
                }
            }
            Operator::new(builder)?
        }
        "gs" | "gcs" => {
            let (bucket, root) = bucket_and_root(location)?;
            let mut builder = Gcs::default().bucket(bucket).root(root);
            if let Some(endpoint) = repository.gcs.as_ref().and_then(|options| options.endpoint.as_ref()) {
                builder = builder.endpoint(endpoint);
            }
            Operator::new(builder)?
        }
        _ => return Err(format!("unsupported repository URI scheme {scheme:?}").into()),
    };
    let timeout = storage.request_timeout_duration()?;
    Ok(operator
        .layer(RetryLayer::new().with_max_times(storage.max_retries))
        .layer(TimeoutLayer::new().with_timeout(timeout))
        .layer(ConcurrentLimitLayer::new(storage.max_concurrent_requests))
        .layer(MetricsLayer::new())
        .layer(OtelTraceLayer::new()))
}

fn bucket_and_root(location: &str) -> Result<(&str, &str), BoxError> {
    let location = location.trim_matches('/');
    let (bucket, root) = location
        .split_once('/')
        .map_or((location, "/"), |(bucket, root)| (bucket, root));
    if bucket.is_empty() {
        return Err("object storage URI bucket must not be empty".into());
    }
    Ok((bucket, root))
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PageKey {
    repository: Arc<str>,
    path: Arc<str>,
    offset: u64,
}

#[derive(Clone)]
struct PageEntry {
    size: u64,
    protected: bool,
}

struct CacheState {
    entries: HashMap<PageKey, PageEntry>,
    probationary: VecDeque<PageKey>,
    protected: VecDeque<PageKey>,
    bytes: u64,
}

impl CacheState {
    fn touch(&mut self, key: &PageKey, policy: CachePolicy, protected_fraction: f64, max_size: u64) {
        let Some(entry) = self.entries.get_mut(key) else { return };
        remove_key(&mut self.probationary, key);
        remove_key(&mut self.protected, key);
        if policy == CachePolicy::Slru && !entry.protected {
            entry.protected = true;
        }
        if entry.protected {
            self.protected.push_back(key.clone());
            let protected_limit = (max_size as f64 * protected_fraction) as u64;
            while self.protected.iter().filter_map(|key| self.entries.get(key)).map(|entry| entry.size).sum::<u64>() > protected_limit {
                let Some(demoted) = self.protected.pop_front() else { break };
                if let Some(entry) = self.entries.get_mut(&demoted) {
                    entry.protected = false;
                }
                self.probationary.push_back(demoted);
            }
        } else {
            self.probationary.push_back(key.clone());
        }
    }
}

/// A local, immutable-object page cache. Metadata is rebuilt by scanning page files on start.
#[derive(Clone)]
pub struct RangeCache {
    directory: Arc<PathBuf>,
    page_size: u64,
    max_size: u64,
    policy: CachePolicy,
    protected_fraction: f64,
    state: Arc<Mutex<CacheState>>,
    flights: Arc<DashMap<PageKey, futures::future::Shared<BoxFuture<'static, Result<Arc<Vec<u8>>, String>>>>>,
}

impl RangeCache {
    pub fn open(config: &ChunkCacheConfig) -> Result<Self, BoxError> {
        std::fs::create_dir_all(&config.directory)?;
        let lock_path = config.directory.join(".lock");
        let lock_file = std::fs::OpenOptions::new().create(true).read(true).write(true).open(lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock_file)?;
        // Leak the lock for the process lifetime: cache ownership must be exclusive.
        std::mem::forget(lock_file);
        let cache = Self {
            directory: Arc::new(config.directory.clone()),
            page_size: config.page_size_bytes()?,
            max_size: config.max_size_bytes()?,
            policy: config.policy,
            protected_fraction: config.protected_fraction,
            state: Arc::new(Mutex::new(CacheState {
                entries: HashMap::new(),
                probationary: VecDeque::new(),
                protected: VecDeque::new(),
                bytes: 0,
            })),
            flights: Arc::new(DashMap::new()),
        };
        cache.remove_temporary_files()?;
        Ok(cache)
    }

    async fn read_range(&self, operator: &Operator, repository: Arc<str>, path: &str, range: Range<u64>) -> Result<Vec<u8>, io::Error> {
        let first_page = range.start / self.page_size * self.page_size;
        let last_page = (range.end - 1) / self.page_size * self.page_size;
        let mut output = Vec::with_capacity(usize::try_from(range.end - range.start).unwrap_or(0));
        for offset in (first_page..=last_page).step_by(usize::try_from(self.page_size).map_err(|_| io::Error::other("page size overflows usize"))?) {
            let page = self.get_page(operator, repository.clone(), path, offset).await?;
            let start = usize::try_from(range.start.saturating_sub(offset)).unwrap_or(0);
            let end = usize::try_from((range.end.min(offset + self.page_size)).saturating_sub(offset)).unwrap_or(0);
            if start > page.len() || end > page.len() || start > end {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "object range ends before requested chunk record"));
            }
            output.extend_from_slice(&page[start..end]);
        }
        Ok(output)
    }

    async fn get_page(&self, operator: &Operator, repository: Arc<str>, path: &str, offset: u64) -> Result<Arc<Vec<u8>>, io::Error> {
        let key = PageKey { repository, path: Arc::from(path), offset };
        if let Some(page) = self.load_cached(&key).await? {
            metrics::counter!("thanos_reader_chunk_cache_hits_total").increment(1);
            return Ok(page);
        }
        metrics::counter!("thanos_reader_chunk_cache_misses_total").increment(1);
        let operator = operator.clone();
        let cache = self.clone();
        let path = path.to_owned();
        let flight_key = key.clone();
        let shared = self.flights.entry(key.clone()).or_insert_with(|| {
            async move {
                let length = operator.stat(&path).await.map_err(|error| error.to_string())?.content_length();
                let end = (offset + cache.page_size).min(length);
                if end <= offset {
                    return Err("requested cache page starts after object end".to_owned());
                }
                let bytes = operator.read_with(&path).range(offset..end).await
                    .map_err(|error| error.to_string())?.to_bytes().to_vec();
                let bytes = Arc::new(bytes);
                cache.store_cached(flight_key, bytes.clone()).await.map_err(|error| error.to_string())?;
                Ok(bytes)
            }.boxed().shared()
        }).clone();
        let result = shared.await;
        self.flights.remove(&key);
        result.map_err(io::Error::other)
    }

    async fn load_cached(&self, key: &PageKey) -> Result<Option<Arc<Vec<u8>>>, io::Error> {
        {
            let mut state = self.state.lock().await;
            if !state.entries.contains_key(key) {
                return Ok(None);
            }
            state.touch(key, self.policy, self.protected_fraction, self.max_size);
        }
        match std::fs::read(self.page_path(key)) {
            Ok(bytes) => Ok(Some(Arc::new(bytes))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.state.lock().await.entries.remove(key);
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn store_cached(&self, key: PageKey, bytes: Arc<Vec<u8>>) -> Result<(), io::Error> {
        if bytes.len() as u64 > self.max_size {
            return Ok(());
        }
        let path = self.page_path(&key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
        std::fs::write(&temporary, bytes.as_slice())?;
        std::fs::rename(temporary, &path)?;
        let evicted = {
            let mut state = self.state.lock().await;
            if let Some(previous) = state.entries.insert(key.clone(), PageEntry { size: bytes.len() as u64, protected: false }) {
                state.bytes = state.bytes.saturating_sub(previous.size);
            }
            state.bytes += bytes.len() as u64;
            state.probationary.push_back(key);
            let mut evicted = Vec::new();
            while state.bytes > self.max_size {
                let candidate = state.probationary.pop_front().or_else(|| state.protected.pop_front());
                let Some(candidate) = candidate else { break };
                if let Some(entry) = state.entries.remove(&candidate) {
                    state.bytes = state.bytes.saturating_sub(entry.size);
                    evicted.push(candidate);
                }
            }
            evicted
        };
        for key in evicted {
            let _ = std::fs::remove_file(self.page_path(&key));
            metrics::counter!("thanos_reader_chunk_cache_evictions_total").increment(1);
        }
        Ok(())
    }

    fn page_path(&self, key: &PageKey) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(key.repository.as_bytes());
        hasher.update([0]);
        hasher.update(key.path.as_bytes());
        hasher.update(key.offset.to_be_bytes());
        let digest = hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        self.directory.join(&digest[..2]).join(digest)
    }

    fn remove_temporary_files(&self) -> Result<(), io::Error> {
        for entry in walk_files(&self.directory)? {
            if entry.extension().is_some_and(|extension| extension.to_string_lossy().starts_with("tmp-")) {
                let _ = std::fs::remove_file(entry);
            }
        }
        Ok(())
    }
}

fn remove_key(queue: &mut VecDeque<PageKey>, key: &PageKey) {
    if let Some(position) = queue.iter().position(|candidate| candidate == key) {
        queue.remove(position);
    }
}

fn walk_files(directory: &Path) -> Result<Vec<PathBuf>, io::Error> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            paths.extend(walk_files(&path)?);
        } else {
            paths.push(path);
        }
    }
    Ok(paths)
}

fn io_error(error: opendal::Error) -> io::Error {
    io::Error::other(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ReaderConfig, StorageConfig, ThanosRepositoryConfig};

    #[tokio::test]
    async fn aligned_disk_cache_serves_a_warm_overlapping_range() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("blocks");
        let cache = root.path().join("cache");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("segment"), b"abcdefghijklmnopqrstuvwxyz").unwrap();
        let config = ReaderConfig {
            listen_addr: "127.0.0.1:1".to_owned(),
            metrics_listen_addr: "127.0.0.1:2".to_owned(),
            index_cache_location: root.path().join("indexes").display().to_string(),
            repositories: vec![ThanosRepositoryConfig {
                name: "local".to_owned(),
                uri: format!("file://{}", source.display()),
                s3: None,
                gcs: None,
            }],
            storage: StorageConfig {
                chunk_cache: Some(ChunkCacheConfig {
                    directory: cache,
                    max_size: "1MiB".to_owned(),
                    page_size: "16KiB".to_owned(),
                    policy: CachePolicy::Slru,
                    protected_fraction: 0.8,
                }),
                ..StorageConfig::default()
            },
        };
        let registry = RepositoryRegistry::new(&config).unwrap();
        let repository = registry.get(&config.repositories[0].uri).unwrap();
        assert_eq!(repository.read_range("segment", 2..10).await.unwrap(), b"cdefghij");
        std::fs::write(source.join("segment"), b"XXXXXXXXXXXXXXXXXXXXXXXXXX").unwrap();
        assert_eq!(repository.read_range("segment", 5..12).await.unwrap(), b"fghijkl");
    }
}
