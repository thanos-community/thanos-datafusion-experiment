use std::{
    error::Error,
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;

pub const DEFAULT_CONFIG_PATH: &str = "dev.toml";
pub const CONFIG_PATH_ENV_VAR: &str = "THANOS_READER_CONFIG";
const DEFAULT_METRICS_LISTEN_ADDR: &str = "127.0.0.1:9090";

#[derive(Debug, Deserialize)]
pub struct ReaderConfig {
    pub listen_addr: String,
    #[serde(default = "default_metrics_listen_addr")]
    pub metrics_listen_addr: String,
    pub index_cache_location: String,
    #[serde(default)]
    pub repositories: Vec<ThanosRepositoryConfig>,
    #[serde(default)]
    pub storage: StorageConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ThanosRepositoryConfig {
    pub name: String,
    pub uri: String,
    #[serde(default)]
    pub s3: Option<S3Config>,
    #[serde(default)]
    pub gcs: Option<GcsConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct S3Config {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    #[serde(default)]
    pub virtual_host_style: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GcsConfig {
    pub endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_request_timeout")]
    pub request_timeout: String,
    #[serde(default = "default_max_retries")]
    pub max_retries: usize,
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
    #[serde(default = "default_bulk_read_chunk_size")]
    pub bulk_read_chunk_size: String,
    #[serde(default = "default_bulk_read_concurrency")]
    pub bulk_read_concurrency: usize,
    #[serde(default = "default_max_concurrent_chunk_reads")]
    pub max_concurrent_chunk_reads: usize,
    #[serde(default = "default_index_build_concurrency")]
    pub index_build_concurrency: usize,
    /// Maximum in-flight metadata and deletion-marker requests while discovering blocks.
    #[serde(default = "default_metadata_read_concurrency")]
    pub metadata_read_concurrency: usize,
    /// Ignore blocks whose maximum timestamp is older than this duration. This is useful for
    /// bringing up an experimental reader against a large historical bucket.
    #[serde(default)]
    pub block_max_age: Option<String>,
    #[serde(default)]
    pub chunk_cache: Option<ChunkCacheConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            request_timeout: default_request_timeout(),
            max_retries: default_max_retries(),
            max_concurrent_requests: default_max_concurrent_requests(),
            bulk_read_chunk_size: default_bulk_read_chunk_size(),
            bulk_read_concurrency: default_bulk_read_concurrency(),
            max_concurrent_chunk_reads: default_max_concurrent_chunk_reads(),
            index_build_concurrency: default_index_build_concurrency(),
            metadata_read_concurrency: default_metadata_read_concurrency(),
            block_max_age: None,
            chunk_cache: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ChunkCacheConfig {
    pub directory: PathBuf,
    #[serde(default = "default_chunk_cache_max_size")]
    pub max_size: String,
    #[serde(default = "default_chunk_cache_page_size")]
    pub page_size: String,
    #[serde(default = "default_chunk_cache_policy")]
    pub policy: CachePolicy,
    #[serde(default = "default_protected_fraction")]
    pub protected_fraction: f64,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CachePolicy {
    Lru,
    Slru,
}

impl ChunkCacheConfig {
    pub fn max_size_bytes(&self) -> Result<u64, Box<dyn Error>> {
        parse_size(&self.max_size, "chunk cache max_size")
    }

    pub fn page_size_bytes(&self) -> Result<u64, Box<dyn Error>> {
        parse_size(&self.page_size, "chunk cache page_size")
    }
}

impl StorageConfig {
    pub fn request_timeout_duration(&self) -> Result<Duration, Box<dyn Error>> {
        Ok(humantime::parse_duration(&self.request_timeout)?)
    }

    pub fn bulk_read_chunk_size_bytes(&self) -> Result<u64, Box<dyn Error>> {
        parse_size(&self.bulk_read_chunk_size, "storage bulk_read_chunk_size")
    }

    pub fn block_max_age_duration(&self) -> Result<Option<Duration>, Box<dyn Error>> {
        self.block_max_age
            .as_deref()
            .map(humantime::parse_duration)
            .transpose()
            .map_err(Into::into)
    }
}

impl ReaderConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path)?;
        let config: Self = toml::from_str(&contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn config_path() -> PathBuf {
        std::env::var_os(CONFIG_PATH_ENV_VAR)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH))
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        self.listen_addr.parse::<SocketAddr>()?;
        self.metrics_listen_addr.parse::<SocketAddr>()?;

        if self.index_cache_location.trim().is_empty() {
            return Err("index cache location must not be empty".into());
        }

        if self.repositories.is_empty() {
            return Err("at least one Thanos repository must be configured".into());
        }

        self.validate_storage()?;
        let mut names = std::collections::BTreeSet::new();
        for repository in &self.repositories {
            if repository.name.trim().is_empty() {
                return Err("Thanos repository name must not be empty".into());
            }

            if repository.uri.trim().is_empty() {
                return Err(format!(
                    "Thanos repository {:?} URI must not be empty",
                    repository.name
                )
                .into());
            }
            if !names.insert(repository.name.as_str()) {
                return Err(
                    format!("duplicate Thanos repository name {:?}", repository.name).into(),
                );
            }
            let scheme = repository
                .uri
                .split_once("://")
                .map(|(scheme, _)| scheme)
                .ok_or_else(|| {
                    format!("repository URI {:?} must include a scheme", repository.uri)
                })?;
            match scheme {
                "file" if repository.s3.is_none() && repository.gcs.is_none() => {}
                "s3" if repository.gcs.is_none() => {}
                "gs" | "gcs" if repository.s3.is_none() => {}
                "file" | "s3" | "gs" | "gcs" => {
                    return Err(format!(
                        "repository {:?} has options for a different storage backend",
                        repository.name
                    )
                    .into());
                }
                _ => {
                    return Err(format!(
                        "unsupported repository URI scheme {scheme:?}; use file://, s3://, or gs://"
                    )
                    .into());
                }
            }
        }

        Ok(())
    }

    fn validate_storage(&self) -> Result<(), Box<dyn Error>> {
        let storage = &self.storage;
        if storage.request_timeout_duration()?.is_zero() {
            return Err("storage request_timeout must be greater than zero".into());
        }
        if storage.max_concurrent_requests == 0
            || storage.bulk_read_concurrency == 0
            || storage.max_concurrent_chunk_reads == 0
            || storage.index_build_concurrency == 0
            || storage.metadata_read_concurrency == 0
        {
            return Err("storage concurrency values must be greater than zero".into());
        }
        if storage.bulk_read_chunk_size_bytes()? == 0 {
            return Err("storage bulk_read_chunk_size must be greater than zero".into());
        }
        if let Some(cache) = &storage.chunk_cache {
            let max_size = cache.max_size_bytes()?;
            let page_size = cache.page_size_bytes()?;
            if max_size == 0 || page_size == 0 || page_size > max_size {
                return Err(
                    "chunk cache page_size must be non-zero and no larger than max_size".into(),
                );
            }
            if !page_size.is_power_of_two() {
                return Err("chunk cache page_size must be a power of two".into());
            }
            if !(0.0..=1.0).contains(&cache.protected_fraction) {
                return Err("chunk cache protected_fraction must be between 0 and 1".into());
            }
        }
        Ok(())
    }
}

fn default_metrics_listen_addr() -> String {
    DEFAULT_METRICS_LISTEN_ADDR.to_owned()
}

fn default_request_timeout() -> String {
    "30s".to_owned()
}
fn default_max_retries() -> usize {
    3
}
fn default_max_concurrent_requests() -> usize {
    64
}
fn default_bulk_read_chunk_size() -> String {
    "8MiB".to_owned()
}
fn default_bulk_read_concurrency() -> usize {
    4
}
fn default_max_concurrent_chunk_reads() -> usize {
    16
}
fn default_index_build_concurrency() -> usize {
    12
}

fn default_metadata_read_concurrency() -> usize {
    64
}
fn default_chunk_cache_max_size() -> String {
    "10GiB".to_owned()
}
fn default_chunk_cache_page_size() -> String {
    "512KiB".to_owned()
}
fn default_chunk_cache_policy() -> CachePolicy {
    CachePolicy::Slru
}
fn default_protected_fraction() -> f64 {
    0.8
}

fn parse_size(value: &str, name: &str) -> Result<u64, Box<dyn Error>> {
    let value = value.trim();
    let suffixes = [
        ("KiB", 1024_u64),
        ("MiB", 1024_u64.pow(2)),
        ("GiB", 1024_u64.pow(3)),
        ("TiB", 1024_u64.pow(4)),
        ("B", 1),
    ];
    for (suffix, multiplier) in suffixes {
        if let Some(number) = value.strip_suffix(suffix) {
            return number
                .trim()
                .parse::<u64>()?
                .checked_mul(multiplier)
                .ok_or_else(|| format!("{name} overflows u64").into());
        }
    }
    Err(format!("{name} must use a binary byte suffix such as 16KiB or 10GiB").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_defaults_are_production_safe() {
        let storage = StorageConfig::default();
        assert_eq!(
            storage.request_timeout_duration().unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            storage.bulk_read_chunk_size_bytes().unwrap(),
            8 * 1024_u64.pow(2)
        );
        assert_eq!(storage.max_concurrent_chunk_reads, 16);
        assert_eq!(storage.index_build_concurrency, 12);
        assert_eq!(storage.metadata_read_concurrency, 64);
        assert_eq!(default_chunk_cache_page_size(), "512KiB");
    }

    #[test]
    fn parses_binary_cache_sizes() {
        let cache = ChunkCacheConfig {
            directory: PathBuf::from("/tmp/chunks"),
            max_size: "10GiB".to_owned(),
            page_size: "16KiB".to_owned(),
            policy: CachePolicy::Slru,
            protected_fraction: 0.8,
        };
        assert_eq!(cache.max_size_bytes().unwrap(), 10 * 1024_u64.pow(3));
        assert_eq!(cache.page_size_bytes().unwrap(), 16 * 1024);
    }
}
