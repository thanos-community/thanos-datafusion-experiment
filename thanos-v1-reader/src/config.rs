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
    #[serde(default = "default_block_sync_interval", with = "humantime_serde")]
    pub block_sync_interval: Duration,
    #[serde(default)]
    pub repositories: Vec<ThanosRepositoryConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ThanosRepositoryConfig {
    pub name: String,
    pub uri: String,
}

impl ReaderConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error + Send + Sync>> {
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

    fn validate(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.listen_addr.parse::<SocketAddr>()?;
        self.metrics_listen_addr.parse::<SocketAddr>()?;

        if self.index_cache_location.trim().is_empty() {
            return Err("index cache location must not be empty".into());
        }
        if self.block_sync_interval.is_zero() {
            return Err("block sync interval must be greater than zero".into());
        }

        if self.repositories.is_empty() {
            return Err("at least one Thanos repository must be configured".into());
        }

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
        }

        Ok(())
    }
}

fn default_metrics_listen_addr() -> String {
    DEFAULT_METRICS_LISTEN_ADDR.to_owned()
}

fn default_block_sync_interval() -> Duration {
    Duration::from_secs(15 * 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_block_sync_interval_and_defaults_to_fifteen_minutes() {
        let base = r#"
listen_addr = "127.0.0.1:4100"
index_cache_location = "cache"
[[repositories]]
name = "test"
uri = "file:///blocks"
"#;
        let default: ReaderConfig = toml::from_str(base).unwrap();
        assert_eq!(default.block_sync_interval, Duration::from_secs(15 * 60));
        default.validate().unwrap();

        let configured: ReaderConfig =
            toml::from_str(&format!("block_sync_interval = \"30s\"\n{base}")).unwrap();
        assert_eq!(configured.block_sync_interval, Duration::from_secs(30));
        configured.validate().unwrap();
    }
}
