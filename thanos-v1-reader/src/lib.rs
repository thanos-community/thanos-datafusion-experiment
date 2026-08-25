use std::{env, net::SocketAddr};

#[cfg(unix)]
use std::os::fd::{FromRawFd, RawFd};

use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{Router, extract::State, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub mod block_index;
pub mod chunk_reader;
pub mod config;
pub mod flight_service;
pub mod metric_table;
pub mod storage;
pub mod store_service;
pub mod thanos_proto;
pub mod tsdb_index;

use std::sync::Arc;

use block_index::MetricTableSchema;
use config::ThanosRepositoryConfig;
use datafusion::{
    catalog::MemorySchemaProvider,
    common::TableReference,
    execution::SessionStateBuilder,
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};
use datafusion_tracing::{
    InstrumentationOptions, RuleInstrumentationOptions, instrument_rules_with_info_spans,
    instrument_with_info_spans,
};
use metric_table::MetricTableProvider;
use storage::RepositoryRegistry;

pub async fn index_context(
    block_index_path: &str,
    chunk_index_path: &str,
    metric_table_schemas: &[MetricTableSchema],
    repositories: &[ThanosRepositoryConfig],
    storage: RepositoryRegistry,
) -> Result<SessionContext, datafusion::error::DataFusionError> {
    let execution_options = InstrumentationOptions::builder()
        .record_metrics(true)
        .build();
    let execution_rule = instrument_with_info_spans!(options: execution_options);
    let session_config = SessionConfig::new().with_information_schema(true);
    let session_state = SessionStateBuilder::new()
        .with_config(session_config)
        .with_default_features()
        .with_physical_optimizer_rule(execution_rule)
        .build();
    let session_state = instrument_rules_with_info_spans!(
        options: RuleInstrumentationOptions::full(),
        state: session_state
    );
    let context = SessionContext::new_with_state(session_state);
    context
        .register_parquet("blocks", block_index_path, ParquetReadOptions::default())
        .await?;
    context
        .register_parquet("chunks", chunk_index_path, ParquetReadOptions::default())
        .await?;
    register_metric_tables(&context, metric_table_schemas, repositories, storage).await?;
    Ok(context)
}

pub async fn register_metric_tables(
    context: &SessionContext,
    metric_table_schemas: &[MetricTableSchema],
    _repositories: &[ThanosRepositoryConfig],
    storage: RepositoryRegistry,
) -> Result<(), datafusion::error::DataFusionError> {
    let catalog = context.catalog("datafusion").ok_or_else(|| {
        datafusion::error::DataFusionError::Internal("default catalog is missing".to_owned())
    })?;
    catalog.register_schema("metrics", Arc::new(MemorySchemaProvider::new()))?;

    let chunk_provider = context.table_provider("chunks").await?;
    for metric_table_schema in metric_table_schemas {
        let table = MetricTableProvider::new(
            metric_table_schema.clone(),
            chunk_provider.clone(),
            storage.clone(),
        )?;
        context.register_table(
            TableReference::full("datafusion", "metrics", metric_table_schema.name.as_str()),
            Arc::new(table),
        )?;
    }
    Ok(())
}

const LISTEN_ADDR_ENV_VAR: &str = "FLIGHT_LISTEN_ADDR";
const METRICS_LISTEN_ADDR_ENV_VAR: &str = "METRICS_LISTEN_ADDR";
/// When set, the reader adopts already-bound TCP listeners from its parent process.
///
/// This is primarily useful for hermetic integration tests: the harness can reserve an
/// ephemeral port and pass its file descriptor to the reader without a close-and-rebind race.
const LISTEN_FD_ENV_VAR: &str = "THANOS_READER_LISTEN_FD";
const METRICS_LISTEN_FD_ENV_VAR: &str = "THANOS_READER_METRICS_LISTEN_FD";

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing()?;
    let prometheus_handle = PrometheusBuilder::new().install_recorder()?;

    let config_path = config::ReaderConfig::config_path();
    let config = config::ReaderConfig::load(&config_path)?;
    let storage = RepositoryRegistry::new(&config)?;
    let address = env::var(LISTEN_ADDR_ENV_VAR).unwrap_or(config.listen_addr);
    let metrics_address =
        env::var(METRICS_LISTEN_ADDR_ENV_VAR).unwrap_or(config.metrics_listen_addr);
    let listener = listener_from_env(LISTEN_FD_ENV_VAR)?;
    let metrics_listener = listener_from_env(METRICS_LISTEN_FD_ENV_VAR)?;
    let socket_address: SocketAddr = address.parse()?;
    start_metrics_server(&metrics_address, metrics_listener, prometheus_handle).await?;

    for repository in &config.repositories {
        tracing::info!(
            name = %repository.name,
            uri = %repository.uri,
            "configured Thanos repository"
        );
    }
    let metric_table_schemas = block_index::build_block_index(
        &config.repositories,
        &config.index_cache_location,
        &storage,
        config.storage.index_build_concurrency,
        config.storage.metadata_read_concurrency,
        config.storage.block_max_age_duration()?,
    )
    .await?;
    let block_index_path = block_index::block_index_file_path(&config.index_cache_location);
    let chunk_index_path = block_index::chunk_index_directory_path(&config.index_cache_location);
    tracing::info!(path = %block_index_path, "generated Thanos blocks index");
    tracing::info!(path = %chunk_index_path, "generated Thanos chunks indexes");

    let context = index_context(
        &block_index_path,
        &chunk_index_path,
        &metric_table_schemas,
        &config.repositories,
        storage.clone(),
    )
    .await?;
    let store_service =
        store_service::ThanosStoreService::new(context.clone(), &config.repositories, storage)
            .await?;
    let service =
        flight_service::DataFusionFlightService::new(context, format!("grpc+tcp://{address}"));
    tracing::info!(
        address = %metrics_address,
        path = "/metrics",
        "starting Prometheus metrics endpoint"
    );
    tracing::info!(%address, "starting DataFusion Arrow Flight server");

    let server = Server::builder()
        .add_service(FlightServiceServer::new(service))
        .add_service(thanos_proto::thanos::store_server::StoreServer::new(
            store_service.clone(),
        ))
        .add_service(thanos_proto::thanos::info::info_server::InfoServer::new(
            store_service,
        ));
    if let Some(listener) = listener {
        server
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await?;
    } else {
        server.serve(socket_address).await?;
    }

    Ok(())
}

fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let exporter = SpanExporter::builder().with_tonic().build()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build();
    let tracer = provider.tracer("thanos-v1-reader");
    global::set_tracer_provider(provider);

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().json())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
    Ok(())
}

async fn start_metrics_server(
    address: &str,
    listener: Option<TcpListener>,
    prometheus_handle: PrometheusHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = match listener {
        Some(listener) => listener,
        None => TcpListener::bind(address).await?,
    };
    let app = Router::new()
        .route("/metrics", get(prometheus_metrics))
        .with_state(prometheus_handle);

    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::error!(%error, "Prometheus metrics endpoint stopped");
        }
    });
    Ok(())
}

fn listener_from_env(name: &str) -> Result<Option<TcpListener>, Box<dyn std::error::Error>> {
    let Some(raw_fd) = env::var_os(name) else {
        return Ok(None);
    };
    let raw_fd = raw_fd
        .to_string_lossy()
        .parse::<i32>()
        .map_err(|error| format!("{name} must be a file descriptor: {error}"))?;
    if raw_fd < 0 {
        return Err(format!("{name} must be non-negative").into());
    }

    #[cfg(unix)]
    {
        // The parent deliberately transfers ownership of this inherited descriptor.
        let listener = unsafe { std::net::TcpListener::from_raw_fd(raw_fd as RawFd) };
        listener.set_nonblocking(true)?;
        return Ok(Some(TcpListener::from_std(listener)?));
    }
    #[cfg(not(unix))]
    {
        let _ = raw_fd;
        Err(format!("{name} is only supported on Unix").into())
    }
}

async fn prometheus_metrics(State(handle): State<PrometheusHandle>) -> String {
    handle.render()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn registers_metric_table_in_metrics_schema() {
        let context = SessionContext::new();
        context
            .register_table(
                "chunks",
                std::sync::Arc::new(
                    datafusion::datasource::MemTable::try_new(
                        std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                        vec![vec![]],
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        register_metric_tables(
            &context,
            &[block_index::MetricTableSchema {
                name: "dummy_requests_total".to_owned(),
                label_columns: BTreeSet::from(["job".to_owned(), "pod".to_owned()]),
            }],
            &[],
            RepositoryRegistry::empty(),
        )
        .await
        .unwrap();

        let table = context
            .table_provider("metrics.dummy_requests_total")
            .await
            .unwrap();
        let schema = table.schema();
        assert!(schema.field_with_name("job").is_ok());
        assert!(schema.field_with_name("pod").is_ok());
        assert_eq!(
            schema
                .field_with_name("downsample_resolution")
                .unwrap()
                .data_type(),
            &arrow::datatypes::DataType::Int64
        );
    }

    #[tokio::test]
    async fn reads_counter_samples_from_fixture_chunk_cache() {
        let fixture_root =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../thanos-block-gen/target");
        if !fixture_root.exists() {
            return;
        }
        let cache = std::env::temp_dir().join(format!(
            "thanos-v1-reader-metric-query-{}",
            std::process::id()
        ));
        let repository = config::ThanosRepositoryConfig {
            name: "fixtures".to_owned(),
            uri: format!("file://{}", fixture_root.display()),
            s3: None,
            gcs: None,
        };
        let repositories = vec![repository];
        let storage = RepositoryRegistry::new(&config::ReaderConfig {
            listen_addr: "127.0.0.1:1".to_owned(),
            metrics_listen_addr: "127.0.0.1:2".to_owned(),
            index_cache_location: cache.display().to_string(),
            repositories: repositories.clone(),
            storage: config::StorageConfig::default(),
        })
        .unwrap();
        let schemas = block_index::build_block_index(
            &repositories,
            cache.to_str().unwrap(),
            &storage,
            config::StorageConfig::default().index_build_concurrency,
            config::StorageConfig::default().metadata_read_concurrency,
            None,
        )
        .await
        .unwrap();
        let context = index_context(
            &block_index::block_index_file_path(cache.to_str().unwrap()),
            &block_index::chunk_index_directory_path(cache.to_str().unwrap()),
            &schemas,
            &repositories,
            storage,
        )
        .await
        .unwrap();

        let batches = context
            .sql(
                "SELECT timestamp, value, downsample_resolution, \"cluster\" \
                 FROM metrics.dummy_requests_total \
                 WHERE downsample_resolution = 0 \
                   AND pod ~ '^pod-falcon-000$' \
                   AND timestamp > to_timestamp_millis(0) \
                 LIMIT 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(
            batches
                .iter()
                .map(arrow::record_batch::RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );
        std::fs::remove_dir_all(cache).unwrap();
    }
}
