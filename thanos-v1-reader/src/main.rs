use std::{env, net::SocketAddr};

use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{Router, extract::State, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
use thanos_v1_reader::{
    block_index::{block_index_file_path, build_block_index, chunk_index_directory_path},
    config::ReaderConfig,
    flight_service::DataFusionFlightService,
    index_context,
    storage::RepositoryRegistry,
    store_service::ThanosStoreService,
    thanos_proto::thanos::{info::info_server::InfoServer, store_server::StoreServer},
};
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

const LISTEN_ADDR_ENV_VAR: &str = "FLIGHT_LISTEN_ADDR";
const METRICS_LISTEN_ADDR_ENV_VAR: &str = "METRICS_LISTEN_ADDR";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing()?;
    let prometheus_handle = PrometheusBuilder::new().install_recorder()?;

    let config_path = ReaderConfig::config_path();
    let config = ReaderConfig::load(&config_path)?;
    let storage = RepositoryRegistry::new(&config)?;
    let address = env::var(LISTEN_ADDR_ENV_VAR).unwrap_or(config.listen_addr);
    let metrics_address =
        env::var(METRICS_LISTEN_ADDR_ENV_VAR).unwrap_or(config.metrics_listen_addr);
    let socket_address: SocketAddr = address.parse()?;
    start_metrics_server(&metrics_address, prometheus_handle).await?;

    for repository in &config.repositories {
        tracing::info!(
            name = %repository.name,
            uri = %repository.uri,
            "configured Thanos repository"
        );
    }
    let metric_table_schemas =
        build_block_index(&config.repositories, &config.index_cache_location, &storage).await?;
    let block_index_path = block_index_file_path(&config.index_cache_location);
    let chunk_index_path = chunk_index_directory_path(&config.index_cache_location);
    tracing::info!(
        path = %block_index_path,
        "generated Thanos blocks index"
    );
    tracing::info!(path = %chunk_index_path, "generated Thanos chunks indexes");

    let context = index_context(
        &block_index_path,
        &chunk_index_path,
        &metric_table_schemas,
        &config.repositories,
        storage.clone(),
    )
    .await?;
    let store_service = ThanosStoreService::new(context.clone(), &config.repositories, storage).await?;
    let service = DataFusionFlightService::new(context, format!("grpc+tcp://{address}"));
    tracing::info!(
        address = %metrics_address,
        path = "/metrics",
        "starting Prometheus metrics endpoint"
    );
    tracing::info!(%address, "starting DataFusion Arrow Flight server");

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .add_service(StoreServer::new(store_service.clone()))
        .add_service(InfoServer::new(store_service))
        .serve(socket_address)
        .await?;

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
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .init();
    Ok(())
}

async fn start_metrics_server(
    address: &str,
    prometheus_handle: PrometheusHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(address).await?;
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

async fn prometheus_metrics(State(handle): State<PrometheusHandle>) -> String {
    handle.render()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use thanos_v1_reader::{
        block_index::MetricTableSchema,
        config::{self, ReaderConfig, StorageConfig},
        register_metric_tables,
        storage::RepositoryRegistry,
    };

    fn storage(repositories: Vec<config::ThanosRepositoryConfig>) -> RepositoryRegistry {
        RepositoryRegistry::new(&ReaderConfig {
            listen_addr: "127.0.0.1:1".to_owned(),
            metrics_listen_addr: "127.0.0.1:2".to_owned(),
            index_cache_location: "target/test-index".to_owned(),
            repositories,
            storage: StorageConfig::default(),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn registers_metric_table_in_metrics_schema() {
        let context = SessionContext::new();
        context
            .register_table(
                "chunks",
                Arc::new(
                    datafusion::datasource::MemTable::try_new(
                        Arc::new(arrow::datatypes::Schema::empty()),
                        vec![vec![]],
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
        register_metric_tables(
            &context,
            &[MetricTableSchema {
                name: "dummy_requests_total".to_owned(),
                label_columns: BTreeSet::from(["job".to_owned(), "pod".to_owned()]),
            }],
            &[],
            storage(vec![]),
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
        let storage = storage(vec![repository]);
        let repositories = vec![config::ThanosRepositoryConfig {
            name: "fixtures".to_owned(),
            uri: format!("file://{}", fixture_root.display()),
            s3: None,
            gcs: None,
        }];
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
