mod block_index;
mod config;
mod flight_service;
mod tsdb_index;

use std::{env, net::SocketAddr};

use arrow_flight::flight_service_server::FlightServiceServer;
use axum::{Router, extract::State, routing::get};
use block_index::{block_index_file_path, build_block_index, chunk_index_directory_path};
use config::ReaderConfig;
use datafusion::{
    execution::SessionStateBuilder,
    prelude::{ParquetReadOptions, SessionConfig, SessionContext},
};
use datafusion_tracing::{
    InstrumentationOptions, RuleInstrumentationOptions, instrument_rules_with_info_spans,
    instrument_with_info_spans,
};
use flight_service::DataFusionFlightService;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use opentelemetry::{global, trace::TracerProvider as _};
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
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
    build_block_index(&config.repositories, &config.index_cache_location).await?;
    let block_index_path = block_index_file_path(&config.index_cache_location);
    let chunk_index_path = chunk_index_directory_path(&config.index_cache_location);
    tracing::info!(
        path = %block_index_path,
        "generated Thanos blocks index"
    );
    tracing::info!(path = %chunk_index_path, "generated Thanos chunks indexes");

    let context = index_context(&block_index_path, &chunk_index_path).await?;
    let service = DataFusionFlightService::new(context, format!("grpc+tcp://{address}"));
    tracing::info!(
        address = %metrics_address,
        path = "/metrics",
        "starting Prometheus metrics endpoint"
    );
    tracing::info!(%address, "starting DataFusion Arrow Flight server");

    Server::builder()
        .add_service(FlightServiceServer::new(service))
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

async fn index_context(
    block_index_path: &str,
    chunk_index_path: &str,
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
    Ok(context)
}
