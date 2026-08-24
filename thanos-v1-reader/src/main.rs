mod config;
mod flight_service;

use std::{env, net::SocketAddr, sync::Arc};

use arrow::{
    array::{Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_flight::flight_service_server::FlightServiceServer;
use config::ReaderConfig;
use datafusion::{execution::SessionStateBuilder, prelude::SessionContext};
use datafusion_tracing::{
    InstrumentationOptions, RuleInstrumentationOptions, instrument_rules_with_info_spans,
    instrument_with_info_spans,
};
use flight_service::DataFusionFlightService;
use tonic::transport::Server;

const LISTEN_ADDR_ENV_VAR: &str = "FLIGHT_LISTEN_ADDR";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config_path = ReaderConfig::config_path();
    let config = ReaderConfig::load(&config_path)?;
    let address = env::var(LISTEN_ADDR_ENV_VAR).unwrap_or(config.listen_addr);
    let socket_address: SocketAddr = address.parse()?;
    let context = example_context()?;
    let service = DataFusionFlightService::new(context, format!("grpc+tcp://{address}"));

    for repository in &config.repositories {
        tracing::info!(
            name = %repository.name,
            uri = %repository.uri,
            "configured Thanos repository"
        );
    }
    tracing::info!(%address, "starting DataFusion Arrow Flight server");

    Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(socket_address)
        .await?;

    Ok(())
}

fn example_context() -> Result<SessionContext, datafusion::error::DataFusionError> {
    let execution_options = InstrumentationOptions::builder()
        .record_metrics(true)
        .build();
    let execution_rule = instrument_with_info_spans!(options: execution_options);
    let session_state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(execution_rule)
        .build();
    let session_state = instrument_rules_with_info_spans!(
        options: RuleInstrumentationOptions::full(),
        state: session_state
    );
    let context = SessionContext::new_with_state(session_state);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("metric", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_725_000_000_000, 1_725_000_060_000])),
            Arc::new(StringArray::from(vec!["up", "up"])),
            Arc::new(Float64Array::from(vec![1.0, 1.0])),
        ],
    )
    .map_err(|error| datafusion::error::DataFusionError::ArrowError(Box::new(error), None))?;

    context.register_batch("metrics", batch)?;
    Ok(context)
}
