pub mod block_index;
pub mod chunk_reader;
pub mod config;
pub mod flight_service;
pub mod metric_table;
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

pub async fn index_context(
    block_index_path: &str,
    chunk_index_path: &str,
    metric_table_schemas: &[MetricTableSchema],
    repositories: &[ThanosRepositoryConfig],
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
    register_metric_tables(&context, metric_table_schemas, repositories).await?;
    Ok(context)
}

pub async fn register_metric_tables(
    context: &SessionContext,
    metric_table_schemas: &[MetricTableSchema],
    repositories: &[ThanosRepositoryConfig],
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
            repositories,
        )?;
        context.register_table(
            TableReference::full("datafusion", "metrics", metric_table_schema.name.as_str()),
            Arc::new(table),
        )?;
    }
    Ok(())
}
