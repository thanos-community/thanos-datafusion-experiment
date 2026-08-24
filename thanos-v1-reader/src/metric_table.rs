use std::{collections::BTreeMap, fmt, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, Float64Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
        StringViewArray, TimestampMillisecondBuilder, UInt64Array,
    },
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{DataFusionError, Result},
    execution::TaskContext,
    logical_expr::{Expr, TableType},
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{TryStreamExt, stream};
use opendal::Operator;

use crate::{
    block_index::{MetricTableSchema, repository_operator},
    chunk_reader,
    config::ThanosRepositoryConfig,
};

#[derive(Debug)]
pub struct MetricTableProvider {
    metric_name: String,
    schema: MetricTableSchema,
    chunk_provider: Arc<dyn TableProvider>,
    operators: BTreeMap<String, Operator>,
}

impl MetricTableProvider {
    pub fn new(
        schema: MetricTableSchema,
        chunk_provider: Arc<dyn TableProvider>,
        repositories: &[ThanosRepositoryConfig],
    ) -> Result<Self> {
        let operators = repositories
            .iter()
            .map(|repository| {
                repository_operator(&repository.uri)
                    .map(|operator| (repository.uri.clone(), operator))
                    .map_err(|error| DataFusionError::Execution(error.to_string()))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            metric_name: schema.name.clone(),
            schema,
            chunk_provider,
            operators,
        })
    }
}

#[async_trait::async_trait]
impl TableProvider for MetricTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.arrow_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let metric_filter = datafusion::logical_expr::col("metric_name")
            .eq(datafusion::logical_expr::lit(self.metric_name.clone()));
        let child = self
            .chunk_provider
            .scan(state, None, &[metric_filter], None)
            .await?;
        Ok(Arc::new(MetricScanExec::new(
            child,
            self.schema.arrow_schema(),
            projection.cloned(),
            self.metric_name.clone(),
            self.metric_name.ends_with("_total"),
            self.operators.clone(),
        )))
    }
}

#[derive(Debug)]
struct MetricScanExec {
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    metric_name: String,
    counter_metric: bool,
    operators: BTreeMap<String, Operator>,
    properties: Arc<PlanProperties>,
}

impl MetricScanExec {
    fn new(
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        metric_name: String,
        counter_metric: bool,
        operators: BTreeMap<String, Operator>,
    ) -> Self {
        let output_schema = projected_schema(&schema, projection.as_deref());
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(child.output_partitioning().partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            child,
            schema,
            projection,
            metric_name,
            counter_metric,
            operators,
            properties,
        }
    }
}

impl DisplayAs for MetricScanExec {
    fn fmt_as(&self, _: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "MetricScanExec")
    }
}

impl ExecutionPlan for MetricScanExec {
    fn name(&self) -> &str {
        "MetricScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "MetricScanExec requires one child".to_owned(),
            ));
        }
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.schema.clone(),
            self.projection.clone(),
            self.metric_name.clone(),
            self.counter_metric,
            self.operators.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child = self.child.execute(partition, context)?;
        let output_schema = self.schema();
        let metric_schema = self.schema.clone();
        let projection = self.projection.clone();
        let metric_name = self.metric_name.clone();
        let operators = self.operators.clone();
        let counter_metric = self.counter_metric;
        let batch = async move {
            let descriptors = child.try_collect::<Vec<_>>().await?;
            build_metric_batch(
                &metric_schema,
                projection.as_deref(),
                &operators,
                &metric_name,
                counter_metric,
                descriptors,
            )
            .await
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream::once(batch),
        )))
    }
}

async fn build_metric_batch(
    schema: &SchemaRef,
    projection: Option<&[usize]>,
    operators: &BTreeMap<String, Operator>,
    metric_name: &str,
    counter_metric: bool,
    descriptors: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    let mut rows = Vec::new();
    for batch in descriptors {
        let repository_uri = string_column(&batch, "repository_uri")?;
        let chunk_file_path = string_column(&batch, "chunk_file_path")?;
        let chunk_file_offset = uint64_column(&batch, "chunk_file_offset")?;
        let resolution = int64_column(&batch, "downsample_resolution")?;
        let labels_json = string_column(&batch, "labels_json")?;
        let descriptor_metric_name = string_column(&batch, "metric_name")?;

        for index in 0..batch.num_rows() {
            if string_value(descriptor_metric_name.as_ref(), index)? != metric_name {
                continue;
            }
            let repository_uri = string_value(repository_uri.as_ref(), index)?;
            let chunk_file_path = string_value(chunk_file_path.as_ref(), index)?;
            let labels_json = string_value(labels_json.as_ref(), index)?;
            let operator = operators.get(&repository_uri).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "no OpenDAL operator for repository URI {:?}",
                    repository_uri
                ))
            })?;
            let labels: BTreeMap<String, String> = serde_json::from_str(&labels_json)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let samples = chunk_reader::read_samples(
                operator,
                &chunk_file_path,
                chunk_file_offset.value(index),
                counter_metric,
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            for sample in samples {
                rows.push(MetricRow {
                    timestamp: sample.timestamp,
                    value: sample.value,
                    downsample_resolution: resolution.value(index),
                    labels: labels.clone(),
                });
            }
        }
    }
    record_batch(schema, projection, &rows)
}

#[derive(Debug)]
struct MetricRow {
    timestamp: i64,
    value: f64,
    downsample_resolution: i64,
    labels: BTreeMap<String, String>,
}

fn record_batch(
    schema: &SchemaRef,
    projection: Option<&[usize]>,
    rows: &[MetricRow],
) -> Result<RecordBatch> {
    let output_schema = projected_schema(schema, projection);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        match (field.name().as_str(), field.data_type()) {
            ("timestamp", DataType::Timestamp(_, _)) => {
                let mut values = TimestampMillisecondBuilder::new();
                for row in rows {
                    values.append_value(row.timestamp);
                }
                columns.push(Arc::new(values.finish()));
            }
            ("value", DataType::Float64) => {
                let mut values = Float64Builder::new();
                for row in rows {
                    values.append_value(row.value);
                }
                columns.push(Arc::new(values.finish()));
            }
            ("downsample_resolution", DataType::Int64) => {
                let mut values = Int64Builder::new();
                for row in rows {
                    values.append_value(row.downsample_resolution);
                }
                columns.push(Arc::new(values.finish()));
            }
            (label, DataType::Utf8) => {
                let mut values = StringBuilder::new();
                for row in rows {
                    values.append_value(row.labels.get(label).map(String::as_str).unwrap_or(""));
                }
                columns.push(Arc::new(values.finish()));
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "unsupported metric output field {field:?}"
                )));
            }
        }
    }
    RecordBatch::try_new(output_schema, columns)
        .map_err(|error| DataFusionError::External(Box::new(error)))
}

fn projected_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> SchemaRef {
    match projection {
        Some(projection) => Arc::new(schema.project(projection).expect("validated projection")),
        None => schema.clone(),
    }
}

fn string_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    batch
        .column_by_name(name)
        .cloned()
        .ok_or_else(|| DataFusionError::Execution(format!("missing chunk index column {name}")))
}

fn string_value(array: &dyn Array, index: usize) -> Result<String> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(index).to_owned());
    }
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(array.value(index).to_owned());
    }
    Err(DataFusionError::Execution(
        "invalid UTF-8 chunk index column".to_owned(),
    ))
}

fn uint64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Execution(format!("missing chunk index column {name}")))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("invalid chunk index column {name}")))
}

fn int64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array> {
    batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Execution(format!("missing chunk index column {name}")))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Execution(format!("invalid chunk index column {name}")))
}
