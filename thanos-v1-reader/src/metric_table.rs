use std::{collections::BTreeMap, fmt, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, Float64Builder, Int32Builder, Int64Array, Int64Builder,
        ListBuilder, StringArray, StringBuilder, StringViewArray, StructBuilder,
        TimestampMillisecondBuilder, UInt8Builder, UInt32Builder, UInt64Array, UInt64Builder,
    },
    datatypes::{DataType, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{DataFusionError, Result, ScalarValue},
    execution::TaskContext,
    logical_expr::{Expr, Operator, TableProviderFilterPushDown, TableType},
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{TryStreamExt, stream};
use opendal::Operator as OpendalOperator;
use regex::Regex;

use crate::{
    block_index::{MetricTableSchema, repository_operator},
    chunk_reader::{self, AggregateSelection},
    config::ThanosRepositoryConfig,
    histogram::{HistogramBuckets, HistogramCount, HistogramSample, Span},
};

#[derive(Debug)]
pub struct MetricTableProvider {
    metric_name: String,
    schema: MetricTableSchema,
    chunk_provider: Arc<dyn TableProvider>,
    operators: BTreeMap<String, OpendalOperator>,
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
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filters = filters
            .iter()
            .map(parse_filter)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "unsupported filter was pushed into metric scan".to_owned(),
                )
            })?;
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
            filters,
            self.metric_name.ends_with("_total"),
            self.operators.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if parse_filter(filter).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

#[derive(Debug)]
struct MetricScanExec {
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    metric_name: String,
    filters: Vec<MetricFilter>,
    counter_metric: bool,
    operators: BTreeMap<String, OpendalOperator>,
    properties: Arc<PlanProperties>,
}

impl MetricScanExec {
    fn new(
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        metric_name: String,
        filters: Vec<MetricFilter>,
        counter_metric: bool,
        operators: BTreeMap<String, OpendalOperator>,
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
            filters,
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
            self.filters.clone(),
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
        let filters = self.filters.clone();
        let operators = self.operators.clone();
        let counter_metric = self.counter_metric;
        let batch = async move {
            let descriptors = child.try_collect::<Vec<_>>().await?;
            build_metric_batch(
                &metric_schema,
                projection.as_deref(),
                &operators,
                &metric_name,
                &filters,
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
    operators: &BTreeMap<String, OpendalOperator>,
    metric_name: &str,
    filters: &[MetricFilter],
    counter_metric: bool,
    descriptors: Vec<RecordBatch>,
) -> Result<RecordBatch> {
    let mut rows = Vec::new();
    let selected_aggregate = filters.iter().find_map(|filter| match filter {
        MetricFilter::Aggregate { value } => match value.as_str() {
            "count" => Some(AggregateSelection::Count),
            "sum" => Some(AggregateSelection::Sum),
            "counter" => Some(AggregateSelection::Counter),
            _ => None,
        },
        _ => None,
    });
    for batch in descriptors {
        let repository_uri = string_column(&batch, "repository_uri")?;
        let chunk_file_path = string_column(&batch, "chunk_file_path")?;
        let chunk_file_offset = uint64_column(&batch, "chunk_file_offset")?;
        let resolution = int64_column(&batch, "downsample_resolution")?;
        let chunk_mint = int64_column(&batch, "chunk_mint")?;
        let chunk_maxt = int64_column(&batch, "chunk_maxt")?;
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
            let descriptor = ChunkDescriptor {
                mint: chunk_mint.value(index),
                maxt: chunk_maxt.value(index),
                downsample_resolution: resolution.value(index),
                labels: &labels,
            };
            if !filters
                .iter()
                .all(|filter| filter.matches_descriptor(&descriptor))
            {
                continue;
            }
            let samples = chunk_reader::read_samples(
                operator,
                &chunk_file_path,
                chunk_file_offset.value(index),
                counter_metric,
                selected_aggregate,
            )
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            for sample in samples {
                if !filters
                    .iter()
                    .all(|filter| filter.matches_sample(&sample, resolution.value(index), &labels))
                {
                    continue;
                }
                rows.push(MetricRow {
                    timestamp: sample.timestamp,
                    value: sample.histogram.is_none().then_some(sample.value),
                    histogram: sample.histogram,
                    downsample_resolution: resolution.value(index),
                    aggregate: sample.aggregate.as_str(),
                    labels: labels.clone(),
                });
            }
        }
    }
    record_batch(schema, projection, &rows)
}

#[derive(Debug, Clone)]
enum MetricFilter {
    Timestamp {
        operator: Operator,
        value: i64,
    },
    Resolution {
        operator: Operator,
        value: i64,
    },
    Label {
        name: String,
        operator: Operator,
        value: String,
    },
    LabelRegex {
        name: String,
        regex: Regex,
        negated: bool,
    },
    Aggregate {
        value: String,
    },
}

impl MetricFilter {
    fn matches_descriptor(&self, descriptor: &ChunkDescriptor<'_>) -> bool {
        match self {
            Self::Timestamp { operator, value } => match operator {
                Operator::Eq => descriptor.mint <= *value && descriptor.maxt >= *value,
                Operator::Gt => descriptor.maxt > *value,
                Operator::GtEq => descriptor.maxt >= *value,
                Operator::Lt => descriptor.mint < *value,
                Operator::LtEq => descriptor.mint <= *value,
                _ => false,
            },
            Self::Resolution { operator, value } => {
                compare_i64(descriptor.downsample_resolution, *operator, *value)
            }
            Self::Aggregate { .. } => true,
            Self::Label {
                name,
                operator,
                value,
            } => compare_str(
                descriptor
                    .labels
                    .get(name)
                    .map(String::as_str)
                    .unwrap_or(""),
                *operator,
                value,
            ),
            Self::LabelRegex {
                name,
                regex,
                negated,
            } => {
                regex.is_match(
                    descriptor
                        .labels
                        .get(name)
                        .map(String::as_str)
                        .unwrap_or(""),
                ) != *negated
            }
        }
    }

    fn matches_sample(
        &self,
        sample: &chunk_reader::Sample,
        downsample_resolution: i64,
        labels: &BTreeMap<String, String>,
    ) -> bool {
        match self {
            Self::Timestamp { operator, value } => compare_i64(sample.timestamp, *operator, *value),
            Self::Resolution { operator, value } => {
                compare_i64(downsample_resolution, *operator, *value)
            }
            Self::Aggregate { value } => sample.aggregate.as_str() == value,
            Self::Label {
                name,
                operator,
                value,
            } => compare_str(
                labels.get(name).map(String::as_str).unwrap_or(""),
                *operator,
                value,
            ),
            Self::LabelRegex {
                name,
                regex,
                negated,
            } => regex.is_match(labels.get(name).map(String::as_str).unwrap_or("")) != *negated,
        }
    }
}

struct ChunkDescriptor<'a> {
    mint: i64,
    maxt: i64,
    downsample_resolution: i64,
    labels: &'a BTreeMap<String, String>,
}

fn parse_filter(expr: &Expr) -> Option<MetricFilter> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    let (column, operator, value) = if let (Some(column), Some(value)) = (
        column_name(binary.left.as_ref()),
        scalar_value(binary.right.as_ref()),
    ) {
        (column, binary.op, value)
    } else if let (Some(column), Some(value)) = (
        column_name(binary.right.as_ref()),
        scalar_value(binary.left.as_ref()),
    ) {
        (column, reverse_operator(binary.op)?, value)
    } else {
        return None;
    };

    match column.as_str() {
        "timestamp" => scalar_i64(&value).map(|value| MetricFilter::Timestamp { operator, value }),
        "downsample_resolution" => {
            scalar_i64(&value).map(|value| MetricFilter::Resolution { operator, value })
        }
        "value" => None,
        "histogram" => None,
        "aggregate_kind" => {
            let value = scalar_string(&value)?;
            (operator == Operator::Eq
                && matches!(value.as_str(), "raw" | "count" | "sum" | "counter"))
            .then_some(MetricFilter::Aggregate { value })
        }
        _ => {
            let value = scalar_string(&value)?;
            match operator {
                Operator::Eq | Operator::NotEq => Some(MetricFilter::Label {
                    name: column,
                    operator,
                    value,
                }),
                Operator::RegexMatch
                | Operator::RegexIMatch
                | Operator::RegexNotMatch
                | Operator::RegexNotIMatch => {
                    let case_insensitive =
                        matches!(operator, Operator::RegexIMatch | Operator::RegexNotIMatch);
                    let negated =
                        matches!(operator, Operator::RegexNotMatch | Operator::RegexNotIMatch);
                    let pattern = if case_insensitive {
                        format!("(?i:{value})")
                    } else {
                        value
                    };
                    Regex::new(&pattern)
                        .ok()
                        .map(|regex| MetricFilter::LabelRegex {
                            name: column,
                            regex,
                            negated,
                        })
                }
                _ => None,
            }
        }
    }
}

fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Cast(cast) => column_name(cast.expr.as_ref()),
        Expr::TryCast(cast) => column_name(cast.expr.as_ref()),
        _ => None,
    }
}

fn scalar_value(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value.clone()),
        Expr::Cast(cast) => scalar_value(cast.expr.as_ref()),
        Expr::TryCast(cast) => scalar_value(cast.expr.as_ref()),
        Expr::ScalarFunction(function)
            if function.name() == "to_timestamp_millis" && function.args.len() == 1 =>
        {
            scalar_i64(&scalar_value(&function.args[0])?)
                .map(|value| ScalarValue::TimestampMillisecond(Some(value), None))
        }
        _ => None,
    }
}

fn scalar_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(*value),
        ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
        ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
        ScalarValue::TimestampMillisecond(Some(value), _) => Some(*value),
        _ => None,
    }
}

fn scalar_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Some(value.clone()),
        _ => None,
    }
}

fn reverse_operator(operator: Operator) -> Option<Operator> {
    match operator {
        Operator::Eq | Operator::NotEq => Some(operator),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None,
    }
}

fn compare_i64(left: i64, operator: Operator, right: i64) -> bool {
    match operator {
        Operator::Eq => left == right,
        Operator::NotEq => left != right,
        Operator::Gt => left > right,
        Operator::GtEq => left >= right,
        Operator::Lt => left < right,
        Operator::LtEq => left <= right,
        _ => false,
    }
}

fn compare_str(left: &str, operator: Operator, right: &str) -> bool {
    match operator {
        Operator::Eq => left == right,
        Operator::NotEq => left != right,
        _ => false,
    }
}

#[derive(Debug)]
struct MetricRow {
    timestamp: i64,
    value: Option<f64>,
    histogram: Option<HistogramSample>,
    downsample_resolution: i64,
    aggregate: &'static str,
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
                    values.append_option(row.value);
                }
                columns.push(Arc::new(values.finish()));
            }
            ("histogram", DataType::Struct(fields)) => {
                columns.push(Arc::new(histogram_array(rows, fields.clone())?));
            }
            ("downsample_resolution", DataType::Int64) => {
                let mut values = Int64Builder::new();
                for row in rows {
                    values.append_value(row.downsample_resolution);
                }
                columns.push(Arc::new(values.finish()));
            }
            ("aggregate_kind", DataType::Utf8) => {
                let mut values = StringBuilder::new();
                for row in rows {
                    values.append_value(row.aggregate);
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

fn histogram_array(
    rows: &[MetricRow],
    fields: arrow::datatypes::Fields,
) -> Result<arrow::array::StructArray> {
    let span_fields = match fields[9].data_type() {
        DataType::List(field) => match field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            _ => {
                return Err(DataFusionError::Internal(
                    "invalid histogram span type".to_owned(),
                ));
            }
        },
        _ => {
            return Err(DataFusionError::Internal(
                "invalid histogram span list".to_owned(),
            ));
        }
    };
    let span_builder = || {
        ListBuilder::new(StructBuilder::new(
            span_fields.clone(),
            vec![
                Box::new(Int32Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(UInt32Builder::new()) as Box<dyn ArrayBuilder>,
            ],
        ))
    };
    let mut builder = StructBuilder::new(
        fields,
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int32Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(UInt64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(UInt8Builder::new()),
            Box::new(span_builder()),
            Box::new(span_builder()),
            Box::new(ListBuilder::new(Int64Builder::new())),
            Box::new(ListBuilder::new(Int64Builder::new())),
            Box::new(ListBuilder::new(Float64Builder::new())),
            Box::new(ListBuilder::new(Float64Builder::new())),
            Box::new(ListBuilder::new(Float64Builder::new())),
        ],
    );
    for row in rows {
        let Some(histogram) = &row.histogram else {
            append_null_histogram(&mut builder);
            continue;
        };
        builder
            .field_builder::<StringBuilder>(0)
            .expect("kind builder")
            .append_value(match histogram.count {
                HistogramCount::Integer(_) => "histogram",
                HistogramCount::Float(_) => "float_histogram",
            });
        builder
            .field_builder::<Int32Builder>(1)
            .expect("schema builder")
            .append_value(histogram.schema);
        append_histogram_counts(&mut builder, histogram);
        builder
            .field_builder::<Float64Builder>(4)
            .expect("sum builder")
            .append_value(histogram.sum);
        builder
            .field_builder::<Float64Builder>(5)
            .expect("threshold builder")
            .append_value(histogram.zero_threshold);
        builder
            .field_builder::<UInt8Builder>(8)
            .expect("reset hint builder")
            .append_value(histogram.reset_hint);
        append_spans(&mut builder, 9, &histogram.positive_spans);
        append_spans(&mut builder, 10, &histogram.negative_spans);
        append_histogram_buckets(&mut builder, histogram);
        let custom_values = builder
            .field_builder::<ListBuilder<Float64Builder>>(15)
            .expect("custom values builder");
        custom_values
            .values()
            .append_slice(&histogram.custom_values);
        custom_values.append(true);
        builder.append(true);
    }
    Ok(builder.finish())
}

fn append_null_histogram(builder: &mut StructBuilder) {
    builder
        .field_builder::<StringBuilder>(0)
        .expect("kind builder")
        .append_null();
    builder
        .field_builder::<Int32Builder>(1)
        .expect("schema builder")
        .append_null();
    builder
        .field_builder::<UInt64Builder>(2)
        .expect("integer count builder")
        .append_null();
    builder
        .field_builder::<Float64Builder>(3)
        .expect("float count builder")
        .append_null();
    builder
        .field_builder::<Float64Builder>(4)
        .expect("sum builder")
        .append_null();
    builder
        .field_builder::<Float64Builder>(5)
        .expect("threshold builder")
        .append_null();
    builder
        .field_builder::<UInt64Builder>(6)
        .expect("integer zero count builder")
        .append_null();
    builder
        .field_builder::<Float64Builder>(7)
        .expect("float zero count builder")
        .append_null();
    builder
        .field_builder::<UInt8Builder>(8)
        .expect("reset hint builder")
        .append_null();
    builder
        .field_builder::<ListBuilder<StructBuilder>>(9)
        .expect("positive spans builder")
        .append_null();
    builder
        .field_builder::<ListBuilder<StructBuilder>>(10)
        .expect("negative spans builder")
        .append_null();
    append_i64_list(builder, 11, None);
    append_i64_list(builder, 12, None);
    append_f64_list(builder, 13, None);
    append_f64_list(builder, 14, None);
    append_f64_list(builder, 15, None);
    builder.append(false);
}

fn append_histogram_counts(builder: &mut StructBuilder, histogram: &HistogramSample) {
    match histogram.count {
        HistogramCount::Integer(value) => {
            builder
                .field_builder::<UInt64Builder>(2)
                .expect("integer count builder")
                .append_value(value);
            builder
                .field_builder::<Float64Builder>(3)
                .expect("float count builder")
                .append_null();
        }
        HistogramCount::Float(value) => {
            builder
                .field_builder::<UInt64Builder>(2)
                .expect("integer count builder")
                .append_null();
            builder
                .field_builder::<Float64Builder>(3)
                .expect("float count builder")
                .append_value(value);
        }
    }
    match histogram.zero_count {
        HistogramCount::Integer(value) => {
            builder
                .field_builder::<UInt64Builder>(6)
                .expect("integer zero count builder")
                .append_value(value);
            builder
                .field_builder::<Float64Builder>(7)
                .expect("float zero count builder")
                .append_null();
        }
        HistogramCount::Float(value) => {
            builder
                .field_builder::<UInt64Builder>(6)
                .expect("integer zero count builder")
                .append_null();
            builder
                .field_builder::<Float64Builder>(7)
                .expect("float zero count builder")
                .append_value(value);
        }
    }
}

fn append_spans(builder: &mut StructBuilder, field: usize, spans: &[Span]) {
    let spans_builder = builder
        .field_builder::<ListBuilder<StructBuilder>>(field)
        .expect("span list builder");
    for span in spans {
        spans_builder
            .values()
            .field_builder::<Int32Builder>(0)
            .expect("span offset builder")
            .append_value(span.offset);
        spans_builder
            .values()
            .field_builder::<UInt32Builder>(1)
            .expect("span length builder")
            .append_value(span.length);
        spans_builder.values().append(true);
    }
    spans_builder.append(true);
}

fn append_histogram_buckets(builder: &mut StructBuilder, histogram: &HistogramSample) {
    match &histogram.positive_buckets {
        HistogramBuckets::Integer(values) => {
            append_i64_list(builder, 11, Some(values));
            append_f64_list(builder, 13, None);
        }
        HistogramBuckets::Float(values) => {
            append_i64_list(builder, 11, None);
            append_f64_list(builder, 13, Some(values));
        }
    }
    match &histogram.negative_buckets {
        HistogramBuckets::Integer(values) => {
            append_i64_list(builder, 12, Some(values));
            append_f64_list(builder, 14, None);
        }
        HistogramBuckets::Float(values) => {
            append_i64_list(builder, 12, None);
            append_f64_list(builder, 14, Some(values));
        }
    }
}

fn append_i64_list(builder: &mut StructBuilder, field: usize, values: Option<&[i64]>) {
    let list = builder
        .field_builder::<ListBuilder<Int64Builder>>(field)
        .expect("integer bucket builder");
    if let Some(values) = values {
        list.values().append_slice(values);
        list.append(true);
    } else {
        list.append_null();
    }
}

fn append_f64_list(builder: &mut StructBuilder, field: usize, values: Option<&[f64]>) {
    let list = builder
        .field_builder::<ListBuilder<Float64Builder>>(field)
        .expect("float bucket builder");
    if let Some(values) = values {
        list.values().append_slice(values);
        list.append(true);
    } else {
        list.append_null();
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_pruning_uses_timestamp_and_missing_label_semantics() {
        let labels = BTreeMap::from([("pod".to_owned(), "pod-falcon-000".to_owned())]);
        let descriptor = ChunkDescriptor {
            mint: 100,
            maxt: 200,
            downsample_resolution: 0,
            labels: &labels,
        };

        assert!(
            MetricFilter::Timestamp {
                operator: Operator::Gt,
                value: 150,
            }
            .matches_descriptor(&descriptor)
        );
        assert!(
            !MetricFilter::Timestamp {
                operator: Operator::Gt,
                value: 200,
            }
            .matches_descriptor(&descriptor)
        );
        assert!(
            MetricFilter::Label {
                name: "missing".to_owned(),
                operator: Operator::Eq,
                value: "".to_owned(),
            }
            .matches_descriptor(&descriptor)
        );
        assert!(
            !MetricFilter::Label {
                name: "pod".to_owned(),
                operator: Operator::Eq,
                value: "pod-amber-000".to_owned(),
            }
            .matches_descriptor(&descriptor)
        );
        assert!(
            MetricFilter::LabelRegex {
                name: "pod".to_owned(),
                regex: Regex::new(r"^pod-falc.*$").unwrap(),
                negated: false,
            }
            .matches_descriptor(&descriptor)
        );
        assert!(
            !MetricFilter::LabelRegex {
                name: "pod".to_owned(),
                regex: Regex::new(r"^pod-falc.*$").unwrap(),
                negated: true,
            }
            .matches_descriptor(&descriptor)
        );
    }
}
