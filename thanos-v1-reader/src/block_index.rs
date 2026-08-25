use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs, io,
    path::Path,
    sync::Arc,
    time::Instant,
};

use arrow::{
    array::{
        ArrayRef, Int32Array, Int64Array, ListBuilder, MapBuilder, StringArray, StringBuilder,
        UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt, stream};
use opendal::{
    Operator,
    layers::{MetricsLayer, OtelTraceLayer},
    services::Fs,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use serde::{Deserialize, Serialize};

use crate::{
    config::ThanosRepositoryConfig,
    storage::RepositoryRegistry,
    tsdb_index::{self, Series},
};

type BoxError = Box<dyn Error>;
const DELETION_MARK_FILE_NAME: &str = "deletion-mark.json";
/// Keep index construction well below the reader's 48 GiB production memory limit.
const CHUNK_INDEX_BATCH_SIZE: usize = 50_000;
/// Each worker holds one downloaded TSDB index and one bounded Parquet batch. Eight workers
/// keep a 12-vCPU reader busy while leaving substantial headroom below its 48 GiB limit.
const INDEX_BUILD_CONCURRENCY: usize = 8;

#[derive(Debug, Deserialize)]
struct BlockMeta {
    ulid: String,
    #[serde(rename = "minTime")]
    min_time: i64,
    #[serde(rename = "maxTime")]
    max_time: i64,
    #[serde(default)]
    version: i32,
    #[serde(default)]
    stats: BlockStats,
    #[serde(default)]
    compaction: Compaction,
    #[serde(default)]
    thanos: ThanosMeta,
}

#[derive(Debug, Default, Deserialize)]
struct BlockStats {
    #[serde(rename = "numSamples", default)]
    num_samples: u64,
    #[serde(rename = "numFloatSamples", default)]
    num_float_samples: u64,
    #[serde(rename = "numHistogramSamples", default)]
    num_histogram_samples: u64,
    #[serde(rename = "numSeries", default)]
    num_series: u64,
    #[serde(rename = "numChunks", default)]
    num_chunks: u64,
}

#[derive(Debug, Default, Deserialize)]
struct Compaction {
    #[serde(default)]
    level: i32,
    #[serde(default)]
    sources: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ThanosMeta {
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    downsample: Downsample,
    #[serde(default)]
    source: String,
    #[serde(default)]
    files: Vec<BlockFile>,
    #[serde(default)]
    index_stats: IndexStats,
    #[serde(default)]
    upload_time: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct Downsample {
    #[serde(default)]
    resolution: i64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct BlockFile {
    #[serde(default)]
    rel_path: String,
    #[serde(default)]
    size_bytes: u64,
}

#[derive(Debug, Default, Deserialize)]
struct IndexStats {
    #[serde(default)]
    series_max_size: Option<i64>,
}

#[derive(Debug)]
struct BlockIndexRow {
    repository_name: String,
    repository_uri: String,
    block_ulid: String,
    block_path: String,
    meta_path: String,
    min_time: i64,
    max_time: i64,
    downsample_resolution: i64,
    external_labels: String,
    external_labels_map: BTreeMap<String, String>,
    source: String,
    upload_time: Option<String>,
    version: i32,
    compaction_level: i32,
    compaction_sources: Vec<String>,
    num_samples: u64,
    num_float_samples: u64,
    num_histogram_samples: u64,
    num_series: u64,
    num_chunks: u64,
    index_series_max_size: Option<i64>,
    index_size_bytes: u64,
    chunks_size_bytes: u64,
    total_size_bytes: u64,
    files: String,
}

#[derive(Debug)]
struct ChunkIndexRow {
    repository_name: String,
    repository_uri: String,
    block_ulid: String,
    block_path: String,
    downsample_resolution: i64,
    metric_name: String,
    chunk_file_path: String,
    chunk_ref: u64,
    chunk_file_seq: u32,
    chunk_file_offset: u64,
    series_ref: u64,
    series_mint: i64,
    series_maxt: i64,
    chunk_mint: i64,
    chunk_maxt: i64,
    labels: BTreeMap<String, String>,
    labels_json: String,
}

struct BlockBuildTask {
    repository: ThanosRepositoryConfig,
    storage_repository: std::sync::Arc<crate::storage::Repository>,
    meta: BlockMeta,
    block_path: String,
    meta_path: String,
}

struct BuiltBlockIndex {
    row: BlockIndexRow,
    block_ulid: String,
    block_path: String,
    index_path: String,
    chunk_index_path: String,
    series_count: usize,
    chunk_count: usize,
    metric_labels: BTreeMap<String, BTreeSet<String>>,
}

/// The DataFusion table schema discovered for one Prometheus metric name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricTableSchema {
    pub name: String,
    pub label_columns: BTreeSet<String>,
}

impl MetricTableSchema {
    pub fn arrow_schema(&self) -> Arc<Schema> {
        let mut fields = vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, true),
            Field::new("downsample_resolution", DataType::Int64, false),
        ];
        fields.extend(
            self.label_columns
                .iter()
                .map(|label| Field::new(label, DataType::Utf8, false)),
        );
        Arc::new(Schema::new(fields))
    }
}

/// Rebuild the block index from all configured repositories and write it through OpenDAL.
pub async fn build_block_index(
    repositories: &[ThanosRepositoryConfig],
    index_cache_location: &str,
    storage: &RepositoryRegistry,
) -> Result<Vec<MetricTableSchema>, BoxError> {
    let mut tasks = Vec::new();
    let mut active_block_ulids = BTreeSet::new();

    for repository in repositories {
        let storage_repository = storage.require(&repository.uri)?;
        let mut lister = storage_repository
            .operator()
            .lister_with("")
            .recursive(true)
            .await?;

        while let Some(entry) = lister.try_next().await? {
            if !entry.path().ends_with("meta.json") {
                continue;
            }

            let meta_path = entry.path().to_owned();
            let block_path = meta_path
                .strip_suffix("/meta.json")
                .ok_or_else(|| invalid_data(format!("invalid metadata path {meta_path:?}")))?
                .to_owned();
            if block_has_deletion_mark(storage_repository.operator(), &block_path).await? {
                tracing::debug!(
                    repository = %repository.name,
                    block_path = %block_path,
                    "skipping deleted Thanos block"
                );
                continue;
            }

            let contents = storage_repository.read(&meta_path).await?;
            let meta: BlockMeta = serde_json::from_slice(&contents).map_err(|error| {
                tracing::error!(
                    repository = %repository.name,
                    repository_uri = %repository.uri,
                    block_path = %block_path,
                    meta_path = %meta_path,
                    error = %error,
                    "failed to parse Thanos block metadata"
                );
                error
            })?;
            active_block_ulids.insert(meta.ulid.clone());

            tasks.push(BlockBuildTask {
                repository: repository.clone(),
                storage_repository: storage_repository.clone(),
                meta,
                block_path,
                meta_path,
            });
        }
    }

    tracing::info!(
        blocks = tasks.len(),
        concurrency = INDEX_BUILD_CONCURRENCY,
        "building Thanos block indexes concurrently"
    );
    let built_indexes = stream::iter(tasks)
        .map(|task| build_chunk_index(task, index_cache_location.to_owned()))
        .buffer_unordered(INDEX_BUILD_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    let mut rows = Vec::with_capacity(built_indexes.len());
    let mut metric_labels = BTreeMap::new();
    for built in built_indexes {
        tracing::debug!(
            repository = %built.row.repository_name,
            block_ulid = %built.block_ulid,
            block_path = %built.block_path,
            index_path = %built.index_path,
            chunk_index_path = %built.chunk_index_path,
            series_count = built.series_count,
            chunk_count = built.chunk_count,
            "processed Thanos block into expanded chunk parquet index"
        );
        merge_metric_labels(&mut metric_labels, built.metric_labels);
        rows.push(built.row);
    }

    cleanup_chunk_index_files(index_cache_location, &active_block_ulids)?;

    rows.sort_by(|left, right| {
        (
            left.repository_name.as_str(),
            left.min_time,
            left.block_ulid.as_str(),
        )
            .cmp(&(
                right.repository_name.as_str(),
                right.min_time,
                right.block_ulid.as_str(),
            ))
    });

    let bytes = block_parquet_bytes(rows)?;
    write_local_file(&block_index_file_path(index_cache_location), bytes).await?;
    Ok(metric_labels
        .into_iter()
        .map(|(name, label_columns)| MetricTableSchema {
            name,
            label_columns,
        })
        .collect())
}

async fn build_chunk_index(
    task: BlockBuildTask,
    index_cache_location: String,
) -> Result<BuiltBlockIndex, BoxError> {
    let index_path = format!("{}/index", task.block_path);
    let started = Instant::now();
    tracing::info!(
        repository = %task.repository.name,
        block_ulid = %task.meta.ulid,
        block_path = %task.block_path,
        "starting Thanos block index build"
    );
    let index = task.storage_repository.read(&index_path).await?;
    tracing::debug!(
        repository = %task.repository.name,
        block_ulid = %task.meta.ulid,
        index_path = %index_path,
        index_bytes = index.len(),
        "downloaded Thanos TSDB index"
    );
    let chunk_index_path = chunk_index_file_path(&index_cache_location, &task.meta.ulid);
    let mut metric_labels = BTreeMap::new();
    let (series_count, chunk_count) = write_chunk_index_streaming(
        &task.repository,
        &task.meta,
        &task.block_path,
        &index,
        &chunk_index_path,
        &mut metric_labels,
    )?;
    let block_ulid = task.meta.ulid.clone();
    let row = index_row(
        &task.repository,
        task.meta,
        task.block_path.clone(),
        task.meta_path,
    )?;
    tracing::info!(
        repository = %task.repository.name,
        block_ulid = %block_ulid,
        elapsed_seconds = started.elapsed().as_secs_f64(),
        series_count,
        chunk_count,
        "finished Thanos block index build"
    );
    Ok(BuiltBlockIndex {
        row,
        block_ulid,
        block_path: task.block_path,
        index_path,
        chunk_index_path,
        series_count,
        chunk_count,
        metric_labels,
    })
}

/// Return the generated block-index parquet location for a configured cache directory.
pub fn block_index_file_path(index_cache_location: &str) -> String {
    let location = index_cache_location
        .strip_prefix("file://")
        .unwrap_or(index_cache_location);
    Path::new(location)
        .join("block_index.parquet")
        .to_string_lossy()
        .into_owned()
}

/// Return the generated per-block expanded chunk-index parquet location.
pub fn chunk_index_file_path(index_cache_location: &str, block_ulid: &str) -> String {
    let location = index_cache_location
        .strip_prefix("file://")
        .unwrap_or(index_cache_location);
    Path::new(location)
        .join("indexes")
        .join(format!("{block_ulid}.parquet"))
        .to_string_lossy()
        .into_owned()
}

/// Return the generated expanded chunk-index directory for a configured cache directory.
pub fn chunk_index_directory_path(index_cache_location: &str) -> String {
    let location = index_cache_location
        .strip_prefix("file://")
        .unwrap_or(index_cache_location);
    Path::new(location)
        .join("indexes")
        .to_string_lossy()
        .into_owned()
}

fn cleanup_chunk_index_files(
    index_cache_location: &str,
    active_block_ulids: &BTreeSet<String>,
) -> Result<(), BoxError> {
    let directory = chunk_index_directory_path(index_cache_location);
    let directory = Path::new(&directory);
    if !directory.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_file()
            || path.extension().and_then(|value| value.to_str()) != Some("parquet")
        {
            continue;
        }
        let Some(block_ulid) = path
            .file_stem()
            .and_then(|value| value.to_str())
            .map(str::to_owned)
        else {
            continue;
        };
        if active_block_ulids.contains(&block_ulid) {
            continue;
        }

        fs::remove_file(&path)?;
        tracing::debug!(path = %path.display(), block_ulid, "removed stale chunk index cache file");
    }
    Ok(())
}

fn index_row(
    repository: &ThanosRepositoryConfig,
    meta: BlockMeta,
    block_path: String,
    meta_path: String,
) -> Result<BlockIndexRow, BoxError> {
    let index_size_bytes = meta
        .thanos
        .files
        .iter()
        .filter(|file| file.rel_path == "index")
        .map(|file| file.size_bytes)
        .sum();
    let chunks_size_bytes = meta
        .thanos
        .files
        .iter()
        .filter(|file| file.rel_path.starts_with("chunks/"))
        .map(|file| file.size_bytes)
        .sum();
    let total_size_bytes = meta.thanos.files.iter().map(|file| file.size_bytes).sum();

    Ok(BlockIndexRow {
        repository_name: repository.name.clone(),
        repository_uri: repository.uri.clone(),
        block_ulid: meta.ulid,
        block_path,
        meta_path,
        min_time: meta.min_time,
        max_time: meta.max_time,
        downsample_resolution: meta.thanos.downsample.resolution,
        external_labels: serde_json::to_string(&meta.thanos.labels)?,
        external_labels_map: meta.thanos.labels,
        source: meta.thanos.source,
        upload_time: meta.thanos.upload_time,
        version: meta.version,
        compaction_level: meta.compaction.level,
        compaction_sources: meta.compaction.sources,
        num_samples: meta.stats.num_samples,
        num_float_samples: meta.stats.num_float_samples,
        num_histogram_samples: meta.stats.num_histogram_samples,
        num_series: meta.stats.num_series,
        num_chunks: meta.stats.num_chunks,
        index_series_max_size: meta.thanos.index_stats.series_max_size,
        index_size_bytes,
        chunks_size_bytes,
        total_size_bytes,
        files: serde_json::to_string(&meta.thanos.files)?,
    })
}

/// Expand a single block's TSDB index into Parquet without holding the block's full series,
/// expanded chunks, or encoded Parquet bytes in memory at once.
fn write_chunk_index_streaming(
    repository: &ThanosRepositoryConfig,
    meta: &BlockMeta,
    block_path: &str,
    index: &[u8],
    chunk_index_path: &str,
    metric_labels: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<(usize, usize), BoxError> {
    let path = Path::new(
        chunk_index_path
            .strip_prefix("file://")
            .unwrap_or(chunk_index_path),
    );
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let file_name = path
        .file_name()
        .ok_or_else(|| invalid_data(format!("invalid chunk index path {path:?}")))?;
    if let Some(parent) = parent {
        fs::create_dir_all(parent)?;
    }
    let temporary_path = path.with_file_name(format!("{}.partial", file_name.to_string_lossy()));
    let file = fs::File::create(&temporary_path)?;
    let schema = chunk_schema();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(parquet_properties()))?;
    let mut rows = Vec::with_capacity(CHUNK_INDEX_BATCH_SIZE);
    let mut series_count = 0;
    let mut chunk_count = 0;
    let mut write_error: Option<BoxError> = None;

    let parsed = tsdb_index::parse_each(index, |series| {
        if write_error.is_some() {
            return;
        }
        series_count += 1;
        collect_metric_labels(
            metric_labels,
            &meta.thanos.labels,
            std::slice::from_ref(&series),
        );
        match append_chunk_rows(repository, meta, block_path, series, &mut rows) {
            Ok(count) => {
                chunk_count += count;
                if rows.len() >= CHUNK_INDEX_BATCH_SIZE {
                    tracing::debug!(
                        block_ulid = %meta.ulid,
                        batch_rows = rows.len(),
                        total_chunks = chunk_count,
                        "flushing bounded chunk-index Parquet batch"
                    );
                    if let Err(error) = flush_chunk_rows(&mut writer, &schema, &mut rows) {
                        write_error = Some(error);
                    }
                }
            }
            Err(error) => write_error = Some(error),
        }
    });

    let result = match parsed {
        Err(error) => Err(Box::new(error) as BoxError),
        Ok(()) => match write_error {
            Some(error) => Err(error),
            None => {
                tracing::debug!(
                    block_ulid = %meta.ulid,
                    batch_rows = rows.len(),
                    total_chunks = chunk_count,
                    "flushing final chunk-index Parquet batch"
                );
                flush_chunk_rows(&mut writer, &schema, &mut rows)?;
                writer.close()?;
                fs::rename(&temporary_path, path)?;
                Ok((series_count, chunk_count))
            }
        },
    };
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn append_chunk_rows(
    repository: &ThanosRepositoryConfig,
    meta: &BlockMeta,
    block_path: &str,
    series: Series,
    rows: &mut Vec<ChunkIndexRow>,
) -> Result<usize, BoxError> {
    let series_mint = series
        .chunks
        .first()
        .map(|chunk| chunk.mint)
        .ok_or_else(|| invalid_data("series has no chunks".to_owned()))?;
    let series_maxt = series
        .chunks
        .last()
        .map(|chunk| chunk.maxt)
        .ok_or_else(|| invalid_data("series has no chunks".to_owned()))?;
    let metric_name = series
        .labels
        .get("__name__")
        .ok_or_else(|| invalid_data("series is missing __name__ label".to_owned()))?
        .clone();
    let mut labels = series.labels;
    for (name, value) in &meta.thanos.labels {
        labels.entry(name.clone()).or_insert_with(|| value.clone());
    }
    let labels_json = serde_json::to_string(&labels)?;
    let count = series.chunks.len();
    for chunk in series.chunks {
        let chunk_file_seq = (chunk.reference >> 32) as u32;
        let chunk_file_offset = chunk.reference & u64::from(u32::MAX);
        rows.push(ChunkIndexRow {
            repository_name: repository.name.clone(),
            repository_uri: repository.uri.clone(),
            block_ulid: meta.ulid.clone(),
            block_path: block_path.to_owned(),
            downsample_resolution: meta.thanos.downsample.resolution,
            metric_name: metric_name.clone(),
            chunk_file_path: format!("{block_path}/chunks/{:06}", chunk_file_seq + 1),
            chunk_ref: chunk.reference,
            chunk_file_seq,
            chunk_file_offset,
            series_ref: series.reference,
            series_mint,
            series_maxt,
            chunk_mint: chunk.mint,
            chunk_maxt: chunk.maxt,
            labels: labels.clone(),
            labels_json: labels_json.clone(),
        });
    }
    Ok(count)
}

fn flush_chunk_rows(
    writer: &mut ArrowWriter<fs::File>,
    schema: &Arc<Schema>,
    rows: &mut Vec<ChunkIndexRow>,
) -> Result<(), BoxError> {
    if rows.is_empty() {
        return Ok(());
    }
    writer.write(&chunk_record_batch(schema, rows)?)?;
    writer.flush()?;
    rows.clear();
    Ok(())
}

fn collect_metric_labels(
    metric_labels: &mut BTreeMap<String, BTreeSet<String>>,
    external_labels: &BTreeMap<String, String>,
    series: &[Series],
) {
    for series in series {
        let Some(metric_name) = series.labels.get("__name__") else {
            continue;
        };
        metric_labels
            .entry(metric_name.clone())
            .or_default()
            .extend(
                series
                    .labels
                    .keys()
                    .chain(external_labels.keys())
                    .filter(|label| label.as_str() != "__name__")
                    .cloned(),
            );
    }
}

fn merge_metric_labels(
    target: &mut BTreeMap<String, BTreeSet<String>>,
    source: BTreeMap<String, BTreeSet<String>>,
) {
    for (metric, labels) in source {
        target.entry(metric).or_default().extend(labels);
    }
}

fn block_parquet_bytes(rows: Vec<BlockIndexRow>) -> Result<Vec<u8>, BoxError> {
    let schema = block_schema();
    let batch = block_record_batch(&schema, &rows)?;
    parquet_bytes(schema, batch)
}

#[cfg(test)]
fn chunk_parquet_bytes(rows: Vec<ChunkIndexRow>) -> Result<Vec<u8>, BoxError> {
    let schema = chunk_schema();
    let batch = chunk_record_batch(&schema, &rows)?;
    parquet_bytes(schema, batch)
}

fn parquet_bytes(schema: Arc<Schema>, batch: RecordBatch) -> Result<Vec<u8>, BoxError> {
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(parquet_properties()))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(bytes)
}

fn parquet_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_write_page_header_statistics(true)
        .set_statistics_truncate_length(None)
        .set_column_index_truncate_length(None)
        .set_max_row_group_row_count(Some(CHUNK_INDEX_BATCH_SIZE))
        .build()
}

fn block_schema() -> Arc<Schema> {
    let string_field = Arc::new(Field::new("item", DataType::Utf8, true));
    let map_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("keys", DataType::Utf8, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ]
            .into(),
        ),
        false,
    ));

    Arc::new(Schema::new(vec![
        Field::new("repository_name", DataType::Utf8, false),
        Field::new("repository_uri", DataType::Utf8, false),
        Field::new("block_ulid", DataType::Utf8, false),
        Field::new("block_path", DataType::Utf8, false),
        Field::new("meta_path", DataType::Utf8, false),
        Field::new("min_time", DataType::Int64, false),
        Field::new("max_time", DataType::Int64, false),
        Field::new("downsample_resolution", DataType::Int64, false),
        Field::new("external_labels", DataType::Utf8, false),
        Field::new(
            "external_labels_map",
            DataType::Map(map_entries, false),
            false,
        ),
        Field::new("source", DataType::Utf8, false),
        Field::new("upload_time", DataType::Utf8, true),
        Field::new("version", DataType::Int32, false),
        Field::new("compaction_level", DataType::Int32, false),
        Field::new_list("compaction_sources", string_field, true),
        Field::new("num_samples", DataType::UInt64, false),
        Field::new("num_float_samples", DataType::UInt64, false),
        Field::new("num_histogram_samples", DataType::UInt64, false),
        Field::new("num_series", DataType::UInt64, false),
        Field::new("num_chunks", DataType::UInt64, false),
        Field::new("index_series_max_size", DataType::Int64, true),
        Field::new("index_size_bytes", DataType::UInt64, false),
        Field::new("chunks_size_bytes", DataType::UInt64, false),
        Field::new("total_size_bytes", DataType::UInt64, false),
        Field::new("files", DataType::Utf8, false),
    ]))
}

fn block_record_batch(
    schema: &Arc<Schema>,
    rows: &[BlockIndexRow],
) -> Result<RecordBatch, BoxError> {
    let mut labels = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    let mut sources = ListBuilder::new(StringBuilder::new());

    for row in rows {
        for (key, value) in &row.external_labels_map {
            labels.keys().append_value(key);
            labels.values().append_value(value);
        }
        labels.append(true)?;

        for source in &row.compaction_sources {
            sources.values().append_value(source);
        }
        sources.append(true);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.repository_name.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.repository_uri.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.block_ulid.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.block_path.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.meta_path.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.min_time).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.max_time).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.downsample_resolution)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.external_labels.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(labels.finish()),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.source.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.upload_time.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            rows.iter().map(|row| row.version).collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(
            rows.iter()
                .map(|row| row.compaction_level)
                .collect::<Vec<_>>(),
        )),
        Arc::new(sources.finish()),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.num_samples).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.num_float_samples)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.num_histogram_samples)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.num_series).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.num_chunks).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.index_series_max_size)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.index_size_bytes)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.chunks_size_bytes)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.total_size_bytes)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.files.as_str())
                .collect::<Vec<_>>(),
        )),
    ];

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

fn chunk_schema() -> Arc<Schema> {
    let map_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Arc::new(Field::new("keys", DataType::Utf8, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            ]
            .into(),
        ),
        false,
    ));

    Arc::new(Schema::new(vec![
        Field::new("repository_name", DataType::Utf8, false),
        Field::new("repository_uri", DataType::Utf8, false),
        Field::new("block_ulid", DataType::Utf8, false),
        Field::new("block_path", DataType::Utf8, false),
        Field::new("downsample_resolution", DataType::Int64, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("chunk_file_path", DataType::Utf8, false),
        Field::new("chunk_ref", DataType::UInt64, false),
        Field::new("chunk_file_seq", DataType::UInt32, false),
        Field::new("chunk_file_offset", DataType::UInt64, false),
        Field::new("series_ref", DataType::UInt64, false),
        Field::new("series_mint", DataType::Int64, false),
        Field::new("series_maxt", DataType::Int64, false),
        Field::new("chunk_mint", DataType::Int64, false),
        Field::new("chunk_maxt", DataType::Int64, false),
        Field::new("labels", DataType::Map(map_entries, false), false),
        Field::new("labels_json", DataType::Utf8, false),
    ]))
}

fn chunk_record_batch(
    schema: &Arc<Schema>,
    rows: &[ChunkIndexRow],
) -> Result<RecordBatch, BoxError> {
    let mut labels = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for row in rows {
        for (name, value) in &row.labels {
            labels.keys().append_value(name);
            labels.values().append_value(value);
        }
        labels.append(true)?;
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.repository_name.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.repository_uri.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.block_ulid.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.block_path.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.downsample_resolution)
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.metric_name.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.chunk_file_path.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.chunk_ref).collect::<Vec<_>>(),
        )),
        Arc::new(UInt32Array::from(
            rows.iter()
                .map(|row| row.chunk_file_seq)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.chunk_file_offset)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter().map(|row| row.series_ref).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.series_mint).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.series_maxt).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.chunk_mint).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.chunk_maxt).collect::<Vec<_>>(),
        )),
        Arc::new(labels.finish()),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.labels_json.as_str())
                .collect::<Vec<_>>(),
        )),
    ];

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

async fn block_has_deletion_mark(operator: &Operator, block_path: &str) -> Result<bool, BoxError> {
    let deletion_mark_path = if block_path.is_empty() {
        DELETION_MARK_FILE_NAME.to_owned()
    } else {
        format!("{block_path}/{DELETION_MARK_FILE_NAME}")
    };

    Ok(operator.exists(&deletion_mark_path).await?)
}

async fn write_local_file(path: &str, bytes: Vec<u8>) -> Result<(), BoxError> {
    let path = path.strip_prefix("file://").unwrap_or(path);
    let path = Path::new(path);
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let file_name = path
        .file_name()
        .ok_or_else(|| invalid_data(format!("invalid block index path {path:?}")))?;
    if let Some(parent) = parent {
        fs::create_dir_all(parent)?;
    }
    let builder = Fs::default().root(parent.map(Path::to_string_lossy).as_deref().unwrap_or("."));
    let operator = Operator::new(builder)?
        .layer(MetricsLayer::new())
        .layer(OtelTraceLayer::new());
    operator
        .write(file_name.to_string_lossy().as_ref(), bytes)
        .await?;
    Ok(())
}

fn invalid_data(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ReaderConfig, StorageConfig};
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

    #[test]
    fn legacy_block_metadata_defaults_missing_statistics() {
        let meta: BlockMeta = serde_json::from_str(
            r#"{
                "ulid": "01M0SQFT00EQ1D78Q8Y8EFD0BZ",
                "minTime": 100,
                "maxTime": 200,
                "stats": { "numSamples": 7 },
                "thanos": { "files": [{}] }
            }"#,
        )
        .unwrap();

        assert_eq!(meta.stats.num_samples, 7);
        assert_eq!(meta.stats.num_float_samples, 0);
        assert_eq!(meta.stats.num_histogram_samples, 0);
        assert_eq!(meta.stats.num_series, 0);
        assert_eq!(meta.stats.num_chunks, 0);
        assert_eq!(meta.thanos.files[0].rel_path, "");
        assert_eq!(meta.thanos.files[0].size_bytes, 0);
    }

    #[tokio::test]
    async fn skips_deletion_marked_blocks_before_parsing_metadata() {
        let root = tempfile::tempdir().unwrap();
        let repository_root = root.path().join("repository");
        let block_path = repository_root.join("01M0SQFT00EQ1D78Q8Y8EFD0BZ");
        fs::create_dir_all(&block_path).unwrap();
        fs::write(block_path.join("meta.json"), b"not valid JSON").unwrap();
        fs::write(block_path.join(DELETION_MARK_FILE_NAME), b"{}").unwrap();

        let repository = ThanosRepositoryConfig {
            name: "repository".to_owned(),
            uri: format!("file://{}", repository_root.display()),
            s3: None,
            gcs: None,
        };
        let storage = RepositoryRegistry::new(&ReaderConfig {
            listen_addr: "127.0.0.1:1".to_owned(),
            metrics_listen_addr: "127.0.0.1:2".to_owned(),
            index_cache_location: root.path().join("cache").display().to_string(),
            repositories: vec![repository.clone()],
            storage: StorageConfig::default(),
        })
        .unwrap();

        let schemas = build_block_index(
            &[repository],
            root.path().join("cache").to_str().unwrap(),
            &storage,
        )
        .await
        .unwrap();

        assert!(schemas.is_empty());
    }

    #[test]
    fn chunk_parquet_contains_row_group_statistics_and_page_indexes() {
        let row = ChunkIndexRow {
            repository_name: "repository".to_owned(),
            repository_uri: "file:///repository".to_owned(),
            block_ulid: "01M0SQFT00EQ1D78Q8Y8EFD0BZ".to_owned(),
            block_path: "01M0SQFT00EQ1D78Q8Y8EFD0BZ".to_owned(),
            downsample_resolution: 300_000,
            metric_name: "up".to_owned(),
            chunk_file_path: "01M0SQFT00EQ1D78Q8Y8EFD0BZ/chunks/000001".to_owned(),
            chunk_ref: (1 << 32) + 8,
            chunk_file_seq: 1,
            chunk_file_offset: 8,
            series_ref: 16,
            series_mint: 100,
            series_maxt: 200,
            chunk_mint: 100,
            chunk_maxt: 200,
            labels: BTreeMap::from([
                ("__name__".to_owned(), "up".to_owned()),
                ("job".to_owned(), "reader".to_owned()),
            ]),
            labels_json: r#"{"__name__":"up","job":"reader"}"#.to_owned(),
        };
        let path = std::env::temp_dir().join(format!(
            "thanos-v1-reader-chunk-index-{}.parquet",
            std::process::id()
        ));
        fs::write(&path, chunk_parquet_bytes(vec![row]).unwrap()).unwrap();

        let file = fs::File::open(&path).unwrap();
        let metadata = ParquetMetaDataReader::new()
            .with_page_index_policy(PageIndexPolicy::Required)
            .parse_and_finish(&file)
            .unwrap();
        fs::remove_file(path).unwrap();

        assert_eq!(metadata.file_metadata().num_rows(), 1);
        assert_eq!(metadata.num_row_groups(), 1);
        assert!(metadata.column_index().is_some());
        assert!(metadata.offset_index().is_some());
        for column in metadata.row_group(0).columns() {
            assert!(column.statistics().is_some(), "{column:?}");
        }
    }

    #[test]
    fn metric_schema_collects_all_labels_except_metric_name() {
        let series = vec![
            Series {
                reference: 1,
                labels: BTreeMap::from([
                    ("__name__".to_owned(), "requests_total".to_owned()),
                    ("job".to_owned(), "reader".to_owned()),
                    ("pod".to_owned(), "one".to_owned()),
                ]),
                chunks: Vec::new(),
            },
            Series {
                reference: 2,
                labels: BTreeMap::from([
                    ("__name__".to_owned(), "requests_total".to_owned()),
                    ("instance".to_owned(), "localhost".to_owned()),
                    ("job".to_owned(), "reader".to_owned()),
                ]),
                chunks: Vec::new(),
            },
        ];
        let mut metric_labels = BTreeMap::new();

        collect_metric_labels(
            &mut metric_labels,
            &BTreeMap::from([("cluster".to_owned(), "production".to_owned())]),
            &series,
        );

        assert_eq!(
            metric_labels["requests_total"],
            BTreeSet::from([
                "cluster".to_owned(),
                "instance".to_owned(),
                "job".to_owned(),
                "pod".to_owned(),
            ])
        );
        let schema = MetricTableSchema {
            name: "requests_total".to_owned(),
            label_columns: metric_labels.remove("requests_total").unwrap(),
        }
        .arrow_schema();
        assert_eq!(
            schema
                .field_with_name("downsample_resolution")
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
        assert!(schema.field_with_name("__name__").is_err());
        assert!(!schema.field_with_name("cluster").unwrap().is_nullable());
    }

    #[test]
    fn chunk_index_labels_include_block_external_labels() {
        let meta = BlockMeta {
            ulid: "01M0SQFT00EQ1D78Q8Y8EFD0BZ".to_owned(),
            min_time: 100,
            max_time: 200,
            version: 1,
            stats: BlockStats::default(),
            compaction: Compaction::default(),
            thanos: ThanosMeta {
                labels: BTreeMap::from([("cluster".to_owned(), "production".to_owned())]),
                ..ThanosMeta::default()
            },
        };
        let repository = ThanosRepositoryConfig {
            name: "repository".to_owned(),
            uri: "file:///repository".to_owned(),
            s3: None,
            gcs: None,
        };
        let series = vec![Series {
            reference: 16,
            labels: BTreeMap::from([
                ("__name__".to_owned(), "up".to_owned()),
                ("job".to_owned(), "reader".to_owned()),
            ]),
            chunks: vec![tsdb_index::Chunk {
                mint: 100,
                maxt: 200,
                reference: (1 << 32) + 8,
            }],
        }];

        let mut rows = Vec::new();
        append_chunk_rows(
            &repository,
            &meta,
            &meta.ulid,
            series.into_iter().next().unwrap(),
            &mut rows,
        )
        .unwrap();

        assert_eq!(rows[0].labels["cluster"], "production");
        assert_eq!(
            rows[0].labels_json,
            r#"{"__name__":"up","cluster":"production","job":"reader"}"#
        );
    }

    #[test]
    fn cleanup_removes_only_inactive_chunk_indexes() {
        let cache_root = std::env::temp_dir().join(format!(
            "thanos-v1-reader-index-cache-{}",
            std::process::id()
        ));
        let indexes = cache_root.join("indexes");
        fs::create_dir_all(&indexes).unwrap();
        let active = "01M0SQFT00EQ1D78Q8Y8EFD0BZ";
        let stale = "01M0SQFSN27VV8YR0RMQE3ETG7";
        fs::write(indexes.join(format!("{active}.parquet")), []).unwrap();
        fs::write(indexes.join(format!("{stale}.parquet")), []).unwrap();
        fs::write(indexes.join("notes.txt"), []).unwrap();

        cleanup_chunk_index_files(
            cache_root.to_str().unwrap(),
            &BTreeSet::from([active.to_owned()]),
        )
        .unwrap();

        assert!(indexes.join(format!("{active}.parquet")).exists());
        assert!(!indexes.join(format!("{stale}.parquet")).exists());
        assert!(indexes.join("notes.txt").exists());
        fs::remove_dir_all(cache_root).unwrap();
    }
}
