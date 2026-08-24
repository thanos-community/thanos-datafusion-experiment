use std::{collections::BTreeMap, error::Error, fs, io, path::Path, sync::Arc};

use arrow::{
    array::{
        ArrayRef, Int32Array, Int64Array, ListBuilder, MapBuilder, StringArray, StringBuilder,
        UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
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
    tsdb_index::{self, Series},
};

type BoxError = Box<dyn Error>;
const DELETION_MARK_FILE_NAME: &str = "deletion-mark.json";

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
    #[serde(rename = "numSamples")]
    num_samples: u64,
    #[serde(rename = "numFloatSamples")]
    num_float_samples: u64,
    #[serde(rename = "numHistogramSamples")]
    num_histogram_samples: u64,
    #[serde(rename = "numSeries")]
    num_series: u64,
    #[serde(rename = "numChunks")]
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

#[derive(Debug, Deserialize, Serialize)]
struct BlockFile {
    rel_path: String,
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

/// Rebuild the block index from all configured repositories and write it through OpenDAL.
pub async fn build_block_index(
    repositories: &[ThanosRepositoryConfig],
    index_cache_location: &str,
) -> Result<(), BoxError> {
    let mut rows = Vec::new();

    for repository in repositories {
        let operator = repository_operator(&repository.uri)?;
        let mut lister = operator.lister_with("").recursive(true).await?;

        while let Some(entry) = lister.try_next().await? {
            if !entry.path().ends_with("meta.json") {
                continue;
            }

            let meta_path = entry.path().to_owned();
            let block_path = meta_path
                .strip_suffix("/meta.json")
                .ok_or_else(|| invalid_data(format!("invalid metadata path {meta_path:?}")))?
                .to_owned();
            if block_has_deletion_mark(&operator, &block_path).await? {
                tracing::debug!(
                    repository = %repository.name,
                    block_path = %block_path,
                    "skipping deleted Thanos block"
                );
                continue;
            }

            let contents = operator.read(&meta_path).await?;
            let meta: BlockMeta = serde_json::from_slice(contents.to_bytes().as_ref())?;

            let index_path = format!("{block_path}/index");
            let index = operator.read(&index_path).await?;
            let series = tsdb_index::parse(index.to_bytes().as_ref())?;
            let chunk_rows = chunk_rows(repository, &meta, &block_path, series)?;
            let chunk_index_path = chunk_index_file_path(index_cache_location, &meta.ulid);
            write_local_file(&chunk_index_path, chunk_parquet_bytes(chunk_rows)?).await?;

            rows.push(index_row(repository, meta, block_path, meta_path)?);
        }
    }

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
    Ok(())
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

fn chunk_rows(
    repository: &ThanosRepositoryConfig,
    meta: &BlockMeta,
    block_path: &str,
    series: Vec<Series>,
) -> Result<Vec<ChunkIndexRow>, BoxError> {
    let mut rows = Vec::new();
    for series in series {
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
        let labels_json = serde_json::to_string(&series.labels)?;

        for chunk in series.chunks {
            let chunk_file_seq = (chunk.reference >> 32) as u32;
            let chunk_file_offset = chunk.reference & u64::from(u32::MAX);
            rows.push(ChunkIndexRow {
                repository_name: repository.name.clone(),
                repository_uri: repository.uri.clone(),
                block_ulid: meta.ulid.clone(),
                block_path: block_path.to_owned(),
                chunk_file_path: format!("{block_path}/chunks/{chunk_file_seq:06}"),
                chunk_ref: chunk.reference,
                chunk_file_seq,
                chunk_file_offset,
                series_ref: series.reference,
                series_mint,
                series_maxt,
                chunk_mint: chunk.mint,
                chunk_maxt: chunk.maxt,
                labels: series.labels.clone(),
                labels_json: labels_json.clone(),
            });
        }
    }
    Ok(rows)
}

fn block_parquet_bytes(rows: Vec<BlockIndexRow>) -> Result<Vec<u8>, BoxError> {
    let schema = block_schema();
    let batch = block_record_batch(&schema, &rows)?;
    parquet_bytes(schema, batch)
}

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
        .set_max_row_group_row_count(Some(usize::MAX))
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

fn repository_operator(uri: &str) -> Result<Operator, BoxError> {
    let root = uri
        .strip_prefix("file://")
        .ok_or_else(|| invalid_data(format!("unsupported repository URI {uri:?}; use file://")))?;
    let builder = Fs::default().root(root);
    Ok(Operator::new(builder)?
        .layer(MetricsLayer::new())
        .layer(OtelTraceLayer::new()))
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
    use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

    #[test]
    fn chunk_parquet_contains_row_group_statistics_and_page_indexes() {
        let row = ChunkIndexRow {
            repository_name: "repository".to_owned(),
            repository_uri: "file:///repository".to_owned(),
            block_ulid: "01M0SQFT00EQ1D78Q8Y8EFD0BZ".to_owned(),
            block_path: "01M0SQFT00EQ1D78Q8Y8EFD0BZ".to_owned(),
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
}
