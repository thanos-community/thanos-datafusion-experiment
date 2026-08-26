use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    pin::Pin,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray, StringViewArray, UInt64Array},
    record_batch::RecordBatch,
};
use datafusion::prelude::SessionContext;
use futures::{Stream, TryStreamExt};
use regex::Regex;
use serde::Deserialize;
use tokio_stream::iter;
use tonic::{Request, Response, Status};

use crate::{
    chunk_reader::{self, EncodedChunk},
    config::ThanosRepositoryConfig,
    storage::RepositoryRegistry,
    thanos_proto::thanos::{
        self, Aggr, AggrChunk, Chunk, Label, LabelMatcher, Series, SeriesBatch, SeriesResponse,
        info, store_server::Store,
    },
};

type BoxError = Box<dyn Error>;
type SeriesStream = Pin<Box<dyn Stream<Item = Result<SeriesResponse, Status>> + Send>>;

#[derive(Clone)]
pub struct ThanosStoreService {
    context: Arc<SessionContext>,
    #[cfg(test)]
    descriptors: Option<Arc<Vec<SeriesDescriptor>>>,
    blocks: Arc<Vec<BlockMetadata>>,
    storage: RepositoryRegistry,
}

#[derive(Clone)]
struct SeriesDescriptor {
    repository_uri: String,
    downsample_resolution: i64,
    series_mint: i64,
    series_maxt: i64,
    labels: BTreeMap<String, String>,
    chunks: Vec<ChunkLocation>,
}

#[derive(Clone, Deserialize)]
struct ChunkLocation {
    chunk_file_path: String,
    chunk_file_offset: u64,
    chunk_mint: i64,
    chunk_maxt: i64,
}

#[derive(Clone)]
struct BlockMetadata {
    min_time: i64,
    max_time: i64,
    external_labels: BTreeMap<String, String>,
}

#[derive(Clone)]
enum Matcher {
    Eq(String, String),
    Neq(String, String),
    Re(String, Regex),
    Nre(String, Regex),
}

impl ThanosStoreService {
    pub async fn new(
        context: SessionContext,
        _repositories: &[ThanosRepositoryConfig],
        storage: RepositoryRegistry,
    ) -> Result<Self, BoxError> {
        let blocks = load_blocks(&context).await?;
        Ok(Self {
            context: Arc::new(context),
            #[cfg(test)]
            descriptors: None,
            blocks: Arc::new(blocks),
            storage,
        })
    }

    async fn matching_series(
        &self,
        start: i64,
        end: i64,
        matchers: &[Matcher],
    ) -> Result<Vec<SeriesDescriptor>, Status> {
        #[cfg(test)]
        if let Some(descriptors) = &self.descriptors {
            return Ok(descriptors
                .iter()
                .filter(|series| {
                    overlaps(series.series_mint, series.series_maxt, start, end)
                        && matches_all(&series.labels, matchers)
                })
                .cloned()
                .collect());
        }

        // The on-disk StoreAPI projection keeps labels as canonical JSON. Apply exact PromQL
        // matchers in DataFusion before projecting `chunks_json`: decoding chunk locations for
        // every series in a large time block made otherwise narrow rate queries time out.
        let label_filters = exact_label_filters(matchers)
            .map_err(|error| Status::internal(error.to_string()))?;
        let label_predicate = label_filters
            .iter()
            .map(|filter| format!(" AND labels_json LIKE {}", sql_literal(filter)))
            .collect::<String>();
        let frame = self
            .context
            .sql(&format!(
                "SELECT repository_uri, downsample_resolution, series_mint, series_maxt, \
                 labels_json, chunks_json \
                 FROM series WHERE series_mint <= {end} AND series_maxt >= {start}{label_predicate}"
            ))
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        let mut stream = frame
            .execute_stream()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        let mut series = Vec::new();
        while let Some(batch) = stream
            .try_next()
            .await
            .map_err(|error| Status::internal(error.to_string()))?
        {
            series.extend(
                series_from_batch(&batch)
                    .map_err(|error| Status::internal(error.to_string()))?
                    .into_iter()
                    .filter(|series| matches_all(&series.labels, matchers)),
            );
        }
        Ok(series)
    }

    fn info_response(&self) -> info::InfoResponse {
        let mut ranges = BTreeMap::<Vec<(String, String)>, (i64, i64)>::new();
        for block in self.blocks.iter() {
            let key = labels_key(&block.external_labels);
            ranges
                .entry(key)
                .and_modify(|range| {
                    range.0 = range.0.min(block.min_time);
                    range.1 = range.1.max(block.max_time);
                })
                .or_insert((block.min_time, block.max_time));
        }

        let tsdb_infos = ranges
            .iter()
            .map(|(labels, (min_time, max_time))| info::TsdbInfo {
                labels: Some(thanos::ZLabelSet {
                    labels: labels
                        .iter()
                        .map(|(name, value)| Label {
                            name: name.clone(),
                            value: value.clone(),
                        })
                        .collect(),
                }),
                min_time: *min_time,
                max_time: *max_time,
            })
            .collect::<Vec<_>>();
        let min_time = ranges.values().map(|(min, _)| *min).min().unwrap_or(0);
        let max_time = ranges.values().map(|(_, max)| *max).max().unwrap_or(0);

        info::InfoResponse {
            label_sets: ranges
                .keys()
                .map(|labels| thanos::ZLabelSet {
                    labels: labels
                        .iter()
                        .map(|(name, value)| Label {
                            name: name.clone(),
                            value: value.clone(),
                        })
                        .collect(),
                })
                .collect(),
            component_type: "store".to_owned(),
            store: Some(info::StoreInfo {
                min_time,
                max_time,
                supports_sharding: false,
                supports_without_replica_labels: false,
                tsdb_infos,
            }),
            rules: None,
            metric_metadata: None,
            targets: None,
            exemplars: None,
            query: None,
            status: None,
        }
    }
}

fn exact_label_filters(matchers: &[Matcher]) -> Result<Vec<String>, serde_json::Error> {
    matchers
        .iter()
        .filter_map(|matcher| match matcher {
            // Prometheus defines `label=""` to also match a missing label. A JSON substring
            // predicate cannot express that without excluding valid series, so retain it for
            // the final matcher evaluation.
            Matcher::Eq(name, value) if !value.is_empty() => Some((name, value)),
            Matcher::Eq(_, _) => None,
            Matcher::Neq(_, _) | Matcher::Re(_, _) | Matcher::Nre(_, _) => None,
        })
        .map(|(name, value)| {
            // `labels` is a BTreeMap, serialized without whitespace, so this fragment is both
            // exact and independent of the position of the label in the JSON object.
            Ok(format!(
                "%{}:{}%",
                serde_json::to_string(name)?,
                serde_json::to_string(value)?
            ))
        })
        .collect()
}

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[tonic::async_trait]
impl Store for ThanosStoreService {
    type SeriesStream = SeriesStream;

    async fn series(
        &self,
        request: Request<thanos::SeriesRequest>,
    ) -> Result<Response<Self::SeriesStream>, Status> {
        let request = request.into_inner();
        reject_unsupported_series_options(&request)?;
        let matchers = compile_matchers(&request.matchers)?;
        let aggregates = aggregates(&request.aggregates)?;
        let limit = limit(request.limit)?;
        let abort_on_error = request.partial_response_disabled
            || request.partial_response_strategy == thanos::PartialResponseStrategy::Abort as i32;

        let mut series_responses = Vec::new();
        let mut warning_responses = Vec::new();
        for descriptor in self
            .matching_series(request.min_time, request.max_time, &matchers)
            .await?
        {
            if series_responses.len() >= limit {
                break;
            }
            let selected_resolution = if aggregates.contains(&(Aggr::Raw as i32)) {
                descriptor.downsample_resolution == 0
            } else {
                descriptor.downsample_resolution <= request.max_resolution_window
            };
            if !selected_resolution {
                continue;
            }

            let labels = descriptor
                .labels
                .iter()
                .map(|(name, value)| Label {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect();
            let mut chunks = Vec::new();
            let mut warnings = Vec::new();
            if !request.skip_chunks {
                for chunk in &descriptor.chunks {
                    if !overlaps(
                        chunk.chunk_mint,
                        chunk.chunk_maxt,
                        request.min_time,
                        request.max_time,
                    ) {
                        continue;
                    }
                    match self
                        .encode_chunk(&descriptor.repository_uri, chunk, &aggregates)
                        .await
                    {
                        Ok(Some(chunk)) => chunks.push(chunk),
                        Ok(None) => {}
                        Err(error) if abort_on_error => return Err(error),
                        Err(error) => warnings.push(SeriesResponse {
                            result: Some(thanos::series_response::Result::Warning(
                                error.message().to_owned(),
                            )),
                        }),
                    }
                }
            }
            if request.skip_chunks || !chunks.is_empty() {
                series_responses.push(SeriesResponse {
                    result: Some(thanos::series_response::Result::Series(Series {
                        labels,
                        chunks,
                    })),
                });
            }
            warning_responses.extend(warnings);
        }

        let batch_size = usize::try_from(request.response_batch_size)
            .ok()
            .filter(|size| *size > 1)
            .unwrap_or(1);
        let mut responses = if batch_size == 1 {
            series_responses
        } else {
            series_responses
                .chunks(batch_size)
                .map(|chunk| {
                    let series = chunk
                        .iter()
                        .filter_map(|response| match &response.result {
                            Some(thanos::series_response::Result::Series(series)) => {
                                Some(series.clone())
                            }
                            _ => None,
                        })
                        .collect();
                    SeriesResponse {
                        result: Some(thanos::series_response::Result::Batch(SeriesBatch {
                            series,
                        })),
                    }
                })
                .collect()
        };
        responses.extend(warning_responses);
        Ok(Response::new(Box::pin(iter(responses.into_iter().map(Ok)))))
    }

    async fn label_names(
        &self,
        request: Request<thanos::LabelNamesRequest>,
    ) -> Result<Response<thanos::LabelNamesResponse>, Status> {
        let request = request.into_inner();
        reject_unsupported_label_options(&request.without_replica_labels, request.hints.is_some())?;
        let matchers = compile_matchers(&request.matchers)?;
        let names = self
            .matching_series(request.start, request.end, &matchers)
            .await?
            .into_iter()
            .flat_map(|descriptor| descriptor.labels.into_keys())
            .collect::<BTreeSet<_>>();
        Ok(Response::new(thanos::LabelNamesResponse {
            names: take_limit(names, request.limit)?,
            warnings: vec![],
            hints: None,
        }))
    }

    async fn label_values(
        &self,
        request: Request<thanos::LabelValuesRequest>,
    ) -> Result<Response<thanos::LabelValuesResponse>, Status> {
        let request = request.into_inner();
        if request.label.is_empty() {
            return Err(Status::invalid_argument("label must not be empty"));
        }
        reject_unsupported_label_options(&request.without_replica_labels, request.hints.is_some())?;
        let matchers = compile_matchers(&request.matchers)?;
        let values = self
            .matching_series(request.start, request.end, &matchers)
            .await?
            .into_iter()
            .filter_map(|descriptor| descriptor.labels.get(&request.label).cloned())
            .collect::<BTreeSet<_>>();
        Ok(Response::new(thanos::LabelValuesResponse {
            values: take_limit(values, request.limit)?,
            warnings: vec![],
            hints: None,
        }))
    }
}

#[tonic::async_trait]
impl info::info_server::Info for ThanosStoreService {
    async fn info(
        &self,
        _request: Request<info::InfoRequest>,
    ) -> Result<Response<info::InfoResponse>, Status> {
        Ok(Response::new(self.info_response()))
    }
}

impl ThanosStoreService {
    async fn encode_chunk(
        &self,
        repository_uri: &str,
        descriptor: &ChunkLocation,
        aggregates: &BTreeSet<i32>,
    ) -> Result<Option<AggrChunk>, Status> {
        let repository = self
            .storage
            .get(repository_uri)
            .ok_or_else(|| {
                Status::internal(format!(
                    "no storage repository for repository {:?}",
                    repository_uri
                ))
            })?;
        let chunk = chunk_reader::read_encoded_chunk(
            repository.as_ref(),
            &descriptor.chunk_file_path,
            descriptor.chunk_file_offset,
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?;
        let mut result = AggrChunk {
            min_time: descriptor.chunk_mint,
            max_time: descriptor.chunk_maxt,
            raw: None,
            count: None,
            sum: None,
            min: None,
            max: None,
            counter: None,
        };
        match chunk {
            EncodedChunk::Xor(data) => {
                result.raw = Some(raw_chunk(data, thanos::chunk::Encoding::Xor));
            }
            EncodedChunk::Histogram(data) => {
                result.raw = Some(raw_chunk(data, thanos::chunk::Encoding::Histogram));
            }
            EncodedChunk::FloatHistogram(data) => {
                result.raw = Some(raw_chunk(data, thanos::chunk::Encoding::FloatHistogram));
            }
            EncodedChunk::Aggregate {
                count,
                sum,
                min,
                max,
                counter,
            } => {
                result.count = aggregate_chunk(count, aggregates, Aggr::Count);
                result.sum = aggregate_chunk(sum, aggregates, Aggr::Sum);
                result.min = aggregate_chunk(min, aggregates, Aggr::Min);
                result.max = aggregate_chunk(max, aggregates, Aggr::Max);
                result.counter = aggregate_chunk(counter, aggregates, Aggr::Counter);
            }
        }
        if result.raw.is_none()
            && result.count.is_none()
            && result.sum.is_none()
            && result.min.is_none()
            && result.max.is_none()
            && result.counter.is_none()
        {
            Ok(None)
        } else {
            Ok(Some(result))
        }
    }
}

fn raw_chunk(data: Vec<u8>, encoding: thanos::chunk::Encoding) -> Chunk {
    let hash = xxhash_rust::xxh64::xxh64(&data, 0);
    Chunk {
        r#type: encoding as i32,
        data,
        hash,
    }
}

fn aggregate_chunk(
    data: Option<Vec<u8>>,
    aggregates: &BTreeSet<i32>,
    aggregate: Aggr,
) -> Option<Chunk> {
    aggregates
        .contains(&(aggregate as i32))
        .then_some(data)
        .flatten()
        .map(|data| raw_chunk(data, thanos::chunk::Encoding::Xor))
}

fn reject_unsupported_series_options(request: &thanos::SeriesRequest) -> Result<(), Status> {
    if request.hints.is_some()
        || request.shard_info.is_some()
        || !request.without_replica_labels.is_empty()
    {
        return Err(Status::unimplemented(
            "opaque hints, sharding, and replica-label removal are not supported",
        ));
    }
    Ok(())
}

fn reject_unsupported_label_options(
    without_replica_labels: &[String],
    has_hints: bool,
) -> Result<(), Status> {
    if has_hints || !without_replica_labels.is_empty() {
        return Err(Status::unimplemented(
            "opaque hints and replica-label removal are not supported",
        ));
    }
    Ok(())
}

fn compile_matchers(matchers: &[LabelMatcher]) -> Result<Vec<Matcher>, Status> {
    matchers
        .iter()
        .map(|matcher| {
            if matcher.name.is_empty() {
                return Err(Status::invalid_argument(
                    "label matcher name must not be empty",
                ));
            }
            match thanos::label_matcher::Type::try_from(matcher.r#type)
                .map_err(|_| Status::invalid_argument("unknown label matcher type"))?
            {
                thanos::label_matcher::Type::Eq => {
                    Ok(Matcher::Eq(matcher.name.clone(), matcher.value.clone()))
                }
                thanos::label_matcher::Type::Neq => {
                    Ok(Matcher::Neq(matcher.name.clone(), matcher.value.clone()))
                }
                thanos::label_matcher::Type::Re | thanos::label_matcher::Type::Nre => {
                    let regex = Regex::new(&format!("^(?:{})$", matcher.value))
                        .map_err(|error| Status::invalid_argument(error.to_string()))?;
                    if thanos::label_matcher::Type::try_from(matcher.r#type).unwrap()
                        == thanos::label_matcher::Type::Re
                    {
                        Ok(Matcher::Re(matcher.name.clone(), regex))
                    } else {
                        Ok(Matcher::Nre(matcher.name.clone(), regex))
                    }
                }
            }
        })
        .collect()
}

fn matches_all(labels: &BTreeMap<String, String>, matchers: &[Matcher]) -> bool {
    matchers.iter().all(|matcher| match matcher {
        Matcher::Eq(name, value) => labels.get(name).map(String::as_str).unwrap_or("") == value,
        Matcher::Neq(name, value) => labels.get(name).map(String::as_str).unwrap_or("") != value,
        Matcher::Re(name, regex) => {
            regex.is_match(labels.get(name).map(String::as_str).unwrap_or(""))
        }
        Matcher::Nre(name, regex) => {
            !regex.is_match(labels.get(name).map(String::as_str).unwrap_or(""))
        }
    })
}

fn overlaps(min: i64, max: i64, start: i64, end: i64) -> bool {
    min <= end && max >= start
}

fn labels_key(labels: &BTreeMap<String, String>) -> Vec<(String, String)> {
    labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn aggregates(values: &[i32]) -> Result<BTreeSet<i32>, Status> {
    let values = if values.is_empty() {
        vec![Aggr::Raw as i32]
    } else {
        values.to_vec()
    };
    values
        .into_iter()
        .map(|value| {
            Aggr::try_from(value)
                .map(|_| value)
                .map_err(|_| Status::invalid_argument("unknown aggregate type"))
        })
        .collect()
}

fn limit(limit: i64) -> Result<usize, Status> {
    if limit < 0 {
        return Err(Status::invalid_argument("limit must not be negative"));
    }
    Ok(if limit == 0 {
        usize::MAX
    } else {
        usize::try_from(limit).unwrap_or(usize::MAX)
    })
}

fn take_limit(values: BTreeSet<String>, requested_limit: i64) -> Result<Vec<String>, Status> {
    let limit = limit(requested_limit)?;
    Ok(values.into_iter().take(limit).collect())
}

async fn load_blocks(context: &SessionContext) -> Result<Vec<BlockMetadata>, BoxError> {
    let batches = context
        .sql("SELECT min_time, max_time, external_labels FROM blocks")
        .await?
        .collect()
        .await?;
    batches.iter().try_fold(Vec::new(), |mut blocks, batch| {
        blocks.extend(blocks_from_batch(batch)?);
        Ok(blocks)
    })
}

fn series_from_batch(batch: &RecordBatch) -> Result<Vec<SeriesDescriptor>, BoxError> {
    let repository_uri = string_column(batch, "repository_uri")?;
    let downsample_resolution = int64_column(batch, "downsample_resolution")?;
    let series_mint = int64_column(batch, "series_mint")?;
    let series_maxt = int64_column(batch, "series_maxt")?;
    let labels_json = string_column(batch, "labels_json")?;
    let chunks_json = string_column(batch, "chunks_json")?;
    (0..batch.num_rows())
        .map(|index| {
            Ok(SeriesDescriptor {
                repository_uri: string_value(repository_uri.as_ref(), index)?,
                downsample_resolution: downsample_resolution.value(index),
                series_mint: series_mint.value(index),
                series_maxt: series_maxt.value(index),
                labels: serde_json::from_str(&string_value(labels_json.as_ref(), index)?)?,
                chunks: serde_json::from_str(&string_value(chunks_json.as_ref(), index)?)?,
            })
        })
        .collect()
}

fn blocks_from_batch(batch: &RecordBatch) -> Result<Vec<BlockMetadata>, BoxError> {
    let min_time = int64_column(batch, "min_time")?;
    let max_time = int64_column(batch, "max_time")?;
    let external_labels = string_column(batch, "external_labels")?;
    (0..batch.num_rows())
        .map(|index| {
            Ok(BlockMetadata {
                min_time: min_time.value(index),
                max_time: max_time.value(index),
                external_labels: serde_json::from_str(&string_value(
                    external_labels.as_ref(),
                    index,
                )?)?,
            })
        })
        .collect()
}

fn string_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef, BoxError> {
    batch
        .column_by_name(name)
        .cloned()
        .ok_or_else(|| format!("missing column {name:?}").into())
}

fn string_value(array: &dyn Array, index: usize) -> Result<String, BoxError> {
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(index).to_owned());
    }
    if let Some(array) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(array.value(index).to_owned());
    }
    Err("expected a UTF-8 column".into())
}

fn int64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array, BoxError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| format!("missing column {name:?}"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| format!("column {name:?} has an unexpected type").into())
}

fn uint64_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array, BoxError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| format!("missing column {name:?}"))?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| format!("column {name:?} has an unexpected type").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_matchers_produce_canonical_json_filters() {
        let filters = exact_label_filters(&[
            Matcher::Eq("__name__".to_owned(), "process_cpu_seconds_total".to_owned()),
            Matcher::Eq("prometheus".to_owned(), "monitoring/reader".to_owned()),
            Matcher::Eq("missing".to_owned(), String::new()),
            Matcher::Re("job".to_owned(), Regex::new("reader").unwrap()),
        ])
        .unwrap();

        assert_eq!(
            filters,
            vec![
                r#"%"__name__":"process_cpu_seconds_total"%"#.to_owned(),
                r#"%"prometheus":"monitoring/reader"%"#.to_owned(),
            ]
        );
    }
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use crate::thanos_proto::thanos::{
        info::{info_client::InfoClient, info_server::InfoServer},
        store_client::StoreClient,
        store_server::StoreServer,
    };

    #[test]
    fn matcher_semantics_follow_prometheus_missing_label_rules() {
        let labels = BTreeMap::from([
            ("__name__".to_owned(), "up".to_owned()),
            ("job".to_owned(), "api".to_owned()),
        ]);
        let matchers = compile_matchers(&[
            LabelMatcher {
                r#type: thanos::label_matcher::Type::Eq as i32,
                name: "missing".to_owned(),
                value: "".to_owned(),
            },
            LabelMatcher {
                r#type: thanos::label_matcher::Type::Nre as i32,
                name: "job".to_owned(),
                value: "worker".to_owned(),
            },
        ])
        .unwrap();
        assert!(matches_all(&labels, &matchers));
    }

    #[test]
    fn regex_matchers_are_anchored() {
        let labels = BTreeMap::from([("job".to_owned(), "api-server".to_owned())]);
        let matcher = compile_matchers(&[LabelMatcher {
            r#type: thanos::label_matcher::Type::Re as i32,
            name: "job".to_owned(),
            value: "api".to_owned(),
        }])
        .unwrap();
        assert!(!matches_all(&labels, &matcher));
    }

    #[test]
    fn raw_queries_choose_raw_resolution() {
        let series = vec![
            SeriesDescriptor {
                repository_uri: "file:///blocks".to_owned(),
                downsample_resolution: 0,
                series_mint: 0,
                series_maxt: 10,
                labels: BTreeMap::new(),
                chunks: vec![],
            },
            SeriesDescriptor {
                repository_uri: "file:///blocks".to_owned(),
                downsample_resolution: 300_000,
                series_mint: 0,
                series_maxt: 10,
                labels: BTreeMap::new(),
                chunks: vec![],
            },
        ];
        let raw = aggregates(&[Aggr::Raw as i32]).unwrap();
        assert!(
            series
                .iter()
                .filter(|descriptor| {
                    raw.contains(&(Aggr::Raw as i32)) && descriptor.downsample_resolution == 0
                })
                .all(|descriptor| descriptor.downsample_resolution == 0)
        );
    }

    #[tokio::test]
    async fn generated_clients_discover_and_query_the_store_service() {
        let service = ThanosStoreService {
            context: Arc::new(SessionContext::new()),
            descriptors: Some(Arc::new(vec![SeriesDescriptor {
                repository_uri: "file:///blocks".to_owned(),
                downsample_resolution: 0,
                series_mint: 100,
                series_maxt: 200,
                labels: BTreeMap::from([
                    ("__name__".to_owned(), "up".to_owned()),
                    ("cluster".to_owned(), "test".to_owned()),
                ]),
                chunks: vec![],
            }])),
            blocks: Arc::new(vec![BlockMetadata {
                min_time: 100,
                max_time: 200,
                external_labels: BTreeMap::from([("cluster".to_owned(), "test".to_owned())]),
            }]),
            storage: RepositoryRegistry::empty(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(StoreServer::new(service.clone()))
                .add_service(InfoServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let endpoint = format!("http://{address}");
        let mut info_client = InfoClient::connect(endpoint.clone()).await.unwrap();
        let info = info_client
            .info(info::InfoRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(info.component_type, "store");
        assert_eq!(info.store.unwrap().min_time, 100);

        let mut store_client = StoreClient::connect(endpoint).await.unwrap();
        let mut stream = store_client
            .series(thanos::SeriesRequest {
                min_time: 100,
                max_time: 200,
                matchers: vec![LabelMatcher {
                    r#type: thanos::label_matcher::Type::Eq as i32,
                    name: "cluster".to_owned(),
                    value: "test".to_owned(),
                }],
                skip_chunks: true,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        let response = stream.message().await.unwrap().unwrap();
        let thanos::series_response::Result::Series(series) = response.result.unwrap() else {
            panic!("StoreAPI did not return a series response");
        };
        assert_eq!(series.labels[0].name, "__name__");
        assert!(series.chunks.is_empty());

        server.abort();
    }
}
