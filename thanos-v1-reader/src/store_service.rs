use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    pin::Pin,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, Int64Array, ListArray, StringArray, StringViewArray, UInt64Array},
    record_batch::RecordBatch,
};
use datafusion::prelude::SessionContext;
use futures::Stream;
use opendal::Operator;
use prost::Message;
use regex::Regex;
use tokio_stream::iter;
use tonic::{Request, Response, Status};

use crate::{
    block_index::repository_operator,
    chunk_reader::{self, EncodedAggregateChunk, EncodedAggregateEncoding, EncodedChunk},
    config::ThanosRepositoryConfig,
    thanos_proto::hintspb,
    thanos_proto::thanos::{
        self, Aggr, AggrChunk, Chunk, Label, LabelMatcher, Series, SeriesBatch, SeriesResponse,
        info, store_server::Store,
    },
};

type BoxError = Box<dyn Error>;
type SeriesStream = Pin<Box<dyn Stream<Item = Result<SeriesResponse, Status>> + Send>>;
const SERIES_RESPONSE_HINTS_TYPE_URL: &str = "type.googleapis.com/hintspb.SeriesResponseHints";
const BLOCK_ID_LABEL: &str = "__block_id";

#[derive(Clone)]
pub struct ThanosStoreService {
    descriptors: Arc<Vec<ChunkDescriptor>>,
    blocks: Arc<Vec<BlockMetadata>>,
    operators: Arc<BTreeMap<String, Operator>>,
}

#[derive(Clone)]
struct ChunkDescriptor {
    repository_uri: String,
    block_ulid: String,
    chunk_file_path: String,
    chunk_file_offset: u64,
    chunk_mint: i64,
    chunk_maxt: i64,
    labels: BTreeMap<String, String>,
}

#[derive(Clone)]
struct BlockMetadata {
    repository_uri: String,
    block_ulid: String,
    min_time: i64,
    max_time: i64,
    downsample_resolution: i64,
    external_labels: BTreeMap<String, String>,
    compaction_sources: Vec<String>,
}

impl ChunkDescriptor {
    fn block_key(&self) -> (String, String) {
        (self.repository_uri.clone(), self.block_ulid.clone())
    }
}

impl BlockMetadata {
    fn key(&self) -> (String, String) {
        (self.repository_uri.clone(), self.block_ulid.clone())
    }
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
        repositories: &[ThanosRepositoryConfig],
    ) -> Result<Self, BoxError> {
        let mut descriptors = load_descriptors(&context).await?;
        let mut blocks = load_blocks(&context).await?;
        let duplicate_blocks = duplicate_block_keys(&blocks);
        blocks.retain(|block| !duplicate_blocks.contains(&block.key()));
        descriptors.retain(|descriptor| !duplicate_blocks.contains(&descriptor.block_key()));
        let operators = repositories
            .iter()
            .map(|repository| {
                repository_operator(&repository.uri)
                    .map(|operator| (repository.uri.clone(), operator))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        Ok(Self {
            descriptors: Arc::new(descriptors),
            blocks: Arc::new(blocks),
            operators: Arc::new(operators),
        })
    }

    fn matching_descriptors<'a>(
        &'a self,
        start: i64,
        end: i64,
        matchers: &[Matcher],
    ) -> Vec<&'a ChunkDescriptor> {
        self.descriptors
            .iter()
            .filter(|descriptor| {
                overlaps(descriptor.chunk_mint, descriptor.chunk_maxt, start, end)
                    && matches_all(&descriptor.labels, matchers)
            })
            .collect()
    }

    fn selected_blocks(
        &self,
        start: i64,
        end: i64,
        max_resolution: i64,
        series_matchers: &[Matcher],
        block_matchers: &[Matcher],
    ) -> Vec<&BlockMetadata> {
        let mut block_sets = BTreeMap::<Vec<(String, String)>, Vec<&BlockMetadata>>::new();
        for block in self.blocks.iter() {
            block_sets
                .entry(labels_key(&block.external_labels))
                .or_default()
                .push(block);
        }
        block_sets
            .into_values()
            .filter(|blocks| block_set_matches_series(blocks[0], series_matchers))
            .flat_map(|blocks| select_blocks(&blocks, start, end, max_resolution, block_matchers))
            .collect()
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
                supports_sharding: true,
                supports_without_replica_labels: true,
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

#[tonic::async_trait]
impl Store for ThanosStoreService {
    type SeriesStream = SeriesStream;

    async fn series(
        &self,
        request: Request<thanos::SeriesRequest>,
    ) -> Result<Response<Self::SeriesStream>, Status> {
        let request = request.into_inner();
        let matchers = compile_matchers(&request.matchers)?;
        let request_hints = decode_series_request_hints(request.hints.as_ref())?;
        let block_matchers = compile_matchers(&request_hints.block_matchers)?;
        let aggregates = aggregates(&request.aggregates)?;
        let limit = limit(request.limit)?;
        let abort_on_error = request.partial_response_disabled
            || request.partial_response_strategy == thanos::PartialResponseStrategy::Abort as i32;

        let selected_blocks = self.selected_blocks(
            request.min_time,
            request.max_time,
            request.max_resolution_window,
            &matchers,
            &block_matchers,
        );
        let selected_block_keys = selected_blocks
            .iter()
            .map(|block| block.key())
            .collect::<BTreeSet<_>>();
        let selected_block_count = selected_blocks.len();
        let mut groups =
            BTreeMap::<(String, String, Vec<(String, String)>), Vec<&ChunkDescriptor>>::new();
        for descriptor in self.matching_descriptors(request.min_time, request.max_time, &matchers) {
            if !selected_block_keys.contains(&descriptor.block_key()) {
                continue;
            }
            groups
                .entry((
                    descriptor.repository_uri.clone(),
                    descriptor.block_ulid.clone(),
                    labels_key(&descriptor.labels),
                ))
                .or_default()
                .push(descriptor);
        }

        let labels_to_remove = request
            .without_replica_labels
            .iter()
            .collect::<BTreeSet<_>>();
        let mut block_counts = BTreeMap::<(String, String), usize>::new();
        let mut visible_groups =
            BTreeMap::<(Vec<(String, String)>, String, String), Vec<&ChunkDescriptor>>::new();
        for ((repository_uri, block_ulid, original_labels), descriptors) in groups {
            let visible_labels = original_labels
                .into_iter()
                .filter(|(name, _)| !labels_to_remove.contains(name))
                .collect::<Vec<_>>();
            if !matches_shard(&visible_labels, request.shard_info.as_ref()) {
                continue;
            }
            let count = block_counts
                .entry((repository_uri.clone(), block_ulid.clone()))
                .or_default();
            if *count >= limit {
                continue;
            }
            *count += 1;
            visible_groups
                .entry((visible_labels, repository_uri, block_ulid))
                .or_default()
                .extend(descriptors);
        }

        let mut merged_groups = BTreeMap::<Vec<(String, String)>, Vec<Vec<AggrChunk>>>::new();
        let mut warning_responses = Vec::new();
        for ((visible_labels, _, _), mut descriptors) in visible_groups {
            descriptors.sort_by_key(|descriptor| (descriptor.chunk_mint, descriptor.chunk_maxt));
            let mut chunks = Vec::new();
            let mut warnings = Vec::new();
            if !request.skip_chunks {
                for descriptor in descriptors {
                    match self.encode_chunk(descriptor, &aggregates).await {
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
                chunks.sort_by(|left, right| match compare_aggr_chunks(left, right) {
                    value if value > 0 => std::cmp::Ordering::Less,
                    value if value < 0 => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                });
                chunks.dedup_by(|left, right| compare_aggr_chunks(left, right) == 0);
            }
            if request.skip_chunks || !chunks.is_empty() {
                merged_groups
                    .entry(visible_labels)
                    .or_default()
                    .push(chunks);
            }
            warning_responses.extend(warnings);
        }

        let series_responses = merged_groups
            .into_iter()
            .map(|(labels, chunk_sets)| {
                let chunks = chunk_sets.into_iter().fold(Vec::new(), merge_chunks);
                SeriesResponse {
                    result: Some(thanos::series_response::Result::Series(Series {
                        labels: labels
                            .into_iter()
                            .map(|(name, value)| Label { name, value })
                            .collect(),
                        chunks,
                    })),
                }
            })
            .collect::<Vec<_>>();
        let merged_series_count = series_responses.len();
        let merged_chunks_count = series_responses
            .iter()
            .filter_map(|response| match &response.result {
                Some(thanos::series_response::Result::Series(series)) => Some(series.chunks.len()),
                _ => None,
            })
            .sum::<usize>();

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
        let response_hints = hintspb::SeriesResponseHints {
            queried_blocks: selected_blocks
                .into_iter()
                .map(|block| hintspb::Block {
                    id: block.block_ulid.clone(),
                })
                .collect(),
            query_stats: request_hints
                .enable_query_stats
                .then(|| hintspb::QueryStats {
                    blocks_queried: i64::try_from(selected_block_count).unwrap_or(i64::MAX),
                    merged_series_count: i64::try_from(merged_series_count).unwrap_or(i64::MAX),
                    merged_chunks_count: i64::try_from(merged_chunks_count).unwrap_or(i64::MAX),
                    ..Default::default()
                }),
        };
        responses.push(SeriesResponse {
            result: Some(thanos::series_response::Result::Hints(prost_types::Any {
                type_url: SERIES_RESPONSE_HINTS_TYPE_URL.to_owned(),
                value: response_hints.encode_to_vec(),
            })),
        });
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
            .matching_descriptors(request.start, request.end, &matchers)
            .into_iter()
            .flat_map(|descriptor| descriptor.labels.keys().cloned())
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
            .matching_descriptors(request.start, request.end, &matchers)
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
        descriptor: &ChunkDescriptor,
        aggregates: &BTreeSet<i32>,
    ) -> Result<Option<AggrChunk>, Status> {
        let operator = self
            .operators
            .get(&descriptor.repository_uri)
            .ok_or_else(|| {
                Status::internal(format!(
                    "no object-store operator for repository {:?}",
                    descriptor.repository_uri
                ))
            })?;
        let chunk = chunk_reader::read_encoded_chunk(
            operator,
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
                result.count = aggregate_chunk(count, aggregates, Aggr::Count)?;
                result.sum = aggregate_chunk(sum, aggregates, Aggr::Sum)?;
                result.min = aggregate_chunk(min, aggregates, Aggr::Min)?;
                result.max = aggregate_chunk(max, aggregates, Aggr::Max)?;
                result.counter = aggregate_chunk(counter, aggregates, Aggr::Counter)?;
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
    chunk: Option<EncodedAggregateChunk>,
    aggregates: &BTreeSet<i32>,
    aggregate: Aggr,
) -> Result<Option<Chunk>, Status> {
    if !aggregates.contains(&(aggregate as i32)) {
        return Ok(None);
    }
    let chunk = chunk.ok_or_else(|| {
        Status::internal(format!(
            "aggregate {} does not exist",
            aggregate.as_str_name().to_ascii_lowercase()
        ))
    })?;
    let encoding = match chunk.encoding {
        EncodedAggregateEncoding::Xor => thanos::chunk::Encoding::Xor,
        EncodedAggregateEncoding::Histogram => thanos::chunk::Encoding::Histogram,
        EncodedAggregateEncoding::FloatHistogram => thanos::chunk::Encoding::FloatHistogram,
    };
    Ok(Some(raw_chunk(chunk.data, encoding)))
}

fn decode_series_request_hints(
    hints: Option<&prost_types::Any>,
) -> Result<hintspb::SeriesRequestHints, Status> {
    let Some(hints) = hints else {
        return Ok(hintspb::SeriesRequestHints::default());
    };
    let Some((_, message_name)) = hints.type_url.rsplit_once('/') else {
        return Err(Status::invalid_argument(format!(
            "unmarshal series request hints: message type url {:?} is invalid",
            hints.type_url
        )));
    };
    if message_name != "hintspb.SeriesRequestHints" {
        return Err(Status::invalid_argument(format!(
            "unmarshal series request hints: mismatched message type: got {message_name:?} want \"hintspb.SeriesRequestHints\""
        )));
    }
    hintspb::SeriesRequestHints::decode(hints.value.as_slice()).map_err(|error| {
        Status::invalid_argument(format!("unmarshal series request hints: {error}"))
    })
}

fn matches_shard(labels: &[(String, String)], shard_info: Option<&thanos::ShardInfo>) -> bool {
    let Some(shard_info) = shard_info.filter(|shard| shard.total_shards >= 1) else {
        return true;
    };
    let sharding_labels = shard_info.labels.iter().collect::<BTreeSet<_>>();
    let mut bytes = Vec::new();
    for (name, value) in labels {
        let listed = sharding_labels.contains(name);
        if (shard_info.by && listed) || (!shard_info.by && !listed) {
            bytes.extend_from_slice(name.as_bytes());
            bytes.push(0xff);
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(0xff);
        }
    }
    xxhash_rust::xxh64::xxh64(&bytes, 0) % shard_info.total_shards as u64
        == shard_info.shard_index as u64
}

fn merge_chunks(left: Vec<AggrChunk>, right: Vec<AggrChunk>) -> Vec<AggrChunk> {
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let (mut left_index, mut right_index) = (0, 0);
    while left_index < left.len() && right_index < right.len() {
        match compare_aggr_chunks(&left[left_index], &right[right_index]) {
            value if value > 0 => {
                merged.push(left[left_index].clone());
                left_index += 1;
            }
            value if value < 0 => {
                merged.push(right[right_index].clone());
                right_index += 1;
            }
            _ => {
                merged.push(left[left_index].clone());
                left_index += 1;
                right_index += 1;
            }
        }
    }
    merged.extend_from_slice(&left[left_index..]);
    merged.extend_from_slice(&right[right_index..]);
    merged
}

fn compare_aggr_chunks(left: &AggrChunk, right: &AggrChunk) -> i32 {
    if left.min_time != right.min_time {
        return if left.min_time < right.min_time {
            1
        } else {
            -1
        };
    }
    if left.max_time != right.max_time {
        return if left.max_time < right.max_time {
            1
        } else {
            -1
        };
    }
    for (left, right) in [
        (&left.raw, &right.raw),
        (&left.count, &right.count),
        (&left.sum, &right.sum),
        (&left.min, &right.min),
        (&left.max, &right.max),
        (&left.counter, &right.counter),
    ] {
        let comparison = compare_chunks(left.as_ref(), right.as_ref());
        if comparison != 0 {
            return comparison;
        }
    }
    0
}

fn compare_chunks(left: Option<&Chunk>, right: Option<&Chunk>) -> i32 {
    match (left, right) {
        (None, None) => 0,
        (Some(_), None) => 1,
        (None, Some(_)) => -1,
        (Some(left), Some(right)) => {
            if left.r#type != right.r#type {
                return if left.r#type < right.r#type { 1 } else { -1 };
            }
            match left.data.cmp(&right.data) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            }
        }
    }
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

const BLOCK_RESOLUTIONS: [i64; 3] = [60 * 60 * 1000, 5 * 60 * 1000, 0];

fn select_blocks<'a>(
    blocks: &[&'a BlockMetadata],
    mint: i64,
    maxt: i64,
    max_resolution: i64,
    block_matchers: &[Matcher],
) -> Vec<&'a BlockMetadata> {
    let Some(resolution_index) = BLOCK_RESOLUTIONS
        .iter()
        .position(|resolution| *resolution <= max_resolution)
    else {
        return Vec::new();
    };
    let by_resolution = BLOCK_RESOLUTIONS
        .iter()
        .map(|resolution| {
            let mut matching = blocks
                .iter()
                .copied()
                .filter(|block| block.downsample_resolution == *resolution)
                .collect::<Vec<_>>();
            matching.sort_by_key(|block| (block.min_time, block.max_time));
            matching
        })
        .collect::<Vec<_>>();
    select_resolution(&by_resolution, resolution_index, mint, maxt, block_matchers)
}

fn select_resolution<'a>(
    blocks: &[Vec<&'a BlockMetadata>],
    resolution_index: usize,
    mint: i64,
    maxt: i64,
    block_matchers: &[Matcher],
) -> Vec<&'a BlockMetadata> {
    if mint > maxt {
        return Vec::new();
    }
    let mut selected = Vec::new();
    let mut start = mint;
    for &block in &blocks[resolution_index] {
        if block.max_time <= mint {
            continue;
        }
        if block.min_time > maxt {
            break;
        }
        if resolution_index + 1 < blocks.len() {
            selected.extend(select_resolution(
                blocks,
                resolution_index + 1,
                start,
                block.min_time.wrapping_sub(1),
                block_matchers,
            ));
        }
        if block_matches_hints(block, block_matchers) {
            selected.push(block);
        }
        start = block.max_time;
    }
    if resolution_index + 1 < blocks.len() {
        selected.extend(select_resolution(
            blocks,
            resolution_index + 1,
            start,
            maxt,
            block_matchers,
        ));
    }
    selected
}

fn block_set_matches_series(block: &BlockMetadata, matchers: &[Matcher]) -> bool {
    matchers.iter().all(|matcher| {
        let name = matcher_name(matcher);
        match block.external_labels.get(name) {
            Some(value) if !value.is_empty() => matcher_matches(matcher, Some(value)),
            _ => true,
        }
    })
}

fn block_matches_hints(block: &BlockMetadata, matchers: &[Matcher]) -> bool {
    matchers.iter().all(|matcher| {
        let value = if matcher_name(matcher) == BLOCK_ID_LABEL {
            Some(block.block_ulid.as_str())
        } else {
            block
                .external_labels
                .get(matcher_name(matcher))
                .map(String::as_str)
        };
        matcher_matches(matcher, value)
    })
}

fn matcher_name(matcher: &Matcher) -> &str {
    match matcher {
        Matcher::Eq(name, _)
        | Matcher::Neq(name, _)
        | Matcher::Re(name, _)
        | Matcher::Nre(name, _) => name,
    }
}

fn matcher_matches(matcher: &Matcher, value: Option<&str>) -> bool {
    let value = value.unwrap_or("");
    match matcher {
        Matcher::Eq(_, expected) => value == expected,
        Matcher::Neq(_, expected) => value != expected,
        Matcher::Re(_, regex) => regex.is_match(value),
        Matcher::Nre(_, regex) => !regex.is_match(value),
    }
}

fn duplicate_block_keys(blocks: &[BlockMetadata]) -> BTreeSet<(String, String)> {
    let mut groups = BTreeMap::<(String, i64, Vec<(String, String)>), Vec<&BlockMetadata>>::new();
    for block in blocks {
        groups
            .entry((
                block.repository_uri.clone(),
                block.downsample_resolution,
                labels_key(&block.external_labels),
            ))
            .or_default()
            .push(block);
    }
    let mut duplicates = BTreeSet::new();
    for mut group in groups.into_values() {
        group.sort_by(|left, right| {
            right
                .compaction_sources
                .len()
                .cmp(&left.compaction_sources.len())
                .then_with(|| left.block_ulid.cmp(&right.block_ulid))
        });
        let mut covering = Vec::<&BlockMetadata>::new();
        'blocks: for block in group {
            for parent in &covering {
                if block
                    .compaction_sources
                    .iter()
                    .all(|source| parent.compaction_sources.contains(source))
                {
                    duplicates.insert(block.key());
                    continue 'blocks;
                }
            }
            covering.push(block);
        }
    }
    duplicates
}

async fn load_descriptors(context: &SessionContext) -> Result<Vec<ChunkDescriptor>, BoxError> {
    let batches = context
        .sql(
            "SELECT repository_uri, block_ulid, chunk_file_path, chunk_file_offset, chunk_mint, \
             chunk_maxt, labels_json FROM chunks",
        )
        .await?
        .collect()
        .await?;
    batches
        .iter()
        .try_fold(Vec::new(), |mut descriptors, batch| {
            descriptors.extend(descriptors_from_batch(batch)?);
            Ok(descriptors)
        })
}

async fn load_blocks(context: &SessionContext) -> Result<Vec<BlockMetadata>, BoxError> {
    let batches = context
        .sql(
            "SELECT repository_uri, block_ulid, min_time, max_time, downsample_resolution, \
             external_labels, compaction_sources FROM blocks",
        )
        .await?
        .collect()
        .await?;
    batches.iter().try_fold(Vec::new(), |mut blocks, batch| {
        blocks.extend(blocks_from_batch(batch)?);
        Ok(blocks)
    })
}

fn descriptors_from_batch(batch: &RecordBatch) -> Result<Vec<ChunkDescriptor>, BoxError> {
    let repository_uri = string_column(batch, "repository_uri")?;
    let block_ulid = string_column(batch, "block_ulid")?;
    let chunk_file_path = string_column(batch, "chunk_file_path")?;
    let chunk_file_offset = uint64_column(batch, "chunk_file_offset")?;
    let chunk_mint = int64_column(batch, "chunk_mint")?;
    let chunk_maxt = int64_column(batch, "chunk_maxt")?;
    let labels_json = string_column(batch, "labels_json")?;
    (0..batch.num_rows())
        .map(|index| {
            Ok(ChunkDescriptor {
                repository_uri: string_value(repository_uri.as_ref(), index)?,
                block_ulid: string_value(block_ulid.as_ref(), index)?,
                chunk_file_path: string_value(chunk_file_path.as_ref(), index)?,
                chunk_file_offset: chunk_file_offset.value(index),
                chunk_mint: chunk_mint.value(index),
                chunk_maxt: chunk_maxt.value(index),
                labels: serde_json::from_str(&string_value(labels_json.as_ref(), index)?)?,
            })
        })
        .collect()
}

fn blocks_from_batch(batch: &RecordBatch) -> Result<Vec<BlockMetadata>, BoxError> {
    let repository_uri = string_column(batch, "repository_uri")?;
    let block_ulid = string_column(batch, "block_ulid")?;
    let min_time = int64_column(batch, "min_time")?;
    let max_time = int64_column(batch, "max_time")?;
    let downsample_resolution = int64_column(batch, "downsample_resolution")?;
    let external_labels = string_column(batch, "external_labels")?;
    let compaction_sources = batch
        .column_by_name("compaction_sources")
        .ok_or_else(|| std::io::Error::other("missing block index column compaction_sources"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| std::io::Error::other("invalid block index column compaction_sources"))?;
    (0..batch.num_rows())
        .map(|index| {
            Ok(BlockMetadata {
                repository_uri: string_value(repository_uri.as_ref(), index)?,
                block_ulid: string_value(block_ulid.as_ref(), index)?,
                min_time: min_time.value(index),
                max_time: max_time.value(index),
                downsample_resolution: downsample_resolution.value(index),
                external_labels: serde_json::from_str(&string_value(
                    external_labels.as_ref(),
                    index,
                )?)?,
                compaction_sources: string_list_value(compaction_sources, index)?,
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

fn string_list_value(array: &ListArray, index: usize) -> Result<Vec<String>, BoxError> {
    let values = array.value(index);
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| std::io::Error::other("invalid UTF-8 list column"))?;
    Ok((0..values.len())
        .map(|index| values.value(index).to_owned())
        .collect())
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
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    fn block(id: &str, min_time: i64, max_time: i64, resolution: i64) -> BlockMetadata {
        BlockMetadata {
            repository_uri: "file:///blocks".to_owned(),
            block_ulid: id.to_owned(),
            min_time,
            max_time,
            downsample_resolution: resolution,
            external_labels: BTreeMap::new(),
            compaction_sources: vec![id.to_owned()],
        }
    }

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
    fn shard_matching_uses_go_label_hashing_semantics() {
        let labels = labels_key(&BTreeMap::from([
            ("pod".to_owned(), "nginx".to_owned()),
            ("node".to_owned(), "node-1".to_owned()),
            ("container".to_owned(), "nginx".to_owned()),
        ]));
        assert!(matches_shard(&labels, None));
        assert!(matches_shard(
            &labels,
            Some(&thanos::ShardInfo {
                shard_index: 0,
                total_shards: 0,
                by: true,
                labels: vec![],
            })
        ));
        assert!(!matches_shard(
            &labels,
            Some(&thanos::ShardInfo {
                shard_index: 0,
                total_shards: 2,
                by: true,
                labels: vec!["pod".to_owned(), "node".to_owned()],
            })
        ));
        assert!(matches_shard(
            &labels,
            Some(&thanos::ShardInfo {
                shard_index: 1,
                total_shards: 2,
                by: true,
                labels: vec!["node".to_owned(), "pod".to_owned()],
            })
        ));
        assert!(matches_shard(
            &labels,
            Some(&thanos::ShardInfo {
                shard_index: 0,
                total_shards: 2,
                by: false,
                labels: vec!["node".to_owned()],
            })
        ));
        assert!(!matches_shard(
            &labels,
            Some(&thanos::ShardInfo {
                shard_index: 2,
                total_shards: 2,
                by: false,
                labels: vec![],
            })
        ));
    }

    #[test]
    fn block_selection_matches_bucket_store_resolution_and_overlap_order() {
        let blocks = [
            block("raw-0", 0, 100, 0),
            block("raw-1a", 100, 200, 0),
            block("raw-1b", 100, 200, 0),
            block("raw-2-short", 200, 299, 0),
            block("raw-2", 200, 300, 0),
            block("raw-3", 300, 400, 0),
            block("raw-long", 300, 600, 0),
            block("raw-4", 400, 500, 0),
            block("5m-0", 0, 100, 300_000),
            block("5m-1", 100, 200, 300_000),
            block("5m-2", 200, 300, 300_000),
            block("5m-3", 300, 400, 300_000),
            block("1h-1", 100, 200, 3_600_000),
            block("1h-2", 200, 300, 3_600_000),
        ];
        let references = blocks.iter().collect::<Vec<_>>();
        let selected = select_blocks(&references, 0, 500, 3_600_000, &[])
            .into_iter()
            .map(|block| block.block_ulid.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            selected,
            vec!["5m-0", "1h-1", "1h-2", "5m-3", "raw-long", "raw-4"]
        );
    }

    #[test]
    fn compaction_source_supersets_filter_only_same_group_duplicates() {
        let mut source = block("source", 0, 100, 0);
        source.compaction_sources = vec!["a".to_owned()];
        let mut compacted = block("compacted", 0, 100, 0);
        compacted.compaction_sources = vec!["a".to_owned(), "b".to_owned()];
        let mut downsampled = block("downsampled", 0, 100, 300_000);
        downsampled.compaction_sources = vec!["a".to_owned(), "b".to_owned()];
        assert_eq!(
            duplicate_block_keys(&[source, compacted, downsampled]),
            BTreeSet::from([("file:///blocks".to_owned(), "source".to_owned())])
        );
    }

    #[tokio::test]
    async fn generated_clients_discover_and_query_the_store_service() {
        let service = ThanosStoreService {
            descriptors: Arc::new(vec![ChunkDescriptor {
                repository_uri: "file:///blocks".to_owned(),
                block_ulid: "block".to_owned(),
                chunk_file_path: "unused-when-skipping-chunks".to_owned(),
                chunk_file_offset: 0,
                chunk_mint: 100,
                chunk_maxt: 200,
                labels: BTreeMap::from([
                    ("__name__".to_owned(), "up".to_owned()),
                    ("cluster".to_owned(), "test".to_owned()),
                ]),
            }]),
            blocks: Arc::new(vec![BlockMetadata {
                repository_uri: "file:///blocks".to_owned(),
                block_ulid: "block".to_owned(),
                min_time: 100,
                max_time: 200,
                downsample_resolution: 0,
                external_labels: BTreeMap::from([("cluster".to_owned(), "test".to_owned())]),
                compaction_sources: vec!["block".to_owned()],
            }]),
            operators: Arc::new(BTreeMap::new()),
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
        let store_info = info.store.unwrap();
        assert_eq!(store_info.min_time, 100);
        assert!(store_info.supports_sharding);
        assert!(store_info.supports_without_replica_labels);

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
