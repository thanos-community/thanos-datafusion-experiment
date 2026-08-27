//! Convert a single Thanos TSDB block into a sorted flattened Vortex sample file.
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
};

use arrow::{
    array::{
        Array as ArrowArray, ArrayRef, BinaryArray, Float64Array, StringArray,
        TimestampMillisecondArray,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use thanos_v1_reader::{
    chunk_reader,
    config::{StorageConfig, ThanosRepositoryConfig},
    storage::{RangeReader, repository_operator},
    tsdb_index,
};
use vortex::{
    VortexSessionDefault,
    array::{
        ArrayRef as VortexArrayRef, IntoArray, VortexSessionExecute,
        arrays::{PrimitiveArray, StructArray, struct_::StructArrayExt},
        dtype::FieldPath,
    },
    arrow::ArrowSessionExt,
    buffer::ByteBufferMut,
    encodings::runend::RunEnd,
    file::{WriteOptionsSessionExt, WriteStrategyBuilder},
    io::session::RuntimeSessionExt,
    layout::{
        LayoutStrategy,
        layouts::{compressed::CompressingStrategy, flat::writer::FlatLayoutStrategy},
    },
    session::VortexSession,
};
use vortex_btrblocks::{
    BtrBlocksCompressorBuilder, SchemeExt,
    schemes::float::{ALPRDScheme, FloatDictScheme, FloatRLEScheme, NullDominatedSparseScheme},
};

const HLL_METADATA_KEY: &str = "thanos.labels_hll.v1";
const HLL_METADATA_MAGIC: &[u8; 4] = b"THLL";
const HLL_REGISTERS: usize = 64;

struct Reader(opendal::Operator);
struct Row {
    name: String,
    labels: BTreeMap<String, String>,
    hash: [u8; 16],
    timestamp: i64,
    value: f64,
}
struct Hll([u8; HLL_REGISTERS]);
impl Hll {
    fn add(&mut self, value: &str) {
        let hash = Sha256::digest(value.as_bytes());
        let bits = u64::from_be_bytes(hash[..8].try_into().unwrap());
        let bucket = (bits >> 58) as usize;
        self.0[bucket] = self.0[bucket].max((bits << 6).leading_zeros() as u8 + 1);
    }
}

#[async_trait]
impl RangeReader for Reader {
    async fn read_range(
        &self,
        path: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, io::Error> {
        self.0
            .read_with(path)
            .range(range)
            .await
            .map(|b| b.to_bytes().to_vec())
            .map_err(|e| io::Error::other(e.to_string()))
    }
}

fn endpoint(uri: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let (root, name) = uri
        .rsplit_once('/')
        .ok_or("output URI must include an object name")?;
    Ok((root.to_owned(), name.to_owned()))
}
fn label_column(name: &str) -> String {
    format!("label.{name}")
}

/// `THLL`, version, count, then sorted (label name, 64 raw HLL registers).
fn encode_hll_metadata(hlls: &BTreeMap<String, Hll>) -> Result<Vec<u8>, io::Error> {
    let count = u32::try_from(hlls.len()).map_err(|_| io::Error::other("too many HLL labels"))?;
    let mut bytes = Vec::with_capacity(9 + hlls.len() * (2 + HLL_REGISTERS));
    bytes.extend_from_slice(HLL_METADATA_MAGIC);
    bytes.push(1);
    bytes.extend_from_slice(&count.to_be_bytes());
    for (name, hll) in hlls {
        let name_len = u16::try_from(name.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "HLL label name is too long")
        })?;
        bytes.extend_from_slice(&name_len.to_be_bytes());
        bytes.extend_from_slice(name.as_bytes());
        bytes.extend_from_slice(&hll.0);
    }
    Ok(bytes)
}
#[cfg(test)]
fn decode_hll_metadata(bytes: &[u8]) -> Result<BTreeMap<String, [u8; HLL_REGISTERS]>, io::Error> {
    if bytes.len() < 9 || &bytes[..4] != HLL_METADATA_MAGIC || bytes[4] != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid HLL metadata header",
        ));
    }
    let count = u32::from_be_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let mut offset = 9;
    let mut hlls = BTreeMap::new();
    for _ in 0..count {
        let len: [u8; 2] = bytes
            .get(offset..offset + 2)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated HLL label length")
            })?
            .try_into()
            .unwrap();
        offset += 2;
        let name_len = u16::from_be_bytes(len) as usize;
        let name = std::str::from_utf8(bytes.get(offset..offset + name_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated HLL label name")
        })?)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid HLL label name"))?
        .to_owned();
        offset += name_len;
        let registers = bytes
            .get(offset..offset + HLL_REGISTERS)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated HLL registers"))?
            .try_into()
            .unwrap();
        offset += HLL_REGISTERS;
        if hlls.insert(name, registers).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate HLL label",
            ));
        }
    }
    if offset != bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing HLL metadata bytes",
        ));
    }
    Ok(hlls)
}

/// Build a run-end array without dictionary-encoding the distinct values.
///
/// `RunEnd::encode` only has a primitive convenience path today.  Label values are UTF-8/Binary,
/// so construct the general form directly: primitive run ends plus the original string/binary
/// value for each run.  This is precisely REE and preserves nullable label columns.
fn run_end_column(
    column: &ArrayRef,
    field: &Field,
    session: &VortexSession,
    ctx: &mut vortex::array::ExecutionCtx,
) -> Result<VortexArrayRef, Box<dyn std::error::Error>> {
    let mut run_end = |run_starts: Vec<usize>, len: usize, values: VortexArrayRef| {
        let ends = run_starts
            .into_iter()
            .skip(1)
            .chain(std::iter::once(len))
            .map(|end| end as u64);
        RunEnd::try_new(PrimitiveArray::from_iter(ends).into_array(), values, ctx)
            .map(IntoArray::into_array)
            .map_err(Into::into)
    };
    if let Some(strings) = column.as_any().downcast_ref::<StringArray>() {
        let mut starts = vec![0];
        let mut values = Vec::new();
        if !strings.is_empty() {
            values.push((!strings.is_null(0)).then(|| strings.value(0)));
            for index in 1..strings.len() {
                let same = strings.is_null(index) == strings.is_null(index - 1)
                    && (strings.is_null(index) || strings.value(index) == strings.value(index - 1));
                if !same {
                    starts.push(index);
                    values.push((!strings.is_null(index)).then(|| strings.value(index)));
                }
            }
        }
        let values = session
            .arrow()
            .from_arrow_array(Arc::new(StringArray::from(values)), field)?;
        return run_end(starts, strings.len(), values);
    }
    if let Some(binary) = column.as_any().downcast_ref::<BinaryArray>() {
        let mut starts = vec![0];
        let mut values = Vec::new();
        if !binary.is_empty() {
            values.push((!binary.is_null(0)).then(|| binary.value(0)));
            for index in 1..binary.len() {
                let same = binary.is_null(index) == binary.is_null(index - 1)
                    && (binary.is_null(index) || binary.value(index) == binary.value(index - 1));
                if !same {
                    starts.push(index);
                    values.push((!binary.is_null(index)).then(|| binary.value(index)));
                }
            }
        }
        let values = session
            .arrow()
            .from_arrow_array(Arc::new(BinaryArray::from(values)), field)?;
        return run_end(starts, binary.len(), values);
    }
    Err(format!(
        "REE is only configured for UTF-8/Binary field {}",
        field.name()
    )
    .into())
}

/// ALP is the only enabled float scheme for scalar samples. Its integer residual child remains
/// adaptively compressed by BtrBlocks; no dictionary encoding is eligible for `value`.
fn value_compressor() -> vortex_btrblocks::BtrBlocksCompressor {
    BtrBlocksCompressorBuilder::default()
        .exclude_schemes([
            ALPRDScheme.id(),
            FloatDictScheme.id(),
            FloatRLEScheme.id(),
            NullDominatedSparseScheme.id(),
        ])
        .build()
}

async fn vortex_bytes(
    batch: RecordBatch,
    hll_metadata: Vec<u8>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let session = VortexSession::default().with_tokio();
    let array = session
        .arrow()
        .from_arrow_record_batch(batch.clone(), batch.schema().as_ref())?;
    let mut ctx = session.create_execution_ctx();
    let root = array.execute::<StructArray>(&mut ctx)?;
    let mut fields = Vec::with_capacity(batch.num_columns());
    for ((field, original), column) in batch
        .schema()
        .fields()
        .iter()
        .zip(root.iter_unmasked_fields())
        .zip(batch.columns())
    {
        // `name`, `labels_hash`, and every shredded label share the sorted series prefix.
        // Store each as REE rather than as a dictionary, including null runs for sparse labels.
        if field.name() == "name"
            || field.name() == "labels_hash"
            || field.name().starts_with("label.")
        {
            fields.push(run_end_column(column, field, &session, &mut ctx)?);
        } else {
            fields.push(original.clone());
        }
    }
    let array = StructArray::try_new(
        root.names().clone(),
        fields,
        batch.num_rows(),
        root.struct_validity(),
    )?
    .into_array();
    let mut strategy = WriteStrategyBuilder::default();
    for field in batch.schema().fields() {
        if field.name() == "name"
            || field.name() == "labels_hash"
            || field.name().starts_with("label.")
        {
            strategy = strategy.with_field_writer(
                FieldPath::from_name(field.name().as_str()),
                Arc::new(FlatLayoutStrategy::default()),
            );
        }
    }
    let value_writer: Arc<dyn LayoutStrategy> = Arc::new(CompressingStrategy::new(
        FlatLayoutStrategy::default(),
        value_compressor(),
    ));
    let strategy = strategy
        .with_field_writer(FieldPath::from_name("value"), value_writer)
        .build();
    let mut output = ByteBufferMut::empty();
    session
        .write_options()
        .with_strategy(strategy)
        .with_metadata_segment(HLL_METADATA_KEY, hll_metadata)
        .write(&mut output, array.to_array_stream())
        .await?;
    Ok(output.as_ref().to_vec())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("usage: convert <block-uri> <output-uri>".into());
    }
    let source = ThanosRepositoryConfig {
        name: "source".into(),
        uri: args[1].clone(),
        s3: None,
        gcs: None,
    };
    let source_op = repository_operator(&source, &StorageConfig::default())?;
    let index = source_op.read("index").await?.to_bytes();
    let external_labels = source_op
        .read("meta.json")
        .await
        .ok()
        .and_then(|meta| serde_json::from_slice::<serde_json::Value>(&meta.to_bytes()).ok())
        .and_then(|meta| meta.pointer("/thanos/labels").cloned())
        .and_then(|labels| serde_json::from_value::<BTreeMap<String, String>>(labels).ok())
        .unwrap_or_default();
    let mut series = Vec::new();
    tsdb_index::parse_each(&index, |mut s| {
        for (name, value) in &external_labels {
            s.labels
                .entry(name.clone())
                .or_insert_with(|| value.clone());
        }
        series.push(s)
    })?;
    let mut label_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut hlls: BTreeMap<String, Hll> = BTreeMap::new();
    for s in &series {
        for (name, value) in &s.labels {
            if name != "__name__" {
                label_values
                    .entry(name.clone())
                    .or_default()
                    .insert(value.clone());
                hlls.entry(name.clone())
                    .or_insert(Hll([0; HLL_REGISTERS]))
                    .add(value);
            }
        }
    }
    let mut labels: Vec<_> = label_values.into_iter().collect();
    labels.sort_by(|(left, left_values), (right, right_values)| {
        right_values
            .len()
            .cmp(&left_values.len())
            .then_with(|| left.cmp(right))
    });
    let labels: Vec<String> = labels.into_iter().map(|(name, _)| name).collect();
    let reader = Reader(source_op);
    let mut rows = Vec::new();
    for s in series {
        let name = s
            .labels
            .get("__name__")
            .ok_or("series missing __name__")?
            .clone();
        let mut hasher = Sha256::new();
        for (k, v) in &s.labels {
            hasher.update((k.len() as u32).to_be_bytes());
            hasher.update(k);
            hasher.update((v.len() as u32).to_be_bytes());
            hasher.update(v);
        }
        let hash: [u8; 16] = hasher.finalize()[..16].try_into()?;
        for c in s.chunks {
            let path = format!("chunks/{:06}", (c.reference >> 32) + 1);
            match chunk_reader::read_samples(
                &reader,
                &path,
                c.reference & u64::from(u32::MAX),
                name.ends_with("_total"),
            )
            .await
            {
                Ok(samples) => rows.extend(samples.into_iter().map(|sample| Row {
                    name: name.clone(),
                    labels: s.labels.clone(),
                    hash,
                    timestamp: sample.timestamp,
                    value: sample.value,
                })),
                // Vortex currently represents the flattened scalar samples table only.
                // `read_samples` validates the chunk before identifying these two
                // Prometheus native-histogram encodings, so corrupted scalar chunks
                // remain conversion errors rather than being silently omitted.
                Err(error)
                    if error.kind() == io::ErrorKind::InvalidData
                        && matches!(
                            error.to_string().as_str(),
                            "unsupported Prometheus chunk encoding 2"
                                | "unsupported Prometheus chunk encoding 3"
                        ) => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    rows.sort_by(|a, b| {
        a.name
            .cmp(&b.name)
            .then_with(|| {
                labels
                    .iter()
                    .map(|label| a.labels.get(label))
                    .cmp(labels.iter().map(|label| b.labels.get(label)))
            })
            .then_with(|| a.timestamp.cmp(&b.timestamp))
    });
    let mut fields = vec![
        Field::new("name", DataType::Utf8, false),
        // Vortex's Arrow bridge currently does not import FixedSizeBinary;
        // all values nevertheless remain canonical 16-byte label hashes.
        Field::new("labels_hash", DataType::Binary, false),
    ];
    fields.extend(
        labels
            .iter()
            .map(|label| Field::new(label_column(label), DataType::Utf8, true)),
    );
    fields.extend([
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]);
    let schema = Arc::new(Schema::new(fields));
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.name.as_str()).collect::<Vec<_>>(),
        )),
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|r| r.hash.as_slice()),
        )),
    ];
    columns.extend(labels.iter().map(|label| {
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.labels.get(label).map(String::as_str))
                .collect::<Vec<_>>(),
        )) as ArrayRef
    }));
    columns.extend([
        Arc::new(TimestampMillisecondArray::from(
            rows.iter().map(|r| r.timestamp).collect::<Vec<_>>(),
        )) as ArrayRef,
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.value).collect::<Vec<_>>(),
        )) as ArrayRef,
    ]);
    let bytes = vortex_bytes(
        RecordBatch::try_new(schema, columns)?,
        encode_hll_metadata(&hlls)?,
    )
    .await?;
    let (out_root, out_name) = endpoint(&args[2])?;
    repository_operator(
        &ThanosRepositoryConfig {
            name: "output".into(),
            uri: out_root,
            s3: None,
            gcs: None,
        },
        &StorageConfig::default(),
    )?
    .write(&out_name, bytes)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex::{
        array::{IntoArray, arrays::PrimitiveArray},
        encodings::{alp::ALP, runend::RunEnd},
        file::OpenOptionsSessionExt,
    };
    #[test]
    fn endpoint_preserves_object_store_root() {
        assert_eq!(
            endpoint("s3://bucket/prefix/block.vortex").unwrap(),
            ("s3://bucket/prefix".to_owned(), "block.vortex".to_owned())
        );
    }
    #[test]
    fn hll_metadata_is_stable_and_round_trips() {
        let mut hll = Hll([0; HLL_REGISTERS]);
        hll.add("api-1");
        let bytes = encode_hll_metadata(&BTreeMap::from([("pod".to_owned(), hll)])).unwrap();
        assert_eq!(
            decode_hll_metadata(&bytes)
                .unwrap()
                .get("pod")
                .unwrap()
                .iter()
                .filter(|value| **value != 0)
                .count(),
            1
        );
    }
    #[test]
    fn identity_columns_use_run_end_encoding_without_dictionary_values() {
        let session = VortexSession::default();
        let field = Field::new("label.pod", DataType::Utf8, true);
        let column: ArrayRef = Arc::new(StringArray::from(vec![
            Some("api-0"),
            Some("api-0"),
            None,
            None,
            Some("api-1"),
        ]));
        let encoded = run_end_column(
            &column,
            &field,
            &session,
            &mut session.create_execution_ctx(),
        )
        .unwrap();
        assert!(encoded.is::<RunEnd>());
    }
    #[test]
    fn values_use_alp_not_a_dictionary_or_rle_scheme() {
        let session = VortexSession::default();
        let values =
            PrimitiveArray::from_iter((0..2048).map(|index| index as f64 * 0.1)).into_array();
        let encoded = value_compressor()
            .compress(&values, &mut session.create_execution_ctx())
            .unwrap();
        assert!(encoded.is::<ALP>());
    }
    #[tokio::test]
    async fn vortex_file_exposes_hll_metadata_segment() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![1.0]))]).unwrap();
        let mut hll = Hll([0; HLL_REGISTERS]);
        hll.add("one");
        let metadata = encode_hll_metadata(&BTreeMap::from([("pod".to_owned(), hll)])).unwrap();
        let bytes = vortex_bytes(batch, metadata.clone()).await.unwrap();
        let file = VortexSession::default()
            .with_tokio()
            .open_options()
            .include_metadata()
            .open_buffer(bytes)
            .unwrap();
        assert_eq!(
            file.metadata_segment(HLL_METADATA_KEY).unwrap().as_ref(),
            metadata.as_slice()
        );
    }
}
