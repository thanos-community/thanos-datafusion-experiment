//! Convert a single Thanos TSDB block into a sorted Parquet sample file.
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
};

use arrow59::{
    array::{ArrayRef, FixedSizeBinaryArray, Float64Array, StringArray, TimestampMillisecondArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use parquet_variant_compute::shred_variant;
use parquet59::{
    arrow::ArrowWriter,
    basic::{Compression, Encoding, ZstdLevel},
    file::{
        metadata::KeyValue,
        properties::{EnabledStatistics, WriterProperties},
    },
    schema::types::ColumnPath,
    variant::json_to_variant,
};
use sha2::{Digest, Sha256};
use thanos_v1_reader::{
    chunk_reader,
    config::{StorageConfig, ThanosRepositoryConfig},
    storage::{RangeReader, repository_operator},
    tsdb_index,
};

struct Reader(opendal::Operator);
struct Hll([u8; 64]);
impl Hll {
    fn add(&mut self, value: &str) {
        let hash = Sha256::digest(value.as_bytes());
        let bits = u64::from_be_bytes(hash[..8].try_into().unwrap());
        let bucket = (bits >> 58) as usize;
        self.0[bucket] = self.0[bucket].max((bits << 6).leading_zeros() as u8 + 1);
    }
    fn hex(&self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
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
                hlls.entry(name.clone()).or_insert(Hll([0; 64])).add(value);
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
        let json = serde_json::to_string(
            &s.labels
                .iter()
                .filter(|(k, _)| k.as_str() != "__name__")
                .collect::<BTreeMap<_, _>>(),
        )?;
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
                Ok(samples) => {
                    for sample in samples {
                        rows.push((
                            name.clone(),
                            s.labels.clone(),
                            json.clone(),
                            hash,
                            sample.timestamp,
                            sample.value,
                        ));
                    }
                }
                // Native histograms and downsampled non-counter aggregate chunks do not have a
                // single float sample representation. They are intentionally omitted here.
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    rows.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| {
                labels
                    .iter()
                    .map(|label| a.1.get(label))
                    .cmp(labels.iter().map(|label| b.1.get(label)))
            })
            .then_with(|| a.4.cmp(&b.4))
    });
    let jsons: ArrayRef = Arc::new(StringArray::from(
        rows.iter().map(|r| Some(r.2.as_str())).collect::<Vec<_>>(),
    ));
    let unshredded = json_to_variant(&jsons)?;
    let label_schema = DataType::Struct(
        labels
            .iter()
            .map(|label| Arc::new(Field::new(label, DataType::Utf8, true)))
            .collect(),
    );
    let variant: ArrayRef = ArrayRef::from(shred_variant(&unshredded, &label_schema)?);
    let variant_field = parquet59::variant::VariantArray::try_new(&variant)?.field("labels");
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("labels_hash", DataType::FixedSizeBinary(16), false),
        variant_field,
        Field::new(
            "timestamp",
            DataType::Timestamp(arrow59::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]));
    let hashes = FixedSizeBinaryArray::try_from_iter(rows.iter().map(|r| r.3.as_slice()))?;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(hashes),
            variant,
            Arc::new(TimestampMillisecondArray::from(
                rows.iter().map(|r| r.4).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.5).collect::<Vec<_>>(),
            )),
        ],
    )?;
    let hll_metadata = hlls
        .iter()
        .map(|(name, hll)| format!("{name}:{}", hll.hex()))
        .collect::<Vec<_>>()
        .join(",");
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_column_encoding(ColumnPath::from("labels_hash"), Encoding::DELTA_BYTE_ARRAY)
        .set_column_encoding(ColumnPath::from("timestamp"), Encoding::DELTA_BINARY_PACKED)
        .set_column_encoding(ColumnPath::from("value"), Encoding::BYTE_STREAM_SPLIT)
        .set_key_value_metadata(Some(vec![KeyValue::new(
            "thanos.labels_hll.v1".into(),
            hll_metadata,
        )]))
        .build();
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    let (out_root, out_name) = endpoint(&args[2])?;
    let out = repository_operator(
        &ThanosRepositoryConfig {
            name: "output".into(),
            uri: out_root,
            s3: None,
            gcs: None,
        },
        &StorageConfig::default(),
    )?;
    out.write(&out_name, bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_registers_are_stable_and_non_empty() {
        let mut hll = Hll([0; 64]);
        hll.add("api-1");
        let encoded = hll.hex();
        assert_eq!(encoded.len(), 128);
        assert_ne!(encoded, "00".repeat(64));
    }

    #[test]
    fn endpoint_preserves_object_store_root() {
        assert_eq!(
            endpoint("s3://bucket/prefix/block.parquet").unwrap(),
            ("s3://bucket/prefix".to_owned(), "block.parquet".to_owned())
        );
    }
}
