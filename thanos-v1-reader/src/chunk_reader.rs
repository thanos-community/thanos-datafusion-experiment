use std::io;

use crc::{CRC_32_ISCSI, Crc};
use rusty_chunkenc::xor::{XORChunk, read_xor_chunk_data};

use crate::histogram::{HistogramSample, decode_float_histogram, decode_histogram};
use crate::storage::RangeReader;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const ENCODING_XOR: u8 = 1;
const ENCODING_HISTOGRAM: u8 = 2;
const ENCODING_FLOAT_HISTOGRAM: u8 = 3;
const ENCODING_AGGR: u8 = 0xff;
const CRC_SIZE: usize = 4;
const MAX_UVARINT_SIZE: usize = 5;
const AGGR_COUNTER_SLOT: usize = 4;

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub timestamp: i64,
    pub value: f64,
    pub histogram: Option<HistogramSample>,
    pub aggregate: SampleAggregate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateSelection {
    Count,
    Sum,
    Counter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SampleAggregate {
    Raw,
    Count,
    Sum,
    Counter,
}

impl SampleAggregate {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Counter => "counter",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodedChunk {
    Xor(Vec<u8>),
    Histogram(Vec<u8>),
    FloatHistogram(Vec<u8>),
    Aggregate {
        count: Option<EncodedAggregateChunk>,
        sum: Option<EncodedAggregateChunk>,
        min: Option<EncodedAggregateChunk>,
        max: Option<EncodedAggregateChunk>,
        counter: Option<EncodedAggregateChunk>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedAggregateChunk {
    pub encoding: EncodedAggregateEncoding,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedAggregateEncoding {
    Xor,
    Histogram,
    FloatHistogram,
}

pub async fn read_encoded_chunk(
    reader: &dyn RangeReader,
    path: &str,
    offset: u64,
) -> Result<EncodedChunk, io::Error> {
    let record = read_record(reader, path, offset).await?;
    decode_encoded_record(&record)
}

pub async fn read_samples(
    reader: &dyn RangeReader,
    path: &str,
    offset: u64,
    counter_metric: bool,
    aggregate: Option<AggregateSelection>,
) -> Result<Vec<Sample>, io::Error> {
    let record = read_record(reader, path, offset).await?;
    decode_query_record(&record, counter_metric, aggregate)
}

async fn read_record(reader: &dyn RangeReader, path: &str, offset: u64) -> Result<Vec<u8>, io::Error> {
    let prefix = reader
        .read_range(path, offset..offset + (MAX_UVARINT_SIZE as u64) + 1)
        .await?;
    let (payload_len, length_size) = read_uvarint(&prefix)?;
    let record_len = length_size
        .checked_add(1)
        .and_then(|size| size.checked_add(payload_len))
        .and_then(|size| size.checked_add(CRC_SIZE))
        .ok_or_else(|| invalid_data("chunk record length overflows usize"))?;
    reader
        .read_range(
            path,
            offset
                ..offset
                    + u64::try_from(record_len)
                        .map_err(|_| invalid_data("chunk record length overflows u64"))?,
        )
        .await
}

pub fn decode_record(record: &[u8], counter_metric: bool) -> Result<Vec<Sample>, io::Error> {
    decode_query_record(record, counter_metric, None)
}

pub fn decode_query_record(
    record: &[u8],
    counter_metric: bool,
    aggregate: Option<AggregateSelection>,
) -> Result<Vec<Sample>, io::Error> {
    let (encoding, payload) = validated_payload(record)?;
    match encoding {
        ENCODING_XOR => decode_xor(payload),
        ENCODING_HISTOGRAM => histogram_samples(decode_histogram(payload)?, SampleAggregate::Raw),
        ENCODING_FLOAT_HISTOGRAM => {
            histogram_samples(decode_float_histogram(payload)?, SampleAggregate::Raw)
        }
        ENCODING_AGGR => decode_aggregate_samples(payload, counter_metric, aggregate),
        _ => Err(invalid_data(format!(
            "unsupported Prometheus chunk encoding {encoding}"
        ))),
    }
}

pub fn decode_encoded_record(record: &[u8]) -> Result<EncodedChunk, io::Error> {
    let (encoding, payload) = validated_payload(record)?;
    match encoding {
        ENCODING_XOR => Ok(EncodedChunk::Xor(payload.to_vec())),
        ENCODING_HISTOGRAM => Ok(EncodedChunk::Histogram(payload.to_vec())),
        ENCODING_FLOAT_HISTOGRAM => Ok(EncodedChunk::FloatHistogram(payload.to_vec())),
        ENCODING_AGGR => decode_aggregate(payload),
        _ => Err(invalid_data(format!(
            "unsupported Prometheus chunk encoding {encoding}"
        ))),
    }
}

fn validated_payload(record: &[u8]) -> Result<(u8, &[u8]), io::Error> {
    let (payload_len, length_size) = read_uvarint(record)?;
    let content_start = length_size;
    let content_end = content_start
        .checked_add(1)
        .and_then(|size| size.checked_add(payload_len))
        .ok_or_else(|| invalid_data("truncated chunk record"))?;
    let checksum_end = content_end
        .checked_add(CRC_SIZE)
        .ok_or_else(|| invalid_data("truncated chunk checksum"))?;
    if checksum_end != record.len() {
        return Err(invalid_data(
            "chunk record length does not match its uvarint",
        ));
    }

    let expected_checksum = u32::from_be_bytes(
        record[content_end..checksum_end]
            .try_into()
            .expect("checksum slice is exactly four bytes"),
    );
    if CRC32C.checksum(&record[content_start..content_end]) != expected_checksum {
        return Err(invalid_data("chunk CRC32C mismatch"));
    }

    let encoding = record[content_start];
    let payload = &record[content_start + 1..content_end];
    Ok((encoding, payload))
}

fn decode_aggregate(payload: &[u8]) -> Result<EncodedChunk, io::Error> {
    let mut remaining = payload;
    let mut slots = Vec::with_capacity(5);
    for _ in 0..=AGGR_COUNTER_SLOT {
        let (slot_len, length_size) = read_uvarint(remaining)?;
        remaining = remaining
            .get(length_size..)
            .ok_or_else(|| invalid_data("truncated aggregate chunk slot"))?;
        if slot_len == 0 {
            slots.push(None);
            continue;
        }
        let slot_size = slot_len
            .checked_add(1)
            .ok_or_else(|| invalid_data("aggregate chunk slot length overflows usize"))?;
        let slot_data = remaining
            .get(..slot_size)
            .ok_or_else(|| invalid_data("truncated aggregate chunk slot payload"))?;
        remaining = &remaining[slot_size..];
        let encoding = match slot_data[0] {
            ENCODING_XOR => EncodedAggregateEncoding::Xor,
            ENCODING_HISTOGRAM => EncodedAggregateEncoding::Histogram,
            ENCODING_FLOAT_HISTOGRAM => EncodedAggregateEncoding::FloatHistogram,
            encoding => {
                return Err(invalid_data(format!(
                    "unsupported aggregate chunk encoding {encoding}"
                )));
            }
        };
        slots.push(Some(EncodedAggregateChunk {
            encoding,
            data: slot_data[1..].to_vec(),
        }));
    }
    if !remaining.is_empty() {
        return Err(invalid_data("unexpected trailing aggregate chunk bytes"));
    }
    Ok(EncodedChunk::Aggregate {
        count: slots.remove(0),
        sum: slots.remove(0),
        min: slots.remove(0),
        max: slots.remove(0),
        counter: slots.remove(0),
    })
}
fn decode_aggregate_samples(
    payload: &[u8],
    counter_metric: bool,
    aggregate: Option<AggregateSelection>,
) -> Result<Vec<Sample>, io::Error> {
    let selected = aggregate.unwrap_or(AggregateSelection::Counter);
    let encoded = decode_aggregate(payload)?;
    let EncodedChunk::Aggregate {
        count,
        sum,
        counter,
        ..
    } = encoded
    else {
        unreachable!("aggregate decoder returns an aggregate chunk");
    };
    let chunk = match selected {
        AggregateSelection::Count => count,
        AggregateSelection::Sum => sum,
        AggregateSelection::Counter => counter,
    }
    .ok_or_else(|| invalid_data(format!("aggregate {} does not exist", selected.as_str())))?;
    let sample_aggregate = match selected {
        AggregateSelection::Count => SampleAggregate::Count,
        AggregateSelection::Sum => SampleAggregate::Sum,
        AggregateSelection::Counter => SampleAggregate::Counter,
    };
    match chunk.encoding {
        EncodedAggregateEncoding::Xor if aggregate.is_none() && !counter_metric => Err(
            invalid_data("received downsampled aggregate chunk for a non-counter metric"),
        ),
        EncodedAggregateEncoding::Xor => {
            let mut samples = decode_xor(&chunk.data)?;
            for sample in &mut samples {
                sample.aggregate = sample_aggregate;
            }
            Ok(samples)
        }
        EncodedAggregateEncoding::Histogram => {
            histogram_samples(decode_histogram(&chunk.data)?, sample_aggregate)
        }
        EncodedAggregateEncoding::FloatHistogram => {
            histogram_samples(decode_float_histogram(&chunk.data)?, sample_aggregate)
        }
    }
}

impl AggregateSelection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Counter => "counter",
        }
    }
}

fn histogram_samples(
    histograms: Vec<HistogramSample>,
    aggregate: SampleAggregate,
) -> Result<Vec<Sample>, io::Error> {
    Ok(histograms
        .into_iter()
        .map(|histogram| Sample {
            timestamp: histogram.timestamp,
            value: f64::NAN,
            histogram: Some(histogram),
            aggregate,
        })
        .collect())
}

fn decode_xor(payload: &[u8]) -> Result<Vec<Sample>, io::Error> {
    let (_, chunk) = read_xor_chunk_data(payload)
        .map_err(|error| invalid_data(format!("invalid XOR chunk payload: {error:?}")))?;
    samples_from_xor(chunk)
}

fn samples_from_xor(chunk: XORChunk) -> Result<Vec<Sample>, io::Error> {
    Ok(chunk
        .samples()
        .iter()
        .map(|sample| Sample {
            timestamp: sample.timestamp,
            value: sample.value,
            histogram: None,
            aggregate: SampleAggregate::Raw,
        })
        .collect())
}

fn read_uvarint(bytes: &[u8]) -> Result<(usize, usize), io::Error> {
    let mut value = 0usize;
    for (index, byte) in bytes.iter().copied().take(MAX_UVARINT_SIZE).enumerate() {
        value |= usize::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(invalid_data("invalid or oversized chunk uvarint"))
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}


#[cfg(test)]
mod tests {
    use super::*;
    use rusty_chunkenc::{Chunk, XORSample};

    #[test]
    fn decodes_xor_chunk_record() {
        let samples = vec![
            XORSample {
                timestamp: 100,
                value: 10.0,
            },
            XORSample {
                timestamp: 200,
                value: 11.0,
            },
        ];
        let mut record = Vec::new();
        Chunk::new_xor(samples).write(&mut record).unwrap();

        assert_eq!(
            decode_record(&record, true).unwrap(),
            vec![
                Sample {
                    timestamp: 100,
                    value: 10.0,
                    histogram: None,
                    aggregate: SampleAggregate::Raw,
                },
                Sample {
                    timestamp: 200,
                    value: 11.0,
                    histogram: None,
                    aggregate: SampleAggregate::Raw,
                },
            ]
        );
    }

    #[test]
    fn exposes_validated_xor_payload_for_store_api() {
        let mut record = Vec::new();
        Chunk::new_xor(vec![XORSample {
            timestamp: 100,
            value: 10.0,
        }])
        .write(&mut record)
        .unwrap();
        let (payload_len, length_size) = read_uvarint(&record).unwrap();

        assert_eq!(
            decode_encoded_record(&record).unwrap(),
            EncodedChunk::Xor(record[length_size + 1..length_size + 1 + payload_len].to_vec())
        );
    }

    #[test]
    fn exposes_native_histogram_payloads_for_store_api() {
        for (encoding, expected) in [
            (ENCODING_HISTOGRAM, EncodedChunk::Histogram(vec![1, 2, 3])),
            (
                ENCODING_FLOAT_HISTOGRAM,
                EncodedChunk::FloatHistogram(vec![1, 2, 3]),
            ),
        ] {
            let record = framed_record(encoding, &[1, 2, 3]);
            assert_eq!(decode_encoded_record(&record).unwrap(), expected);
        }
    }

    #[test]
    fn decodes_aggregate_counter_slot() {
        let mut xor_record = Vec::new();
        Chunk::new_xor(vec![XORSample {
            timestamp: 300,
            value: 42.0,
        }])
        .write(&mut xor_record)
        .unwrap();
        let (payload_len, length_size) = read_uvarint(&xor_record).unwrap();
        let encoded_xor = &xor_record[length_size..length_size + 1 + payload_len];

        let mut aggregate_payload = Vec::new();
        for _ in 0..AGGR_COUNTER_SLOT {
            aggregate_payload.push(0);
        }
        aggregate_payload.push((encoded_xor.len() - 1) as u8);
        aggregate_payload.extend_from_slice(encoded_xor);
        let aggregate_record = framed_record(ENCODING_AGGR, &aggregate_payload);

        assert_eq!(
            decode_record(&aggregate_record, true).unwrap(),
            vec![Sample {
                timestamp: 300,
                value: 42.0,
                histogram: None,
                aggregate: SampleAggregate::Counter,
            }]
        );
    }

    #[test]
    fn exposes_float_histogram_aggregate_slots() {
        let mut aggregate_payload = vec![0];
        aggregate_payload.extend_from_slice(&[2, ENCODING_FLOAT_HISTOGRAM, 10, 11]);
        aggregate_payload.extend_from_slice(&[0, 0]);
        aggregate_payload.extend_from_slice(&[2, ENCODING_FLOAT_HISTOGRAM, 20, 21]);
        let aggregate_record = framed_record(ENCODING_AGGR, &aggregate_payload);

        assert_eq!(
            decode_encoded_record(&aggregate_record).unwrap(),
            EncodedChunk::Aggregate {
                count: None,
                sum: Some(EncodedAggregateChunk {
                    encoding: EncodedAggregateEncoding::FloatHistogram,
                    data: vec![10, 11],
                }),
                min: None,
                max: None,
                counter: Some(EncodedAggregateChunk {
                    encoding: EncodedAggregateEncoding::FloatHistogram,
                    data: vec![20, 21],
                }),
            }
        );
    }

    #[test]
    fn rejects_invalid_crc() {
        let mut record = framed_record(ENCODING_XOR, &[0, 0]);
        *record.last_mut().unwrap() ^= 1;

        assert!(decode_record(&record, false).is_err());
    }

    fn framed_record(encoding: u8, payload: &[u8]) -> Vec<u8> {
        let mut record = vec![payload.len() as u8, encoding];
        record.extend_from_slice(payload);
        record.extend_from_slice(&CRC32C.checksum(&record[1..]).to_be_bytes());
        record
    }
}
