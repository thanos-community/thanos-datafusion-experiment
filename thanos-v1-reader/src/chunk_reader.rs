use std::io;

use crc::{CRC_32_ISCSI, Crc};
use opendal::Operator;
use rusty_chunkenc::xor::{XORChunk, read_xor_chunk_data};

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const ENCODING_XOR: u8 = 1;
const ENCODING_AGGR: u8 = 0xff;
const CRC_SIZE: usize = 4;
const MAX_UVARINT_SIZE: usize = 5;
const AGGR_COUNTER_SLOT: usize = 4;

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub timestamp: i64,
    pub value: f64,
}
pub async fn read_samples(
    operator: &Operator,
    path: &str,
    offset: u64,
    counter_metric: bool,
) -> Result<Vec<Sample>, io::Error> {
    let prefix = operator
        .read_with(path)
        .range(offset..offset + (MAX_UVARINT_SIZE as u64) + 1)
        .await
        .map_err(io_error)?;
    let (payload_len, length_size) = read_uvarint(prefix.to_bytes().as_ref())?;
    let record_len = length_size
        .checked_add(1)
        .and_then(|size| size.checked_add(payload_len))
        .and_then(|size| size.checked_add(CRC_SIZE))
        .ok_or_else(|| invalid_data("chunk record length overflows usize"))?;
    let record = operator
        .read_with(path)
        .range(
            offset
                ..offset
                    + u64::try_from(record_len)
                        .map_err(|_| invalid_data("chunk record length overflows u64"))?,
        )
        .await
        .map_err(io_error)?;
    decode_record(record.to_bytes().as_ref(), counter_metric)
}

pub fn decode_record(record: &[u8], counter_metric: bool) -> Result<Vec<Sample>, io::Error> {
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
    match encoding {
        ENCODING_XOR => decode_xor(payload),
        ENCODING_AGGR if counter_metric => decode_aggregate_counter(payload),
        ENCODING_AGGR => Err(invalid_data(
            "received downsampled aggregate chunk for a non-counter metric",
        )),
        _ => Err(invalid_data(format!(
            "unsupported Prometheus chunk encoding {encoding}"
        ))),
    }
}
fn decode_aggregate_counter(payload: &[u8]) -> Result<Vec<Sample>, io::Error> {
    let mut remaining = payload;
    for slot in 0..=AGGR_COUNTER_SLOT {
        let (slot_len, length_size) = read_uvarint(remaining)?;
        remaining = remaining
            .get(length_size..)
            .ok_or_else(|| invalid_data("truncated aggregate chunk slot"))?;
        if slot_len == 0 {
            if slot == AGGR_COUNTER_SLOT {
                return Ok(Vec::new());
            }
            continue;
        }
        let slot_size = slot_len
            .checked_add(1)
            .ok_or_else(|| invalid_data("aggregate chunk slot length overflows usize"))?;
        let slot_data = remaining
            .get(..slot_size)
            .ok_or_else(|| invalid_data("truncated aggregate chunk slot payload"))?;
        remaining = &remaining[slot_size..];
        if slot != AGGR_COUNTER_SLOT {
            continue;
        }
        if slot_data[0] != ENCODING_XOR {
            return Err(invalid_data(format!(
                "unsupported aggregate counter chunk encoding {}",
                slot_data[0]
            )));
        }
        return decode_xor(&slot_data[1..]);
    }
    unreachable!("the counter slot is always visited");
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

fn io_error(error: opendal::Error) -> io::Error {
    io::Error::other(error)
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
                    value: 10.0
                },
                Sample {
                    timestamp: 200,
                    value: 11.0
                },
            ]
        );
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
                value: 42.0
            }]
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
