use std::io;

const HISTOGRAM_HEADER_SIZE: usize = 3;
const COUNTER_RESET_MASK: u8 = 0b1100_0000;
const GAUGE_TYPE: u8 = 0b1100_0000;
const CUSTOM_BUCKET_SCHEMA: i32 = -53;
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Span {
    pub offset: i32,
    pub length: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum HistogramCount {
    Integer(u64),
    Float(f64),
}

#[derive(Debug, Clone, PartialEq)]
pub enum HistogramBuckets {
    Integer(Vec<i64>),
    Float(Vec<f64>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSample {
    pub timestamp: i64,
    pub schema: i32,
    pub count: HistogramCount,
    pub sum: f64,
    pub zero_threshold: f64,
    pub zero_count: HistogramCount,
    pub reset_hint: u8,
    pub positive_spans: Vec<Span>,
    pub negative_spans: Vec<Span>,
    pub positive_buckets: HistogramBuckets,
    pub negative_buckets: HistogramBuckets,
    pub custom_values: Vec<f64>,
}

pub fn decode_histogram(payload: &[u8]) -> Result<Vec<HistogramSample>, io::Error> {
    decode_integer_histograms(payload)
}

pub fn decode_float_histogram(payload: &[u8]) -> Result<Vec<HistogramSample>, io::Error> {
    decode_float_histograms(payload)
}

fn decode_integer_histograms(payload: &[u8]) -> Result<Vec<HistogramSample>, io::Error> {
    let (sample_count, header, mut reader) = chunk_header(payload)?;
    if sample_count == 0 {
        return Ok(Vec::new());
    }
    let layout = read_layout(&mut reader)?;
    let positive_bucket_count = bucket_count(&layout.positive_spans)?;
    let negative_bucket_count = bucket_count(&layout.negative_spans)?;
    let mut timestamp = reader.read_varbit_int()?;
    let mut count = reader.read_varbit_uint()?;
    let mut zero_count = reader.read_varbit_uint()?;
    let mut sum = f64::from_bits(reader.read_bits(64)?);
    let mut positive_buckets = read_integer_buckets(&mut reader, positive_bucket_count)?;
    let mut negative_buckets = read_integer_buckets(&mut reader, negative_bucket_count)?;
    let mut timestamp_delta = 0_i64;
    let mut count_delta = 0_i64;
    let mut zero_count_delta = 0_i64;
    let mut positive_deltas = vec![0_i64; positive_bucket_count];
    let mut negative_deltas = vec![0_i64; negative_bucket_count];
    let mut sum_leading = 0_u8;
    let mut sum_trailing = 0_u8;
    let mut result = Vec::with_capacity(sample_count);

    result.push(integer_sample(
        timestamp,
        count,
        zero_count,
        sum,
        header,
        1,
        &layout,
        &positive_buckets,
        &negative_buckets,
    ));
    for sample_index in 2..=sample_count {
        timestamp_delta = timestamp_delta.wrapping_add(reader.read_varbit_int()?);
        timestamp = timestamp.wrapping_add(timestamp_delta);
        count_delta = count_delta.wrapping_add(reader.read_varbit_int()?);
        count = (count as i64).wrapping_add(count_delta) as u64;
        zero_count_delta = zero_count_delta.wrapping_add(reader.read_varbit_int()?);
        zero_count = (zero_count as i64).wrapping_add(zero_count_delta) as u64;
        read_xor(&mut reader, &mut sum, &mut sum_leading, &mut sum_trailing)?;
        if sum.to_bits() != STALE_NAN_BITS {
            read_integer_deltas(&mut reader, &mut positive_buckets, &mut positive_deltas)?;
            read_integer_deltas(&mut reader, &mut negative_buckets, &mut negative_deltas)?;
        }
        result.push(integer_sample(
            timestamp,
            count,
            zero_count,
            sum,
            header,
            sample_index,
            &layout,
            &positive_buckets,
            &negative_buckets,
        ));
    }
    Ok(result)
}

fn decode_float_histograms(payload: &[u8]) -> Result<Vec<HistogramSample>, io::Error> {
    let (sample_count, header, mut reader) = chunk_header(payload)?;
    if sample_count == 0 {
        return Ok(Vec::new());
    }
    let layout = read_layout(&mut reader)?;
    let positive_bucket_count = bucket_count(&layout.positive_spans)?;
    let negative_bucket_count = bucket_count(&layout.negative_spans)?;
    let mut timestamp = reader.read_varbit_int()?;
    let mut count = XorValue::new(f64::from_bits(reader.read_bits(64)?));
    let mut zero_count = XorValue::new(f64::from_bits(reader.read_bits(64)?));
    let mut sum = XorValue::new(f64::from_bits(reader.read_bits(64)?));
    let mut positive_buckets = (0..positive_bucket_count)
        .map(|_| {
            reader
                .read_bits(64)
                .map(|bits| XorValue::new(f64::from_bits(bits)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut negative_buckets = (0..negative_bucket_count)
        .map(|_| {
            reader
                .read_bits(64)
                .map(|bits| XorValue::new(f64::from_bits(bits)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut timestamp_delta = 0_i64;
    let mut result = Vec::with_capacity(sample_count);

    result.push(float_sample(
        timestamp,
        &count,
        &zero_count,
        &sum,
        header,
        1,
        &layout,
        &positive_buckets,
        &negative_buckets,
    ));
    for sample_index in 2..=sample_count {
        timestamp_delta = timestamp_delta.wrapping_add(reader.read_varbit_int()?);
        timestamp = timestamp.wrapping_add(timestamp_delta);
        count.read(&mut reader)?;
        zero_count.read(&mut reader)?;
        sum.read(&mut reader)?;
        if sum.value.to_bits() != STALE_NAN_BITS {
            for bucket in &mut positive_buckets {
                bucket.read(&mut reader)?;
            }
            for bucket in &mut negative_buckets {
                bucket.read(&mut reader)?;
            }
        }
        result.push(float_sample(
            timestamp,
            &count,
            &zero_count,
            &sum,
            header,
            sample_index,
            &layout,
            &positive_buckets,
            &negative_buckets,
        ));
    }
    Ok(result)
}

fn chunk_header(payload: &[u8]) -> Result<(usize, u8, BitReader<'_>), io::Error> {
    if payload.len() < HISTOGRAM_HEADER_SIZE {
        return Err(invalid_data("truncated histogram chunk header"));
    }
    let count = usize::from(u16::from_be_bytes([payload[0], payload[1]]));
    Ok((
        count,
        payload[2] & COUNTER_RESET_MASK,
        BitReader::new(&payload[HISTOGRAM_HEADER_SIZE..]),
    ))
}

#[derive(Debug)]
struct Layout {
    schema: i32,
    zero_threshold: f64,
    positive_spans: Vec<Span>,
    negative_spans: Vec<Span>,
    custom_values: Vec<f64>,
}

fn read_layout(reader: &mut BitReader<'_>) -> Result<Layout, io::Error> {
    let zero_threshold = match reader.read_byte()? {
        0 => 0.0,
        255 => f64::from_bits(reader.read_bits(64)?),
        encoded => 0.5_f64 * 2.0_f64.powi(i32::from(encoded) - 243),
    };
    let schema = i32::try_from(reader.read_varbit_int()?)
        .map_err(|_| invalid_data("histogram schema overflows i32"))?;
    if schema != CUSTOM_BUCKET_SCHEMA && !(-4..=8).contains(&schema) {
        return Err(invalid_data(format!("unknown histogram schema {schema}")));
    }
    let positive_spans = read_spans(reader)?;
    let negative_spans = read_spans(reader)?;
    let custom_values = if schema == CUSTOM_BUCKET_SCHEMA {
        let count = usize::try_from(reader.read_varbit_uint()?)
            .map_err(|_| invalid_data("custom bound count overflows usize"))?;
        (0..count)
            .map(|_| {
                let encoded = reader.read_varbit_uint()?;
                if encoded == 0 {
                    Ok(f64::from_bits(reader.read_bits(64)?))
                } else {
                    Ok((encoded - 1) as f64 / 1000.0)
                }
            })
            .collect::<Result<Vec<_>, io::Error>>()?
    } else {
        Vec::new()
    };
    Ok(Layout {
        schema,
        zero_threshold,
        positive_spans,
        negative_spans,
        custom_values,
    })
}

fn read_spans(reader: &mut BitReader<'_>) -> Result<Vec<Span>, io::Error> {
    let count = usize::try_from(reader.read_varbit_uint()?)
        .map_err(|_| invalid_data("histogram span count overflows usize"))?;
    (0..count)
        .map(|_| {
            Ok(Span {
                length: u32::try_from(reader.read_varbit_uint()?)
                    .map_err(|_| invalid_data("histogram span length overflows u32"))?,
                offset: i32::try_from(reader.read_varbit_int()?)
                    .map_err(|_| invalid_data("histogram span offset overflows i32"))?,
            })
        })
        .collect()
}

fn bucket_count(spans: &[Span]) -> Result<usize, io::Error> {
    spans.iter().try_fold(0_usize, |count, span| {
        count
            .checked_add(span.length as usize)
            .ok_or_else(|| invalid_data("histogram bucket count overflows usize"))
    })
}

fn read_integer_buckets(reader: &mut BitReader<'_>, count: usize) -> Result<Vec<i64>, io::Error> {
    (0..count).map(|_| reader.read_varbit_int()).collect()
}

fn read_integer_deltas(
    reader: &mut BitReader<'_>,
    buckets: &mut [i64],
    deltas: &mut [i64],
) -> Result<(), io::Error> {
    for (bucket, delta) in buckets.iter_mut().zip(deltas) {
        *delta = delta.wrapping_add(reader.read_varbit_int()?);
        *bucket = bucket.wrapping_add(*delta);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn integer_sample(
    timestamp: i64,
    count: u64,
    zero_count: u64,
    sum: f64,
    header: u8,
    sample_index: usize,
    layout: &Layout,
    positive_buckets: &[i64],
    negative_buckets: &[i64],
) -> HistogramSample {
    if sum.to_bits() == STALE_NAN_BITS {
        return stale_sample(timestamp, sum, false);
    }
    HistogramSample {
        timestamp,
        schema: layout.schema,
        count: HistogramCount::Integer(count),
        sum,
        zero_threshold: layout.zero_threshold,
        zero_count: HistogramCount::Integer(zero_count),
        reset_hint: reset_hint(header, sample_index),
        positive_spans: layout.positive_spans.clone(),
        negative_spans: layout.negative_spans.clone(),
        positive_buckets: HistogramBuckets::Integer(positive_buckets.to_vec()),
        negative_buckets: HistogramBuckets::Integer(negative_buckets.to_vec()),
        custom_values: layout.custom_values.clone(),
    }
}

#[allow(clippy::too_many_arguments)]
fn float_sample(
    timestamp: i64,
    count: &XorValue,
    zero_count: &XorValue,
    sum: &XorValue,
    header: u8,
    sample_index: usize,
    layout: &Layout,
    positive_buckets: &[XorValue],
    negative_buckets: &[XorValue],
) -> HistogramSample {
    if sum.value.to_bits() == STALE_NAN_BITS {
        return stale_sample(timestamp, sum.value, true);
    }
    HistogramSample {
        timestamp,
        schema: layout.schema,
        count: HistogramCount::Float(count.value),
        sum: sum.value,
        zero_threshold: layout.zero_threshold,
        zero_count: HistogramCount::Float(zero_count.value),
        reset_hint: reset_hint(header, sample_index),
        positive_spans: layout.positive_spans.clone(),
        negative_spans: layout.negative_spans.clone(),
        positive_buckets: HistogramBuckets::Float(
            positive_buckets.iter().map(|bucket| bucket.value).collect(),
        ),
        negative_buckets: HistogramBuckets::Float(
            negative_buckets.iter().map(|bucket| bucket.value).collect(),
        ),
        custom_values: layout.custom_values.clone(),
    }
}

fn stale_sample(timestamp: i64, sum: f64, float: bool) -> HistogramSample {
    let count = if float {
        HistogramCount::Float(0.0)
    } else {
        HistogramCount::Integer(0)
    };
    let buckets = if float {
        HistogramBuckets::Float(Vec::new())
    } else {
        HistogramBuckets::Integer(Vec::new())
    };
    HistogramSample {
        timestamp,
        schema: 0,
        count: count.clone(),
        sum,
        zero_threshold: 0.0,
        zero_count: count,
        reset_hint: 0,
        positive_spans: Vec::new(),
        negative_spans: Vec::new(),
        positive_buckets: buckets.clone(),
        negative_buckets: buckets,
        custom_values: Vec::new(),
    }
}

fn reset_hint(header: u8, sample_index: usize) -> u8 {
    if header == GAUGE_TYPE {
        3
    } else if sample_index > 1 {
        2
    } else {
        0
    }
}

#[derive(Debug)]
struct XorValue {
    value: f64,
    leading: u8,
    trailing: u8,
}

impl XorValue {
    fn new(value: f64) -> Self {
        Self {
            value,
            leading: 0,
            trailing: 0,
        }
    }

    fn read(&mut self, reader: &mut BitReader<'_>) -> Result<(), io::Error> {
        read_xor(
            reader,
            &mut self.value,
            &mut self.leading,
            &mut self.trailing,
        )
    }
}

fn read_xor(
    reader: &mut BitReader<'_>,
    value: &mut f64,
    leading: &mut u8,
    trailing: &mut u8,
) -> Result<(), io::Error> {
    if !reader.read_bit()? {
        return Ok(());
    }
    let (new_leading, new_trailing, significant_bits) = if !reader.read_bit()? {
        (*leading, *trailing, 64 - *leading - *trailing)
    } else {
        let new_leading = reader.read_bits(5)? as u8;
        let mut significant_bits = reader.read_bits(6)? as u8;
        if significant_bits == 0 {
            significant_bits = 64;
        }
        let new_trailing = 64 - new_leading - significant_bits;
        *leading = new_leading;
        *trailing = new_trailing;
        (new_leading, new_trailing, significant_bits)
    };
    let _ = new_leading;
    let delta = reader.read_bits(significant_bits)?;
    *value = f64::from_bits(value.to_bits() ^ (delta << new_trailing));
    Ok(())
}

struct BitReader<'a> {
    bytes: &'a [u8],
    bit_offset: usize,
}

impl<'a> BitReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            bit_offset: 0,
        }
    }

    fn read_bit(&mut self) -> Result<bool, io::Error> {
        Ok(self.read_bits(1)? != 0)
    }

    fn read_byte(&mut self) -> Result<u8, io::Error> {
        Ok(self.read_bits(8)? as u8)
    }

    fn read_bits(&mut self, count: u8) -> Result<u64, io::Error> {
        if count > 64
            || self
                .bit_offset
                .checked_add(usize::from(count))
                .is_none_or(|end| end > self.bytes.len() * 8)
        {
            return Err(invalid_data("truncated histogram bit stream"));
        }
        let mut value = 0_u64;
        for _ in 0..count {
            let byte = self.bytes[self.bit_offset / 8];
            let bit = (byte >> (7 - self.bit_offset % 8)) & 1;
            value = (value << 1) | u64::from(bit);
            self.bit_offset += 1;
        }
        Ok(value)
    }

    fn read_varbit_int(&mut self) -> Result<i64, io::Error> {
        let (prefix, size) = self.read_varbit_prefix()?;
        if prefix == 0xff {
            return Ok(self.read_bits(64)? as i64);
        }
        if size == 0 {
            return Ok(0);
        }
        let bits = self.read_bits(size)?;
        let midpoint = 1_u64 << (size - 1);
        Ok(if bits > midpoint {
            bits.wrapping_sub(1_u64 << size) as i64
        } else {
            bits as i64
        })
    }

    fn read_varbit_uint(&mut self) -> Result<u64, io::Error> {
        let (prefix, size) = self.read_varbit_prefix()?;
        if prefix == 0xff {
            return self.read_bits(64);
        }
        if size == 0 {
            return Ok(0);
        }
        self.read_bits(size)
    }

    fn read_varbit_prefix(&mut self) -> Result<(u8, u8), io::Error> {
        let mut prefix = 0_u8;
        for _ in 0..8 {
            prefix <<= 1;
            if !self.read_bit()? {
                break;
            }
            prefix |= 1;
        }
        let size = match prefix {
            0 => 0,
            0b10 => 3,
            0b110 => 6,
            0b1110 => 9,
            0b1_1110 => 12,
            0b11_1110 => 18,
            0b111_1110 => 25,
            0b1111_1110 => 56,
            0xff => 64,
            _ => {
                return Err(invalid_data(format!(
                    "invalid histogram varbit prefix {prefix:b}"
                )));
            }
        };
        Ok((prefix, size))
    }
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_truncated_histogram_header_and_payload() {
        assert_eq!(
            decode_histogram(&[0, 1]).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            decode_float_histogram(&[0, 1, 0]).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
