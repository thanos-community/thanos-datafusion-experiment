use std::{collections::BTreeMap, fmt};

use crc::{CRC_32_ISCSI, Crc};

const INDEX_MAGIC: u32 = 0xBAAA_D700;
const MIN_SUPPORTED_INDEX_VERSION: u8 = 1;
const MAX_SUPPORTED_INDEX_VERSION: u8 = 2;
const TOC_SIZE: usize = 48;
const CHECKSUM_SIZE: usize = 4;
const SERIES_ALIGNMENT: usize = 16;
const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Series {
    pub reference: u64,
    pub labels: BTreeMap<String, String>,
    pub chunks: Vec<Chunk>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub mint: i64,
    pub maxt: i64,
    pub reference: u64,
}

#[derive(Debug)]
pub struct IndexError(String);

impl fmt::Display for IndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for IndexError {}

/// Parse a Prometheus TSDB index v1 or v2 file into expanded series and chunk metadata.
pub fn parse(bytes: &[u8]) -> Result<Vec<Series>, IndexError> {
    if bytes.len() < 5 + TOC_SIZE + CHECKSUM_SIZE {
        return Err(error("index is too short"));
    }
    if read_u32(bytes, 0)? != INDEX_MAGIC {
        return Err(error("invalid index magic"));
    }
    if !(MIN_SUPPORTED_INDEX_VERSION..=MAX_SUPPORTED_INDEX_VERSION).contains(&bytes[4]) {
        return Err(error(format!("unsupported index version {}", bytes[4])));
    }

    let toc_start = bytes
        .len()
        .checked_sub(TOC_SIZE + CHECKSUM_SIZE)
        .ok_or_else(|| error("index is too short for TOC"))?;
    verify_crc(bytes, toc_start, TOC_SIZE, "TOC")?;
    let toc = Toc::parse(&bytes[toc_start..toc_start + TOC_SIZE])?;
    let symbols = parse_symbols(bytes, toc.symbols as usize)?;

    let series_start = toc.series as usize;
    let series_end = toc.label_indices_start as usize;
    if series_start > series_end || series_end > toc_start {
        return Err(error("invalid series section bounds"));
    }

    let mut series = Vec::new();
    let mut offset = series_start;
    while offset < series_end {
        if bytes[offset] == 0 {
            offset = align_series(offset)?;
            continue;
        }

        let series_reference = (offset / SERIES_ALIGNMENT) as u64;
        let (length, length_size) = read_uvarint(bytes, offset)?;
        let body_start = offset
            .checked_add(length_size)
            .ok_or_else(|| error("series length offset overflow"))?;
        let body_end = body_start
            .checked_add(length as usize)
            .ok_or_else(|| error("series entry length overflow"))?;
        let checksum_end = body_end
            .checked_add(CHECKSUM_SIZE)
            .ok_or_else(|| error("series checksum offset overflow"))?;
        if checksum_end > series_end {
            return Err(error("truncated series entry"));
        }

        verify_crc(bytes, body_start, length as usize, "series entry")?;
        series.push(parse_series(
            &bytes[body_start..body_end],
            series_reference,
            &symbols,
        )?);
        offset = align_series(checksum_end)?;
    }

    Ok(series)
}

#[derive(Debug)]
struct Toc {
    symbols: u64,
    series: u64,
    label_indices_start: u64,
}

impl Toc {
    fn parse(bytes: &[u8]) -> Result<Self, IndexError> {
        Ok(Self {
            symbols: read_u64(bytes, 0)?,
            series: read_u64(bytes, 8)?,
            label_indices_start: read_u64(bytes, 16)?,
        })
    }
}

fn parse_symbols(bytes: &[u8], offset: usize) -> Result<Vec<String>, IndexError> {
    let content_length = read_u32(bytes, offset)? as usize;
    let content_start = offset
        .checked_add(4)
        .ok_or_else(|| error("symbol table offset overflow"))?;
    let content_end = content_start
        .checked_add(content_length)
        .ok_or_else(|| error("symbol table length overflow"))?;
    if content_end + CHECKSUM_SIZE > bytes.len() {
        return Err(error("truncated symbol table"));
    }
    verify_crc(bytes, content_start, content_length, "symbol table")?;

    let symbol_count = read_u32(bytes, content_start)? as usize;
    let mut symbols = Vec::with_capacity(symbol_count);
    let mut position = content_start + 4;
    for _ in 0..symbol_count {
        let (length, length_size) = read_uvarint(bytes, position)?;
        position = position
            .checked_add(length_size)
            .ok_or_else(|| error("symbol length offset overflow"))?;
        let end = position
            .checked_add(length as usize)
            .ok_or_else(|| error("symbol length overflow"))?;
        if end > content_end {
            return Err(error("truncated symbol"));
        }
        let symbol = std::str::from_utf8(&bytes[position..end])
            .map_err(|_| error("symbol is not valid UTF-8"))?
            .to_owned();
        symbols.push(symbol);
        position = end;
    }
    if position != content_end {
        return Err(error("unexpected bytes in symbol table"));
    }
    Ok(symbols)
}

fn parse_series(bytes: &[u8], reference: u64, symbols: &[String]) -> Result<Series, IndexError> {
    let mut position = 0;
    let (label_count, size) = read_uvarint(bytes, position)?;
    position += size;

    let mut labels = BTreeMap::new();
    for _ in 0..label_count {
        let (name_ref, size) = read_uvarint(bytes, position)?;
        position += size;
        let (value_ref, size) = read_uvarint(bytes, position)?;
        position += size;
        let name = symbol(symbols, name_ref)?;
        let value = symbol(symbols, value_ref)?;
        if labels.insert(name.to_owned(), value.to_owned()).is_some() {
            return Err(error("series contains a duplicate label name"));
        }
    }

    let (chunk_count, size) = read_uvarint(bytes, position)?;
    position += size;
    let mut chunks = Vec::with_capacity(chunk_count as usize);
    let mut previous_maxt: Option<i64> = None;
    let mut previous_reference: Option<u64> = None;

    for _ in 0..chunk_count {
        let mint = if let Some(maxt) = previous_maxt {
            let (delta, size) = read_uvarint(bytes, position)?;
            position += size;
            maxt.checked_add(
                i64::try_from(delta).map_err(|_| error("chunk mint delta overflows i64"))?,
            )
            .ok_or_else(|| error("chunk mint overflows i64"))?
        } else {
            let (value, size) = read_varint(bytes, position)?;
            position += size;
            value
        };
        let (duration, size) = read_uvarint(bytes, position)?;
        position += size;
        let maxt = mint
            .checked_add(
                i64::try_from(duration).map_err(|_| error("chunk duration overflows i64"))?,
            )
            .ok_or_else(|| error("chunk maxt overflows i64"))?;

        let chunk_reference = if let Some(previous) = previous_reference {
            let (delta, size) = read_varint(bytes, position)?;
            position += size;
            let value = (previous as i128) + (delta as i128);
            u64::try_from(value).map_err(|_| error("chunk reference underflows or overflows"))?
        } else {
            let (value, size) = read_uvarint(bytes, position)?;
            position += size;
            value
        };

        if let Some(previous) = previous_maxt
            && mint < previous
        {
            return Err(error("chunk time ranges overlap or are out of order"));
        }
        if let Some(previous) = previous_reference
            && chunk_reference <= previous
        {
            return Err(error("chunk references are not increasing"));
        }

        chunks.push(Chunk {
            mint,
            maxt,
            reference: chunk_reference,
        });
        previous_maxt = Some(maxt);
        previous_reference = Some(chunk_reference);
    }

    if position != bytes.len() {
        return Err(error("unexpected bytes in series entry"));
    }

    Ok(Series {
        reference,
        labels,
        chunks,
    })
}

fn symbol(symbols: &[String], reference: u64) -> Result<&str, IndexError> {
    let index = usize::try_from(reference)
        .ok()
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| error("invalid symbol reference"))?;
    symbols
        .get(index)
        .map(String::as_str)
        .ok_or_else(|| error("symbol reference is out of bounds"))
}

fn verify_crc(
    bytes: &[u8],
    content_start: usize,
    content_length: usize,
    section: &str,
) -> Result<(), IndexError> {
    let content_end = content_start
        .checked_add(content_length)
        .ok_or_else(|| error(format!("{section} CRC offset overflow")))?;
    let checksum = read_u32(bytes, content_end)?;
    if CRC32C.checksum(&bytes[content_start..content_end]) != checksum {
        return Err(error(format!("{section} CRC mismatch")));
    }
    Ok(())
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, IndexError> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| error("truncated u32"))?;
    Ok(u32::from_be_bytes(
        value.try_into().expect("slice length is checked"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, IndexError> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| error("truncated u64"))?;
    Ok(u64::from_be_bytes(
        value.try_into().expect("slice length is checked"),
    ))
}

fn read_uvarint(bytes: &[u8], offset: usize) -> Result<(u64, usize), IndexError> {
    let mut value = 0u64;
    for index in 0..10 {
        let byte = *bytes
            .get(offset + index)
            .ok_or_else(|| error("truncated uvarint"))?;
        if index == 9 && byte > 1 {
            return Err(error("uvarint overflows u64"));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(error("uvarint is too long"))
}

fn read_varint(bytes: &[u8], offset: usize) -> Result<(i64, usize), IndexError> {
    let (value, size) = read_uvarint(bytes, offset)?;
    Ok((((value >> 1) as i64) ^ -((value & 1) as i64), size))
}

fn align_series(offset: usize) -> Result<usize, IndexError> {
    offset
        .checked_add(SERIES_ALIGNMENT - 1)
        .map(|value| value & !(SERIES_ALIGNMENT - 1))
        .ok_or_else(|| error("series alignment overflow"))
}

fn error(message: impl Into<String>) -> IndexError {
    IndexError(message.into())
}
