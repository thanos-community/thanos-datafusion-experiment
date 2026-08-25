use std::collections::BTreeMap;

use futures::StreamExt;
use serde::Deserialize;
use thanos_v1_reader::{
    store_service::ThanosStoreService,
    thanos_proto::thanos::{
        self, Aggr, LabelMatcher, PartialResponseStrategy, store_server::Store,
    },
};

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct OracleSeries {
    labels: BTreeMap<String, String>,
    chunks: Vec<OracleAggrChunk>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct OracleAggrChunk {
    min_time: i64,
    max_time: i64,
    encoding: i32,
    data: String,
    hash: u64,
    #[serde(default)]
    samples: Vec<OracleSample>,
    count: Option<OracleEncodedChunk>,
    sum: Option<OracleEncodedChunk>,
    min: Option<OracleEncodedChunk>,
    max: Option<OracleEncodedChunk>,
    counter: Option<OracleEncodedChunk>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct OracleEncodedChunk {
    encoding: i32,
    data: String,
    hash: u64,
    samples: Vec<OracleSample>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct OracleSample {
    timestamp: i64,
    value_bits: u64,
}

#[tokio::test]
async fn scalar_downsample_aggregates_match_go_bucket_store() {
    let fixture = crate::fixture::generated_fixture();
    let (context, service) = crate::fixture::store_service(&fixture, "downsample-cache").await;
    let aggregates = [Aggr::Count, Aggr::Sum, Aggr::Min, Aggr::Max, Aggr::Counter];

    for (metric, expected_series) in [
        ("dummy_requests_total", 2),
        ("dummy_temperature_celsius", 2),
        ("dummy_request_duration_seconds_bucket", 20),
        ("dummy_request_duration_seconds_count", 4),
        ("dummy_request_duration_seconds_sum", 4),
    ] {
        let expected = crate::fixture::go_bucket_store_series(
            &fixture,
            metric,
            Some("count,sum,min,max,counter"),
            Some(crate::fixture::RESOLUTION_5M),
        );
        let actual =
            reader_series(&service, metric, &aggregates, crate::fixture::RESOLUTION_5M).await;
        assert_eq!(
            actual, expected,
            "downsampled StoreAPI mismatch for {metric}"
        );
        assert_eq!(actual.len(), expected_series, "series count for {metric}");
        assert!(
            actual
                .iter()
                .flat_map(|series| &series.chunks)
                .all(|chunk| {
                    [
                        chunk.count.as_ref(),
                        chunk.sum.as_ref(),
                        chunk.min.as_ref(),
                        chunk.max.as_ref(),
                        chunk.counter.as_ref(),
                    ]
                    .into_iter()
                    .flatten()
                    .all(|slot| slot.encoding == 0 && slot.hash != 0 && !slot.samples.is_empty())
                })
        );
    }

    let counter = crate::fixture::go_bucket_store_series(
        &fixture,
        "dummy_requests_total",
        Some("count,sum,min,max,counter"),
        Some(crate::fixture::RESOLUTION_5M),
    );
    let expected_counter_values = aggregate_values(&counter, |chunk| chunk.counter.as_ref());
    let actual_counter_values = query_values(
        &context,
        "dummy_requests_total",
        crate::fixture::RESOLUTION_5M,
    )
    .await;
    assert_eq!(actual_counter_values, expected_counter_values);
    assert!(
        expected_counter_values
            .windows(2)
            .any(|pair| pair[0].0 == pair[1].0),
        "counter aggregate must preserve the reset marker at a duplicate timestamp"
    );
    assert!(
        aggregate_values(&counter, |chunk| chunk.counter.as_ref())
            .iter()
            .all(|(_, bits)| *bits != 0x7ff0_0000_0000_0002),
        "counter aggregate must not contain stale markers"
    );

    let gauge = crate::fixture::go_bucket_store_series(
        &fixture,
        "dummy_temperature_celsius",
        Some("count,sum,min,max,counter"),
        Some(crate::fixture::RESOLUTION_5M),
    );
    let gauge_values = [
        aggregate_values(&gauge, |chunk| chunk.count.as_ref()),
        aggregate_values(&gauge, |chunk| chunk.sum.as_ref()),
        aggregate_values(&gauge, |chunk| chunk.min.as_ref()),
        aggregate_values(&gauge, |chunk| chunk.max.as_ref()),
        aggregate_values(&gauge, |chunk| chunk.counter.as_ref()),
    ];
    assert!(
        gauge_values
            .iter()
            .flatten()
            .all(|(_, bits)| { *bits != 0x7ff0_0000_0000_0002 }),
        "stale markers must be removed during authoritative downsampling"
    );
    assert!(
        gauge_values[2]
            .iter()
            .any(|(_, bits)| *bits == f64::NEG_INFINITY.to_bits())
    );
    assert!(
        gauge_values[3]
            .iter()
            .any(|(_, bits)| *bits == f64::INFINITY.to_bits())
    );

    let histogram_count = crate::fixture::go_bucket_store_series(
        &fixture,
        "dummy_request_duration_seconds_count",
        Some("count,sum,min,max,counter"),
        Some(crate::fixture::RESOLUTION_5M),
    );
    assert!(
        aggregate_values(&histogram_count, |chunk| chunk.min.as_ref())
            .iter()
            .any(|(_, bits)| *bits == 0.0_f64.to_bits()),
        "classic histogram zero-count window must survive downsampling"
    );

    let expected_raw_fallback = crate::fixture::go_bucket_store_series(
        &fixture,
        "dummy_requests_total",
        Some("count"),
        Some(0),
    );
    let actual_raw_fallback =
        reader_series(&service, "dummy_requests_total", &[Aggr::Count], 0).await;
    assert_eq!(actual_raw_fallback, expected_raw_fallback);
    assert!(actual_raw_fallback.iter().all(|series| {
        series.chunks.iter().all(|chunk| {
            !chunk.data.is_empty()
                && chunk.count.is_none()
                && chunk.sum.is_none()
                && chunk.min.is_none()
                && chunk.max.is_none()
                && chunk.counter.is_none()
        })
    }));
}

async fn reader_series(
    service: &ThanosStoreService,
    metric: &str,
    aggregates: &[Aggr],
    max_resolution: i64,
) -> Vec<OracleSeries> {
    let request = thanos::SeriesRequest {
        min_time: i64::MIN,
        max_time: i64::MAX,
        matchers: vec![LabelMatcher {
            r#type: thanos::label_matcher::Type::Eq as i32,
            name: "__name__".to_owned(),
            value: metric.to_owned(),
        }],
        aggregates: aggregates
            .iter()
            .map(|aggregate| *aggregate as i32)
            .collect(),
        max_resolution_window: max_resolution,
        partial_response_strategy: PartialResponseStrategy::Abort as i32,
        ..Default::default()
    };
    let mut stream = service
        .series(tonic::Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let mut result = Vec::new();
    while let Some(response) = stream.next().await {
        let response = response.unwrap();
        let Some(thanos::series_response::Result::Series(series)) = response.result else {
            panic!("reader returned a non-series response");
        };
        result.push(OracleSeries {
            labels: series
                .labels
                .into_iter()
                .map(|label| (label.name, label.value))
                .collect(),
            chunks: series.chunks.into_iter().map(convert_aggr_chunk).collect(),
        });
    }
    result
}

fn convert_aggr_chunk(chunk: thanos::AggrChunk) -> OracleAggrChunk {
    let raw = chunk.raw.map(convert_chunk);
    OracleAggrChunk {
        min_time: chunk.min_time,
        max_time: chunk.max_time,
        encoding: raw.as_ref().map(|chunk| chunk.encoding).unwrap_or(0),
        data: raw
            .as_ref()
            .map(|chunk| chunk.data.clone())
            .unwrap_or_default(),
        hash: raw.as_ref().map(|chunk| chunk.hash).unwrap_or(0),
        samples: raw.map(|chunk| chunk.samples).unwrap_or_default(),
        count: chunk.count.map(convert_chunk),
        sum: chunk.sum.map(convert_chunk),
        min: chunk.min.map(convert_chunk),
        max: chunk.max.map(convert_chunk),
        counter: chunk.counter.map(convert_chunk),
    }
}

fn convert_chunk(chunk: thanos::Chunk) -> OracleEncodedChunk {
    let samples = thanos_v1_reader::chunk_reader::decode_record(&framed_record(&chunk), false)
        .unwrap()
        .into_iter()
        .map(|sample| OracleSample {
            timestamp: sample.timestamp,
            value_bits: sample.value.to_bits(),
        })
        .collect();
    OracleEncodedChunk {
        encoding: chunk.r#type,
        data: encode_hex(&chunk.data),
        hash: chunk.hash,
        samples,
    }
}

fn framed_record(chunk: &thanos::Chunk) -> Vec<u8> {
    let mut record = encode_uvarint(chunk.data.len());
    record.push(1);
    record.extend_from_slice(&chunk.data);
    let checksum = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI)
        .checksum(&record[record.len() - chunk.data.len() - 1..]);
    record.extend_from_slice(&checksum.to_be_bytes());
    record
}

fn encode_uvarint(mut value: usize) -> Vec<u8> {
    let mut encoded = Vec::new();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        encoded.push(byte);
        if value == 0 {
            return encoded;
        }
    }
}

fn aggregate_values(
    series: &[OracleSeries],
    slot: impl Fn(&OracleAggrChunk) -> Option<&OracleEncodedChunk>,
) -> Vec<(i64, u64)> {
    let mut values = series
        .iter()
        .flat_map(|series| &series.chunks)
        .filter_map(slot)
        .flat_map(|chunk| &chunk.samples)
        .map(|sample| (sample.timestamp, sample.value_bits))
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

async fn query_values(
    context: &datafusion::prelude::SessionContext,
    metric: &str,
    resolution: i64,
) -> Vec<(i64, u64)> {
    let batches = context
        .sql(&format!(
            "SELECT timestamp, value FROM metrics.{metric} WHERE downsample_resolution = {resolution}"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut values = batches
        .iter()
        .flat_map(|batch| {
            let timestamps = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .unwrap();
            let samples = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(move |index| (timestamps.value(index), samples.value(index).to_bits()))
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[usize::from(byte >> 4)] as char);
        result.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    result
}
