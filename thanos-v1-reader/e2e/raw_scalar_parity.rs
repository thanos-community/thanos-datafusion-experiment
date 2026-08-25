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
    chunks: Vec<OracleChunk>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct OracleChunk {
    min_time: i64,
    max_time: i64,
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
async fn raw_scalar_and_classic_histogram_series_match_go_bucket_store() {
    let fixture = crate::fixture::generated_fixture();
    let (context, service) = crate::fixture::store_service(&fixture, "raw-scalar-cache").await;

    for (metric, expected_series) in [
        ("dummy_requests_total", 2),
        ("dummy_temperature_celsius", 2),
        ("dummy_request_duration_seconds_bucket", 20),
        ("dummy_request_duration_seconds_count", 4),
        ("dummy_request_duration_seconds_sum", 4),
    ] {
        let expected = crate::fixture::go_bucket_store_series(&fixture, metric, None, None);
        let actual = reader_series(&service, metric).await;
        assert_eq!(actual, expected, "StoreAPI mismatch for {metric}");
        assert_eq!(actual.len(), expected_series, "series count for {metric}");
        assert!(
            actual
                .iter()
                .flat_map(|series| &series.chunks)
                .all(|chunk| chunk.encoding == 0 && chunk.hash != 0)
        );
        assert!(
            actual.iter().all(|series| series.chunks.len() > 1),
            "{metric} must exercise multiple TSDB chunks"
        );
        assert_eq!(
            actual
                .iter()
                .flat_map(|series| &series.chunks)
                .map(|chunk| chunk.samples.len())
                .sum::<usize>(),
            expected_series * crate::fixture::SAMPLE_COUNT,
            "decoded sample count for {metric}"
        );

        let expected_values = oracle_values(&expected);
        let actual_values = query_values(&context, metric).await;
        assert_eq!(
            actual_values, expected_values,
            "query-visible values differ from Go chunks for {metric}"
        );

        if metric.ends_with("_bucket") {
            assert!(
                actual
                    .iter()
                    .any(|series| { series.labels.get("le").map(String::as_str) == Some("+Inf") })
            );
        }
        if metric == "dummy_requests_total" {
            assert!(actual.iter().all(series_has_counter_reset));
        }
        if metric.starts_with("dummy_request_duration_seconds_") {
            assert!(
                oracle_values(&actual)
                    .iter()
                    .any(|(_, bits)| *bits == 0.0_f64.to_bits()),
                "{metric} must exercise zero-valued histogram components"
            );
        }
    }

    let gauge =
        crate::fixture::go_bucket_store_series(&fixture, "dummy_temperature_celsius", None, None);
    let gauge_bits = oracle_values(&gauge)
        .into_iter()
        .map(|(_, bits)| bits)
        .collect::<Vec<_>>();
    for expected in [
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
        (-0.0_f64).to_bits(),
        0x7ff0_0000_0000_0002,
    ] {
        assert!(
            gauge_bits.contains(&expected),
            "missing gauge edge {expected:#x}"
        );
    }
}

async fn reader_series(service: &ThanosStoreService, metric: &str) -> Vec<OracleSeries> {
    let request = thanos::SeriesRequest {
        min_time: i64::MIN,
        max_time: i64::MAX,
        matchers: vec![LabelMatcher {
            r#type: thanos::label_matcher::Type::Eq as i32,
            name: "__name__".to_owned(),
            value: metric.to_owned(),
        }],
        aggregates: vec![Aggr::Raw as i32],
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
            chunks: series
                .chunks
                .into_iter()
                .map(|chunk| {
                    let raw = chunk.raw.expect("raw query must return a raw chunk");
                    let samples =
                        thanos_v1_reader::chunk_reader::decode_record(&framed_record(&raw), false)
                            .unwrap()
                            .into_iter()
                            .map(|sample| OracleSample {
                                timestamp: sample.timestamp,
                                value_bits: sample.value.to_bits(),
                            })
                            .collect();
                    OracleChunk {
                        min_time: chunk.min_time,
                        max_time: chunk.max_time,
                        encoding: raw.r#type,
                        data: encode_hex(&raw.data),
                        hash: raw.hash,
                        samples,
                    }
                })
                .collect(),
        });
    }
    result
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

fn oracle_values(series: &[OracleSeries]) -> Vec<(i64, u64)> {
    let mut values = series
        .iter()
        .flat_map(|series| &series.chunks)
        .flat_map(|chunk| &chunk.samples)
        .map(|sample| (sample.timestamp, sample.value_bits))
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

fn series_has_counter_reset(series: &OracleSeries) -> bool {
    let values = series
        .chunks
        .iter()
        .flat_map(|chunk| &chunk.samples)
        .map(|sample| f64::from_bits(sample.value_bits))
        .collect::<Vec<_>>();
    values.windows(2).any(|pair| pair[1] < pair[0])
}

async fn query_values(
    context: &datafusion::prelude::SessionContext,
    metric: &str,
) -> Vec<(i64, u64)> {
    let batches = context
        .sql(&format!(
            "SELECT timestamp, value FROM metrics.{metric} WHERE downsample_resolution = 0"
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
