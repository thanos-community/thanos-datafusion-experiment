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
}

#[tokio::test]
async fn native_histogram_chunks_match_go_bucket_store() {
    let (_, service) = crate::fixture::store_service("native-cache").await;

    for (metric, expected_encoding) in [
        ("dummy_native_histogram", 1),
        ("dummy_float_native_histogram", 2),
    ] {
        let expected = crate::fixture::go_bucket_store_series(metric, None, None);
        let actual = reader_series(&service, metric).await;
        assert_eq!(actual, expected, "StoreAPI mismatch for {metric}");
        assert_eq!(actual.len(), crate::fixture::POD_COUNT);
        assert!(
            actual
                .iter()
                .flat_map(|series| &series.chunks)
                .all(|chunk| chunk.encoding == expected_encoding)
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
                    OracleChunk {
                        min_time: chunk.min_time,
                        max_time: chunk.max_time,
                        encoding: raw.r#type,
                        data: encode_hex(&raw.data),
                        hash: raw.hash,
                    }
                })
                .collect(),
        });
    }
    result
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
