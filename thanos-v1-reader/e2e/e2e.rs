mod fixture;

use arrow::{
    array::{Array, Float64Array, Int64Array, StringArray, TimestampMillisecondArray},
    record_batch::RecordBatch,
};
use fixture::{MAXT, MINT, POD_COUNT, RESOLUTION_5M, SAMPLE_COUNT};
use vortex::{VortexSessionDefault, file::OpenOptionsSessionExt, io::session::RuntimeSessionExt};

#[tokio::test]
async fn convert_writes_flattened_vortex_with_hll_metadata() {
    let fixture = fixture::generated_fixture();
    let block = fixture.raw_block();
    let output = tempfile::NamedTempFile::new().expect("create Vortex output");
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_convert"))
        .arg(format!("file://{}", block.display()))
        .arg(format!("file://{}", output.path().display()))
        .status()
        .expect("run convert");
    assert!(status.success(), "convert failed with {status}");
    let bytes = std::fs::read(output.path()).expect("read Vortex output");
    let file = vortex::session::VortexSession::default()
        .with_tokio()
        .open_options()
        .include_metadata()
        .open_buffer(bytes)
        .expect("open Vortex output");
    assert!(file.metadata_segment("thanos.labels_hll.v1").is_some());
    let names = format!("{:?}", file.dtype());
    assert!(names.contains("label.pod"), "Vortex schema: {names}");
}

#[tokio::test]
async fn flight_sql_raw_counter_rows_are_parsed_and_ordered() {
    let fixture = fixture::generated_fixture();
    let reader = fixture.start_reader().await;
    let batches = reader
        .query(
            "SELECT timestamp, value, pod \
             FROM metrics.dummy_requests_total \
             WHERE downsample_resolution = 0 \
             ORDER BY timestamp, pod",
        )
        .await;
    let actual = timestamp_value_label_rows(&batches, "pod");

    let step = (MAXT - MINT) / SAMPLE_COUNT as i64;
    let expected = (0..SAMPLE_COUNT)
        .flat_map(|sample| {
            [("pod-falcon-000", 0), ("pod-marble-001", 1)]
                .into_iter()
                .map(move |(pod, index)| {
                    let value = if sample == SAMPLE_COUNT / 2 {
                        10 + index
                    } else {
                        1_000 + index * 100 + sample * 7
                    };
                    (MINT + sample as i64 * step, value as f64, pod.to_owned())
                })
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn flight_sql_filters_labels_timestamps_and_resolution() {
    let fixture = fixture::generated_fixture();
    let reader = fixture.start_reader().await;
    let step = (MAXT - MINT) / SAMPLE_COUNT as i64;
    let start = MINT + 10 * step;
    let end = MINT + 14 * step;
    let batches = reader
        .query(&format!(
            "SELECT timestamp, value, downsample_resolution, pod \
             FROM metrics.dummy_requests_total \
             WHERE downsample_resolution = 0 \
               AND pod = 'pod-falcon-000' \
               AND timestamp >= to_timestamp_millis({start}) \
               AND timestamp <= to_timestamp_millis({end}) \
             ORDER BY timestamp"
        ))
        .await;
    let batch = one_batch(&batches);
    let timestamps = timestamp_column(batch, "timestamp");
    let values = float_column(batch, "value");
    let resolutions = int64_column(batch, "downsample_resolution");
    let pods = string_column(batch, "pod");
    assert_eq!(batch.num_rows(), 5);
    for index in 0..batch.num_rows() {
        assert_eq!(timestamps.value(index), start + index as i64 * step);
        assert_eq!(values.value(index), (1_000 + (10 + index) * 7) as f64);
        assert_eq!(resolutions.value(index), 0);
        assert_eq!(pods.value(index), "pod-falcon-000");
    }
}

#[tokio::test]
async fn flight_sql_exposes_classic_histogram_labels_and_downsampled_rows() {
    let fixture = fixture::generated_fixture();
    let reader = fixture.start_reader().await;
    let histogram = reader
        .query(
            "SELECT timestamp, value, le, route, pod \
             FROM metrics.dummy_request_duration_seconds_bucket \
             WHERE downsample_resolution = 0 \
               AND le = '1' \
               AND route = 'route-harbor-000' \
             ORDER BY timestamp, pod",
        )
        .await;
    let histogram = one_batch(&histogram);
    assert_eq!(histogram.num_rows(), SAMPLE_COUNT * POD_COUNT);
    assert!(
        string_column(histogram, "le")
            .iter()
            .flatten()
            .all(|value| value == "1")
    );
    assert!(
        string_column(histogram, "route")
            .iter()
            .flatten()
            .all(|value| value == "route-harbor-000")
    );

    let downsampled = reader
        .query(
            "SELECT timestamp, value, downsample_resolution, pod \
             FROM metrics.dummy_requests_total \
             WHERE downsample_resolution = 300000 \
             ORDER BY timestamp, pod",
        )
        .await;
    let downsampled = one_batch(&downsampled);
    assert!(downsampled.num_rows() > 0);
    assert!(
        int64_column(downsampled, "downsample_resolution")
            .iter()
            .flatten()
            .all(|value| value == RESOLUTION_5M)
    );
}

#[tokio::test]
async fn flight_sql_preserves_special_float_values_and_returns_query_errors() {
    let fixture = fixture::generated_fixture();
    let reader = fixture.start_reader().await;
    let batches = reader
        .query(
            "SELECT value FROM metrics.dummy_temperature_celsius \
             WHERE downsample_resolution = 0 \
             ORDER BY timestamp, pod",
        )
        .await;
    let bits = float_column(one_batch(&batches), "value")
        .iter()
        .flatten()
        .map(f64::to_bits)
        .collect::<Vec<_>>();
    for expected in [
        f64::INFINITY.to_bits(),
        f64::NEG_INFINITY.to_bits(),
        (-0.0_f64).to_bits(),
        0x7ff0_0000_0000_0002,
    ] {
        assert!(
            bits.contains(&expected),
            "missing float bit pattern {expected:#x}"
        );
    }

    let error = reader
        .query_error("SELECT * FROM metrics.does_not_exist")
        .await;
    assert!(
        error.contains("does_not_exist"),
        "unexpected SQL error: {error}"
    );
}

fn timestamp_value_label_rows(batches: &[RecordBatch], label: &str) -> Vec<(i64, f64, String)> {
    batches
        .iter()
        .flat_map(|batch| {
            let timestamps = timestamp_column(batch, "timestamp");
            let values = float_column(batch, "value");
            let labels = string_column(batch, label);
            (0..batch.num_rows()).map(move |index| {
                (
                    timestamps.value(index),
                    values.value(index),
                    labels.value(index).to_owned(),
                )
            })
        })
        .collect()
}

fn one_batch(batches: &[RecordBatch]) -> &RecordBatch {
    assert_eq!(batches.len(), 1, "expected one Flight record batch");
    &batches[0]
}

fn timestamp_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMillisecondArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing {name} column"))
        .as_any()
        .downcast_ref()
        .unwrap_or_else(|| panic!("{name} is not a millisecond timestamp"))
}

fn float_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Float64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing {name} column"))
        .as_any()
        .downcast_ref()
        .unwrap_or_else(|| panic!("{name} is not a float"))
}

fn int64_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a Int64Array {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing {name} column"))
        .as_any()
        .downcast_ref()
        .unwrap_or_else(|| panic!("{name} is not an int64"))
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing {name} column"))
        .as_any()
        .downcast_ref()
        .unwrap_or_else(|| panic!("{name} is not a string"))
}
