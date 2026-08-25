mod fixture;
mod native_histogram_parity;
mod raw_scalar_parity;
mod scalar_downsample_parity;

use fixture::{MAXT, MINT, POD_COUNT, SAMPLE_COUNT};

#[tokio::test]
async fn counter_samples_match_generated_block_values() {
    let context = fixture::indexed_context("counter-cache").await;

    let batches = context
        .sql(
            "SELECT timestamp, value \
             FROM metrics.dummy_requests_total \
             WHERE downsample_resolution = 0 \
             ORDER BY timestamp, pod",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let actual = batches
        .iter()
        .flat_map(|batch| {
            let timestamps = batch
                .column_by_name("timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::TimestampMillisecondArray>()
                .unwrap();
            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            (0..batch.num_rows()).map(move |index| (timestamps.value(index), values.value(index)))
        })
        .collect::<Vec<_>>();

    let step = (MAXT - MINT) / SAMPLE_COUNT as i64;
    let expected = (0..SAMPLE_COUNT)
        .flat_map(|sample| {
            (0..POD_COUNT).map(move |pod| {
                let value = if sample == SAMPLE_COUNT / 2 {
                    10 + pod
                } else {
                    1_000 + pod * 100 + sample * 7
                };
                (MINT + sample as i64 * step, value as f64)
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
