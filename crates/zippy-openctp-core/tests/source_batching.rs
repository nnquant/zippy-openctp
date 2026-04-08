use std::time::{Duration, Instant};

use arrow::array::StringArray;
use zippy_openctp_core::source::{FakeOpenCtpSourceRuntime, SourceError};
use zippy_openctp_core::OpenCtpMarketDataSourceConfig;

#[test]
fn batching_flushes_immediately_when_rows_per_batch_is_one() {
    let mut source = FakeOpenCtpSourceRuntime::new(default_source_config());

    let batch = source
        .push_tick(sample_tick("IF2506", 1))
        .expect("push_tick should succeed");

    assert!(batch.is_some());

    let batch = batch.expect("rows_per_batch=1 should flush immediately");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(instrument_ids(&batch), vec!["IF2506"]);
}

#[test]
fn batching_flushes_when_interval_is_due() {
    let mut config = default_source_config();
    config.rows_per_batch = 2;
    config.flush_interval_ms = 50;

    let mut source = FakeOpenCtpSourceRuntime::new(config);
    let started_at = Instant::now();
    let first = source
        .push_tick_at(sample_tick("IF2506", 1), started_at)
        .expect("first push_tick should succeed");
    assert!(first.is_none());

    let before_due = source
        .flush_if_due(started_at + Duration::from_millis(49))
        .expect("flush_if_due before interval should succeed");
    assert!(before_due.is_none());

    let due = source
        .flush_if_due(started_at + Duration::from_millis(50))
        .expect("flush_if_due at interval should succeed");

    let batch = due.expect("flush_if_due should emit buffered rows once due");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(instrument_ids(&batch), vec!["IF2506"]);
}

#[test]
fn batching_rejects_invalid_ticks_from_runtime() {
    let mut source = FakeOpenCtpSourceRuntime::new(default_source_config());
    let mut raw = sample_tick("IF2506", 1);
    raw.action_day = " ".to_string();

    let error = source
        .push_tick(raw)
        .expect_err("invalid tick must be rejected");

    assert!(matches!(error, SourceError::Normalize(_)));
}

fn default_source_config() -> OpenCtpMarketDataSourceConfig {
    OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["IF2506".to_string()],
        ".cache/openctp/md".to_string(),
    )
}

fn sample_tick(instrument_id: &str, volume: i64) -> zippy_openctp_core::normalize::RawTickSnapshot {
    zippy_openctp_core::normalize::RawTickSnapshot {
        instrument_id: instrument_id.to_string(),
        exchange_id: "CFFEX".to_string(),
        trading_day: "20260408".to_string(),
        action_day: "20260408".to_string(),
        update_time: "09:30:00".to_string(),
        update_millisec: 500,
        last_price: 3912.4,
        volume,
        turnover: 987654.0,
        open_interest: 56789.0,
        bid_price_1: 3912.2,
        bid_volume_1: 10,
        ask_price_1: 3912.6,
        ask_volume_1: 8,
    }
}

fn instrument_ids(batch: &arrow::record_batch::RecordBatch) -> Vec<&str> {
    let column = batch
        .column_by_name("instrument_id")
        .expect("instrument_id column must exist");
    let array = column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("instrument_id must be Utf8");

    array.iter().map(|value| value.expect("non-null instrument_id")).collect()
}
