use std::sync::{Arc, Mutex};

use arrow::array::{Float64Array, Int64Array, StringArray};
use zippy_core::{Result as CoreResult, Source, SourceEvent, SourceSink};
use zippy_openctp_core::{
    FakeMdDriver, FakeMdDriverHandle, OpenCtpMarketDataSource, OpenCtpMarketDataSourceConfig,
};

#[test]
fn source_callback_advances_segment_runtime_without_breaking_batch_path() {
    let sink = Arc::new(RecordingSink::default());
    let (source, driver) = make_test_source_with_segment_ingress();
    let segment_metrics = source.segment_debug_metrics_handle();
    let handle = Box::new(source).start(sink.clone()).unwrap();

    driver.emit_trade_tick("rb2510", 4123.5).unwrap();
    driver.emit_stop().unwrap();

    handle.join().unwrap();

    let metrics = segment_metrics.lock().unwrap().clone().unwrap();
    let snapshot = metrics.active_snapshot.unwrap();
    assert_eq!(metrics.committed_rows, 1);
    assert_eq!(sink.data_rows(), vec![1]);
    assert_eq!(snapshot.instrument_id.as_deref(), Some("rb2510"));
    assert_eq!(snapshot.last_price, Some(4123.5));
    assert!(snapshot.localtime_ns.unwrap() > 0);
    assert!(snapshot.source_emit_ns.unwrap() >= snapshot.localtime_ns.unwrap());

    let batch = sink.single_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch_instrument_id(&batch), "rb2510");
    assert_eq!(batch_last_price(&batch), 4123.5);
    assert!(batch_localtime_ns(&batch) > 0);
    assert!(batch_source_emit_ns(&batch) >= batch_localtime_ns(&batch));
}

#[test]
fn source_rejects_segment_ingress_with_buffered_batch_config() {
    let sink = Arc::new(RecordingSink::default());
    let (driver, _) = FakeMdDriver::pair();
    let mut config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["rb2510".to_string()],
        ".cache/openctp/md".to_string(),
    );
    config.segment_ingress_enabled = true;
    config.rows_per_batch = 2;

    let source = OpenCtpMarketDataSource::from_driver(config, Box::new(driver));
    let error = Box::new(source)
        .start(sink)
        .err()
        .expect("segment ingress should only support low-latency config");

    assert!(error
        .to_string()
        .contains("segment ingress requires rows_per_batch=1 and flush_interval_ms=0"));
}

#[test]
fn source_keeps_running_after_more_than_sixty_four_segment_rows() {
    let sink = Arc::new(RecordingSink::default());
    let (source, driver) = make_test_source_with_segment_ingress();
    let segment_metrics = source.segment_debug_metrics_handle();
    let handle = Box::new(source).start(sink.clone()).unwrap();

    for index in 0..65 {
        driver
            .emit_trade_tick("rb2510", 4123.5 + index as f64)
            .unwrap();
    }
    driver.emit_stop().unwrap();

    handle.join().unwrap();

    let metrics = segment_metrics.lock().unwrap().clone().unwrap();
    let snapshot = metrics.active_snapshot.unwrap();
    assert_eq!(metrics.committed_rows, 65);
    assert_eq!(sink.data_rows().len(), 65);
    assert!(sink.data_rows().iter().all(|rows| *rows == 1));
    assert_eq!(snapshot.instrument_id.as_deref(), Some("rb2510"));
    assert_eq!(snapshot.last_price, Some(4123.5 + 64.0));
}

fn make_test_source_with_segment_ingress() -> (OpenCtpMarketDataSource, FakeMdDriverHandle) {
    let (driver, handle) = FakeMdDriver::pair();
    let mut config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["rb2510".to_string()],
        ".cache/openctp/md".to_string(),
    );
    config.segment_ingress_enabled = true;

    (
        OpenCtpMarketDataSource::from_driver(config, Box::new(driver)),
        handle,
    )
}

#[derive(Default)]
struct RecordingSink {
    data_rows: Mutex<Vec<usize>>,
    batches: Mutex<Vec<arrow::record_batch::RecordBatch>>,
}

impl RecordingSink {
    fn data_rows(&self) -> Vec<usize> {
        self.data_rows.lock().unwrap().clone()
    }

    fn single_batch(&self) -> Result<arrow::record_batch::RecordBatch, &'static str> {
        let batches = self.batches.lock().unwrap();
        if batches.len() != 1 {
            return Err("expected exactly one batch");
        }
        Ok(batches[0].clone())
    }
}

impl SourceSink for RecordingSink {
    fn emit(&self, event: SourceEvent) -> CoreResult<()> {
        if let SourceEvent::Data(batch) = event {
            self.data_rows.lock().unwrap().push(batch.num_rows());
            self.batches.lock().unwrap().push(batch);
        }
        Ok(())
    }
}

fn batch_instrument_id(batch: &arrow::record_batch::RecordBatch) -> &str {
    batch
        .column_by_name("instrument_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
}

fn batch_last_price(batch: &arrow::record_batch::RecordBatch) -> f64 {
    batch
        .column_by_name("last_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

fn batch_localtime_ns(batch: &arrow::record_batch::RecordBatch) -> i64 {
    batch
        .column_by_name("localtime_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn batch_source_emit_ns(batch: &arrow::record_batch::RecordBatch) -> i64 {
    batch
        .column_by_name("source_emit_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
