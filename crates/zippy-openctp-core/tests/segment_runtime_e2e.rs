use std::sync::{Arc, Mutex};

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
    assert_eq!(metrics.committed_rows, 1);
    assert_eq!(sink.data_rows(), vec![1]);
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
}

impl RecordingSink {
    fn data_rows(&self) -> Vec<usize> {
        self.data_rows.lock().unwrap().clone()
    }
}

impl SourceSink for RecordingSink {
    fn emit(&self, event: SourceEvent) -> CoreResult<()> {
        if let SourceEvent::Data(batch) = event {
            self.data_rows.lock().unwrap().push(batch.num_rows());
        }
        Ok(())
    }
}
