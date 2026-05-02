use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use arrow::record_batch::RecordBatch;
use zippy_core::{Result as CoreResult, Source, SourceEvent, SourceSink};
use zippy_openctp_core::{
    MdDriver, MdDriverEvent, OpenCtpColumnarGeneratorSource, OpenCtpMarketGeneratorConfig,
    OpenCtpMarketGeneratorDriver, OpenCtpMarketGeneratorSource, OpenCtpNormalizedGeneratorDriver,
    OpenCtpNormalizedGeneratorSource, OpenCtpSourceStatus,
};

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<RecordedEvent>>,
    batches: Mutex<Vec<RecordBatch>>,
}

impl RecordingSink {
    fn events(&self) -> Vec<RecordedEvent> {
        self.events.lock().unwrap().clone()
    }

    fn batches(&self) -> Vec<RecordBatch> {
        self.batches.lock().unwrap().clone()
    }
}

impl SourceSink for RecordingSink {
    fn emit(&self, event: SourceEvent) -> CoreResult<()> {
        match event {
            SourceEvent::Hello(hello) => {
                self.events
                    .lock()
                    .unwrap()
                    .push(RecordedEvent::Hello(hello.stream_name));
            }
            SourceEvent::Data(batch) => {
                let rows = batch.num_rows();
                self.batches.lock().unwrap().push(batch.to_record_batch()?);
                self.events
                    .lock()
                    .unwrap()
                    .push(RecordedEvent::Data { rows });
            }
            SourceEvent::Flush => {
                self.events.lock().unwrap().push(RecordedEvent::Flush);
            }
            SourceEvent::Stop => {
                self.events.lock().unwrap().push(RecordedEvent::Stop);
            }
            SourceEvent::Error(reason) => {
                self.events
                    .lock()
                    .unwrap()
                    .push(RecordedEvent::Error(reason));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RecordedEvent {
    Hello(String),
    Data { rows: usize },
    Flush,
    Stop,
    Error(String),
}

#[test]
fn generator_source_emits_full_instrument_rounds_in_configured_order() {
    let config =
        OpenCtpMarketGeneratorConfig::new(vec!["IF2606".to_string(), "IH2606".to_string()], 1)
            .unwrap()
            .with_seed(42)
            .with_max_ticks(Some(4));
    let source = OpenCtpMarketGeneratorSource::new(config);
    let status = source.status_handle();
    let sink = Arc::new(RecordingSink::default());

    let handle = Box::new(source).start(sink.clone()).unwrap();
    handle.join().unwrap();

    assert_eq!(*status.lock().unwrap(), OpenCtpSourceStatus::Stopped);
    let events = sink.events();
    assert_eq!(
        events.first(),
        Some(&RecordedEvent::Hello("openctp.tick".to_string()))
    );
    assert_eq!(events.last(), Some(&RecordedEvent::Stop));
    assert_eq!(
        events
            .iter()
            .map(|event| match event {
                RecordedEvent::Data { rows } => *rows,
                _ => 0,
            })
            .sum::<usize>(),
        4
    );

    let batches = sink.batches();
    let instruments = batches
        .iter()
        .flat_map(batch_instrument_ids)
        .collect::<Vec<_>>();
    assert_eq!(instruments, vec!["IF2606", "IH2606", "IF2606", "IH2606"]);

    let dt_values = batches
        .iter()
        .flat_map(batch_dt_ns_values)
        .collect::<Vec<_>>();
    assert_eq!(dt_values[0], dt_values[1]);
    assert!(dt_values[2] > dt_values[0]);
    assert_eq!(dt_values[2], dt_values[3]);

    let prices = batches
        .iter()
        .flat_map(batch_last_price_values)
        .collect::<Vec<_>>();
    assert!(prices.iter().all(|price| *price > 0.0));
    assert_ne!(prices[0], prices[2]);
}

#[test]
fn generator_config_rejects_empty_instruments_and_zero_interval() {
    let empty = OpenCtpMarketGeneratorConfig::new(Vec::new(), 1).unwrap_err();
    assert!(empty.to_string().contains("instruments"));

    let zero_interval =
        OpenCtpMarketGeneratorConfig::new(vec!["IF2606".to_string()], 0).unwrap_err();
    assert!(zero_interval.to_string().contains("interval_ms"));
}

#[test]
fn generator_driver_emits_one_raw_tick_batch_per_instrument_round() {
    let config = OpenCtpMarketGeneratorConfig::new(
        vec![
            "IF2606".to_string(),
            "IH2606".to_string(),
            "IC2606".to_string(),
        ],
        1,
    )
    .unwrap()
    .with_seed(42)
    .with_max_ticks(Some(6));
    let (tx, rx) = crossbeam_channel::unbounded();

    let _handle = Box::new(OpenCtpMarketGeneratorDriver::new(config))
        .start(tx)
        .unwrap();

    let first = rx.recv().unwrap();
    let second = rx.recv().unwrap();
    let stop = rx.recv().unwrap();

    assert_raw_round_batch(first, &["IF2606", "IH2606", "IC2606"]);
    assert_raw_round_batch(second, &["IF2606", "IH2606", "IC2606"]);
    assert!(matches!(stop, MdDriverEvent::Stop));
}

#[test]
fn normalized_generator_driver_emits_normalized_row_batches_without_raw_normalization() {
    let config = OpenCtpMarketGeneratorConfig::new(
        vec![
            "IF2606".to_string(),
            "IH2606".to_string(),
            "IC2606".to_string(),
        ],
        1,
    )
    .unwrap()
    .with_seed(42)
    .with_max_ticks(Some(6));
    let (tx, rx) = crossbeam_channel::unbounded();

    let _handle = Box::new(OpenCtpNormalizedGeneratorDriver::new(config))
        .start(tx)
        .unwrap();

    let first = rx.recv().unwrap();
    let second = rx.recv().unwrap();
    let stop = rx.recv().unwrap();

    assert_normalized_round_batch(first, &["IF2606", "IH2606", "IC2606"]);
    assert_normalized_round_batch(second, &["IF2606", "IH2606", "IC2606"]);
    assert!(matches!(stop, MdDriverEvent::Stop));
}

#[test]
fn normalized_generator_interval_controls_event_time_without_wall_clock_pacing() {
    let config = OpenCtpMarketGeneratorConfig::new(vec!["IF2606".to_string()], 60_000)
        .unwrap()
        .with_seed(42)
        .with_max_ticks(Some(2));
    let (tx, rx) = crossbeam_channel::unbounded();

    let _handle = Box::new(OpenCtpNormalizedGeneratorDriver::new(config))
        .start(tx)
        .unwrap();

    let first = rx.recv_timeout(Duration::from_millis(200)).unwrap();
    let second = rx.recv_timeout(Duration::from_millis(200)).unwrap();
    let stop = rx.recv_timeout(Duration::from_millis(200)).unwrap();

    assert_normalized_round_batch(first, &["IF2606"]);
    assert_normalized_round_batch(second, &["IF2606"]);
    assert!(matches!(stop, MdDriverEvent::Stop));
}

#[test]
fn normalized_generator_source_uses_distinct_source_identity() {
    let config =
        OpenCtpMarketGeneratorConfig::new(vec!["IF2606".to_string(), "IH2606".to_string()], 1)
            .unwrap()
            .with_seed(42)
            .with_max_ticks(Some(4));
    let source = OpenCtpNormalizedGeneratorSource::new(config);
    let sink = Arc::new(RecordingSink::default());

    let handle = Box::new(source).start(sink.clone()).unwrap();
    handle.join().unwrap();

    let instruments = sink
        .batches()
        .iter()
        .flat_map(batch_instrument_ids)
        .collect::<Vec<_>>();
    assert_eq!(instruments, vec!["IF2606", "IH2606", "IF2606", "IH2606"]);
}

#[test]
fn columnar_generator_source_writes_ticks_without_using_market_data_source_path() {
    let config =
        OpenCtpMarketGeneratorConfig::new(vec!["IF2606".to_string(), "IH2606".to_string()], 1)
            .unwrap()
            .with_seed(42)
            .with_max_ticks(Some(4));
    let source = OpenCtpColumnarGeneratorSource::new(config);
    let status = source.status_handle();
    let sink = Arc::new(RecordingSink::default());

    let handle = Box::new(source).start(sink.clone()).unwrap();
    handle.join().unwrap();

    assert_eq!(*status.lock().unwrap(), OpenCtpSourceStatus::Stopped);
    let events = sink.events();
    assert_eq!(
        events.first(),
        Some(&RecordedEvent::Hello("openctp.tick".to_string()))
    );
    assert_eq!(events.last(), Some(&RecordedEvent::Stop));

    let instruments = sink
        .batches()
        .iter()
        .flat_map(batch_instrument_ids)
        .collect::<Vec<_>>();
    assert_eq!(instruments, vec!["IF2606", "IH2606", "IF2606", "IH2606"]);
}

fn batch_instrument_ids(batch: &RecordBatch) -> Vec<String> {
    batch
        .column_by_name("instrument_id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_string())
        .collect()
}

fn batch_last_price_values(batch: &RecordBatch) -> Vec<f64> {
    batch
        .column_by_name("last_price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap())
        .collect()
}

fn batch_dt_ns_values(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column_by_name("dt")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap())
        .collect()
}

fn assert_raw_round_batch(event: MdDriverEvent, expected_instruments: &[&str]) {
    let MdDriverEvent::Ticks(ticks) = event else {
        panic!("expected raw tick batch event");
    };
    let instruments = ticks
        .iter()
        .map(|tick| tick.instrument_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(instruments, expected_instruments);
    assert!(ticks.iter().all(|tick| !tick.update_time.is_empty()));
    assert!(ticks
        .iter()
        .all(|tick| (0..=999).contains(&tick.update_millisec)));
}

fn assert_normalized_round_batch(event: MdDriverEvent, expected_instruments: &[&str]) {
    let MdDriverEvent::Rows(rows) = event else {
        panic!("expected normalized row batch event");
    };
    let instruments = rows
        .iter()
        .map(|tick| tick.instrument_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(instruments, expected_instruments);
    assert!(rows.iter().all(|tick| tick.dt_ns > 0));
    assert!(rows.iter().all(|tick| tick.localtime_ns == 0));
    assert!(rows.iter().all(|tick| tick.source_emit_ns == 0));
}
