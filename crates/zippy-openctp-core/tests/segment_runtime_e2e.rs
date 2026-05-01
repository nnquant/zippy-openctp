use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array, StringArray};
use zippy_core::{Result as CoreResult, SegmentTableView, Source, SourceEvent, SourceSink};
use zippy_openctp_core::{
    normalize_tick, openctp_segment_schema, FakeMdDriver, FakeMdDriverHandle,
    OpenCtpMarketDataSource, OpenCtpMarketDataSourceConfig, OpenCtpSegmentDescriptorPublisher,
    OpenCtpSegmentIngress, RawTickSnapshot, TickSchemaType, TICK_SCHEMA_FIELDS,
};
use zippy_operator_runtime::PartitionReader;
use zippy_segment_store::{
    compile_schema, ActiveSegmentDescriptor, ActiveSegmentReader, ColumnSpec, ColumnType,
    LayoutPlan,
};

const SOURCE_SEGMENT_ROW_CAPACITY: usize = 32768;

#[test]
fn source_callback_advances_segment_runtime() {
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
fn source_emits_segment_table_views_without_record_batch_bridge() {
    let sink = Arc::new(RecordingSink::default());
    let (source, driver) = make_test_source_with_segment_ingress();
    let handle = Box::new(source).start(sink.clone()).unwrap();

    driver.emit_trade_tick("rb2510", 4123.5).unwrap();
    driver.emit_stop().unwrap();

    handle.join().unwrap();

    assert_eq!(sink.data_view_kinds(), vec!["segment"]);
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
    let data_rows = sink.data_rows();
    assert_eq!(data_rows.iter().sum::<usize>(), 65);
    assert!(data_rows.iter().all(|rows| *rows > 0));
    assert_eq!(snapshot.instrument_id.as_deref(), Some("rb2510"));
    assert_eq!(snapshot.last_price, Some(4123.5 + 64.0));
}

#[test]
fn source_publishes_segment_descriptor_when_segment_ingress_starts() {
    let sink = Arc::new(RecordingSink::default());
    let publisher = Arc::new(RecordingSegmentDescriptorPublisher::default());
    let (source, driver) = make_test_source_with_segment_ingress();
    let source = source.with_segment_descriptor_publisher(publisher.clone());
    let handle = Box::new(source).start(sink).unwrap();

    driver.emit_stop().unwrap();
    handle.join().unwrap();

    let envelopes = publisher.envelopes();
    assert_eq!(envelopes.len(), 1);
    let descriptor = active_descriptor_from_envelope_for_test(&envelopes[0]);
    assert_eq!(descriptor.segment_id(), 1);
    assert_eq!(descriptor.generation(), 0);
}

#[test]
fn source_publishes_segment_descriptor_after_segment_rollover() {
    let sink = Arc::new(RecordingSink::default());
    let publisher = Arc::new(RecordingSegmentDescriptorPublisher::default());
    let (source, driver) = make_test_source_with_segment_ingress();
    let source = source.with_segment_descriptor_publisher(publisher.clone());
    let handle = Box::new(source).start(sink).unwrap();

    for index in 0..=SOURCE_SEGMENT_ROW_CAPACITY {
        driver
            .emit_trade_tick("rb2510", 4123.5 + index as f64)
            .unwrap();
    }
    driver.emit_stop().unwrap();
    handle.join().unwrap();

    let envelopes = publisher.envelopes();
    assert_eq!(envelopes.len(), 2);
    let first = active_descriptor_from_envelope_for_test(&envelopes[0]);
    let second = active_descriptor_from_envelope_for_test(&envelopes[1]);
    assert_eq!(first.segment_id(), 1);
    assert_eq!(first.generation(), 0);
    assert_eq!(second.segment_id(), 2);
    assert_eq!(second.generation(), 1);
}

#[test]
fn source_published_segment_descriptor_supports_live_active_reader_after_rollover() {
    let sink = Arc::new(RecordingSink::default());
    let publisher = Arc::new(RecordingSegmentDescriptorPublisher::default());
    let (source, driver) = make_test_source_with_segment_ingress();
    let source = source.with_segment_descriptor_publisher(publisher.clone());
    let handle = Box::new(source).start(sink).unwrap();

    let envelopes = wait_for_envelopes(&publisher, 1);
    let mut reader = active_reader_from_envelope_for_test(&envelopes[0]);
    driver.emit_trade_tick("rb2510", 4123.5).unwrap();

    let first = wait_for_reader_batch(&mut reader);
    assert_eq!(first.num_rows(), 1);
    assert_eq!(batch_instrument_id(&first), "rb2510");
    assert_eq!(batch_last_price(&first), 4123.5);

    for index in 0..SOURCE_SEGMENT_ROW_CAPACITY {
        driver
            .emit_trade_tick("rb2510", 4200.0 + index as f64)
            .unwrap();
    }

    let envelopes = wait_for_envelopes(&publisher, 2);
    update_active_reader_from_envelope_for_test(&mut reader, &envelopes[1]);
    let second = wait_for_reader_batch(&mut reader);
    assert_eq!(second.num_rows(), 1);
    assert_eq!(batch_instrument_id(&second), "rb2510");
    assert_eq!(
        batch_last_price(&second),
        4200.0 + SOURCE_SEGMENT_ROW_CAPACITY as f64 - 1.0
    );

    driver.emit_stop().unwrap();
    handle.join().unwrap();
}

fn make_test_source_with_segment_ingress() -> (OpenCtpMarketDataSource, FakeMdDriverHandle) {
    let (driver, handle) = FakeMdDriver::pair();
    let config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["rb2510".to_string()],
        ".cache/openctp/md".to_string(),
    );

    (
        OpenCtpMarketDataSource::from_driver(config, Box::new(driver)),
        handle,
    )
}

#[test]
fn source_segment_primary_emits_batches_from_segment_reader() {
    let sink = Arc::new(RecordingSink::default());
    let (driver, handle) = FakeMdDriver::pair();
    let config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["rb2510".to_string()],
        ".cache/openctp/md".to_string(),
    );

    let source = OpenCtpMarketDataSource::from_driver(config, Box::new(driver));
    let runtime = Box::new(source).start(sink.clone()).unwrap();

    handle.emit_trade_tick("rb2510", 4123.5).unwrap();
    handle.emit_stop().unwrap();
    runtime.join().unwrap();

    let batch = sink.single_batch().unwrap();
    assert_eq!(sink.data_rows(), vec![1]);
    assert_eq!(batch_instrument_id(&batch), "rb2510");
    assert_eq!(batch_last_price(&batch), 4123.5);
    assert!(batch_localtime_ns(&batch) > 0);
    assert!(batch_source_emit_ns(&batch) >= batch_localtime_ns(&batch));
}

#[test]
fn segment_ingress_supports_in_process_partition_reader() {
    let mut ingress = OpenCtpSegmentIngress::for_test().unwrap();
    let (store, handle) = ingress.reader_context_for_test();
    let mut reader = PartitionReader::new(store, handle, "reader-a").unwrap();

    let tick = RawTickSnapshot::for_test("rb2510", 4123.5);
    let mut row = normalize_tick(&tick).unwrap();
    row.localtime_ns = 1_700_000_000_000_000_100;
    row.source_emit_ns = 1_700_000_000_000_000_200;

    ingress.write_row(&row).unwrap();
    assert!(reader.wait_timeout(Duration::from_millis(20)).unwrap());

    let span = reader.read_available().unwrap().expect("expected span");
    let batch = span.as_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch_instrument_id(&batch), "rb2510");
    assert_eq!(batch_last_price(&batch), 4123.5);
    assert_eq!(batch_localtime_ns(&batch), row.localtime_ns);
    assert_eq!(batch_source_emit_ns(&batch), row.source_emit_ns);
}

#[derive(Default)]
struct RecordingSink {
    data_rows: Mutex<Vec<usize>>,
    data_view_kinds: Mutex<Vec<&'static str>>,
    batches: Mutex<Vec<arrow::record_batch::RecordBatch>>,
}

#[derive(Default)]
struct RecordingSegmentDescriptorPublisher {
    envelopes: Mutex<Vec<Vec<u8>>>,
}

impl RecordingSegmentDescriptorPublisher {
    fn envelopes(&self) -> Vec<Vec<u8>> {
        self.envelopes.lock().unwrap().clone()
    }
}

impl OpenCtpSegmentDescriptorPublisher for RecordingSegmentDescriptorPublisher {
    fn publish(&self, descriptor_envelope: Vec<u8>) -> CoreResult<()> {
        self.envelopes.lock().unwrap().push(descriptor_envelope);
        Ok(())
    }
}

impl RecordingSink {
    fn data_rows(&self) -> Vec<usize> {
        self.data_rows.lock().unwrap().clone()
    }

    fn data_view_kinds(&self) -> Vec<&'static str> {
        self.data_view_kinds.lock().unwrap().clone()
    }

    fn single_batch(&self) -> Result<arrow::record_batch::RecordBatch, &'static str> {
        let batches = self.batches.lock().unwrap();
        if batches.len() != 1 {
            return Err("expected exactly one batch");
        }
        Ok(batches[0].clone())
    }
}

fn active_descriptor_from_envelope_for_test(bytes: &[u8]) -> ActiveSegmentDescriptor {
    let schema = compile_openctp_segment_schema_for_test();
    let layout = LayoutPlan::for_schema(&schema, SOURCE_SEGMENT_ROW_CAPACITY).unwrap();
    ActiveSegmentDescriptor::from_envelope_bytes(bytes, schema, layout).unwrap()
}

fn active_reader_from_envelope_for_test(bytes: &[u8]) -> ActiveSegmentReader {
    let schema = openctp_segment_schema().unwrap();
    let layout = LayoutPlan::for_schema(&schema, SOURCE_SEGMENT_ROW_CAPACITY).unwrap();
    ActiveSegmentReader::from_descriptor_envelope(bytes, schema, layout).unwrap()
}

fn update_active_reader_from_envelope_for_test(reader: &mut ActiveSegmentReader, bytes: &[u8]) {
    let schema = openctp_segment_schema().unwrap();
    let layout = LayoutPlan::for_schema(&schema, SOURCE_SEGMENT_ROW_CAPACITY).unwrap();
    reader
        .update_descriptor_envelope(bytes, schema, layout)
        .unwrap();
}

fn wait_for_envelopes(
    publisher: &RecordingSegmentDescriptorPublisher,
    expected_len: usize,
) -> Vec<Vec<u8>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let envelopes = publisher.envelopes();
        if envelopes.len() >= expected_len {
            return envelopes;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for segment descriptor envelopes"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_for_reader_batch(reader: &mut ActiveSegmentReader) -> arrow::record_batch::RecordBatch {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        if let Some(span) = reader.read_available().unwrap() {
            return span.as_record_batch().unwrap();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for active segment rows"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn compile_openctp_segment_schema_for_test() -> zippy_segment_store::CompiledSchema {
    let columns = TICK_SCHEMA_FIELDS
        .iter()
        .map(|field| {
            let data_type = match field.data_type {
                TickSchemaType::Utf8 => ColumnType::Utf8,
                TickSchemaType::TimestampNsShanghai => ColumnType::TimestampNsTz("Asia/Shanghai"),
                TickSchemaType::Float64 => ColumnType::Float64,
                TickSchemaType::Int64 => ColumnType::Int64,
            };
            if field.nullable {
                ColumnSpec::nullable(field.name, data_type)
            } else {
                ColumnSpec::new(field.name, data_type)
            }
        })
        .collect::<Vec<_>>();
    compile_schema(&columns).unwrap()
}

impl SourceSink for RecordingSink {
    fn emit(&self, event: SourceEvent) -> CoreResult<()> {
        if let SourceEvent::Data(batch) = event {
            let view_kind = match &batch {
                SegmentTableView::Segment(_) => "segment",
                SegmentTableView::Memory(_) => "memory",
            };
            self.data_rows.lock().unwrap().push(batch.num_rows());
            self.data_view_kinds.lock().unwrap().push(view_kind);
            self.batches.lock().unwrap().push(batch.to_record_batch()?);
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
