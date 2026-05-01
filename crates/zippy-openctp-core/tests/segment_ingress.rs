use arrow::array::{Float64Array, Int64Array, StringArray};
use zippy_openctp_core::{
    normalize_tick, openctp_segment_schema, OpenCtpSegmentIngress, RawTickSnapshot,
    TICK_SCHEMA_FIELDS,
};
use zippy_segment_store::{ActiveSegmentReader, LayoutPlan};

#[test]
fn normalized_tick_is_written_into_active_segment_with_all_segment_columns() {
    let mut ingress = OpenCtpSegmentIngress::for_test().unwrap();
    let tick = RawTickSnapshot::for_test("rb2510", 4123.5);
    let mut row = normalize_tick(&tick).unwrap();
    row.localtime_ns = 1_700_000_000_000_000_100;
    row.source_emit_ns = 1_700_000_000_000_000_200;

    ingress.write_row(&row).unwrap();

    let snapshot = ingress.active_snapshot().unwrap();
    assert_eq!(snapshot.committed_row_count, 1);
    assert_eq!(snapshot.instrument_id.as_deref(), Some("rb2510"));
    assert_eq!(snapshot.dt_ns, Some(row.dt_ns));
    assert_eq!(snapshot.localtime_ns, Some(row.localtime_ns));
    assert_eq!(snapshot.source_emit_ns, Some(row.source_emit_ns));
    assert_eq!(snapshot.last_price, Some(4123.5));

    let (_, partition) = ingress.reader_context_for_test();
    let batch = partition.debug_snapshot_record_batch().unwrap();
    assert_eq!(batch.num_columns(), 15);
    assert_eq!(utf8_value(&batch, "exchange_id"), "SHFE");
    assert_eq!(utf8_value(&batch, "trading_day"), "20260408");
    assert_eq!(utf8_value(&batch, "action_day"), "20260408");
    assert_eq!(i64_value(&batch, "volume"), 1);
    assert_eq!(f64_value(&batch, "turnover"), 41235.0);
    assert_eq!(f64_value(&batch, "open_interest"), 100.0);
    assert_eq!(f64_value(&batch, "bid_price_1"), 4123.0);
    assert_eq!(i64_value(&batch, "bid_volume_1"), 5);
    assert_eq!(f64_value(&batch, "ask_price_1"), 4124.0);
    assert_eq!(i64_value(&batch, "ask_volume_1"), 6);
}

#[test]
fn segment_ingress_exports_active_descriptor_envelope_for_cross_process_reader() {
    let mut ingress = OpenCtpSegmentIngress::for_test().unwrap();
    let tick = RawTickSnapshot::for_test("rb2510", 4123.5);
    let mut row = normalize_tick(&tick).unwrap();
    row.localtime_ns = 1_700_000_000_000_000_100;
    row.source_emit_ns = 1_700_000_000_000_000_200;

    ingress.write_row(&row).unwrap();

    let bytes = ingress.active_descriptor_envelope_bytes().unwrap();
    let schema = openctp_segment_schema().unwrap();
    let layout = LayoutPlan::for_schema(&schema, 64).unwrap();
    let mut reader = ActiveSegmentReader::from_descriptor_envelope(&bytes, schema, layout).unwrap();
    let span = reader.read_available().unwrap().expect("expected row");
    let batch = span.as_record_batch().unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(utf8_value(&batch, "instrument_id"), "rb2510");
}

#[test]
fn openctp_segment_schema_is_exported_for_descriptor_readers() {
    let schema = openctp_segment_schema().unwrap();

    assert_eq!(schema.columns().len(), TICK_SCHEMA_FIELDS.len());
    assert_eq!(schema.columns()[0].name, "instrument_id");
}

#[test]
fn segment_ingress_rolls_over_after_sixty_four_rows_and_keeps_last_row_visible() {
    let mut ingress = OpenCtpSegmentIngress::for_test().unwrap();

    for index in 0..65 {
        let tick = RawTickSnapshot::for_test("rb2510", 4123.5 + index as f64);
        let mut row = normalize_tick(&tick).unwrap();
        row.localtime_ns = 1_700_000_000_000_000_000 + index as i64;
        row.source_emit_ns = row.localtime_ns + 10;
        ingress.write_row(&row).unwrap();
    }

    let snapshot = ingress.active_snapshot().unwrap();
    assert_eq!(snapshot.committed_row_count, 65);
    assert_eq!(snapshot.instrument_id.as_deref(), Some("rb2510"));
    assert_eq!(snapshot.last_price, Some(4123.5 + 64.0));
    assert_eq!(snapshot.localtime_ns, Some(1_700_000_000_000_000_064));
    assert_eq!(snapshot.source_emit_ns, Some(1_700_000_000_000_000_074));
}

#[test]
fn segment_ingress_writes_multiple_rows_with_one_batch_call() {
    let mut ingress = OpenCtpSegmentIngress::for_test().unwrap();
    let rows = (0..3)
        .map(|index| normalized_row("rb2510", 4123.5 + index as f64, index))
        .collect::<Vec<_>>();

    let descriptor_changed = ingress.write_rows(&rows).unwrap();

    assert!(!descriptor_changed);
    let snapshot = ingress.active_snapshot().unwrap();
    assert_eq!(snapshot.committed_row_count, 3);
    assert_eq!(snapshot.last_price, Some(4125.5));

    let (_, partition) = ingress.reader_context_for_test();
    let batch = partition.debug_snapshot_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(utf8_value_at(&batch, "instrument_id", 0), "rb2510");
    assert_eq!(utf8_value_at(&batch, "instrument_id", 2), "rb2510");
    assert_eq!(f64_value_at(&batch, "last_price", 0), 4123.5);
    assert_eq!(f64_value_at(&batch, "last_price", 2), 4125.5);
}

#[test]
fn segment_ingress_batch_write_rolls_over_and_keeps_last_row_visible() {
    let mut ingress = OpenCtpSegmentIngress::for_test().unwrap();
    let rows = (0..65)
        .map(|index| normalized_row("rb2510", 4123.5 + index as f64, index))
        .collect::<Vec<_>>();

    let descriptor_changed = ingress.write_rows(&rows).unwrap();

    assert!(descriptor_changed);
    let snapshot = ingress.active_snapshot().unwrap();
    assert_eq!(snapshot.committed_row_count, 65);
    assert_eq!(snapshot.instrument_id.as_deref(), Some("rb2510"));
    assert_eq!(snapshot.last_price, Some(4123.5 + 64.0));
    assert_eq!(snapshot.localtime_ns, Some(1_700_000_000_000_000_064));
    assert_eq!(snapshot.source_emit_ns, Some(1_700_000_000_000_000_074));

    let (_, partition) = ingress.reader_context_for_test();
    let batch = partition.debug_snapshot_record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(f64_value_at(&batch, "last_price", 0), 4123.5 + 64.0);
}

fn normalized_row(
    instrument_id: &str,
    last_price: f64,
    offset: usize,
) -> zippy_openctp_core::NormalizedTickRow {
    let tick = RawTickSnapshot::for_test(instrument_id, last_price);
    let mut row = normalize_tick(&tick).unwrap();
    row.localtime_ns = 1_700_000_000_000_000_000 + offset as i64;
    row.source_emit_ns = row.localtime_ns + 10;
    row
}

fn utf8_value(batch: &arrow::record_batch::RecordBatch, column_name: &str) -> String {
    utf8_value_at(batch, column_name, 0)
}

fn utf8_value_at(
    batch: &arrow::record_batch::RecordBatch,
    column_name: &str,
    row: usize,
) -> String {
    batch
        .column_by_name(column_name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(row)
        .to_string()
}

fn i64_value(batch: &arrow::record_batch::RecordBatch, column_name: &str) -> i64 {
    batch
        .column_by_name(column_name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn f64_value(batch: &arrow::record_batch::RecordBatch, column_name: &str) -> f64 {
    f64_value_at(batch, column_name, 0)
}

fn f64_value_at(batch: &arrow::record_batch::RecordBatch, column_name: &str, row: usize) -> f64 {
    batch
        .column_by_name(column_name)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(row)
}
