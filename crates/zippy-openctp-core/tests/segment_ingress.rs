use zippy_openctp_core::{normalize_tick, OpenCtpSegmentIngress, RawTickSnapshot};

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
