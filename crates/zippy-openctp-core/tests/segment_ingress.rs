use zippy_openctp_core::{normalize_tick, OpenCtpSegmentIngress, RawTickSnapshot};

#[test]
fn normalized_tick_is_written_into_active_segment() {
    let mut ingress = OpenCtpSegmentIngress::for_test().unwrap();
    let tick = RawTickSnapshot::for_test("rb2510", 4123.5);
    let row = normalize_tick(&tick).unwrap();

    ingress.write_row(&row).unwrap();

    let snapshot = ingress.active_snapshot().unwrap();
    assert_eq!(snapshot.committed_row_count, 1);
    assert_eq!(snapshot.last_instrument_id.as_deref(), Some("rb2510"));
}
