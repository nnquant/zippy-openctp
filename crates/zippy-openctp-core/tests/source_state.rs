use zippy_openctp_core::{OpenCtpSourceMetrics, OpenCtpSourceStatus};

#[test]
fn source_status_starts_created_and_metrics_are_zeroed() {
    assert_eq!(OpenCtpSourceStatus::Created.as_str(), "created");

    let metrics = OpenCtpSourceMetrics::default();
    assert_eq!(metrics.ticks_received_total, 0);
    assert_eq!(metrics.subscribe_failures_total, 0);
}
