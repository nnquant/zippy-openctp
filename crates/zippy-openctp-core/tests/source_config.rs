use zippy_openctp_core::{OpenCtpMarketDataSourceConfig, OpenCtpSourceMetrics};

#[test]
fn source_config_defaults_to_single_tick_publish() {
    let config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["IF2506".to_string()],
        ".cache/openctp/md".to_string(),
    );

    assert_eq!(config.password, "secret");
    assert!(config.reconnect);
    assert_eq!(config.login_timeout_sec, 10);
    assert_eq!(config.rows_per_batch, 1);
    assert_eq!(config.flush_interval_ms, 0);
}

#[test]
fn source_metrics_snapshot_defaults_to_zero_counters() {
    let metrics = OpenCtpSourceMetrics::default();

    assert_eq!(metrics.ticks_received_total, 0);
    assert_eq!(metrics.ticks_emitted_total, 0);
    assert_eq!(metrics.batches_emitted_total, 0);
    assert_eq!(metrics.reconnects_total, 0);
    assert_eq!(metrics.login_failures_total, 0);
    assert_eq!(metrics.subscribe_failures_total, 0);
}

#[test]
fn source_config_debug_redacts_password() {
    let config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["IF2506".to_string()],
        ".cache/openctp/md".to_string(),
    );

    let rendered = format!("{config:?}");

    assert!(rendered.contains("***redacted***"));
    assert!(!rendered.contains("secret"));
}
