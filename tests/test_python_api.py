import zippy_openctp


def test_tick_data_schema_is_exposed():
    schema = zippy_openctp.TickDataSchema()

    assert schema is not None
    assert schema.names[:5] == [
        "instrument_id",
        "exchange_id",
        "trading_day",
        "action_day",
        "dt",
    ]


def test_openctp_source_exposes_config_status_and_metrics():
    source = zippy_openctp.OpenCtpMarketDataSource(
        front="tcp://127.0.0.1:12345",
        broker_id="9999",
        user_id="000001",
        password="secret",
        instruments=["IF2506"],
    )

    config = source.config()
    metrics = source.metrics()

    assert config["front"] == "tcp://127.0.0.1:12345"
    assert config["rows_per_batch"] == 1
    assert config["flush_interval_ms"] == 0
    assert config["password"] == "***redacted***"
    assert source.status() == "created"
    assert metrics["ticks_received_total"] == 0
    assert metrics["ticks_emitted_total"] == 0
