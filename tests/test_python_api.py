import zippy
import zippy_openctp
from zippy_openctp import _internal as zippy_openctp_internal


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
    assert set(metrics) == {
        "ticks_received_total",
        "ticks_emitted_total",
        "batches_emitted_total",
        "reconnects_total",
        "login_failures_total",
        "subscribe_failures_total",
    }
    assert metrics["ticks_received_total"] == 0
    assert metrics["ticks_emitted_total"] == 0
    assert metrics["batches_emitted_total"] == 0
    assert metrics["reconnects_total"] == 0
    assert metrics["login_failures_total"] == 0
    assert metrics["subscribe_failures_total"] == 0


def test_openctp_source_can_be_used_as_zippy_timeseries_source():
    source = zippy_openctp.OpenCtpMarketDataSource(
        front="tcp://127.0.0.1:12345",
        broker_id="9999",
        user_id="000001",
        password="secret",
        instruments=["IF2506"],
    )

    engine = zippy.TimeSeriesEngine(
        name="openctp_bar_1m",
        source=source,
        input_schema=zippy_openctp.TickDataSchema(),
        id_column="instrument_id",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="last_price", output="close")],
        target=zippy.NullPublisher(),
    )

    assert engine is not None
    assert engine.config()["source_linked"] is True


def test_runtime_handle_join_and_stop_release_gil():
    assert zippy_openctp_internal.runtime_handle_releases_gil("join") is True
    assert zippy_openctp_internal.runtime_handle_releases_gil("stop") is True
