import importlib
from pathlib import Path
import sys
import time
from types import SimpleNamespace

import pytest
import zippy
import zippy_openctp
import pyarrow as pa
from zippy_openctp import _internal as zippy_openctp_internal


def start_master_server(tmp_path: Path) -> tuple[zippy.MasterServer, str]:
    control_endpoint = str(tmp_path / "zippy-master.sock")
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    server.start()
    return server, control_endpoint


def test_tick_data_schema_is_exposed():
    schema = zippy_openctp.TickDataSchema()

    assert schema is not None
    assert schema.names[:7] == [
        "instrument_id",
        "exchange_id",
        "trading_day",
        "action_day",
        "dt",
        "localtime_ns",
        "source_emit_ns",
    ]
    assert schema.field("dt").type == pa.timestamp("ns", tz="Asia/Shanghai")
    assert schema.field("localtime_ns").type == pa.int64()
    assert schema.field("source_emit_ns").type == pa.int64()


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
    assert "rows_per_batch" not in config
    assert "flush_interval_ms" not in config
    assert "data_path_mode" not in config
    assert "segment_ingress_enabled" not in config
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


def test_openctp_market_generator_source_exposes_config_status_and_metrics():
    source = zippy_openctp.OpenCtpMarketGeneratorSource(
        instruments=["IF2606", "IH2606"],
        interval_ms=10,
        seed=42,
        max_ticks=4,
    )

    config = source.config()
    metrics = source.metrics()

    assert config["instruments"] == ["IF2606", "IH2606"]
    assert config["interval_ms"] == 10
    assert config["seed"] == 42
    assert config["max_ticks"] == 4
    assert config["base_price"] == 4000.0
    assert config["price_step"] == 0.2
    assert source.status() == "created"
    assert metrics["ticks_received_total"] == 0
    assert metrics["ticks_emitted_total"] == 0
    assert metrics["batches_emitted_total"] == 0


def test_openctp_sources_expose_pipeline_metadata_contract():
    market_source = zippy_openctp.OpenCtpMarketDataSource(
        front="tcp://127.0.0.1:12345",
        broker_id="9999",
        user_id="000001",
        password="secret",
        instruments=["IF2506"],
    )
    generator_source = zippy_openctp.OpenCtpMarketGeneratorSource(
        instruments=["IF2606"],
        interval_ms=10,
    )

    assert market_source._zippy_source_name() == "openctp-market-data-source"
    assert market_source._zippy_source_type() == "openctp"
    assert market_source._zippy_source_mode() == "pipeline"
    assert market_source._zippy_output_schema() == zippy_openctp.TickDataSchema()

    assert generator_source._zippy_source_name() == "openctp-market-generator-source"
    assert generator_source._zippy_source_type() == "openctp.generator"
    assert generator_source._zippy_source_mode() == "pipeline"
    assert generator_source._zippy_output_schema() == zippy_openctp.TickDataSchema()


def test_openctp_market_generator_source_can_be_used_as_stream_table_source():
    source = zippy_openctp.OpenCtpMarketGeneratorSource(
        instruments=["IF2606", "IH2606"],
        interval_ms=10,
        max_ticks=4,
    )

    engine = zippy.StreamTableEngine(
        name="openctp_generated_ticks",
        source=source,
        input_schema=zippy_openctp.TickDataSchema(),
        target=zippy.NullPublisher(),
    )

    assert engine is not None
    assert engine.config()["source_linked"] is True


def test_openctp_generator_pipeline_feeds_query_tail(tmp_path):
    reset_default_master = getattr(zippy, "_reset_default_master_for_test", None)
    if reset_default_master is not None:
        reset_default_master()

    server, control_endpoint = start_master_server(tmp_path)
    client = zippy.connect(uri=control_endpoint)

    class RecordingMaster:
        def __init__(self, inner):
            self.inner = inner
            self.source_records = []

        def process_id(self):
            return self.inner.process_id()

        def register_process(self, app):
            return self.inner.register_process(app)

        def register_stream(self, stream_name, schema, buffer_size, frame_size):
            return self.inner.register_stream(stream_name, schema, buffer_size, frame_size)

        def register_source(self, source_name, source_type, output_stream, config):
            self.source_records.append((source_name, source_type, output_stream, config))
            return self.inner.register_source(source_name, source_type, output_stream, config)

        def publish_segment_descriptor(self, stream_name, descriptor):
            return self.inner.publish_segment_descriptor(stream_name, descriptor)

    source = zippy_openctp.OpenCtpMarketGeneratorSource(
        instruments=["IF2606", "IH2606"],
        interval_ms=1,
        seed=42,
    )
    master = RecordingMaster(client)
    pipeline = zippy.Pipeline("openctp_ingest", master=master).source(source).stream_table(
        "ctp_ticks"
    )

    try:
        assert master.source_records == [
            ("openctp-market-generator-source", "openctp.generator", "ctp_ticks", {}),
        ]

        pipeline.start()
        deadline = time.time() + 2.0
        table = zippy.read_table("ctp_ticks", master=client, wait=True, timeout="2s")
        latest = table.tail(10)
        while latest.num_rows < 4 and time.time() < deadline:
            time.sleep(0.01)
            latest = table.tail(10)

        assert latest.num_rows >= 4
        assert set(latest.column("instrument_id").to_pylist()) == {"IF2606", "IH2606"}
        assert all(price > 0.0 for price in latest.column("last_price").to_pylist())
    finally:
        pipeline.stop()
        if reset_default_master is not None:
            reset_default_master()
        server.stop()


def test_openctp_source_rejects_legacy_data_path_knobs():
    kwargs = {
        "front": "tcp://127.0.0.1:12345",
        "broker_id": "9999",
        "user_id": "000001",
        "password": "secret",
        "instruments": ["IF2506"],
    }

    with pytest.raises(TypeError):
        zippy_openctp.OpenCtpMarketDataSource(**kwargs, data_path_mode="batch")

    with pytest.raises(TypeError):
        zippy_openctp.OpenCtpMarketDataSource(**kwargs, segment_ingress_enabled=True)

    with pytest.raises(TypeError):
        zippy_openctp.OpenCtpMarketDataSource(**kwargs, rows_per_batch=2)

    with pytest.raises(TypeError):
        zippy_openctp.OpenCtpMarketDataSource(**kwargs, flush_interval_ms=10)


def test_openctp_source_accepts_segment_descriptor_publisher_callback():
    published = []

    source = zippy_openctp.OpenCtpMarketDataSource(
        front="tcp://127.0.0.1:12345",
        broker_id="9999",
        user_id="000001",
        password="secret",
        instruments=["IF2506"],
        segment_descriptor_publisher=published.append,
    )

    config = source.config()

    assert published == []
    assert "data_path_mode" not in config
    assert "segment_ingress_enabled" not in config


def test_remote_pipeline_segment_descriptor_publisher_forwards_to_master():
    examples_dir = Path(__file__).resolve().parents[1] / "examples"
    sys.path.insert(0, str(examples_dir))
    try:
        md_to_remote_pipeline = importlib.import_module("md_to_remote_pipeline")
    finally:
        sys.path.remove(str(examples_dir))

    class FakeMaster:
        def __init__(self) -> None:
            self.published = []

        def publish_segment_descriptor(self, stream_name: str, descriptor: object) -> None:
            self.published.append((stream_name, descriptor))

    master = FakeMaster()
    publisher = md_to_remote_pipeline.build_segment_descriptor_publisher(
        master,
        "openctp_ticks",
    )

    publisher(
        b'{"magic":"zippy.segment.active","version":1,"schema_id":7,'
        b'"row_capacity":64,"shm_os_id":"/tmp/zippy-segment",'
        b'"payload_offset":64,"committed_row_count_offset":40,'
        b'"segment_id":1,"generation":0}'
    )

    assert master.published == [
        (
            "openctp_ticks",
            {
                "magic": "zippy.segment.active",
                "version": 1,
                "schema_id": 7,
                "row_capacity": 64,
                "shm_os_id": "/tmp/zippy-segment",
                "payload_offset": 64,
                "committed_row_count_offset": 40,
                "segment_id": 1,
                "generation": 0,
            },
        )
    ]


def test_remote_pipeline_build_master_registers_source_owner(monkeypatch, tmp_path):
    examples_dir = Path(__file__).resolve().parents[1] / "examples"
    sys.path.insert(0, str(examples_dir))
    try:
        md_to_remote_pipeline = importlib.import_module("md_to_remote_pipeline")
    finally:
        sys.path.remove(str(examples_dir))

    class FakeMaster:
        def __init__(self, control_endpoint: str) -> None:
            self.control_endpoint = control_endpoint
            self.calls = []

        def register_process(self, app: str) -> None:
            self.calls.append(("register_process", app))

        def register_stream(
            self,
            stream_name: str,
            schema: object,
            buffer_size: int,
            frame_size: int,
        ) -> None:
            self.calls.append(("register_stream", stream_name, buffer_size, frame_size))

        def register_source(
            self,
            source_name: str,
            source_type: str,
            output_stream: str,
            config: object,
        ) -> None:
            self.calls.append(
                ("register_source", source_name, source_type, output_stream, config)
            )

    created = []

    def fake_master_client(control_endpoint: str) -> FakeMaster:
        master = FakeMaster(control_endpoint)
        created.append(master)
        return master

    monkeypatch.setattr(md_to_remote_pipeline.zippy, "MasterClient", fake_master_client)
    args = SimpleNamespace(
        broker_id="9999",
        buffer_size=1024,
        control_endpoint=str(tmp_path / "master.sock"),
        flow_path=".cache/openctp/md",
        front="tcp://127.0.0.1:12345",
        frame_size=4096,
        instruments="IF2606",
        start_master=False,
        stream_name="openctp_ticks",
        user_id="000001",
    )

    master, server = md_to_remote_pipeline.build_master(args)

    assert server is None
    assert master is created[0]
    assert created[0].calls[0] == ("register_process", "openctp_md_to_bus")
    assert created[0].calls[1] == ("register_stream", "openctp_ticks", 1024, 4096)
    source_call = created[0].calls[2]
    assert source_call[:4] == (
        "register_source",
        "openctp_md",
        "openctp",
        "openctp_ticks",
    )
    assert source_call[4]["data_path"] == "segment"
    assert "rows_per_batch" not in source_call[4]
    assert "flush_interval_ms" not in source_call[4]
    assert source_call[4]["password"] == "***redacted***"


def test_remote_pipeline_build_source_uses_segment_path_by_default():
    examples_dir = Path(__file__).resolve().parents[1] / "examples"
    sys.path.insert(0, str(examples_dir))
    try:
        md_to_remote_pipeline = importlib.import_module("md_to_remote_pipeline")
    finally:
        sys.path.remove(str(examples_dir))

    args = SimpleNamespace(
        broker_id="9999",
        flow_path=".cache/openctp/md",
        front="tcp://127.0.0.1:12345",
        instruments="IF2606",
        stream_name="openctp_ticks",
        user_id="000001",
        password="secret",
    )

    source = md_to_remote_pipeline.build_source(args)
    config = source.config()

    assert "data_path_mode" not in config
    assert "segment_ingress_enabled" not in config


def test_openctp_segment_reader_reads_descriptor_rows():
    writer = zippy_openctp_internal.OpenCtpSegmentTestWriter()
    writer.append_tick("IF2606", 4112.5)

    reader = zippy_openctp.OpenCtpSegmentReader(writer.descriptor())
    first = reader.read_available()

    assert isinstance(first, pa.RecordBatch)
    assert first.num_rows == 1
    assert first.column("instrument_id").to_pylist() == ["IF2606"]
    assert first.column("last_price").to_pylist() == [4112.5]
    assert reader.read_available() is None

    writer.append_tick("IF2606", 4113.5)
    second = reader.read_available()

    assert isinstance(second, pa.RecordBatch)
    assert second.num_rows == 1
    assert second.column("last_price").to_pylist() == [4113.5]


def test_openctp_segment_reader_updates_descriptor_after_rollover():
    writer = zippy_openctp_internal.OpenCtpSegmentTestWriter()
    writer.append_tick("IF2606", 4112.5)
    reader = zippy_openctp.OpenCtpSegmentReader(writer.descriptor())
    assert reader.read_available().num_rows == 1

    for index in range(64):
        writer.append_tick("IF2606", 4200.0 + index)
    reader.update_descriptor(writer.descriptor())
    batch = reader.read_available()

    assert isinstance(batch, pa.RecordBatch)
    assert batch.column("last_price").to_pylist() == [4263.0]


def test_segment_reader_probe_builds_reader_from_master_descriptor():
    examples_dir = Path(__file__).resolve().parents[1] / "examples"
    sys.path.insert(0, str(examples_dir))
    try:
        segment_reader_probe = importlib.import_module("segment_reader_probe")
    finally:
        sys.path.remove(str(examples_dir))

    writer = zippy_openctp_internal.OpenCtpSegmentTestWriter()
    writer.append_tick("IF2606", 4112.5)

    class FakeMaster:
        def get_segment_descriptor(self, stream_name: str) -> dict[str, object] | None:
            assert stream_name == "openctp_ticks"
            return writer.descriptor()

    reader = segment_reader_probe.build_segment_reader(
        FakeMaster(),
        "openctp_ticks",
    )
    batch = reader.read_available()

    assert isinstance(batch, pa.RecordBatch)
    assert batch.column("last_price").to_pylist() == [4112.5]


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


def test_openctp_segment_primary_source_can_be_used_as_stream_table_source():
    source = zippy_openctp.OpenCtpMarketDataSource(
        front="tcp://127.0.0.1:12345",
        broker_id="9999",
        user_id="000001",
        password="secret",
        instruments=["IF2506"],
    )

    engine = zippy.StreamTableEngine(
        name="openctp_tick_table",
        source=source,
        input_schema=zippy_openctp.TickDataSchema(),
        target=zippy.NullPublisher(),
    )

    assert engine is not None
    assert engine.config()["source_linked"] is True


def test_runtime_handle_join_and_stop_release_gil():
    assert zippy_openctp_internal.runtime_handle_releases_gil("join") is True
    assert zippy_openctp_internal.runtime_handle_releases_gil("stop") is True
