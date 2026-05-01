"""
Subscribe the bus-backed MID_PRICE_DIFF_200_STD_200 factor stream and print updates.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import time

from _runtime_lease import ExampleRuntimeLoopState, advance_runtime_loop
import pyarrow as pa

import zippy
import zippy_openctp

DEFAULT_CONTROL_ENDPOINT = "~/.zippy/master.sock"
DEFAULT_SOURCE_STREAM_NAME = "openctp_mid_price_factors"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_LEVEL = "info"
DEFAULT_METRICS_INTERVAL_SEC = 5.0
DEFAULT_RECV_TIMEOUT_MS = 500


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the factor subscriber example.

    :returns: Parsed command-line arguments.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="subscribe bus MID_PRICE_DIFF_200_STD_200 factors and print latest values",
    )
    parser.add_argument(
        "--control-endpoint",
        default=DEFAULT_CONTROL_ENDPOINT,
        help=f"zippy-master control endpoint, default [{DEFAULT_CONTROL_ENDPOINT}]",
    )
    parser.add_argument(
        "--source-stream-name",
        default=DEFAULT_SOURCE_STREAM_NAME,
        help=f"factor bus stream name, default [{DEFAULT_SOURCE_STREAM_NAME}]",
    )
    parser.add_argument(
        "--recv-timeout-ms",
        type=int,
        default=DEFAULT_RECV_TIMEOUT_MS,
        help=f"subscriber receive timeout in milliseconds, default [{DEFAULT_RECV_TIMEOUT_MS}]",
    )
    parser.add_argument(
        "--log-dir",
        default=DEFAULT_LOG_DIR,
        help=f"log root directory, default [{DEFAULT_LOG_DIR}]",
    )
    parser.add_argument(
        "--log-level",
        default=DEFAULT_LOG_LEVEL,
        help=f"log level, default [{DEFAULT_LOG_LEVEL}]",
    )
    parser.add_argument(
        "--no-console-log",
        action="store_true",
        help="disable console log output and keep file logging only",
    )
    parser.add_argument(
        "--metrics-interval-sec",
        type=float,
        default=DEFAULT_METRICS_INTERVAL_SEC,
        help=f"heartbeat metrics interval in seconds, default [{DEFAULT_METRICS_INTERVAL_SEC}]",
    )
    args = parser.parse_args()
    if args.metrics_interval_sec <= 0:
        parser.error("--metrics-interval-sec must be positive")
    if args.recv_timeout_ms <= 0:
        parser.error("--recv-timeout-ms must be positive")
    return args


def factor_specs() -> list[object]:
    """
    Build the factor graph used to probe the remote factor schema.

    :returns: Ordered factor specifications.
    :rtype: list[object]
    """
    return [
        zippy.Expr(
            expression="(bid_price_1 + ask_price_1) / 2.0",
            output="mid_price",
        ),
        zippy.Expr(
            expression="TS_DIFF(mid_price, 200) / TS_STD(TS_DIFF(mid_price, 200), 200)",
            output="MID_PRICE_DIFF_200_STD_200",
        ),
    ]


def factor_schema() -> pa.Schema:
    """
    Probe the enriched factor output schema.

    :returns: Factor output schema.
    :rtype: pyarrow.Schema
    """
    engine = zippy.ReactiveStateEngine(
        name="openctp_mid_price_factor_probe",
        input_schema=zippy_openctp.schemas.TickDataSchema(),
        id_column="instrument_id",
        factors=factor_specs(),
        target=zippy.NullPublisher(),
    )
    return engine.output_schema()


def build_source(
    args: argparse.Namespace,
    master: zippy.MasterClient | None = None,
) -> zippy.BusStreamSource:
    """
    Build the factor bus stream source.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param master: Registered master client. When omitted, ``zippy.connect()`` is used.
    :type master: zippy.MasterClient | None
    :returns: Configured factor source.
    :rtype: zippy.BusStreamSource
    """
    master = master or zippy.master()
    return zippy.BusStreamSource(
        stream_name=args.source_stream_name,
        expected_schema=factor_schema(),
        master=master,
        mode=zippy.SourceMode.CONSUMER,
    )


def build_pipeline(
    source: zippy.BusStreamSource,
) -> zippy.StreamTableEngine:
    """
    Build the local stream-table bridge that keeps the source runtime alive.

    :param source: Pre-built factor source.
    :type source: zippy.BusStreamSource
    :returns: Configured stream-table engine.
    :rtype: zippy.StreamTableEngine
    """
    return zippy.StreamTableEngine(
        name="openctp_mid_price_factor_subscriber",
        source=source,
        input_schema=factor_schema(),
        target=zippy.NullPublisher(),
    )


def latest_row(batch: pa.RecordBatch) -> dict[str, object]:
    """
    Convert the last row of a batch into a Python dictionary.

    :param batch: Received batch.
    :type batch: pyarrow.RecordBatch
    :returns: Last-row snapshot.
    :rtype: dict[str, object]
    """
    columns = batch.to_pydict()
    last_index = batch.num_rows - 1
    return {name: values[last_index] for name, values in columns.items()}


def _format_dt(value: object) -> str:
    """
    Render a timestamp-like value for logs.

    :param value: Timestamp object from pyarrow conversion.
    :type value: object
    :returns: Human-readable UTC timestamp.
    :rtype: str
    """
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


if __name__ == "__main__":
    cli_args = parse_args()
    log_snapshot = zippy.setup_log(
        app="openctp_mid_price_factor_subscriber",
        level=cli_args.log_level,
        log_dir=cli_args.log_dir,
        to_console=not cli_args.no_console_log,
        to_file=True,
    )
    zippy.log_info(
        "openctp_factor_subscriber",
        "log_setup",
        f"initialized zippy logging log_snapshot=[{log_snapshot}]",
    )
    control_endpoint = str(Path(cli_args.control_endpoint).expanduser())
    master = zippy.connect(
        uri=control_endpoint,
        app="openctp_mid_price_factor_subscriber",
    )
    zippy.log_info(
        "openctp_factor_subscriber",
        "master_config",
        "initialized master client "
        f"control_endpoint=[{control_endpoint}] source_stream=[{cli_args.source_stream_name}]",
    )
    source = build_source(cli_args)
    zippy.log_info(
        "openctp_factor_subscriber",
        "source_config",
        "built factor source "
        f"control_endpoint=[{control_endpoint}] source_stream=[{cli_args.source_stream_name}]",
    )
    engine = build_pipeline(source)
    zippy.log_info(
        "openctp_factor_subscriber",
        "pipeline_schema",
        f"built subscriber pipeline output_schema=[{engine.output_schema()}]",
    )
    reader = zippy.read_from(cli_args.source_stream_name)
    zippy.log_info(
        "openctp_factor_subscriber",
        "start",
        "starting factor subscriber "
        f"control_endpoint=[{control_endpoint}] source_stream=[{cli_args.source_stream_name}]",
    )
    engine.start()

    loop_state = ExampleRuntimeLoopState.initial(
        start_monotonic=time.monotonic(),
        metrics_interval_sec=cli_args.metrics_interval_sec,
    )

    try:
        while True:
            poll_timeout_ms = min(
                cli_args.recv_timeout_ms,
                int(loop_state.heartbeat_interval_sec * 1000),
            )
            try:
                batch = reader.read(timeout_ms=poll_timeout_ms)
            except RuntimeError as error:
                if "reader timed out" in str(error):
                    batch = None
                else:
                    raise

            if batch is not None and batch.num_rows > 0:
                row = latest_row(batch)
                zippy.log_info(
                    "openctp_factor_subscriber",
                    "factor_value",
                    "received latest factor row "
                    f"instrument_id=[{row['instrument_id']}] "
                    f"dt=[{_format_dt(row['dt'])}] "
                    f"mid_price=[{row['mid_price']}] "
                    f"MID_PRICE_DIFF_200_STD_200=[{row['MID_PRICE_DIFF_200_STD_200']}]",
                    status=engine.status(),
                )

            loop_tick = advance_runtime_loop(
                state=loop_state,
                now_monotonic=time.monotonic(),
            )
            loop_state = loop_tick.state
            if loop_tick.send_heartbeat:
                master.heartbeat()
            if loop_tick.send_metrics:
                zippy.log_info(
                    "openctp_factor_subscriber",
                    "engine_heartbeat",
                    f"engine metrics heartbeat metrics=[{engine.metrics()}]",
                    status=engine.status(),
                )
    except KeyboardInterrupt:
        zippy.log_info(
            "openctp_factor_subscriber",
            "stop_request",
            "stopping factor subscriber after keyboard interrupt",
        )
    finally:
        reader.close()
        engine.stop()
        zippy.log_info(
            "openctp_factor_subscriber",
            "engine_state",
            f"engine metrics after stop metrics=[{engine.metrics()}]",
            status=engine.status(),
        )
