"""
Master/bus OpenCTP factor example for MID_PRICE_DIFF_200_STD_200.

This file demonstrates a downstream process that subscribes to the raw OpenCTP
tick stream from zippy-master, computes a reactive factor pipeline, and writes
the enriched factor stream back into the same local bus.
"""

import argparse
from pathlib import Path
import time

from _runtime_lease import ExampleRuntimeLoopState, advance_runtime_loop
import zippy
import zippy_openctp

DEFAULT_CONTROL_ENDPOINT = "~/.zippy/master.sock"
DEFAULT_SOURCE_STREAM_NAME = "openctp_ticks"
DEFAULT_OUTPUT_STREAM_NAME = "openctp_mid_price_factors"
DEFAULT_OUTPUT_PATH = "data/openctp_mid_price_factors"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_LEVEL = "info"
DEFAULT_METRICS_INTERVAL_SEC = 5.0
DEFAULT_BUFFER_SIZE = 131072
DEFAULT_FRAME_SIZE = 4096


def parse_id_filter(value: str | None) -> list[str] | None:
    """
    Parse a comma-separated id filter string.

    :param value: Raw CLI value, or ``None`` when filtering is disabled.
    :type value: str | None
    :returns: Normalized whitelist ids, or ``None`` when disabled.
    :rtype: list[str] | None
    :raises ValueError: If the parsed whitelist is empty.
    """
    if value is None:
        return None

    values = [item.strip() for item in value.split(",") if item.strip()]
    if not values:
        raise ValueError("--id-filter must contain at least one instrument id")
    return values


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the remote factor example.

    :returns: Parsed command-line arguments.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="read OpenCTP ticks from zippy-master and compute MID_PRICE_DIFF_200_STD_200",
    )
    parser.add_argument(
        "--control-endpoint",
        default=DEFAULT_CONTROL_ENDPOINT,
        help=f"zippy-master control endpoint, default [{DEFAULT_CONTROL_ENDPOINT}]",
    )
    parser.add_argument(
        "--source-stream-name",
        default=DEFAULT_SOURCE_STREAM_NAME,
        help=f"input bus stream name, default [{DEFAULT_SOURCE_STREAM_NAME}]",
    )
    parser.add_argument(
        "--output-stream-name",
        default=DEFAULT_OUTPUT_STREAM_NAME,
        help=f"factor output stream name, default [{DEFAULT_OUTPUT_STREAM_NAME}]",
    )
    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
        help=f"factor parquet output directory, default [{DEFAULT_OUTPUT_PATH}]",
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
    parser.add_argument(
        "--id-filter",
        default=None,
        help="optional comma-separated instrument whitelist for factor computation",
    )
    parser.add_argument(
        "--buffer-size",
        type=int,
        default=DEFAULT_BUFFER_SIZE,
        help=f"buffer size used when creating the factor output stream, default [{DEFAULT_BUFFER_SIZE}]",
    )
    parser.add_argument(
        "--frame-size",
        type=int,
        default=DEFAULT_FRAME_SIZE,
        help=f"frame size used when creating the factor output stream, default [{DEFAULT_FRAME_SIZE}]",
    )
    args = parser.parse_args()
    if args.metrics_interval_sec <= 0:
        parser.error("--metrics-interval-sec must be positive")
    if args.buffer_size <= 0:
        parser.error("--buffer-size must be positive")
    if args.frame_size <= 0:
        parser.error("--frame-size must be positive")
    try:
        args.id_filter = parse_id_filter(args.id_filter)
    except ValueError as error:
        parser.error(str(error))
    return args


def factor_specs() -> list[object]:
    """
    Build the reactive factor list for the remote mid-price factor pipeline.

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


def output_schema() -> object:
    """
    Probe the factor output schema for the remote publisher.

    :returns: Reactive factor output schema.
    :rtype: object
    """
    engine = zippy.ReactiveStateEngine(
        name="openctp_mid_price_factor_probe",
        input_schema=zippy_openctp.schemas.TickDataSchema(),
        id_column="instrument_id",
        factors=factor_specs(),
        target=zippy.NullPublisher(),
    )
    return engine.output_schema()


def _register_stream_if_needed(
    master: zippy.MasterClient,
    stream_name: str,
    schema: object,
    buffer_size: int,
    frame_size: int,
) -> None:
    """
    Ensure a master bus stream exists before a writer attaches to it.

    :param master: Master client used for control-plane registration.
    :type master: zippy.MasterClient
    :param stream_name: Logical bus stream name.
    :type stream_name: str
    :param schema: Expected stream schema.
    :type schema: object
    :param buffer_size: Buffer size used when creating the stream.
    :type buffer_size: int
    :param frame_size: Frame size used when creating the stream.
    :type frame_size: int
    :raises RuntimeError: If stream creation fails for reasons other than an
        existing stream.
    """
    try:
        master.register_stream(stream_name, schema, buffer_size, frame_size)
    except RuntimeError as error:
        if "stream already exists" not in str(error):
            raise


def build_master(args: argparse.Namespace) -> zippy.MasterClient:
    """
    Build the process-scoped master client for the factor worker.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :returns: Registered master client.
    :rtype: zippy.MasterClient
    """
    control_endpoint = str(Path(args.control_endpoint).expanduser())
    master = zippy.connect(uri=control_endpoint, app="openctp_mid_price_factor")
    _register_stream_if_needed(
        master,
        args.output_stream_name,
        output_schema(),
        args.buffer_size,
        args.frame_size,
    )
    return master


def build_source(
    args: argparse.Namespace,
    master: zippy.MasterClient | None = None,
) -> zippy.BusStreamSource:
    """
    Build the bus-backed tick source.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param master: Registered master client. When omitted, ``zippy.connect()`` is used.
    :type master: zippy.MasterClient | None
    :returns: Configured bus source.
    :rtype: zippy.BusStreamSource
    """
    master = master or zippy.master()
    return zippy.BusStreamSource(
        stream_name=args.source_stream_name,
        expected_schema=zippy_openctp.schemas.TickDataSchema(),
        master=master,
        mode=zippy.SourceMode.PIPELINE,
    )


def build_target(
    args: argparse.Namespace,
    master: zippy.MasterClient | None = None,
) -> zippy.BusStreamTarget:
    """
    Build the factor output bus target.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param master: Registered master client. When omitted, ``zippy.connect()`` is used.
    :type master: zippy.MasterClient | None
    :returns: Configured factor stream target.
    :rtype: zippy.BusStreamTarget
    """
    master = master or zippy.master()
    return zippy.BusStreamTarget(
        stream_name=args.output_stream_name,
        master=master,
    )


def build_pipeline(
    args: argparse.Namespace,
    master: zippy.MasterClient | None = None,
    source: zippy.BusStreamSource | None = None,
    target: zippy.BusStreamTarget | None = None,
) -> zippy.ReactiveStateEngine:
    """
    Build the bus tick -> reactive factor -> stream pipeline.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param master: Optional master client used when source or target must be
        built inside this helper.
    :type master: zippy.MasterClient | None
    :param source: Optional pre-built bus source.
    :type source: zippy.BusStreamSource | None
    :param target: Optional pre-built output bus target.
    :type target: zippy.BusStreamTarget | None
    :returns: Configured reactive factor engine.
    :rtype: zippy.ReactiveStateEngine
    """
    if source is None:
        source = build_source(args, master)
    if target is None:
        target = build_target(args, master)
    sink = zippy.ParquetSink(
        path=args.output_path,
        write_output=True,
        rows_per_batch=8192,
        flush_interval_ms=1000,
    )
    return zippy.ReactiveStateEngine(
        name="openctp_mid_price_factor_engine",
        source=source,
        input_schema=zippy_openctp.schemas.TickDataSchema(),
        id_column="instrument_id",
        id_filter=args.id_filter,
        factors=factor_specs(),
        target=target,
        parquet_sink=sink,
    )


if __name__ == "__main__":
    cli_args = parse_args()
    log_snapshot = zippy.setup_log(
        app="openctp_remote_mid_price_factor",
        level=cli_args.log_level,
        log_dir=cli_args.log_dir,
        to_console=not cli_args.no_console_log,
        to_file=True,
    )
    zippy.log_info(
        "openctp_factor_example",
        "log_setup",
        f"initialized zippy logging log_snapshot=[{log_snapshot}]",
    )
    master = build_master(cli_args)
    zippy.log_info(
        "openctp_factor_example",
        "master_config",
        "prepared master client "
        f"control_endpoint=[{Path(cli_args.control_endpoint).expanduser()}] source_stream=[{cli_args.source_stream_name}] "
        f"output_stream=[{cli_args.output_stream_name}] buffer_size=[{cli_args.buffer_size}] "
        f"frame_size=[{cli_args.frame_size}]",
    )
    source = build_source(cli_args, master)
    zippy.log_info(
        "openctp_factor_example",
        "source_config",
        "built bus source "
        f"control_endpoint=[{Path(cli_args.control_endpoint).expanduser()}] source_stream=[{cli_args.source_stream_name}] "
        f"id_filter=[{cli_args.id_filter}]",
    )
    target = build_target(cli_args, master)
    zippy.log_info(
        "openctp_factor_example",
        "target_config",
        "built factor bus target "
        f"control_endpoint=[{Path(cli_args.control_endpoint).expanduser()}] stream_name=[{cli_args.output_stream_name}] "
        f"output_path=[{cli_args.output_path}]",
    )
    engine = build_pipeline(cli_args, master=master, source=source, target=target)
    zippy.log_info(
        "openctp_factor_example",
        "pipeline_schema",
        f"built reactive factor pipeline output_schema=[{engine.output_schema()}]",
    )
    zippy.log_info(
        "openctp_factor_example",
        "start",
        "starting remote MID_PRICE_DIFF_200_STD_200 factor pipeline",
    )
    engine.start()
    loop_state = ExampleRuntimeLoopState.initial(
        start_monotonic=time.monotonic(),
        metrics_interval_sec=cli_args.metrics_interval_sec,
    )
    try:
        while True:
            loop_tick = advance_runtime_loop(
                state=loop_state,
                now_monotonic=time.monotonic(),
            )
            loop_state = loop_tick.state
            if loop_tick.send_heartbeat:
                master.heartbeat()
            if loop_tick.send_metrics:
                zippy.log_info(
                    "openctp_factor_example",
                    "engine_heartbeat",
                    f"engine metrics heartbeat metrics=[{engine.metrics()}]",
                    status=engine.status(),
                )
            time.sleep(loop_tick.sleep_sec)
    except KeyboardInterrupt:
        zippy.log_info(
            "openctp_factor_example",
            "stop_request",
            "stopping remote MID_PRICE_DIFF_200_STD_200 factor pipeline after keyboard interrupt",
        )
    finally:
        engine.stop()
        zippy.log_info(
            "openctp_factor_example",
            "engine_state",
            f"engine metrics after stop metrics=[{engine.metrics()}]",
            status=engine.status(),
        )
