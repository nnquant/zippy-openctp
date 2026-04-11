"""
Minimal master/bus market-data fanout example for zippy-openctp.

This file demonstrates how an OpenCTP source can feed a local stream table,
persist raw ticks, and then publish the same raw stream into the local
zippy-master bus for downstream consumers.
"""

import argparse
from pathlib import Path
import time

import zippy
import zippy_openctp

DEFAULT_INSTRUMENTS = "IF2606"
DEFAULT_FLOW_PATH = ".cache/openctp/md"
DEFAULT_CONTROL_ENDPOINT = "~/.zippy/master.sock"
DEFAULT_STREAM_NAME = "openctp_ticks"
DEFAULT_OUTPUT_PATH = "data/openctp_ticks"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_LEVEL = "info"
DEFAULT_METRICS_INTERVAL_SEC = 5.0
DEFAULT_RING_CAPACITY = 131072


def _parse_instruments(raw: str) -> list[str]:
    """
    Parse a comma-separated instrument list.

    :param raw: Comma-separated instruments string.
    :type raw: str
    :returns: Parsed instrument identifiers.
    :rtype: list[str]
    :raises RuntimeError: If no instruments remain after trimming.
    """
    instruments = [item.strip() for item in raw.split(",") if item.strip()]
    if not instruments:
        raise RuntimeError("instruments resolved to an empty instrument list")
    return instruments


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the remote pipeline example.

    :returns: Parsed command-line arguments.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="run a live OpenCTP -> stream table -> master bus pipeline",
    )
    parser.add_argument("--front", required=True, help="OpenCTP market data front address")
    parser.add_argument("--broker-id", required=True, help="broker identifier")
    parser.add_argument("--user-id", required=True, help="user identifier")
    parser.add_argument("--password", required=True, help="user password")
    parser.add_argument(
        "--instruments",
        default=DEFAULT_INSTRUMENTS,
        help=f"comma-separated instruments, default [{DEFAULT_INSTRUMENTS}]",
    )
    parser.add_argument(
        "--flow-path",
        default=DEFAULT_FLOW_PATH,
        help=f"OpenCTP flow path, default [{DEFAULT_FLOW_PATH}]",
    )
    parser.add_argument(
        "--rows-per-batch",
        type=int,
        default=1,
        help="ticks per emitted batch, default [1]",
    )
    parser.add_argument(
        "--flush-interval-ms",
        type=int,
        default=0,
        help="max batch flush interval in milliseconds, default [0]",
    )
    parser.add_argument(
        "--control-endpoint",
        default=DEFAULT_CONTROL_ENDPOINT,
        help=f"zippy-master control endpoint, default [{DEFAULT_CONTROL_ENDPOINT}]",
    )
    parser.add_argument(
        "--start-master",
        action="store_true",
        help="start a local in-process master daemon before attaching to the bus",
    )
    parser.add_argument(
        "--ring-capacity",
        type=int,
        default=DEFAULT_RING_CAPACITY,
        help=f"master bus ring capacity, default [{DEFAULT_RING_CAPACITY}]",
    )
    parser.add_argument(
        "--stream-name",
        default=DEFAULT_STREAM_NAME,
        help=f"stream name, default [{DEFAULT_STREAM_NAME}]",
    )
    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Parquet output directory, default [{DEFAULT_OUTPUT_PATH}]",
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
    if args.ring_capacity <= 0:
        parser.error("--ring-capacity must be positive")
    return args


def build_source(args: argparse.Namespace) -> zippy_openctp.OpenCtpMarketDataSource:
    """
    Build a live-capable OpenCTP market data source from command-line arguments.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :returns: Configured OpenCTP market data source.
    :rtype: zippy_openctp.OpenCtpMarketDataSource
    """
    return zippy_openctp.OpenCtpMarketDataSource(
        front=args.front,
        broker_id=args.broker_id,
        user_id=args.user_id,
        password=args.password,
        instruments=_parse_instruments(args.instruments),
        flow_path=args.flow_path,
        rows_per_batch=args.rows_per_batch,
        flush_interval_ms=args.flush_interval_ms,
    )


def _register_stream_if_needed(
    master: zippy.MasterClient,
    stream_name: str,
    schema: object,
    ring_capacity: int,
) -> None:
    """
    Ensure a master bus stream exists before a writer attaches to it.

    :param master: Master client used for control-plane registration.
    :type master: zippy.MasterClient
    :param stream_name: Logical bus stream name.
    :type stream_name: str
    :param schema: Expected stream schema.
    :type schema: object
    :param ring_capacity: Ring capacity used when creating the stream.
    :type ring_capacity: int
    :raises RuntimeError: If stream creation fails for reasons other than an
        existing stream.
    """
    try:
        master.register_stream(stream_name, schema, ring_capacity)
    except RuntimeError as error:
        if "stream already exists" not in str(error):
            raise


def build_master(
    args: argparse.Namespace,
) -> tuple[zippy.MasterClient, zippy.MasterServer | None]:
    """
    Create the process-scoped master client and optionally start a local master.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :returns: Connected master client and optional local daemon handle.
    :rtype: tuple[zippy.MasterClient, zippy.MasterServer | None]
    """
    server = None
    control_endpoint = str(Path(args.control_endpoint).expanduser())
    if args.start_master:
        Path(control_endpoint).parent.mkdir(parents=True, exist_ok=True)
        server = zippy.MasterServer(control_endpoint=control_endpoint)
        server.start()

    master = zippy.MasterClient(control_endpoint=control_endpoint)
    master.register_process("openctp_md_to_bus")
    _register_stream_if_needed(
        master,
        args.stream_name,
        zippy_openctp.schemas.TickDataSchema(),
        args.ring_capacity,
    )
    return master, server


def build_target(
    args: argparse.Namespace,
    master: zippy.MasterClient,
) -> zippy.BusStreamTarget:
    """
    Build the master bus target for raw tick fanout.

    :returns: Configured stream publisher.
    :rtype: zippy.BusStreamTarget
    """
    return zippy.BusStreamTarget(
        stream_name=args.stream_name,
        master=master,
    )


def build_pipeline(
    args: argparse.Namespace,
    master: zippy.MasterClient | None = None,
    source: zippy_openctp.OpenCtpMarketDataSource | None = None,
    target: zippy.BusStreamTarget | None = None,
) -> zippy.StreamTableEngine:
    """
    Build an OpenCTP tick -> stream table -> master bus pipeline.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param master: Optional master client used when a target must be built
        inside this helper.
    :type master: zippy.MasterClient | None
    :param source: Optional pre-built OpenCTP source for callers that want to
        inspect config, status, or metrics before starting the engine.
    :type source: zippy_openctp.OpenCtpMarketDataSource | None
    :param target: Optional pre-built master bus target for callers that want to
        inspect the bound stream before starting the engine.
    :type target: zippy.BusStreamTarget | None
    :returns: Configured stream-table engine ready to be started by the caller.
    :rtype: zippy.StreamTableEngine
    """
    source = source or build_source(args)
    if target is None:
        if master is None:
            raise RuntimeError("master is required when target is not provided")
        target = build_target(args, master)
    sink = zippy.ParquetSink(
        path=args.output_path,
        write_output=True,
        rows_per_batch=8192,
        flush_interval_ms=1000,
    )

    return zippy.StreamTableEngine(
        name="openctp_tick_table_remote",
        source=source,
        input_schema=zippy_openctp.schemas.TickDataSchema(),
        target=target,
        sink=sink,
    )


if __name__ == "__main__":
    cli_args = parse_args()
    log_snapshot = zippy.setup_log(
        app="openctp_md_to_remote_pipeline",
        level=cli_args.log_level,
        log_dir=cli_args.log_dir,
        to_console=not cli_args.no_console_log,
        to_file=True,
    )
    zippy.log_info("openctp_example", "log_setup", f"initialized zippy logging log_snapshot=[{log_snapshot}]")
    master, server = build_master(cli_args)
    zippy.log_info(
        "openctp_example",
        "master_config",
        "prepared master client "
        f"control_endpoint=[{Path(cli_args.control_endpoint).expanduser()}] stream_name=[{cli_args.stream_name}] "
        f"ring_capacity=[{cli_args.ring_capacity}] start_master=[{cli_args.start_master}]",
    )
    source = build_source(cli_args)
    zippy.log_info("openctp_example", "source_config", f"built openctp source source_config=[{source.config()}]")
    zippy.log_info(
        "openctp_example",
        "source_state",
        f"source metrics before start metrics=[{source.metrics()}]",
        status=source.status(),
    )
    target = build_target(cli_args, master)
    zippy.log_info(
        "openctp_example",
        "target_config",
        "built bus target "
        f"control_endpoint=[{Path(cli_args.control_endpoint).expanduser()}] stream_name=[{cli_args.stream_name}]",
    )
    engine = build_pipeline(cli_args, master=master, source=source, target=target)
    zippy.log_info(
        "openctp_example",
        "pipeline_schema",
        f"built stream table remote pipeline output_schema=[{engine.output_schema()}]",
    )
    zippy.log_info("openctp_example", "start", "starting live stream-table remote pipeline")
    engine.start()
    try:
        while True:
            zippy.log_info(
                "openctp_example",
                "source_heartbeat",
                f"source metrics heartbeat metrics=[{source.metrics()}]",
                status=source.status(),
            )
            zippy.log_info(
                "openctp_example",
                "engine_heartbeat",
                f"engine metrics heartbeat metrics=[{engine.metrics()}]",
                status=engine.status(),
            )
            time.sleep(cli_args.metrics_interval_sec)
    except KeyboardInterrupt:
        zippy.log_info(
            "openctp_example",
            "stop_request",
            "stopping stream-table remote pipeline after keyboard interrupt",
        )
    finally:
        engine.stop()
        zippy.log_info(
            "openctp_example",
            "source_state",
            f"source metrics after stop metrics=[{source.metrics()}]",
            status=source.status(),
        )
        if server is not None:
            server.stop()
            server.join()
