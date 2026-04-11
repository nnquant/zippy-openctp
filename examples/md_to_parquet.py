"""
Minimal live market-data-to-stream-table-to-parquet pipeline example for zippy-openctp.
"""

import argparse
import time

import zippy
import zippy_openctp

DEFAULT_INSTRUMENTS = "IF2606"
DEFAULT_FLOW_PATH = ".cache/openctp/md"
DEFAULT_OUTPUT_PATH = "data/openctp_ticks"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_LEVEL = "info"
DEFAULT_METRICS_INTERVAL_SEC = 5.0


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
    Parse command-line arguments for the live parquet example.

    :returns: Parsed command-line arguments.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="run a live OpenCTP -> stream table -> parquet pipeline",
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
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Parquet output directory, default [{DEFAULT_OUTPUT_PATH}]",
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


def build_pipeline(
    args: argparse.Namespace,
    source: zippy_openctp.OpenCtpMarketDataSource | None = None,
) -> zippy.StreamTableEngine:
    """
    Build a local OpenCTP tick -> stream table -> Parquet archive pipeline.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param source: Optional pre-built OpenCTP source for callers that want to
        inspect config, status, or metrics before starting the engine.
    :type source: zippy_openctp.OpenCtpMarketDataSource | None
    :returns: Configured time-series engine ready to be started by the caller.
    :rtype: zippy.StreamTableEngine
    """
    source = source or build_source(args)
    archive = zippy.ParquetSink(
        path=args.output_path,
        write_output=True,
        rows_per_batch=8192,
        flush_interval_ms=1000,
    )

    return zippy.StreamTableEngine(
        name="openctp_tick_table",
        source=source,
        input_schema=zippy_openctp.schemas.TickDataSchema(),
        target=zippy.NullPublisher(),
        sink=archive,
    )


if __name__ == "__main__":
    cli_args = parse_args()
    log_snapshot = zippy.setup_log(
        app="openctp_md_to_parquet",
        level=cli_args.log_level,
        log_dir=cli_args.log_dir,
        to_console=not cli_args.no_console_log,
        to_file=True,
    )
    zippy.log_info("openctp_example", "log_setup", f"initialized zippy logging log_snapshot=[{log_snapshot}]")
    source = build_source(cli_args)
    zippy.log_info("openctp_example", "source_config", f"built openctp source source_config=[{source.config()}]")
    zippy.log_info(
        "openctp_example",
        "source_state",
        f"source metrics before start metrics=[{source.metrics()}]",
        status=source.status(),
    )
    engine = build_pipeline(cli_args, source)
    zippy.log_info(
        "openctp_example",
        "pipeline_schema",
        f"built stream table pipeline output_schema=[{engine.output_schema()}]",
    )
    zippy.log_info("openctp_example", "start", "starting live stream-table parquet pipeline")
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
            "stopping stream-table parquet pipeline after keyboard interrupt",
        )
    finally:
        engine.stop()
        zippy.log_info(
            "openctp_example",
            "source_state",
            f"source metrics after stop metrics=[{source.metrics()}]",
            status=source.status(),
        )
