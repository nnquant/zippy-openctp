"""
Minimal live market-data-to-parquet pipeline example for zippy-openctp.
"""

import argparse
import time

import zippy
import zippy_openctp

DEFAULT_INSTRUMENTS = "IF2506"
DEFAULT_FLOW_PATH = ".cache/openctp/md"
DEFAULT_OUTPUT_PATH = "data/openctp_bars"


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
        description="run a live OpenCTP -> 1m bars -> parquet pipeline",
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
    return parser.parse_args()


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
) -> zippy.TimeSeriesEngine:
    """
    Build a local OpenCTP tick -> 1m bars -> Parquet archive pipeline.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param source: Optional pre-built OpenCTP source for callers that want to
        inspect config, status, or metrics before starting the engine.
    :type source: zippy_openctp.OpenCtpMarketDataSource | None
    :returns: Configured time-series engine ready to be started by the caller.
    :rtype: zippy.TimeSeriesEngine
    """
    source = source or build_source(args)
    archive = zippy.ParquetSink(
        path=args.output_path,
        write_output=True,
    )

    return zippy.TimeSeriesEngine(
        name="openctp_bar_1m",
        source=source,
        input_schema=zippy_openctp.schemas.TickDataSchema(),
        id_column="instrument_id",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[
            zippy.AGG_FIRST(column="last_price", output="open"),
            zippy.AGG_LAST(column="last_price", output="close"),
            zippy.AGG_MAX(column="last_price", output="high"),
            zippy.AGG_MIN(column="last_price", output="low"),
        ],
        target=zippy.NullPublisher(),
        parquet_sink=archive,
    )


if __name__ == "__main__":
    cli_args = parse_args()
    source = build_source(cli_args)
    print("source config:", source.config())
    print("source status before start:", source.status())
    print("source metrics before start:", source.metrics())
    engine = build_pipeline(cli_args, source)
    print("engine output schema:", engine.output_schema())
    print("starting live parquet pipeline; press Ctrl-C to stop")
    engine.start()
    try:
        while True:
            print("source status:", source.status(), "source metrics:", source.metrics())
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("stopping parquet pipeline")
    finally:
        engine.stop()
        print("source status after stop:", source.status())
        print("source metrics after stop:", source.metrics())
