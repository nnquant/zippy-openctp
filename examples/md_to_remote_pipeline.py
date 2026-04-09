"""
Minimal remote market-data fanout example for zippy-openctp.

This file demonstrates how an OpenCTP source can feed a local stream table,
persist raw ticks, and then publish the same raw stream to another process.
"""

import argparse
import time

import zippy
import zippy_openctp

DEFAULT_INSTRUMENTS = "IF2606"
DEFAULT_FLOW_PATH = ".cache/openctp/md"
DEFAULT_STREAM_ENDPOINT = "tcp://127.0.0.1:7001"
DEFAULT_STREAM_NAME = "openctp_ticks"
DEFAULT_OUTPUT_PATH = "data/openctp_ticks_remote"


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
        description="run a live OpenCTP -> stream table -> zmq stream pipeline",
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
        "--stream-endpoint",
        default=DEFAULT_STREAM_ENDPOINT,
        help=f"ZMQ stream endpoint, default [{DEFAULT_STREAM_ENDPOINT}]",
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


def build_target(args: argparse.Namespace) -> zippy.ZmqStreamPublisher:
    """
    Build the remote stream publisher for raw tick fanout.

    :returns: Configured stream publisher.
    :rtype: zippy.ZmqStreamPublisher
    """
    return zippy.ZmqStreamPublisher(
        endpoint=args.stream_endpoint,
        stream_name=args.stream_name,
        schema=zippy_openctp.schemas.TickDataSchema(),
    )


def build_pipeline(
    args: argparse.Namespace,
    source: zippy_openctp.OpenCtpMarketDataSource | None = None,
    target: zippy.ZmqStreamPublisher | None = None,
) -> zippy.StreamTableEngine:
    """
    Build an OpenCTP tick -> stream table -> remote ZMQ stream pipeline.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param source: Optional pre-built OpenCTP source for callers that want to
        inspect config, status, or metrics before starting the engine.
    :type source: zippy_openctp.OpenCtpMarketDataSource | None
    :param target: Optional pre-built stream publisher for callers that want to
        inspect the bound endpoint before starting the engine.
    :type target: zippy.ZmqStreamPublisher | None
    :returns: Configured time-series engine ready to be started by the caller.
    :rtype: zippy.StreamTableEngine
    """
    source = source or build_source(args)
    target = target or build_target(args)
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
    source = build_source(cli_args)
    print("source config:", source.config())
    print("source status before start:", source.status())
    print("source metrics before start:", source.metrics())
    target = build_target(cli_args)
    print("stream endpoint:", target.last_endpoint())
    engine = build_pipeline(cli_args, source, target)
    print("engine output schema:", engine.output_schema())
    print("starting live stream-table remote pipeline; press Ctrl-C to stop")
    engine.start()
    try:
        while True:
            print("source status:", source.status(), "source metrics:", source.metrics())
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("stopping stream-table remote pipeline")
    finally:
        engine.stop()
        print("source status after stop:", source.status())
        print("source metrics after stop:", source.metrics())
