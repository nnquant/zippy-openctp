"""
Minimal master/bus market-data fanout example for zippy-openctp.

This file demonstrates how an OpenCTP source can feed a local stream table,
persist raw ticks, and then publish the same raw stream into the local
zippy-master bus for downstream consumers.
"""

import argparse
from collections.abc import Callable
import json
from pathlib import Path
import time

from _runtime_lease import ExampleRuntimeLoopState, advance_runtime_loop
import zippy
import zippy_openctp

DEFAULT_INSTRUMENTS = "IF2606"
DEFAULT_FLOW_PATH = ".cache/openctp/md"
DEFAULT_CONTROL_ENDPOINT = "~/.zippy/master.sock"
DEFAULT_STREAM_NAME = "openctp_ticks"
DEFAULT_SOURCE_NAME = "openctp_md"
DEFAULT_OUTPUT_PATH = "data/openctp_ticks"
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_LEVEL = "info"
DEFAULT_METRICS_INTERVAL_SEC = 5.0
DEFAULT_BUFFER_SIZE = 131072
DEFAULT_FRAME_SIZE = 4096


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
        "--buffer-size",
        type=int,
        default=DEFAULT_BUFFER_SIZE,
        help=f"master bus buffer size, default [{DEFAULT_BUFFER_SIZE}]",
    )
    parser.add_argument(
        "--frame-size",
        type=int,
        default=DEFAULT_FRAME_SIZE,
        help=f"master bus frame size, default [{DEFAULT_FRAME_SIZE}]",
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
    if args.buffer_size <= 0:
        parser.error("--buffer-size must be positive")
    if args.frame_size <= 0:
        parser.error("--frame-size must be positive")
    return args


def build_segment_descriptor_publisher(
    master: zippy.MasterClient,
    stream_name: str,
) -> Callable[[bytes], None]:
    """
    Build a callback that publishes active segment metadata through master.

    :param master: Registered master client for the OpenCTP source process.
    :type master: zippy.MasterClient
    :param stream_name: Stream whose active segment descriptor should be updated.
    :type stream_name: str
    :returns: Callback accepted by ``OpenCtpMarketDataSource``.
    :rtype: Callable[[bytes], None]
    """

    def publish_segment_descriptor(descriptor_envelope: bytes) -> None:
        descriptor = json.loads(descriptor_envelope.decode("utf-8"))
        master.publish_segment_descriptor(stream_name, descriptor)
        zippy.log_info(
            "openctp_example",
            "segment_descriptor",
            f"published active segment descriptor stream_name=[{stream_name}] "
            f"segment_id=[{descriptor.get('segment_id')}] generation=[{descriptor.get('generation')}]",
        )

    return publish_segment_descriptor


def build_source(
    args: argparse.Namespace,
    master: zippy.MasterClient | None = None,
) -> zippy_openctp.OpenCtpMarketDataSource:
    """
    Build a live-capable OpenCTP market data source from command-line arguments.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :param master: Optional master client used to publish segment descriptors in
        segment mode.
    :type master: zippy.MasterClient | None
    :returns: Configured OpenCTP market data source.
    :rtype: zippy_openctp.OpenCtpMarketDataSource
    """
    segment_descriptor_publisher = None
    if master is not None:
        segment_descriptor_publisher = build_segment_descriptor_publisher(
            master,
            args.stream_name,
        )

    return zippy_openctp.OpenCtpMarketDataSource(
        front=args.front,
        broker_id=args.broker_id,
        user_id=args.user_id,
        password=args.password,
        instruments=_parse_instruments(args.instruments),
        flow_path=args.flow_path,
        segment_descriptor_publisher=segment_descriptor_publisher,
    )


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


def _register_source_if_needed(
    master: zippy.MasterClient,
    args: argparse.Namespace,
) -> None:
    """
    Ensure the OpenCTP source owns the stream in master control-plane metadata.

    :param master: Master client used for control-plane registration.
    :type master: zippy.MasterClient
    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :raises RuntimeError: If source creation fails for reasons other than an
        existing source.
    """
    source_config = {
        "broker_id": args.broker_id,
        "data_path": "segment",
        "flow_path": args.flow_path,
        "front": args.front,
        "instruments": _parse_instruments(args.instruments),
        "password": "***redacted***",
        "user_id": args.user_id,
    }
    try:
        master.register_source(
            DEFAULT_SOURCE_NAME,
            "openctp",
            args.stream_name,
            source_config,
        )
    except RuntimeError as error:
        if "source already exists" not in str(error):
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
        args.buffer_size,
        args.frame_size,
    )
    _register_source_if_needed(master, args)
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
) -> zippy._internal.StreamTableMaterializer:
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
    :rtype: zippy._internal.StreamTableMaterializer
    """
    source = source or build_source(args, master)
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

    return zippy._internal.StreamTableMaterializer(
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
        f"buffer_size=[{cli_args.buffer_size}] frame_size=[{cli_args.frame_size}] "
        f"start_master=[{cli_args.start_master}]",
    )
    source = build_source(cli_args, master)
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
            time.sleep(loop_tick.sleep_sec)
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
