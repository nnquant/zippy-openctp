"""
Probe OpenCTP active segment rows through zippy-master descriptor metadata.
"""

import argparse
from pathlib import Path
import time

import zippy
import zippy_openctp

DEFAULT_CONTROL_ENDPOINT = "~/.zippy/master.sock"
DEFAULT_STREAM_NAME = "openctp_ticks"
DEFAULT_POLL_INTERVAL_SEC = 0.001
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_LEVEL = "info"


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the active segment reader probe.

    :returns: Parsed command-line arguments.
    :rtype: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        description="read OpenCTP active segment rows from zippy-master descriptor metadata",
    )
    parser.add_argument(
        "--control-endpoint",
        default=DEFAULT_CONTROL_ENDPOINT,
        help=f"zippy-master control endpoint, default [{DEFAULT_CONTROL_ENDPOINT}]",
    )
    parser.add_argument(
        "--stream-name",
        default=DEFAULT_STREAM_NAME,
        help=f"source stream name, default [{DEFAULT_STREAM_NAME}]",
    )
    parser.add_argument(
        "--poll-interval-sec",
        type=float,
        default=DEFAULT_POLL_INTERVAL_SEC,
        help=f"idle poll interval in seconds, default [{DEFAULT_POLL_INTERVAL_SEC}]",
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
    args = parser.parse_args()
    if args.poll_interval_sec <= 0:
        parser.error("--poll-interval-sec must be positive")
    return args


def build_master(args: argparse.Namespace) -> zippy.MasterClient:
    """
    Build a registered master client for the probe process.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    :returns: Registered master client.
    :rtype: zippy.MasterClient
    """
    control_endpoint = str(Path(args.control_endpoint).expanduser())
    master = zippy.MasterClient(control_endpoint=control_endpoint)
    master.register_process("openctp_segment_reader_probe")
    return master


def build_segment_reader(
    master: zippy.MasterClient,
    stream_name: str,
) -> zippy_openctp.OpenCtpSegmentReader:
    """
    Build an active segment reader from descriptor metadata stored in master.

    :param master: Registered master client.
    :type master: zippy.MasterClient
    :param stream_name: Source stream name whose descriptor should be read.
    :type stream_name: str
    :returns: OpenCTP active segment reader.
    :rtype: zippy_openctp.OpenCtpSegmentReader
    :raises RuntimeError: If the stream does not have an active segment descriptor yet.
    """
    descriptor = master.get_segment_descriptor(stream_name)
    if descriptor is None:
        raise RuntimeError(f"segment descriptor is not published stream_name=[{stream_name}]")
    return zippy_openctp.OpenCtpSegmentReader(descriptor)


def poll_once(
    master: zippy.MasterClient,
    stream_name: str,
    reader: zippy_openctp.OpenCtpSegmentReader,
) -> int:
    """
    Refresh descriptor metadata and drain currently visible rows once.

    :param master: Registered master client.
    :type master: zippy.MasterClient
    :param stream_name: Source stream name.
    :type stream_name: str
    :param reader: Active segment reader to update and drain.
    :type reader: zippy_openctp.OpenCtpSegmentReader
    :returns: Number of rows read in this poll.
    :rtype: int
    """
    descriptor = master.get_segment_descriptor(stream_name)
    if descriptor is not None:
        reader.update_descriptor(descriptor)

    rows = 0
    while True:
        batch = reader.read_available()
        if batch is None:
            return rows
        rows += batch.num_rows


def run_probe(args: argparse.Namespace) -> None:
    """
    Run the active segment reader probe loop.

    :param args: Parsed command-line arguments.
    :type args: argparse.Namespace
    """
    zippy.setup_log(
        app="openctp_segment_reader_probe",
        level=args.log_level,
        log_dir=args.log_dir,
        to_console=not args.no_console_log,
        to_file=True,
    )
    master = build_master(args)
    reader = build_segment_reader(master, args.stream_name)
    total_rows = 0

    zippy.log_info(
        "openctp_segment_probe",
        "start",
        f"started active segment reader probe stream_name=[{args.stream_name}]",
    )
    try:
        while True:
            rows = poll_once(master, args.stream_name, reader)
            if rows > 0:
                total_rows += rows
                zippy.log_info(
                    "openctp_segment_probe",
                    "rows",
                    f"read active segment rows rows=[{rows}] total_rows=[{total_rows}]",
                )
            else:
                time.sleep(args.poll_interval_sec)
    except KeyboardInterrupt:
        zippy.log_info(
            "openctp_segment_probe",
            "stop",
            f"stopped active segment reader probe total_rows=[{total_rows}]",
        )


if __name__ == "__main__":
    run_probe(parse_args())
