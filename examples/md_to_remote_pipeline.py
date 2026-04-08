"""
Minimal remote market-data fanout example for zippy-openctp.

This file demonstrates how an OpenCTP source can feed a local bar engine and
then publish its output to another process through zippy's stream publisher.
"""

import os
import time

import zippy
import zippy_openctp


def _required_env(name: str) -> str:
    """
    Read a required OpenCTP environment variable.

    :param name: Environment variable name.
    :type name: str
    :returns: Non-empty environment variable value.
    :rtype: str
    :raises RuntimeError: If the environment variable is missing or empty.
    """
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def _load_instruments() -> list[str]:
    """
    Parse the optional OpenCTP instruments list from the environment.

    :returns: Instrument identifiers to subscribe.
    :rtype: list[str]
    """
    raw = os.getenv("OPENCTP_INSTRUMENTS", "IF2506")
    instruments = [item.strip() for item in raw.split(",") if item.strip()]
    if not instruments:
        raise RuntimeError("OPENCTP_INSTRUMENTS resolved to an empty instrument list")
    return instruments


def build_source() -> zippy_openctp.OpenCtpMarketDataSource:
    """
    Build a live-capable OpenCTP market data source from environment variables.

    Required environment variables:
    - OPENCTP_MD_FRONT
    - OPENCTP_BROKER_ID
    - OPENCTP_USER_ID
    - OPENCTP_PASSWORD

    Optional environment variables:
    - OPENCTP_INSTRUMENTS: comma-separated instruments, defaults to ``IF2506``

    :returns: Configured OpenCTP market data source.
    :rtype: zippy_openctp.OpenCtpMarketDataSource
    """
    return zippy_openctp.OpenCtpMarketDataSource(
        front=_required_env("OPENCTP_MD_FRONT"),
        broker_id=_required_env("OPENCTP_BROKER_ID"),
        user_id=_required_env("OPENCTP_USER_ID"),
        password=_required_env("OPENCTP_PASSWORD"),
        instruments=_load_instruments(),
        flow_path=".cache/openctp/md",
    )


def build_bar_schema() -> object:
    """
    Build the output schema for the 1m bar stream.

    :returns: Time-series output schema for the remote stream.
    :rtype: object
    """
    schema_probe = zippy.TimeSeriesEngine(
        name="openctp_bar_stream_probe",
        input_schema=zippy_openctp.schemas.TickDataSchema(),
        id_column="instrument_id",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[
            zippy.AGG_FIRST(column="last_price", output="open"),
            zippy.AGG_LAST(column="last_price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
        ],
        target=zippy.NullPublisher(),
    )
    return schema_probe.output_schema()


def build_target() -> zippy.ZmqStreamPublisher:
    """
    Build the remote stream publisher for bar fanout.

    :returns: Configured stream publisher.
    :rtype: zippy.ZmqStreamPublisher
    """
    return zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:7001",
        stream_name="openctp_bar_1m",
        schema=build_bar_schema(),
    )


def build_pipeline(
    source: zippy_openctp.OpenCtpMarketDataSource | None = None,
    target: zippy.ZmqStreamPublisher | None = None,
) -> zippy.TimeSeriesEngine:
    """
    Build an OpenCTP tick -> 1m bars -> remote ZMQ stream pipeline.

    :param source: Optional pre-built OpenCTP source for callers that want to
        inspect config, status, or metrics before starting the engine.
    :type source: zippy_openctp.OpenCtpMarketDataSource | None
    :param target: Optional pre-built stream publisher for callers that want to
        inspect the bound endpoint before starting the engine.
    :type target: zippy.ZmqStreamPublisher | None
    :returns: Configured time-series engine ready to be started by the caller.
    :rtype: zippy.TimeSeriesEngine
    """
    source = source or build_source()
    target = target or build_target()

    return zippy.TimeSeriesEngine(
        name="openctp_bar_stream",
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
            zippy.AGG_SUM(column="volume", output="volume"),
        ],
        target=target,
    )


if __name__ == "__main__":
    source = build_source()
    print("source config:", source.config())
    print("source status before start:", source.status())
    print("source metrics before start:", source.metrics())
    target = build_target()
    print("stream endpoint:", target.last_endpoint())
    engine = build_pipeline(source, target)
    print("engine output schema:", engine.output_schema())
    print("starting live remote pipeline; press Ctrl-C to stop")
    engine.start()
    try:
        while True:
            print("source status:", source.status(), "source metrics:", source.metrics())
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("stopping remote pipeline")
    finally:
        engine.stop()
        print("source status after stop:", source.status())
        print("source metrics after stop:", source.metrics())
