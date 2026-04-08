"""
Minimal remote market-data fanout example for zippy-openctp.

This file demonstrates how an OpenCTP source can feed a local bar engine and
then publish its output to another process through zippy's stream publisher.
"""

import zippy
import zippy_openctp


def build_pipeline() -> zippy.TimeSeriesEngine:
    """
    Build an OpenCTP tick -> 1m bars -> remote ZMQ stream pipeline.

    :returns: Configured time-series engine ready to be started by the caller.
    :rtype: zippy.TimeSeriesEngine
    """
    source = zippy_openctp.OpenCtpMarketDataSource(
        front="tcp://127.0.0.1:12345",
        broker_id="9999",
        user_id="000001",
        password="secret",
        instruments=["IF2506"],
        flow_path=".cache/openctp/md",
    )
    target = zippy.ZmqStreamPublisher(endpoint="tcp://127.0.0.1:7001")

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
    engine = build_pipeline()
    print(engine.output_schema())
