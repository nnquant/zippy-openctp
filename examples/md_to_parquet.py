"""
Minimal market-data-to-parquet pipeline example for zippy-openctp.

This file is intentionally a static example: it demonstrates how the plugin
should be wired into a local zippy pipeline, but it does not attempt to connect
to a live OpenCTP endpoint during import or py_compile smoke checks.
"""

import zippy
import zippy_openctp


def build_pipeline() -> zippy.TimeSeriesEngine:
    """
    Build a local OpenCTP tick -> 1m bars -> Parquet archive pipeline.

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
    archive = zippy.ParquetSink(
        output_dir="data/openctp_bars",
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
        parquet_sink=archive,
    )


if __name__ == "__main__":
    engine = build_pipeline()
    print(engine.output_schema())
