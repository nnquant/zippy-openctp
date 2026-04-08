# zippy-openctp

Python-first OpenCTP market data plugin for `zippy`.

## Quickstart

```python
import zippy
import zippy_openctp

source = zippy_openctp.OpenCtpMarketDataSource(
    front="tcp://127.0.0.1:12345",
    broker_id="9999",
    user_id="000001",
    password="secret",
    instruments=["IF2506"],
)

bars = zippy.TimeSeriesEngine(
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
    ],
)
```

## Status

This repository is the standalone plugin home for OpenCTP market data support.

The current stage provides:

- an independent git repository
- a fixed tick schema contract via `zippy_openctp.schemas.TickDataSchema()`
- a Python-facing `OpenCtpMarketDataSource` with stable config, status, and metrics accessors
- Rust core modules for schema, source lifecycle, normalization, and metrics
- minimal examples for local Parquet and remote ZMQ pipelines

It still does not implement a live OpenCTP market data connection. The current
`OpenCtpMarketDataSource` Python object is a minimal source wrapper that matches
the planned production API shape while real OpenCTP callback integration lands
in later tasks.
