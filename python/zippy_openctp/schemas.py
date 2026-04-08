import pyarrow as pa


def TickDataSchema():
    """Return the stable OpenCTP tick schema as a pyarrow schema."""

    from ._internal import tick_data_schema_fields

    type_mapping = {
        "utf8": pa.string(),
        "timestamp_ns_utc": pa.timestamp("ns", tz="UTC"),
        "float64": pa.float64(),
        "int64": pa.int64(),
    }

    return pa.schema(
        [
            pa.field(name, type_mapping[data_type], nullable=nullable)
            for name, data_type, nullable in tick_data_schema_fields()
        ]
    )
