from collections.abc import Callable
from typing import Literal, TypedDict

import pyarrow as pa


class OpenCtpSourceMetricsDict(TypedDict):
    ticks_received_total: int
    ticks_emitted_total: int
    batches_emitted_total: int
    reconnects_total: int
    login_failures_total: int
    subscribe_failures_total: int


class OpenCtpMarketDataSource:
    def __init__(
        self,
        front: str,
        broker_id: str,
        user_id: str,
        password: str,
        instruments: list[str],
        flow_path: str = ".cache/openctp/md",
        reconnect: bool = True,
        login_timeout_sec: int = 10,
        segment_descriptor_publisher: Callable[[bytes], None] | None = None,
    ) -> None: ...
    def status(
        self,
    ) -> Literal["created", "connecting", "running", "degraded", "stopped", "failed"]: ...
    def config(self) -> dict[str, object]: ...
    def metrics(self) -> OpenCtpSourceMetricsDict: ...
    def _zippy_output_schema(self) -> pa.Schema: ...
    def _zippy_source_mode(self) -> Literal["pipeline"]: ...
    def _zippy_source_name(self) -> Literal["openctp-market-data-source"]: ...
    def _zippy_source_type(self) -> Literal["openctp"]: ...


class OpenCtpMarketGeneratorSource:
    def __init__(
        self,
        instruments: list[str],
        interval_ms: int,
        *,
        exchange_id: str = "CFFEX",
        trading_day: str | None = None,
        action_day: str | None = None,
        seed: int | None = None,
        base_price: float = 4000.0,
        price_step: float = 0.2,
        max_ticks: int | None = None,
        segment_descriptor_publisher: Callable[[bytes], None] | None = None,
    ) -> None: ...
    def status(
        self,
    ) -> Literal["created", "connecting", "running", "degraded", "stopped", "failed"]: ...
    def config(self) -> dict[str, object]: ...
    def metrics(self) -> OpenCtpSourceMetricsDict: ...
    def _zippy_output_schema(self) -> pa.Schema: ...
    def _zippy_source_mode(self) -> Literal["pipeline"]: ...
    def _zippy_source_name(self) -> Literal["openctp-market-generator-source"]: ...
    def _zippy_source_type(self) -> Literal["openctp.generator"]: ...


class OpenCtpSegmentReader:
    def __init__(self, descriptor: dict[str, object]) -> None: ...
    def update_descriptor(self, descriptor: dict[str, object]) -> None: ...
    def committed_row_count(self) -> int: ...
    def read_available(self) -> pa.RecordBatch | None: ...


def tick_data_schema_fields() -> list[tuple[str, str, bool]]: ...
