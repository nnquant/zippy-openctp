from typing import Literal, TypedDict


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
        rows_per_batch: int = 1,
        flush_interval_ms: int = 0,
        reconnect: bool = True,
        login_timeout_sec: int = 10,
    ) -> None: ...
    def status(
        self,
    ) -> Literal["created", "connecting", "running", "degraded", "stopped", "failed"]: ...
    def config(self) -> dict[str, object]: ...
    def metrics(self) -> OpenCtpSourceMetricsDict: ...


def tick_data_schema_fields() -> list[tuple[str, str, bool]]: ...
