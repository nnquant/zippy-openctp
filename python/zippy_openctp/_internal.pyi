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
    def status(self) -> str: ...
    def config(self) -> dict[str, object]: ...
    def metrics(self) -> dict[str, int]: ...


def tick_data_schema_fields() -> list[tuple[str, str, bool]]: ...
