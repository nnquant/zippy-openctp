# OpenCTP Market Generator Source Design

## Goal

Provide `zippy_openctp.OpenCtpMarketGeneratorSource` for live-system and performance tests when
the real CTP market is closed. The generator must emit rows compatible with
`zippy_openctp.TickDataSchema()` and use the same segment-primary source path as the real
`OpenCtpMarketDataSource`.

## User API

```python
source = zippy_openctp.OpenCtpMarketGeneratorSource(
    instruments=["IF2606", "IH2606"],
    interval_ms=10,
    seed=42,
    max_ticks=100_000,
)
```

`interval_ms` means one full generation round every `T` milliseconds. Each round emits one tick per
instrument in the configured instrument order. The approximate total rate is
`len(instruments) * 1000 / interval_ms` ticks per second.

## Data Path

The generator is implemented as a native `MdDriver`:

```text
OpenCtpMarketGeneratorDriver
  -> MdDriverEvent::Tick(RawTickSnapshot)
  -> normalize_tick()
  -> OpenCtpSegmentIngress
  -> ActiveSegmentReader
  -> SourceEvent::Data
```

This keeps generator tests representative of the real OpenCTP source path. It also avoids Python
loops and avoids bypassing segment ingress.

## Generated Data

The first version uses deterministic random walk prices:

- `last_price` starts from `base_price` plus a stable per-instrument offset.
- Each emitted tick applies a bounded deterministic delta controlled by `seed`.
- `bid_price_1` and `ask_price_1` surround `last_price`.
- `volume` is monotonically increasing per instrument.
- `turnover` accumulates from generated trade notional.
- `open_interest` changes slowly and remains positive.
- `trading_day`, `action_day`, `update_time`, and `update_millisec` are CTP-format raw fields and
  are normalized through the existing normalizer.

## Stop Semantics

`max_ticks=None` means run until stopped. `max_ticks=N` means emit exactly `N` ticks and then emit
`Stop`. Calling `stop()` on the runtime handle wakes the generator and causes it to emit `Stop`
without waiting for the full interval.

## Scope

This feature does not attempt to simulate exchange microstructure. It is a deterministic schema-
correct traffic generator for system tests, performance tests, and non-trading-hours integration.
