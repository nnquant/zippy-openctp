# zippy-openctp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付一个面向 Python 用户的独立 `zippy-openctp` 插件包，提供 `TickDataSchema()`、`OpenCtpMarketDataSource`、自动连接/登录/静态订阅/重连，并可作为 `zippy` engine 的 `source=` 使用。

**Architecture:** Rust core 直接依赖 `ctp2rs` 实现 schema、tick 标准化、行情 source 和指标；PyO3 绑定层把 Rust Source 暴露给 Python 用户；Python 包仅作为稳定入口导出 schema 和 `OpenCtpMarketDataSource`。实现顺序按 schema/normalize、source 核心、Python 绑定、示例与验证分阶段推进。

**Tech Stack:** Rust, Arrow, PyO3, maturin, `ctp2rs`, Python package skeleton

---

### Task 1: 固定 Tick schema 与 Python schema 入口

**Files:**
- Modify: `crates/zippy-openctp-core/src/lib.rs`
- Modify: `crates/zippy-openctp-core/src/schema.rs`
- Modify: `python/zippy_openctp/schemas.py`
- Modify: `python/zippy_openctp/__init__.py`
- Modify: `python/zippy_openctp/_internal.pyi`
- Create: `crates/zippy-openctp-core/tests/schema_contract.rs`

- [ ] **Step 1: 写 schema failing test**

```rust
#[test]
fn tick_data_schema_contains_required_columns_in_stable_order() {
    let schema = zippy_openctp_core::schema::tick_data_schema();
    let fields: Vec<_> = schema.fields().iter().map(|field| field.name().as_str()).collect();

    assert_eq!(
        fields,
        vec![
            "instrument_id",
            "exchange_id",
            "trading_day",
            "action_day",
            "dt",
            "last_price",
            "volume",
            "turnover",
            "open_interest",
            "bid_price_1",
            "bid_volume_1",
            "ask_price_1",
            "ask_volume_1",
        ]
    );
}
```

- [ ] **Step 2: 跑 schema test，确认红灯**

Run: `cargo test -p zippy-openctp-core --test schema_contract -v`
Expected: FAIL，原因是 `tick_data_schema()` 尚不存在或不返回稳定 Arrow schema

- [ ] **Step 3: 实现最小 schema 代码**

```rust
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use zippy_core::SchemaRef;

pub fn tick_data_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("instrument_id", DataType::Utf8, false),
        Field::new("exchange_id", DataType::Utf8, true),
        Field::new("trading_day", DataType::Utf8, true),
        Field::new("action_day", DataType::Utf8, true),
        Field::new("dt", DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None), false),
        Field::new("last_price", DataType::Float64, true),
        Field::new("volume", DataType::Int64, true),
        Field::new("turnover", DataType::Float64, true),
        Field::new("open_interest", DataType::Float64, true),
        Field::new("bid_price_1", DataType::Float64, true),
        Field::new("bid_volume_1", DataType::Int64, true),
        Field::new("ask_price_1", DataType::Float64, true),
        Field::new("ask_volume_1", DataType::Int64, true),
    ]))
}
```

- [ ] **Step 4: 实现 Python schema 入口**

```python
from ._internal import tick_data_schema


def TickDataSchema():
    return tick_data_schema()
```

- [ ] **Step 5: 重跑 schema test，确认绿灯**

Run: `cargo test -p zippy-openctp-core --test schema_contract -v`
Expected: PASS

- [ ] **Step 6: 提交 schema 任务**

```bash
git add crates/zippy-openctp-core/src/lib.rs \
  crates/zippy-openctp-core/src/schema.rs \
  crates/zippy-openctp-core/tests/schema_contract.rs \
  python/zippy_openctp/schemas.py \
  python/zippy_openctp/__init__.py \
  python/zippy_openctp/_internal.pyi
git commit -m "feat: add tick data schema contract"
```

### Task 2: 实现 tick 标准化模型

**Files:**
- Modify: `crates/zippy-openctp-core/src/normalize.rs`
- Create: `crates/zippy-openctp-core/tests/normalize_tick.rs`

- [ ] **Step 1: 写 normalize failing test**

```rust
#[test]
fn normalize_tick_maps_raw_values_into_schema_row() {
    let raw = RawTickSnapshot {
        instrument_id: "IF2506".to_string(),
        exchange_id: "CFFEX".to_string(),
        trading_day: "20260408".to_string(),
        action_day: "20260408".to_string(),
        update_time: "09:30:00".to_string(),
        update_millisec: 500,
        last_price: 3912.4,
        volume: 1234,
        turnover: 987654.0,
        open_interest: 56789.0,
        bid_price_1: 3912.2,
        bid_volume_1: 10,
        ask_price_1: 3912.6,
        ask_volume_1: 8,
    };

    let row = normalize_tick(&raw).unwrap();
    assert_eq!(row.instrument_id, "IF2506");
    assert_eq!(row.exchange_id.as_deref(), Some("CFFEX"));
    assert_eq!(row.volume, Some(1234));
    assert!(row.dt_ns > 0);
}
```

- [ ] **Step 2: 跑 normalize test，确认红灯**

Run: `cargo test -p zippy-openctp-core normalize_tick -v`
Expected: FAIL，原因是 `RawTickSnapshot` / `normalize_tick()` 尚未实现

- [ ] **Step 3: 实现最小 normalize 结构**

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct RawTickSnapshot {
    pub instrument_id: String,
    pub exchange_id: String,
    pub trading_day: String,
    pub action_day: String,
    pub update_time: String,
    pub update_millisec: i32,
    pub last_price: f64,
    pub volume: i64,
    pub turnover: f64,
    pub open_interest: f64,
    pub bid_price_1: f64,
    pub bid_volume_1: i64,
    pub ask_price_1: f64,
    pub ask_volume_1: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedTickRow {
    pub instrument_id: String,
    pub exchange_id: Option<String>,
    pub trading_day: Option<String>,
    pub action_day: Option<String>,
    pub dt_ns: i64,
    pub last_price: Option<f64>,
    pub volume: Option<i64>,
    pub turnover: Option<f64>,
    pub open_interest: Option<f64>,
    pub bid_price_1: Option<f64>,
    pub bid_volume_1: Option<i64>,
    pub ask_price_1: Option<f64>,
    pub ask_volume_1: Option<i64>,
}
```

- [ ] **Step 4: 实现最小 `normalize_tick()`**

```rust
pub fn normalize_tick(raw: &RawTickSnapshot) -> Result<NormalizedTickRow, NormalizeError> {
    let dt_ns = compose_exchange_timestamp_ns(
        &raw.action_day,
        &raw.update_time,
        raw.update_millisec,
    )?;

    Ok(NormalizedTickRow {
        instrument_id: raw.instrument_id.clone(),
        exchange_id: Some(raw.exchange_id.clone()),
        trading_day: Some(raw.trading_day.clone()),
        action_day: Some(raw.action_day.clone()),
        dt_ns,
        last_price: Some(raw.last_price),
        volume: Some(raw.volume),
        turnover: Some(raw.turnover),
        open_interest: Some(raw.open_interest),
        bid_price_1: Some(raw.bid_price_1),
        bid_volume_1: Some(raw.bid_volume_1),
        ask_price_1: Some(raw.ask_price_1),
        ask_volume_1: Some(raw.ask_volume_1),
    })
}
```

- [ ] **Step 5: 重跑 normalize test**

Run: `cargo test -p zippy-openctp-core normalize_tick -v`
Expected: PASS

- [ ] **Step 6: 提交 normalize 任务**

```bash
git add crates/zippy-openctp-core/src/normalize.rs \
  crates/zippy-openctp-core/tests/normalize_tick.rs
git commit -m "feat: add tick normalization primitives"
```

### Task 3: 实现 source 配置与指标快照

**Files:**
- Modify: `crates/zippy-openctp-core/src/metrics.rs`
- Modify: `crates/zippy-openctp-core/src/source.rs`
- Create: `crates/zippy-openctp-core/tests/source_config.rs`

- [ ] **Step 1: 写 source config failing test**

```rust
#[test]
fn source_config_defaults_to_single_tick_publish() {
    let config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        vec!["IF2506".to_string()],
        ".cache/openctp/md".to_string(),
    );

    assert_eq!(config.rows_per_batch, 1);
    assert_eq!(config.flush_interval_ms, 0);
    assert!(config.reconnect);
}
```

- [ ] **Step 2: 跑 config test，确认红灯**

Run: `cargo test -p zippy-openctp-core source_config -v`
Expected: FAIL，原因是 `OpenCtpMarketDataSourceConfig::new` 还未实现默认 low-latency 语义

- [ ] **Step 3: 扩展 config 和 metrics**

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCtpMarketDataSourceConfig {
    pub front: String,
    pub broker_id: String,
    pub user_id: String,
    pub password: String,
    pub instruments: Vec<String>,
    pub flow_path: String,
    pub reconnect: bool,
    pub login_timeout_sec: u64,
    pub rows_per_batch: usize,
    pub flush_interval_ms: u64,
}

impl OpenCtpMarketDataSourceConfig {
    pub fn new(
        front: String,
        broker_id: String,
        user_id: String,
        password: String,
        instruments: Vec<String>,
        flow_path: String,
    ) -> Self {
        Self {
            front,
            broker_id,
            user_id,
            password,
            instruments,
            flow_path,
            reconnect: true,
            login_timeout_sec: 10,
            rows_per_batch: 1,
            flush_interval_ms: 0,
        }
    }
}
```

- [ ] **Step 4: 定义 metrics snapshot**

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenCtpSourceMetrics {
    pub ticks_received_total: u64,
    pub ticks_emitted_total: u64,
    pub batches_emitted_total: u64,
    pub reconnects_total: u64,
    pub login_failures_total: u64,
    pub subscribe_failures_total: u64,
}
```

- [ ] **Step 5: 重跑 config test**

Run: `cargo test -p zippy-openctp-core source_config -v`
Expected: PASS

- [ ] **Step 6: 提交 source config 任务**

```bash
git add crates/zippy-openctp-core/src/source.rs \
  crates/zippy-openctp-core/src/metrics.rs \
  crates/zippy-openctp-core/tests/source_config.rs
git commit -m "feat: add source config and metrics snapshot"
```

### Task 4: 实现 batching 状态机与 fake source 驱动测试

**Files:**
- Modify: `crates/zippy-openctp-core/src/source.rs`
- Create: `crates/zippy-openctp-core/tests/source_batching.rs`

- [ ] **Step 1: 写 batching failing test**

```rust
#[test]
fn batching_flushes_immediately_when_rows_per_batch_is_one() {
    let mut source = FakeOpenCtpSourceRuntime::new(default_source_config());
    let batch = source.push_tick(sample_tick("IF2506", 1)).unwrap();

    assert!(batch.is_some());
    assert_eq!(batch.unwrap().num_rows(), 1);
}
```

- [ ] **Step 2: 跑 batching test，确认红灯**

Run: `cargo test -p zippy-openctp-core source_batching -v`
Expected: FAIL，原因是 fake runtime 和 batching 逻辑尚未实现

- [ ] **Step 3: 实现最小 batching runtime**

```rust
pub struct BufferedTickEmitter {
    schema: SchemaRef,
    rows_per_batch: usize,
    flush_interval_ms: u64,
    buffer: Vec<NormalizedTickRow>,
    last_flush_at: Option<Instant>,
}
```

- [ ] **Step 4: 实现 `push_tick()` 与 `flush_if_due()`**

```rust
impl BufferedTickEmitter {
    pub fn push_tick(&mut self, row: NormalizedTickRow) -> Result<Option<RecordBatch>, SourceError> {
        self.buffer.push(row);
        if self.buffer.len() >= self.rows_per_batch.max(1) {
            return self.flush();
        }
        Ok(None)
    }

    pub fn flush_if_due(&mut self, now: Instant) -> Result<Option<RecordBatch>, SourceError> {
        if self.flush_interval_ms == 0 {
            return Ok(None);
        }
        if self.last_flush_at.map(|last| now.duration_since(last).as_millis() as u64 >= self.flush_interval_ms).unwrap_or(false) {
            return self.flush();
        }
        Ok(None)
    }
}
```

- [ ] **Step 5: 重跑 batching test**

Run: `cargo test -p zippy-openctp-core source_batching -v`
Expected: PASS

- [ ] **Step 6: 提交 batching 任务**

```bash
git add crates/zippy-openctp-core/src/source.rs \
  crates/zippy-openctp-core/tests/source_batching.rs
git commit -m "feat: add source batching runtime"
```

### Task 5: 接入 `ctp2rs` 行情回调与 Source 实现

**Files:**
- Modify: `crates/zippy-openctp-core/Cargo.toml`
- Modify: `crates/zippy-openctp-core/src/lib.rs`
- Modify: `crates/zippy-openctp-core/src/source.rs`
- Create: `crates/zippy-openctp-core/tests/source_lifecycle.rs`

- [ ] **Step 1: 写 source lifecycle failing test**

```rust
#[test]
fn fake_md_driver_emits_data_and_stop_events() {
    let mut driver = FakeMdDriver::default();
    let source = OpenCtpMarketDataSource::from_driver(default_source_config(), driver.handle());
    let emitted = run_source_once(source, driver.emit_sample_sequence()).unwrap();

    assert!(emitted.data_batches > 0);
    assert!(emitted.stopped);
}
```

- [ ] **Step 2: 跑 lifecycle test，确认红灯**

Run: `cargo test -p zippy-openctp-core source_lifecycle -v`
Expected: FAIL，原因是 `OpenCtpMarketDataSource` 和 fake md driver 尚未实现

- [ ] **Step 3: 加入 `ctp2rs` 依赖并定义 md driver 边界**

```toml
[dependencies]
ctp2rs = "0.6"
arrow = "53.3.0"
zippy-core = { path = "../../../../crates/zippy-core" }
```

- [ ] **Step 4: 实现 `OpenCtpMarketDataSource` 最小 Source**

```rust
pub struct OpenCtpMarketDataSource {
    config: OpenCtpMarketDataSourceConfig,
    schema: SchemaRef,
    metrics: Arc<Mutex<OpenCtpSourceMetrics>>,
    driver: Box<dyn MdDriver>,
}
```

- [ ] **Step 5: 实现 fake driver 路径并重跑 test**

Run: `cargo test -p zippy-openctp-core source_lifecycle -v`
Expected: PASS

- [ ] **Step 6: 提交 source lifecycle 任务**

```bash
git add crates/zippy-openctp-core/Cargo.toml \
  crates/zippy-openctp-core/src/lib.rs \
  crates/zippy-openctp-core/src/source.rs \
  crates/zippy-openctp-core/tests/source_lifecycle.rs
git commit -m "feat: add openctp source lifecycle"
```

### Task 6: 实现 Python 绑定与包入口

**Files:**
- Modify: `crates/zippy-openctp-python/Cargo.toml`
- Modify: `crates/zippy-openctp-python/src/lib.rs`
- Modify: `python/zippy_openctp/__init__.py`
- Modify: `python/zippy_openctp/_internal.pyi`
- Modify: `python/zippy_openctp/schemas.py`
- Create: `tests/test_python_api.py`

- [ ] **Step 1: 写 Python API failing test**

```python
def test_tick_data_schema_is_exposed():
    import zippy_openctp

    schema = zippy_openctp.schemas.TickDataSchema()
    assert schema is not None
```

- [ ] **Step 2: 跑 Python test，确认红灯**

Run: `uv run pytest tests/test_python_api.py::test_tick_data_schema_is_exposed -v`
Expected: FAIL，原因是 Python 扩展尚未暴露 schema/source

- [ ] **Step 3: 配置 PyO3 crate**

```toml
[lib]
name = "zippy_openctp_internal"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.22", features = ["extension-module"] }
zippy-openctp-core = { path = "../zippy-openctp-core" }
```

- [ ] **Step 4: 暴露最小 Python API**

```rust
#[pyfunction]
fn tick_data_schema(py: Python<'_>) -> PyResult<PyObject> {
    let schema_name = zippy_openctp_core::schema::tick_data_schema_name();
    Ok(schema_name.into_py(py))
}

#[pyclass]
struct OpenCtpMarketDataSource {
    #[pyo3(get)]
    front: String,
}
```

- [ ] **Step 5: 重跑 Python test**

Run: `uv run pytest tests/test_python_api.py::test_tick_data_schema_is_exposed -v`
Expected: PASS

- [ ] **Step 6: 提交 Python 绑定任务**

```bash
git add crates/zippy-openctp-python/Cargo.toml \
  crates/zippy-openctp-python/src/lib.rs \
  python/zippy_openctp/__init__.py \
  python/zippy_openctp/_internal.pyi \
  python/zippy_openctp/schemas.py \
  tests/test_python_api.py
git commit -m "feat: expose openctp plugin python api"
```

### Task 7: 实现最小 example 与端到端验证

**Files:**
- Modify: `examples/md_to_parquet.py`
- Modify: `examples/md_to_remote_pipeline.py`
- Modify: `README.md`

- [ ] **Step 1: 写 README/example failing smoke**

```bash
uv run python -m py_compile examples/md_to_parquet.py examples/md_to_remote_pipeline.py
```

Expected: FAIL，原因是示例文件当前只有 bootstrap docstring，不是可执行 pipeline

- [ ] **Step 2: 写最小 example**

```python
import zippy
import zippy_openctp

source = zippy_openctp.OpenCtpMarketDataSource(
    front="tcp://127.0.0.1:12345",
    broker_id="9999",
    user_id="000001",
    password="secret",
    instruments=["IF2506"],
    flow_path=".cache/openctp/md",
)
engine = zippy.TimeSeriesEngine(
    name="bars",
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

- [ ] **Step 3: 更新 README**

```markdown
## Quickstart

import zippy
import zippy_openctp
```

- [ ] **Step 4: 重跑 example smoke**

Run: `uv run python -m py_compile examples/md_to_parquet.py examples/md_to_remote_pipeline.py`
Expected: PASS

- [ ] **Step 5: 跑首版验证集**

Run: `cargo test --workspace`
Expected: PASS

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS

Run: `uv run pytest tests -v`
Expected: PASS

- [ ] **Step 6: 提交 example 与验证任务**

```bash
git add examples/md_to_parquet.py \
  examples/md_to_remote_pipeline.py \
  README.md
git commit -m "docs: add openctp plugin examples"
```
