# OpenCTP Live Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `OpenCtpMarketDataSource` 从 fake driver 测试壳层推进成默认使用 `ctp2rs` 真驱动的 live market data source，同时保留 fake driver 作为 CI 可测路径。

**Architecture:** 继续保留 `MdDriver` 抽象，但将真实接入收敛到新增的 `driver_ctp.rs`。`source.rs` 只负责 source 状态、metrics、normalize、batching 和 `SourceEvent` 发射；Python 层继续暴露同一个 `OpenCtpMarketDataSource` API，只新增真实状态和指标快照，不泄露 `ctp2rs` 细节。

**Tech Stack:** Rust, `ctp2rs`, `crossbeam-channel`, `arrow`, `pyo3`, `maturin`, `pytest`

---

### Task 1: 收口 live source 状态模型与核心类型

**Files:**
- Modify: `crates/zippy-openctp-core/src/source.rs`
- Modify: `crates/zippy-openctp-core/src/metrics.rs`
- Modify: `crates/zippy-openctp-core/src/lib.rs`
- Test: `crates/zippy-openctp-core/tests/source_state.rs`

- [ ] **Step 1: 写 source 状态 failing test**

```rust
use zippy_openctp_core::{OpenCtpSourceMetrics, OpenCtpSourceStatus};

#[test]
fn source_status_starts_created_and_metrics_are_zeroed() {
    assert_eq!(OpenCtpSourceStatus::Created.as_str(), "created");

    let metrics = OpenCtpSourceMetrics::default();
    assert_eq!(metrics.ticks_received_total, 0);
    assert_eq!(metrics.subscribe_failures_total, 0);
}
```

- [ ] **Step 2: 跑测试确认红灯**

Run: `cargo test -p zippy-openctp-core --test source_state -v`
Expected: FAIL with `OpenCtpSourceStatus` not found

- [ ] **Step 3: 在 core 中增加状态模型**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenCtpSourceStatus {
    Created,
    Connecting,
    Running,
    Degraded,
    Stopped,
    Failed,
}

impl OpenCtpSourceStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Connecting => "connecting",
            Self::Running => "running",
            Self::Degraded => "degraded",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }
}
```

- [ ] **Step 4: 导出状态类型并重跑测试**

Run: `cargo test -p zippy-openctp-core --test source_state -v`
Expected: PASS

- [ ] **Step 5: 提交状态模型**

```bash
git add crates/zippy-openctp-core/src/source.rs \
  crates/zippy-openctp-core/src/metrics.rs \
  crates/zippy-openctp-core/src/lib.rs \
  crates/zippy-openctp-core/tests/source_state.rs
git commit -m "feat: add openctp source status model"
```

### Task 2: 新增 `driver_ctp.rs` 并定义真实驱动壳层

**Files:**
- Create: `crates/zippy-openctp-core/src/driver_ctp.rs`
- Modify: `crates/zippy-openctp-core/src/lib.rs`
- Modify: `crates/zippy-openctp-core/src/source.rs`
- Test: `crates/zippy-openctp-core/tests/driver_ctp_config.rs`

- [ ] **Step 1: 写真实驱动配置契约 failing test**

```rust
use zippy_openctp_core::{Ctp2rsMdDriver, OpenCtpMarketDataSourceConfig};

#[test]
fn ctp_driver_keeps_static_subscription_config() {
    let config = OpenCtpMarketDataSourceConfig::new(
        "tcp://127.0.0.1:12345".to_string(),
        "9999".to_string(),
        "000001".to_string(),
        "secret".to_string(),
        vec!["IF2506".to_string(), "IH2506".to_string()],
        ".cache/openctp/md".to_string(),
    );

    let driver = Ctp2rsMdDriver::new(config.clone());
    assert_eq!(driver.instruments(), config.instruments.as_slice());
}
```

- [ ] **Step 2: 跑测试确认红灯**

Run: `cargo test -p zippy-openctp-core --test driver_ctp_config -v`
Expected: FAIL with `Ctp2rsMdDriver` not found

- [ ] **Step 3: 实现最小真实驱动壳层**

```rust
pub struct Ctp2rsMdDriver {
    config: OpenCtpMarketDataSourceConfig,
}

impl Ctp2rsMdDriver {
    pub fn new(config: OpenCtpMarketDataSourceConfig) -> Self {
        Self { config }
    }

    pub fn instruments(&self) -> &[String] {
        self.config.instruments.as_slice()
    }
}
```

- [ ] **Step 4: 在 `source.rs` 中保留 `MdDriver` 抽象，但将默认构造路径指向真实驱动**

```rust
impl OpenCtpMarketDataSource {
    pub fn new(config: OpenCtpMarketDataSourceConfig) -> Self {
        Self::from_driver(config.clone(), Box::new(Ctp2rsMdDriver::new(config)))
    }
}
```

- [ ] **Step 5: 重跑测试**

Run: `cargo test -p zippy-openctp-core --test driver_ctp_config -v`
Expected: PASS

- [ ] **Step 6: 提交真实驱动壳层**

```bash
git add crates/zippy-openctp-core/src/driver_ctp.rs \
  crates/zippy-openctp-core/src/lib.rs \
  crates/zippy-openctp-core/src/source.rs \
  crates/zippy-openctp-core/tests/driver_ctp_config.rs
git commit -m "feat: add ctp2rs driver shell"
```

### Task 3: 实现 connect/login/subscribe 事件桥接

**Files:**
- Modify: `crates/zippy-openctp-core/src/driver_ctp.rs`
- Modify: `crates/zippy-openctp-core/src/source.rs`
- Test: `crates/zippy-openctp-core/tests/source_lifecycle.rs`

- [ ] **Step 1: 写 lifecycle failing test，锁定 hello/data/stop 顺序**

```rust
#[test]
fn live_source_emits_hello_data_stop_from_driver_sequence() {
    let sink = Arc::new(RecordingSink::default());
    let source = OpenCtpMarketDataSource::from_driver(
        default_source_config(),
        Box::new(FakeMdDriver {
            events: vec![MdDriverEvent::Tick(sample_tick("IF2506", 1)), MdDriverEvent::Stop],
        }),
    );

    let handle = Box::new(source).start(sink.clone()).unwrap();
    handle.join().unwrap();

    assert_eq!(sink.snapshot()[0], RecordedEvent::Hello { stream_name: "openctp.tick".to_string() });
    assert!(matches!(sink.snapshot()[1], RecordedEvent::Data { rows: 1 }));
    assert_eq!(sink.snapshot()[2], RecordedEvent::Stop);
}
```

- [ ] **Step 2: 跑测试确认当前基线仍通过**

Run: `cargo test -p zippy-openctp-core --test source_lifecycle -v`
Expected: PASS

- [ ] **Step 3: 在 `driver_ctp.rs` 中实现真实 `start()` 壳层，先完成结构不完成真实回调细节**

```rust
impl MdDriver for Ctp2rsMdDriver {
    fn start(self: Box<Self>, tx: Sender<MdDriverEvent>) -> CoreResult<MdDriverHandle> {
        let join_handle = thread::spawn(move || -> CoreResult<()> {
            // connect -> login -> subscribe static instruments
            // later: wire ctp2rs SPI callbacks into tx.send(MdDriverEvent::Tick(...))
            let _ = tx;
            Ok(())
        });

        Ok(MdDriverHandle::new(join_handle))
    }
}
```

- [ ] **Step 4: 把 `source.rs` 的 lifecycle 路径与状态更新对齐**

```rust
// before driver.start():
set_status(&status, OpenCtpSourceStatus::Connecting);

// after hello emitted and event loop starts:
set_status(&status, OpenCtpSourceStatus::Running);

// on stop:
set_status(&status, OpenCtpSourceStatus::Stopped);
```

- [ ] **Step 5: 跑核心 Rust 测试**

Run: `cargo test -p zippy-openctp-core --test source_lifecycle -v`
Expected: PASS

- [ ] **Step 6: 提交 lifecycle 收口**

```bash
git add crates/zippy-openctp-core/src/driver_ctp.rs \
  crates/zippy-openctp-core/src/source.rs \
  crates/zippy-openctp-core/tests/source_lifecycle.rs
git commit -m "feat: wire openctp source lifecycle"
```

### Task 4: 实现部分订阅失败继续运行与降级状态

**Files:**
- Modify: `crates/zippy-openctp-core/src/driver_ctp.rs`
- Modify: `crates/zippy-openctp-core/src/source.rs`
- Modify: `crates/zippy-openctp-core/src/metrics.rs`
- Test: `crates/zippy-openctp-core/tests/source_subscription.rs`

- [ ] **Step 1: 写订阅失败 failing test**

```rust
#[test]
fn source_stays_degraded_when_some_instruments_fail_to_subscribe() {
    let result = evaluate_subscription_results(
        &["IF2506".to_string(), "IH2506".to_string()],
        &["IF2506".to_string()],
    );

    assert_eq!(result.status, OpenCtpSourceStatus::Degraded);
    assert_eq!(result.subscribe_failures_total, 1);
}
```

- [ ] **Step 2: 跑测试确认红灯**

Run: `cargo test -p zippy-openctp-core --test source_subscription -v`
Expected: FAIL with helper/result not found

- [ ] **Step 3: 实现订阅结果归约逻辑**

```rust
struct SubscriptionOutcome {
    status: OpenCtpSourceStatus,
    subscribe_failures_total: u64,
}

fn evaluate_subscription_results(requested: &[String], succeeded: &[String]) -> SubscriptionOutcome {
    let failures = requested.len().saturating_sub(succeeded.len()) as u64;
    SubscriptionOutcome {
        status: if failures == 0 {
            OpenCtpSourceStatus::Running
        } else {
            OpenCtpSourceStatus::Degraded
        },
        subscribe_failures_total: failures,
    }
}
```

- [ ] **Step 4: 在 driver/source 中接入该结果，并更新 metrics/status**

```rust
metrics.subscribe_failures_total += failures;
set_status(&status, outcome.status);
```

- [ ] **Step 5: 重跑测试**

Run: `cargo test -p zippy-openctp-core --test source_subscription -v`
Expected: PASS

- [ ] **Step 6: 提交订阅失败降级语义**

```bash
git add crates/zippy-openctp-core/src/driver_ctp.rs \
  crates/zippy-openctp-core/src/source.rs \
  crates/zippy-openctp-core/src/metrics.rs \
  crates/zippy-openctp-core/tests/source_subscription.rs
git commit -m "feat: handle partial subscription failures"
```

### Task 5: 实现固定间隔重连状态机

**Files:**
- Modify: `crates/zippy-openctp-core/src/driver_ctp.rs`
- Modify: `crates/zippy-openctp-core/src/source.rs`
- Test: `crates/zippy-openctp-core/tests/source_reconnect.rs`

- [ ] **Step 1: 写重连 failing test**

```rust
#[test]
fn source_marks_degraded_then_recovers_after_reconnect() {
    let mut state = ReconnectState::new(Duration::from_secs(3));

    state.mark_disconnected();
    assert_eq!(state.status(), OpenCtpSourceStatus::Degraded);

    state.mark_reconnected();
    assert_eq!(state.status(), OpenCtpSourceStatus::Running);
    assert_eq!(state.reconnects_total(), 1);
}
```

- [ ] **Step 2: 跑测试确认红灯**

Run: `cargo test -p zippy-openctp-core --test source_reconnect -v`
Expected: FAIL with `ReconnectState` not found

- [ ] **Step 3: 实现最小固定间隔重连状态机**

```rust
struct ReconnectState {
    reconnect_interval: Duration,
    reconnects_total: u64,
    status: OpenCtpSourceStatus,
}
```

- [ ] **Step 4: 在真实 driver 中接入固定 3s 重连**

```rust
let reconnect_interval = Duration::from_secs(3);
thread::sleep(reconnect_interval);
// reconnect -> login -> resubscribe
```

- [ ] **Step 5: 重跑测试**

Run: `cargo test -p zippy-openctp-core --test source_reconnect -v`
Expected: PASS

- [ ] **Step 6: 提交重连状态机**

```bash
git add crates/zippy-openctp-core/src/driver_ctp.rs \
  crates/zippy-openctp-core/src/source.rs \
  crates/zippy-openctp-core/tests/source_reconnect.rs
git commit -m "feat: add fixed-interval reconnect loop"
```

### Task 6: 对齐 Python `status()` / `metrics()` 到 live source 语义

**Files:**
- Modify: `crates/zippy-openctp-python/src/lib.rs`
- Modify: `python/zippy_openctp/_internal.pyi`
- Modify: `tests/test_python_api.py`

- [ ] **Step 1: 写 Python failing test**

```python
def test_openctp_source_status_defaults_to_created():
    source = zippy_openctp.OpenCtpMarketDataSource(
        front="tcp://127.0.0.1:12345",
        broker_id="9999",
        user_id="000001",
        password="secret",
        instruments=["IF2506"],
    )

    assert source.status() == "created"
    assert source.metrics()["subscribe_failures_total"] == 0
```

- [ ] **Step 2: 跑测试确认基线**

Run: `uv run python -m pytest tests/test_python_api.py -v`
Expected: PASS or add missing assertions for live semantics

- [ ] **Step 3: 将 Python 包装切到真实 source 状态快照**

```rust
fn status(&self) -> &'static str {
    self.status.as_str()
}
```

- [ ] **Step 4: 重跑 Python 验证**

Run: `uv run maturin develop --manifest-path crates/zippy-openctp-python/Cargo.toml && uv run python -m pytest tests/test_python_api.py -v`
Expected: PASS

- [ ] **Step 5: 提交 Python live 语义对齐**

```bash
git add crates/zippy-openctp-python/src/lib.rs \
  python/zippy_openctp/_internal.pyi \
  tests/test_python_api.py
git commit -m "feat: align python source status with live driver"
```

### Task 7: 补手工验证脚本与文档

**Files:**
- Modify: `examples/md_to_parquet.py`
- Modify: `examples/md_to_remote_pipeline.py`
- Modify: `README.md`

- [ ] **Step 1: 写 example smoke 命令**

Run: `uv run python -m py_compile examples/md_to_parquet.py examples/md_to_remote_pipeline.py`
Expected: PASS

- [ ] **Step 2: 在 README 中补 live source 手工验证步骤**

```md
1. 配置 OpenCTP front、broker_id、user_id、password
2. 运行 `uv run python examples/md_to_parquet.py`
3. 观察 `source.status()` 从 `connecting` 进入 `running`
4. 观察 parquet 或 downstream pipeline 是否收到 tick
```

- [ ] **Step 3: 更新 example，展示真实 source 构造**

```python
source = zippy_openctp.OpenCtpMarketDataSource(
    front=os.environ["OPENCTP_MD_FRONT"],
    broker_id=os.environ["OPENCTP_BROKER_ID"],
    user_id=os.environ["OPENCTP_USER_ID"],
    password=os.environ["OPENCTP_PASSWORD"],
    instruments=["IF2506"],
)
```

- [ ] **Step 4: 运行 smoke**

Run: `uv run python -m py_compile examples/md_to_parquet.py examples/md_to_remote_pipeline.py`
Expected: PASS

- [ ] **Step 5: 提交手工验证文档**

```bash
git add examples/md_to_parquet.py \
  examples/md_to_remote_pipeline.py \
  README.md
git commit -m "docs: document live openctp source verification"
```

### Task 8: 全量验证与收尾

**Files:**
- Modify: `docs/superpowers/plans/2026-04-08-openctp-live-source.md`

- [ ] **Step 1: 跑 Rust 全量验证**

Run: `cargo test --workspace`
Expected: PASS

- [ ] **Step 2: 跑 Rust lint**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS

- [ ] **Step 3: 跑 Python 构建与测试**

Run: `uv run maturin develop --manifest-path crates/zippy-openctp-python/Cargo.toml && uv run python -m pytest tests -v`
Expected: PASS

- [ ] **Step 4: 跑 example smoke**

Run: `uv run python -m py_compile examples/md_to_parquet.py examples/md_to_remote_pipeline.py`
Expected: PASS

- [ ] **Step 5: 提交最终收尾**

```bash
git add .
git commit -m "feat: add live openctp market data source"
```
