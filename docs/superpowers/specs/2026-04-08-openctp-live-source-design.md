# OpenCTP Live Source Design

## Goal

让 `OpenCtpMarketDataSource` 从当前的 fake/lifecycle 测试壳层，推进成默认使用
`ctp2rs` 真驱动的行情 `Source`，供 Python 用户直接接入 `zippy` runtime。

首版范围只覆盖行情接口，不覆盖交易接口。

## Architecture

对外仍然只暴露一个主入口：

- `zippy_openctp.OpenCtpMarketDataSource(...)`

对 Python 用户，这个对象默认就是“真实 OpenCTP 行情 Source”，而不是测试注入壳层。

Rust core 内部仍保留 driver 分层：

- `MdDriver`
- `Ctp2rsMdDriver`
- `FakeMdDriver`

其中：

- `Ctp2rsMdDriver` 是默认真实驱动
- `FakeMdDriver` 只保留给 Rust 测试
- `OpenCtpMarketDataSource::from_driver(...)` 只保留给内部测试，不作为 Python 主 API

## Runtime Flow

`OpenCtpMarketDataSource` 首版真实运行时流程固定为：

1. `created`
2. `connecting`
3. 建立行情连接
4. 登录
5. 按静态 `instruments` 列表发起订阅
6. 发出 `Hello`
7. 接收 tick
8. `normalize_tick()`
9. 进入 `BufferedTickEmitter`
10. 发出 `SourceEvent::Data`

停止流程：

1. 停止接收行情
2. flush 剩余缓冲
3. 发出 `SourceEvent::Stop`
4. 释放驱动资源

## Driver Model

真实驱动通过 `ctp2rs` 实现，但 `Source` 层不直接散落 OpenCTP 细节。

建议内部拆分：

- `source.rs`
  - `OpenCtpMarketDataSource`
  - `MdDriver`
  - `BufferedTickEmitter`
- `driver_ctp.rs`
  - `Ctp2rsMdDriver`
  - 行情 API 生命周期
  - SPI/回调桥接

`MdDriver` 的职责是：

- connect
- login
- subscribe
- 将柜台回调转换成 `MdDriverEvent`

`OpenCtpMarketDataSource` 的职责是：

- 维护 source 状态
- 维护 metrics
- 接收 `MdDriverEvent`
- 做 normalize + batching + emit

## Subscription Semantics

订阅范围在构造期固定：

- `instruments: list[str]`

首版不支持运行时动态增删订阅。

订阅失败语义：

- 单个合约订阅失败，不打死整个 source
- 记录失败指标
- source 继续运行
- 只输出成功订阅到的合约数据

这意味着 source 允许“部分成功订阅”的降级运行。

## Reconnect Semantics

首版支持自动重连，并固定为：

- 固定间隔重连
- 默认 `3s`

重连流程：

1. 检测断线
2. 状态切到 `degraded`
3. 等待 `3s`
4. 重新 connect
5. 重新 login
6. 按静态 `instruments` 重订阅
7. 成功后回到 `running`

首版不做：

- 指数退避
- 历史数据补发
- 本地 backlog 回放

## Status Model

`status()` 首版至少区分：

- `created`
- `connecting`
- `running`
- `degraded`
- `stopped`
- `failed`

语义：

- `created`
  - 对象已构造，尚未启动
- `connecting`
  - 正在 connect/login/初始订阅
- `running`
  - 已成功连通并处于正常收流
- `degraded`
  - 正在重连，或发生了部分订阅失败
- `stopped`
  - 正常关闭
- `failed`
  - 不可恢复错误

## Metrics

首版必须暴露并维护这些指标：

- `ticks_received_total`
- `ticks_emitted_total`
- `batches_emitted_total`
- `reconnects_total`
- `login_failures_total`
- `subscribe_failures_total`

如果后续能稳定获得更细粒度事件，可以再补：

- `connect_failures_total`
- `last_error`
- `subscribed_instruments`
- `failed_instruments`

这些不是首版硬要求。

## Python API Contract

Python 用户入口保持简单：

```python
import zippy_openctp

source = zippy_openctp.OpenCtpMarketDataSource(
    front="tcp://127.0.0.1:12345",
    broker_id="9999",
    user_id="000001",
    password="******",
    instruments=["IF2506", "IH2506"],
    flow_path=".cache/openctp/md",
    rows_per_batch=1,
    flush_interval_ms=0,
    reconnect=True,
    login_timeout_sec=10,
)
```

Python 层首版仍只暴露：

- `config()`
- `status()`
- `metrics()`

并保持：

- `config()["password"] == "***redacted***"`

## Testing Boundary

自动化测试边界固定为：

- fake driver 路径进 CI
- 真实 OpenCTP 连通性不进 CI

自动化测试覆盖：

- source 状态机
- batching
- normalize
- lifecycle
- Python 包装

手工/integration 验证覆盖：

- `connect -> login -> subscribe -> recv tick -> emit`
- 断线重连
- 重订阅

## Non-Goals

首版明确不做：

- 交易接口
- 动态订阅
- 多账户/多前置统一管理
- 行情回放
- 历史补发
- 指数退避重连
- 主仓库插件扫描器

## Implementation Notes

这一阶段最关键的工程原则有三条：

1. 真实 `ctp2rs` 细节收敛在 driver 层，不把柜台 API 直接散到 Python 包装或
   source 状态机里。
2. fake driver 继续存在，用来保证 CI 可跑、状态机可测。
3. `OpenCtpMarketDataSource` 的对外 API 不因为接入真实驱动而改变，Python 用户
   继续使用同一个入口。
