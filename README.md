# zippy-openctp

Python-first OpenCTP market data plugin for `zippy`.

## Quickstart

```python
import os

import zippy
import zippy_openctp

instruments = [
    item.strip()
    for item in os.getenv("OPENCTP_INSTRUMENTS", "IF2506").split(",")
    if item.strip()
]

source = zippy_openctp.OpenCtpMarketDataSource(
    front=os.environ["OPENCTP_MD_FRONT"],
    broker_id=os.environ["OPENCTP_BROKER_ID"],
    user_id=os.environ["OPENCTP_USER_ID"],
    password=os.environ["OPENCTP_PASSWORD"],
    instruments=instruments,
    flow_path=".cache/openctp/md",
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
    target=zippy.NullPublisher(),
)
```

`OpenCtpMarketDataSource(...)` 只会构造 source 对象；不会在 import 或构造阶段立刻发起真实连接。
真实 OpenCTP 登录、订阅与收流会在关联的 engine 执行 `start()` 后发生。

## Live Source 手工验证

先用环境变量配置真实 OpenCTP 参数：

```bash
export OPENCTP_MD_FRONT='tcp://127.0.0.1:12345'
export OPENCTP_BROKER_ID='9999'
export OPENCTP_USER_ID='000001'
export OPENCTP_PASSWORD='secret'
export OPENCTP_INSTRUMENTS='IF2506,IH2506'
```

其中 `OPENCTP_INSTRUMENTS` 是可选项，格式为逗号分隔；未设置时，示例默认订阅 `IF2506`。

### 1. 先做语法 smoke

先确认两个 example 至少能成功导入和编译：

```bash
uv run python -m py_compile examples/md_to_parquet.py examples/md_to_remote_pipeline.py
```

### 2. 做 live source 联调

本地落盘验证：

```bash
uv run python examples/md_to_parquet.py
```

远程分发验证：

```bash
uv run python examples/md_to_remote_pipeline.py
```

这两个脚本都会：

- 打印 `source config`
- 打印启动前的 `source status` 与 `source metrics`
- 调用 `engine.start()` 进入真实 OpenCTP 连接
- 持续打印运行期 `source.status()` 与 `source.metrics()`
- 在 `Ctrl-C` 后执行 `engine.stop()`

`examples/md_to_parquet.py` 会把 1 分钟 bar 写到 `data/openctp_bars`。  
`examples/md_to_remote_pipeline.py` 会把 1 分钟 bar 发布到 `tcp://127.0.0.1:7001`，供下游 `ZmqSource`
或订阅程序继续消费。

### 3. 观察 `source.status()` / `source.metrics()`

`source.status()` 的生命周期含义：

- `created`：source 已构造，但尚未启动
- `connecting`：engine 已启动，正在建立连接或登录
- `running`：登录成功且订阅完成，正在收流
- `degraded`：部分订阅失败或重连期间处于降级态
- `failed`：登录/订阅/底层驱动发生不可恢复错误
- `stopped`：engine/source 已停止

`source.metrics()` 当前返回一个 Python `dict`，可重点关注：

- `ticks_received_total`：底层收到的 tick 数
- `ticks_emitted_total`：source 向 zippy 发出的 tick 数
- `batches_emitted_total`：发出的 Arrow batch 数
- `reconnects_total`：发生的重连次数
- `login_failures_total`：登录失败次数
- `subscribe_failures_total`：订阅失败次数

判断手工验证是否通过，可以按这个顺序看：

1. 启动前是 `created`，且 metrics 全零。
2. 运行 example 后状态从 `connecting` 进入 `running`，或在部分订阅失败时进入 `degraded`。
3. 有真实行情流入时，`ticks_received_total` 与 `ticks_emitted_total` 持续增加。
4. `md_to_parquet.py` 写出 parquet 文件，或 `md_to_remote_pipeline.py` 的下游 pipeline 能收到数据。
5. `Ctrl-C` 后状态收敛到 `stopped`。

## Status

This repository is the standalone plugin home for OpenCTP market data support.

The current stage provides:

- an independent git repository
- a fixed tick schema contract via `zippy_openctp.schemas.TickDataSchema()`
- a Python-facing `OpenCtpMarketDataSource` with stable config, status, and metrics accessors
- Rust core modules for live source lifecycle, normalization, and metrics
- minimal examples for local Parquet and remote ZMQ pipelines
- documented manual verification steps for live OpenCTP connectivity
