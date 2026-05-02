# zippy-openctp

Python-first OpenCTP market data plugin for `zippy`.

## Logging

`zippy-openctp` 本身的 live source tracing 依赖 `zippy` 主仓库的统一日志系统。
推荐在任何真实联调或生产启动脚本最前面显式调用：

```python
import zippy

log_snapshot = zippy.setup_log(
    app="openctp_gateway",
    level="info",
    log_dir="logs",
    to_console=True,
    to_file=True,
)
print(log_snapshot)
```

默认会同时输出：

- 终端可读文本日志
- `logs/<app>/<date>_<run_id>.jsonl` 文件日志

文件 JSONL 会包含统一的 `message` 字段，以及 `component`、`event`、`run_id` 等结构化字段。

## Quickstart

```python
import os

import zippy
import zippy_openctp

instruments = [
    item.strip()
    for item in os.getenv("OPENCTP_INSTRUMENTS", "IF2606").split(",")
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

pipeline = (
    zippy.Pipeline("openctp_quickstart")
    .source(source)
    .stream_table(
        "openctp_ticks",
        schema=zippy_openctp.schemas.TickDataSchema(),
        persist_path="data/openctp_ticks",
    )
)
```

如果你希望 Quickstart 也真正落地日志，建议在构造 source 前先调用：

```python
zippy.setup_log(
    app="openctp_quickstart",
    level="info",
    log_dir="logs",
    to_console=True,
    to_file=True,
)
```

`OpenCtpMarketDataSource(...)` 只会构造 source 对象；不会在 import 或构造阶段立刻发起真实连接。
真实 OpenCTP 登录、订阅与收流会在关联的 engine 执行 `start()` 后发生。
插件会在 source 内部管理 segment 写入、active descriptor 和 reader attach，`Pipeline.stream_table()`
会直接消费这个 source，不需要用户手写 `SegmentStreamSource` 或 segment writer 生命周期代码。

## Market Generator Source

非开盘时段可以用 native generator source 生成 CTP md schema 兼容 tick，用于系统测试和性能测试：

```python
source = zippy_openctp.OpenCtpMarketGeneratorSource(
    instruments=["IF2606", "IH2606"],
    interval_ms=10,
    seed=42,
    max_ticks=100_000,
)
```

`interval_ms` 表示每隔 `T` ms 生成一轮完整行情；每轮会按配置顺序为所有 instrument 各生成
一条 tick。因此理论输入速率约为 `len(instruments) * 1000 / interval_ms` ticks/s。generator
复用真实 OpenCTP source 的 normalize、segment ingress、descriptor publisher 和 zippy source 生命周期，
下游 `Pipeline.stream_table()`、`TimeSeriesEngine`、`ReactiveStateEngine` 不需要改代码。

## Live Source 手工验证

三个 example 现在都通过命令行参数接收真实 OpenCTP 配置，不再依赖环境变量。

```bash
uv run python examples/md_to_parquet.py --help
uv run python examples/md_to_remote_pipeline.py --help
uv run python examples/remote_mid_price_diff_200_std_200.py --help
uv run python examples/subscribe_mid_price_factors.py --help
```

两个脚本共同需要的核心参数是：

- `--front`
- `--broker-id`
- `--user-id`
- `--password`
- `--instruments`
- `--log-dir`
- `--log-level`
- `--metrics-interval-sec`

其中 `--instruments` 是可选项，格式为逗号分隔；未传时默认订阅 `IF2606`。
三个脚本都会自动调用 `zippy.setup_log(...)`，并通过 `zippy.log_info(...)` 记录本次运行对应的 `log snapshot`、
运行配置、状态与 metrics。`--metrics-interval-sec` 用于控制 heartbeat 频率，默认 `5.0` 秒；联调时可以调小，
生产化运行建议保持较大间隔，避免日志过密。
涉及 master bus 的脚本还会额外使用：

- `--control-endpoint`
- `--stream-name`
- `--source-stream-name`
- `--output-stream-name`
- `--buffer-size`
- `--frame-size`

### 1. 先做语法 smoke

先确认四个 example 至少能成功导入和编译：

```bash
uv run python -m py_compile \
  examples/md_to_parquet.py \
  examples/md_to_remote_pipeline.py \
  examples/remote_mid_price_diff_200_std_200.py \
  examples/subscribe_mid_price_factors.py
```

### 2. 做 live source 联调

本地原始 tick 落盘验证：

```bash
uv run python examples/md_to_parquet.py \
  --front 'tcp://127.0.0.1:12345' \
  --broker-id '9999' \
  --user-id '000001' \
  --password 'secret' \
  --instruments 'IF2606,IH2606' \
  --log-dir 'logs'
```

原始 tick 数据中心验证：

```bash
uv run python examples/md_to_remote_pipeline.py \
  --front 'tcp://127.0.0.1:12345' \
  --broker-id '9999' \
  --user-id '000001' \
  --password 'secret' \
  --instruments 'IF2606,IH2606' \
  --start-master \
  --control-endpoint '~/.zippy/master.sock' \
  --stream-name 'openctp_ticks' \
  --buffer-size 131072 \
  --frame-size 4096 \
  --log-dir 'logs'
```

这两个脚本都会：

- 记录 `log snapshot`
- 记录 `source config`
- 记录启动前的 `source status` 与 `source metrics`
- 调用 `engine.start()` 进入真实 OpenCTP 连接
- 持续通过日志记录运行期 `source.status()` 与 `source.metrics()`
- 在 `Ctrl-C` 后执行 `engine.stop()`

如果只想保留文件日志、不想在终端看到 tracing 输出，可以额外传：

```bash
--no-console-log
```

`examples/md_to_parquet.py` 会把原始 tick 写到 `data/openctp_ticks`。  
`examples/md_to_remote_pipeline.py` 会把原始 tick 同时写到 `data/openctp_ticks`，并写入 master bus 的
`openctp_ticks` stream，供下游 `BusStreamSource`、`ReactiveStateEngine` 或其他订阅程序继续消费。

### 3. 从 bus tick stream 计算 `MID_PRICE_DIFF_200_STD_200`

下面这个独立脚本会从 master bus 的 `openctp_ticks` stream 读取原始 tick，在本进程里计算：

- `mid_price = (bid_price_1 + ask_price_1) / 2.0`
- `MID_PRICE_DIFF_200_STD_200 = TS_DIFF(mid_price, 200) / TS_STD(TS_DIFF(mid_price, 200), 200)`

然后把增强后的因子流继续写回 master bus 的 `openctp_mid_price_factors` stream：

```bash
uv run python examples/remote_mid_price_diff_200_std_200.py \
  --control-endpoint '~/.zippy/master.sock' \
  --source-stream-name 'openctp_ticks' \
  --output-stream-name 'openctp_mid_price_factors' \
  --output-path 'data/openctp_mid_price_factors' \
  --id-filter 'IF2606,IH2606' \
  --buffer-size 131072 \
  --frame-size 4096 \
  --log-dir 'logs' \
  --metrics-interval-sec 5
```

这个脚本默认使用 `BusStreamSource(..., mode=PIPELINE)` 连接本地 master bus，因此上游 flush/stop
边界会继续传递到本地下游因子引擎。如果只想对部分合约计算因子，可以通过 `--id-filter` 传逗号分隔的
`instrument_id` 白名单。同时它会把增强后的因子流：

- 写到新的 bus stream
- 写入 `data/openctp_mid_price_factors` 下的 parquet 文件

### 4. 订阅因子流并打印最新值

下面这个脚本会从 master bus 的 `openctp_mid_price_factors` stream 持续读取，并打印每个 batch 的最新一条因子值：

```bash
uv run python examples/subscribe_mid_price_factors.py \
  --control-endpoint '~/.zippy/master.sock' \
  --source-stream-name 'openctp_mid_price_factors' \
  --metrics-interval-sec 5
```

它内部会直接通过 `MasterClient.read_from(...)` 读取 factor stream，并打印：

- `instrument_id`
- `dt`
- `mid_price`
- `MID_PRICE_DIFF_200_STD_200`

这个订阅脚本不负责创建 stream，因此不需要 `--buffer-size` 或 `--frame-size`。

### 5. 观察 `source.status()` / `source.metrics()`

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
4. `md_to_parquet.py` 写出原始 tick parquet 文件，或 `md_to_remote_pipeline.py` 的下游 pipeline 能收到原始 tick 流。
5. `Ctrl-C` 后状态收敛到 `stopped`。

## Status

This repository is the standalone plugin home for OpenCTP market data support.

The current stage provides:

- an independent git repository
- a fixed tick schema contract via `zippy_openctp.schemas.TickDataSchema()`
- `dt` as event time in `timestamp(ns, Asia/Shanghai)`
- `localtime_ns` as local receive time in Unix epoch nanoseconds
- a Python-facing `OpenCtpMarketDataSource` with stable config, status, and metrics accessors
- a segment-primary source path that hides segment writer/reader lifecycle behind the source plugin
- Rust core modules for live source lifecycle, normalization, and metrics
- minimal examples for local Parquet, remote ZMQ pipelines, and remote factor computation
- documented manual verification steps for live OpenCTP connectivity
