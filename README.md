# zippy-openctp

Python-first OpenCTP market data plugin for `zippy`.

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

table = zippy.StreamTableEngine(
    name="openctp_ticks",
    source=source,
    input_schema=zippy_openctp.schemas.TickDataSchema(),
    target=zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:7001",
        stream_name="openctp_ticks",
        schema=zippy_openctp.schemas.TickDataSchema(),
    ),
    sink=zippy.ParquetSink(
        path="data/openctp_ticks",
        write_output=True,
        rows_per_batch=8192,
        flush_interval_ms=1000,
    ),
)
```

`OpenCtpMarketDataSource(...)` 只会构造 source 对象；不会在 import 或构造阶段立刻发起真实连接。
真实 OpenCTP 登录、订阅与收流会在关联的 engine 执行 `start()` 后发生。

## Live Source 手工验证

两个 example 现在都通过命令行参数接收真实 OpenCTP 配置，不再依赖环境变量。

```bash
uv run python examples/md_to_parquet.py --help
uv run python examples/md_to_remote_pipeline.py --help
```

两个脚本共同需要的核心参数是：

- `--front`
- `--broker-id`
- `--user-id`
- `--password`
- `--instruments`

其中 `--instruments` 是可选项，格式为逗号分隔；未传时默认订阅 `IF2606`。

### 1. 先做语法 smoke

先确认两个 example 至少能成功导入和编译：

```bash
uv run python -m py_compile examples/md_to_parquet.py examples/md_to_remote_pipeline.py
```

### 2. 做 live source 联调

本地原始 tick 落盘验证：

```bash
uv run python examples/md_to_parquet.py \
  --front 'tcp://127.0.0.1:12345' \
  --broker-id '9999' \
  --user-id '000001' \
  --password 'secret' \
  --instruments 'IF2606,IH2606'
```

原始 tick 数据中心验证：

```bash
uv run python examples/md_to_remote_pipeline.py \
  --front 'tcp://127.0.0.1:12345' \
  --broker-id '9999' \
  --user-id '000001' \
  --password 'secret' \
  --instruments 'IF2606,IH2606' \
  --stream-endpoint 'tcp://127.0.0.1:7001' \
  --stream-name 'openctp_ticks'
```

这两个脚本都会：

- 打印 `source config`
- 打印启动前的 `source status` 与 `source metrics`
- 调用 `engine.start()` 进入真实 OpenCTP 连接
- 持续打印运行期 `source.status()` 与 `source.metrics()`
- 在 `Ctrl-C` 后执行 `engine.stop()`

`examples/md_to_parquet.py` 会把原始 tick 写到 `data/openctp_ticks`。  
`examples/md_to_remote_pipeline.py` 会把原始 tick 同时写到 `data/openctp_ticks_remote`，并发布到
`tcp://127.0.0.1:7001`，供下游 `ZmqSource`、`StreamTableEngine` 或其他订阅程序继续消费。

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
4. `md_to_parquet.py` 写出原始 tick parquet 文件，或 `md_to_remote_pipeline.py` 的下游 pipeline 能收到原始 tick 流。
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
