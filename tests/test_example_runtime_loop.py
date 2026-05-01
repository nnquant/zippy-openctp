import importlib.util
from pathlib import Path
import sys


EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
RUNTIME_LEASE_PATH = EXAMPLES_DIR / "_runtime_lease.py"


def _load_runtime_lease_module():
    spec = importlib.util.spec_from_file_location(
        "zippy_openctp_examples_runtime_lease",
        RUNTIME_LEASE_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_runtime_loop_schedules_heartbeats_independent_of_metrics_interval():
    runtime_lease = _load_runtime_lease_module()

    state = runtime_lease.ExampleRuntimeLoopState.initial(
        start_monotonic=100.0,
        metrics_interval_sec=15.0,
        heartbeat_interval_sec=2.0,
    )

    first_tick = runtime_lease.advance_runtime_loop(
        state=state,
        now_monotonic=100.0,
    )
    assert first_tick.send_metrics is True
    assert first_tick.send_heartbeat is False
    assert first_tick.sleep_sec == 2.0

    second_tick = runtime_lease.advance_runtime_loop(
        state=first_tick.state,
        now_monotonic=102.0,
    )
    assert second_tick.send_heartbeat is True
    assert second_tick.send_metrics is False
    assert second_tick.sleep_sec == 2.0
