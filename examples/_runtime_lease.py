"""
Helpers for keeping example runtime metrics and master leases on separate cadences.
"""

from __future__ import annotations

from dataclasses import dataclass


DEFAULT_HEARTBEAT_INTERVAL_SEC = 2.0


@dataclass(frozen=True)
class ExampleRuntimeLoopState:
    """
    Track the next due times for metrics logging and master heartbeats.

    :param next_metrics_at: Monotonic deadline for the next metrics log.
    :type next_metrics_at: float
    :param next_heartbeat_at: Monotonic deadline for the next lease heartbeat.
    :type next_heartbeat_at: float
    :param metrics_interval_sec: Metrics logging cadence in seconds.
    :type metrics_interval_sec: float
    :param heartbeat_interval_sec: Lease heartbeat cadence in seconds.
    :type heartbeat_interval_sec: float
    """

    next_metrics_at: float
    next_heartbeat_at: float
    metrics_interval_sec: float
    heartbeat_interval_sec: float

    @classmethod
    def initial(
        cls,
        start_monotonic: float,
        metrics_interval_sec: float,
        heartbeat_interval_sec: float = DEFAULT_HEARTBEAT_INTERVAL_SEC,
    ) -> "ExampleRuntimeLoopState":
        """
        Build the initial runtime deadlines for a long-running example loop.

        :param start_monotonic: Loop start time from ``time.monotonic()``.
        :type start_monotonic: float
        :param metrics_interval_sec: Metrics logging cadence in seconds.
        :type metrics_interval_sec: float
        :param heartbeat_interval_sec: Lease heartbeat cadence in seconds.
        :type heartbeat_interval_sec: float
        :returns: Fresh loop state with immediate metrics and delayed heartbeat.
        :rtype: ExampleRuntimeLoopState
        :raises ValueError: If any interval is not positive.
        """
        if metrics_interval_sec <= 0:
            raise ValueError("metrics_interval_sec must be positive")
        if heartbeat_interval_sec <= 0:
            raise ValueError("heartbeat_interval_sec must be positive")
        return cls(
            next_metrics_at=start_monotonic,
            next_heartbeat_at=start_monotonic + heartbeat_interval_sec,
            metrics_interval_sec=metrics_interval_sec,
            heartbeat_interval_sec=heartbeat_interval_sec,
        )


@dataclass(frozen=True)
class ExampleRuntimeLoopTick:
    """
    Describe which loop-side actions are due and when the loop should wake again.

    :param state: Updated runtime loop state after processing the current tick.
    :type state: ExampleRuntimeLoopState
    :param send_metrics: Whether metrics logging is due now.
    :type send_metrics: bool
    :param send_heartbeat: Whether a master lease heartbeat is due now.
    :type send_heartbeat: bool
    :param sleep_sec: Seconds until the next runtime action is due.
    :type sleep_sec: float
    """

    state: ExampleRuntimeLoopState
    send_metrics: bool
    send_heartbeat: bool
    sleep_sec: float


def advance_runtime_loop(
    state: ExampleRuntimeLoopState,
    now_monotonic: float,
) -> ExampleRuntimeLoopTick:
    """
    Advance the example runtime schedule for the current monotonic timestamp.

    :param state: Current runtime loop state.
    :type state: ExampleRuntimeLoopState
    :param now_monotonic: Current monotonic timestamp.
    :type now_monotonic: float
    :returns: Due actions plus the updated schedule.
    :rtype: ExampleRuntimeLoopTick
    """
    send_metrics = now_monotonic >= state.next_metrics_at
    send_heartbeat = now_monotonic >= state.next_heartbeat_at

    next_metrics_at = state.next_metrics_at
    next_heartbeat_at = state.next_heartbeat_at

    if send_metrics:
        next_metrics_at = now_monotonic + state.metrics_interval_sec
    if send_heartbeat:
        next_heartbeat_at = now_monotonic + state.heartbeat_interval_sec

    next_sleep_at = min(next_metrics_at, next_heartbeat_at)
    sleep_sec = max(0.0, next_sleep_at - now_monotonic)

    return ExampleRuntimeLoopTick(
        state=ExampleRuntimeLoopState(
            next_metrics_at=next_metrics_at,
            next_heartbeat_at=next_heartbeat_at,
            metrics_interval_sec=state.metrics_interval_sec,
            heartbeat_interval_sec=state.heartbeat_interval_sec,
        ),
        send_metrics=send_metrics,
        send_heartbeat=send_heartbeat,
        sleep_sec=sleep_sec,
    )
