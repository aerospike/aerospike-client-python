# Copyright 2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Periodic metrics export for the legacy client.

Exporters implement one method, ``export(snapshot)``. The tend thread only
copies listener objects into :class:`~aerospike_helpers.metrics.snapshot.MetricsSnapshot`.
A daemon thread named ``aerospike-metrics-export`` calls ``export``. Disabling
metrics stops that thread and leaves the exporter list registered. It does
not call ``close`` on application-owned exporters.

``file`` with a non-empty ``report_dir`` keeps the C learn-metrics writer.
``custom`` calls the registered exporters and does not install that writer.
``none`` collects and emits nothing. Omitting ``exporter`` keeps today's
file-writer behavior.
"""

from __future__ import annotations

from enum import Enum
import inspect
import logging
import threading
import time
from typing import Any, List, Optional, Protocol, Tuple, runtime_checkable

from aerospike_helpers.metrics.snapshot import (
    MetricsSnapshot,
    NodeMetricsSnapshot,
    node_from_listener,
    sample_process,
)

log = logging.getLogger(__name__)

DEFAULT_EXPORT_INTERVAL_SECONDS = 30.0
DEFAULT_MAX_CONSECUTIVE_FAILURES = 3
DEFAULT_SUSPEND_RETRY_INTERVAL_SECONDS = 60.0


class MetricsExporterType(str, Enum):
    """Where a periodic push goes. The values are the configuration spellings.

    Construction is case-insensitive, so ``MetricsExporterType("FILE")`` is
    :attr:`FILE`.
    """

    NONE = "none"
    FILE = "file"
    CUSTOM = "custom"

    @classmethod
    def _missing_(cls, value: object) -> Optional["MetricsExporterType"]:
        if isinstance(value, str):
            lowered = value.strip().lower()
            for member in cls:
                if member.value == lowered:
                    return member
        return None

    def __str__(self) -> str:
        return self.value


@runtime_checkable
class MetricsExporter(Protocol):
    """Receives snapshots from this synchronous client.

    The call runs on the ``aerospike-metrics-export`` thread. The snapshot is
    valid for this call. Copy ``snapshot.to_canonical_dict()`` before
    returning if the data must be retained. The application opens and closes
    whatever the exporter writes to. ``disable_metrics`` does not call
    ``close``.
    """

    def export(self, snapshot: MetricsSnapshot) -> None:
        """A snapshot was taken, once per export interval."""


def check_exporter(exporter: Any) -> None:
    """Reject an exporter this synchronous client cannot call.

    Raises:
        TypeError: The object has no ``export`` method, or ``export`` is a
            coroutine function.
    """
    method = getattr(exporter, "export", None)
    if not callable(method):
        raise TypeError(
            f"{type(exporter).__name__} is not a metrics exporter: missing export()"
        )
    if inspect.iscoroutinefunction(method):
        raise TypeError(
            f"{type(exporter).__name__} defines an async export(); "
            "this client needs a MetricsExporter (plain def export)"
        )


class _Health:
    __slots__ = ("failures", "suspended_at")

    def __init__(self) -> None:
        self.failures = 0
        self.suspended_at: Optional[float] = None


class _EnablePlan:
    __slots__ = (
        "apply",
        "mode",
        "policy_for_c",
        "exporters",
        "export_interval",
        "labels",
        "latency_columns",
        "latency_shift",
        "resume_policy",
    )

    def __init__(
        self,
        apply: bool,
        mode: Optional[MetricsExporterType] = None,
        policy_for_c: Any = None,
        exporters: Optional[Tuple[Any, ...]] = None,
        export_interval: float = DEFAULT_EXPORT_INTERVAL_SECONDS,
        labels: Optional[dict] = None,
        latency_columns: int = 7,
        latency_shift: int = 1,
        resume_policy: Any = None,
    ) -> None:
        self.apply = apply
        self.mode = mode
        self.policy_for_c = policy_for_c
        self.exporters = exporters
        self.export_interval = export_interval
        self.labels = labels or {}
        self.latency_columns = latency_columns
        self.latency_shift = latency_shift
        self.resume_policy = resume_policy


class ExportSession:
    """Exporter list, latest snapshot, and the export thread for one client.

    The C client stores one of these on the client object. ``disable_metrics``
    and ``close`` call :meth:`stop`. The next ``enable_metrics`` calls
    :meth:`prepare_enable` and then :meth:`start`.
    """

    def __init__(self) -> None:
        self.mode: Optional[MetricsExporterType] = None
        self.exporters: Tuple[Any, ...] = ()
        self.export_interval = DEFAULT_EXPORT_INTERVAL_SECONDS
        self.labels: dict = {}
        self.latency_columns = 7
        self.latency_shift = 1
        self.max_consecutive_failures = DEFAULT_MAX_CONSECUTIVE_FAILURES
        self.suspend_retry_interval = DEFAULT_SUSPEND_RETRY_INTERVAL_SECONDS
        self._resume_policy: Any = None
        self._latest: Optional[MetricsSnapshot] = None
        self._departed: List[NodeMetricsSnapshot] = []
        self._health: dict = {}
        self._paused = False
        self._pending: Optional[_EnablePlan] = None
        self._checkpoint: Optional[dict] = None
        self._lock = threading.Lock()
        self._lifecycle = threading.Lock()
        self._stop_event: Optional[threading.Event] = None
        self._thread: Optional[threading.Thread] = None

    def prepare_enable(self, policy: Any) -> Any:
        """Choose listeners for the C client and stage this session.

        Returns the policy object ``enable_metrics`` should hand to the C
        client, or ``None`` when the C default policy should be used. Does
        not start the export thread. :meth:`start` does that after the C
        call succeeds. :meth:`abort_enable` undoes the staging if it fails.
        """
        # A non-policy object is returned unchanged so the C client can raise
        # its existing parameter error.
        if policy is not None and not _is_metrics_policy(policy):
            return policy
        with self._lifecycle:
            self._checkpoint = self._capture()
            plan = self._plan(policy)
            self._pending = plan
            if plan.apply:
                self._apply(plan)
            return plan.policy_for_c

    def start(self) -> None:
        """Start exporting after the C client has enabled metrics."""
        with self._lifecycle:
            self._pending = None
            self._checkpoint = None
            self._paused = False
            if self.mode is MetricsExporterType.CUSTOM and self.exporters:
                self._start_thread()

    def abort_enable(self) -> None:
        """Drop a staged enable that the C client rejected."""
        with self._lifecycle:
            if self._checkpoint is not None:
                self._restore(self._checkpoint)
            self._pending = None
            self._checkpoint = None

    def stop(self) -> None:
        """Stop the export thread.

        The exporter list stays registered. Exporter ``close`` methods are
        not called. Safe to call more than once.
        """
        with self._lifecycle:
            self._paused = True
            thread = self._thread
            if thread is None:
                return
            if self._stop_event is not None:
                self._stop_event.set()
            self._thread = None
            self._stop_event = None
        if thread is threading.current_thread():
            return
        thread.join()

    def store_cluster(self, cluster: Any) -> None:
        """Copy a listener cluster. Called from the tend thread."""
        snapshot = MetricsSnapshot.from_cluster(
            cluster,
            labels=self.labels,
            latency_columns=self.latency_columns,
            latency_shift=self.latency_shift,
            metrics_enabled=True,
        )
        with self._lock:
            self._latest = snapshot

    def store_departed(self, node: Any) -> None:
        """Copy a node that is leaving. Called from the tend thread."""
        departed = node_from_listener(node)
        with self._lock:
            self._departed.append(departed)

    def peek(self) -> MetricsSnapshot:
        """On-demand snapshot. Departed nodes are included and kept."""
        with self._lock:
            departed = self._visible_departed_locked()
            latest = self._latest
            enabled = latest is not None or (
                self.mode is not None and not self._paused
            )
            labels = dict(self.labels)
            columns = self.latency_columns
            shift = self.latency_shift
        if latest is None:
            latest = MetricsSnapshot.empty(
                labels=labels,
                latency_columns=columns,
                latency_shift=shift,
                metrics_enabled=enabled,
            )
        return latest.with_departed(departed).with_process(*sample_process())

    def export_once(self) -> None:
        """Push the latest snapshot to every exporter that is not suspended.

        Departed nodes are removed only when every exporter attempted in this
        cycle returns. A failure isolates that exporter and keeps the finals
        for a later cycle.
        """
        if self.mode is not MetricsExporterType.CUSTOM or self._paused:
            return
        with self._lock:
            snapshot, departed_ids = self._compose_locked()
            exporters = self.exporters
        if snapshot is None:
            return
        runnable = self._runnable(exporters)
        if not runnable:
            return
        snapshot = snapshot.with_process(*sample_process())
        failed = False
        for exporter in runnable:
            try:
                exporter.export(snapshot)
            except Exception:
                failed = True
                self._record_failure(exporter)
            else:
                self._record_success(exporter)
        if not failed:
            self._drain(departed_ids)

    def _run(self) -> None:
        stop = self._stop_event
        if stop is None:
            return
        while not stop.wait(self.export_interval):
            try:
                self.export_once()
            except Exception:
                log.warning(
                    "Metrics export failed; will retry next interval", exc_info=True
                )

    def _start_thread(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name="aerospike-metrics-export", daemon=True
        )
        self._thread.start()

    def _plan(self, policy: Any) -> _EnablePlan:
        if policy is None:
            if (
                self.mode is MetricsExporterType.CUSTOM
                and self.exporters
                and self._resume_policy is not None
            ):
                return _EnablePlan(
                    True,
                    MetricsExporterType.CUSTOM,
                    self._resume_policy,
                    None,
                    self.export_interval,
                    dict(self.labels),
                    self.latency_columns,
                    self.latency_shift,
                    self._resume_policy,
                )
            return _EnablePlan(False)

        mode = _resolve_mode(policy)
        exporters = tuple(policy.exporters) if policy.exporters else None
        labels = dict(policy.labels or {})
        interval = float(policy.export_interval)
        columns = int(policy.latency_columns)
        shift = int(policy.latency_shift)
        if mode is MetricsExporterType.CUSTOM:
            wrapped = _copy_policy(
                policy,
                _listener_callbacks(
                    self, policy.metrics_listeners, store_snapshot=True, store_departed=True
                ),
            )
            return _EnablePlan(
                True, mode, wrapped, exporters, interval, labels, columns, shift, wrapped
            )
        explicit = policy.exporter is not None
        if mode is MetricsExporterType.NONE or (
            mode is MetricsExporterType.FILE and explicit and not (policy.report_dir or "")
        ):
            # Listeners replace the C file writer. Copy snapshots for on-demand
            # reads, but do not call export and do not retain departed nodes.
            wrapped = _copy_policy(
                policy,
                _listener_callbacks(
                    self, policy.metrics_listeners, store_snapshot=True, store_departed=False
                ),
            )
            return _EnablePlan(
                True, mode, wrapped, exporters, interval, labels, columns, shift, None
            )
        return _EnablePlan(
            True, mode, policy, exporters, interval, labels, columns, shift, None
        )

    def _apply(self, plan: _EnablePlan) -> None:
        self.mode = plan.mode
        if plan.exporters is not None:
            self.exporters = plan.exporters
        self.export_interval = plan.export_interval
        self.labels = dict(plan.labels)
        self.latency_columns = plan.latency_columns
        self.latency_shift = plan.latency_shift
        if plan.resume_policy is not None:
            self._resume_policy = plan.resume_policy

    def _capture(self) -> dict:
        return {
            "mode": self.mode,
            "exporters": self.exporters,
            "export_interval": self.export_interval,
            "labels": dict(self.labels),
            "latency_columns": self.latency_columns,
            "latency_shift": self.latency_shift,
            "resume_policy": self._resume_policy,
        }

    def _restore(self, checkpoint: dict) -> None:
        self.mode = checkpoint["mode"]
        self.exporters = checkpoint["exporters"]
        self.export_interval = checkpoint["export_interval"]
        self.labels = checkpoint["labels"]
        self.latency_columns = checkpoint["latency_columns"]
        self.latency_shift = checkpoint["latency_shift"]
        self._resume_policy = checkpoint["resume_policy"]

    def _visible_departed_locked(self) -> Tuple[NodeMetricsSnapshot, ...]:
        live_names = set()
        if self._latest is not None:
            live_names = {node.node_name for node in self._latest.nodes}
        return tuple(
            node for node in self._departed if node.node_name not in live_names
        )

    def _compose_locked(self) -> Tuple[Optional[MetricsSnapshot], set]:
        live_names = set()
        if self._latest is not None:
            live_names = {node.node_name for node in self._latest.nodes}
        exporting = []
        kept = []
        for node in self._departed:
            if node.node_name in live_names:
                continue
            kept.append(node)
            exporting.append(node)
        self._departed = kept
        if self._latest is None and not exporting:
            return None, set()
        base = self._latest
        if base is None:
            base = MetricsSnapshot.empty(
                labels=self.labels,
                latency_columns=self.latency_columns,
                latency_shift=self.latency_shift,
                metrics_enabled=True,
            )
        return base.with_departed(exporting), {id(node) for node in exporting}

    def _drain(self, departed_ids: set) -> None:
        with self._lock:
            self._departed = [
                node for node in self._departed if id(node) not in departed_ids
            ]

    def _runnable(self, exporters: Tuple[Any, ...]) -> List[Any]:
        now = time.monotonic()
        live = {id(exporter) for exporter in exporters}
        self._health = {
            key: health for key, health in self._health.items() if key in live
        }
        runnable = []
        for exporter in exporters:
            health = self._health.setdefault(id(exporter), _Health())
            if health.suspended_at is None:
                runnable.append(exporter)
                continue
            if now - health.suspended_at >= self.suspend_retry_interval:
                runnable.append(exporter)
        return runnable

    def _record_success(self, exporter: Any) -> None:
        health = self._health.get(id(exporter))
        if health is None:
            return
        if health.suspended_at is not None:
            log.info(
                "Metrics exporter %s recovered; resuming its normal export cadence",
                type(exporter).__name__,
            )
        health.failures = 0
        health.suspended_at = None

    def _record_failure(self, exporter: Any) -> None:
        health = self._health.setdefault(id(exporter), _Health())
        health.failures += 1
        if health.suspended_at is not None or (
            health.failures >= self.max_consecutive_failures
        ):
            health.suspended_at = time.monotonic()
            log.warning(
                "Metrics exporter %s failed %d consecutive exports; suspending it "
                "and retrying after %g seconds",
                type(exporter).__name__,
                health.failures,
                self.suspend_retry_interval,
                exc_info=True,
            )
            return
        log.warning(
            "Metrics exporter %s raised; continuing with the others",
            type(exporter).__name__,
            exc_info=True,
        )


def _resolve_mode(policy: Any) -> MetricsExporterType:
    exporter = getattr(policy, "exporter", None)
    if exporter is None:
        if getattr(policy, "exporters", ()):
            return MetricsExporterType.CUSTOM
        return MetricsExporterType.FILE
    if isinstance(exporter, MetricsExporterType):
        return exporter
    return MetricsExporterType(exporter)


def _is_metrics_policy(policy: Any) -> bool:
    return (
        policy.__class__.__module__ == "aerospike_helpers.metrics"
        and policy.__class__.__name__ == "MetricsPolicy"
    )


def _listener_callbacks(
    session: ExportSession,
    user: Any,
    store_snapshot: bool,
    store_departed: bool,
) -> Any:
    from aerospike_helpers.metrics import MetricsListeners

    def enable() -> None:
        if user is not None:
            user.enable_listener()

    def snapshot(cluster: Any) -> None:
        if store_snapshot:
            session.store_cluster(cluster)
        if user is not None:
            user.snapshot_listener(cluster)

    def node_close(node: Any) -> None:
        if store_departed:
            session.store_departed(node)
        if user is not None:
            user.node_close_listener(node)

    def disable(cluster: Any) -> None:
        session.stop()
        if user is not None:
            user.disable_listener(cluster)

    return MetricsListeners(
        enable_listener=enable,
        snapshot_listener=snapshot,
        node_close_listener=node_close,
        disable_listener=disable,
    )


def _copy_policy(policy: Any, listeners: Any) -> Any:
    from aerospike_helpers.metrics import MetricsPolicy

    copied = MetricsPolicy(
        metrics_listeners=listeners,
        report_dir=policy.report_dir,
        report_size_limit=policy.report_size_limit,
        interval=policy.interval,
        latency_columns=policy.latency_columns,
        latency_shift=policy.latency_shift,
        labels=dict(policy.labels or {}),
        exporter=policy.exporter,
        export_interval=policy.export_interval,
    )
    for exporter in policy.exporters:
        copied.add_exporter(exporter)
    return copied
