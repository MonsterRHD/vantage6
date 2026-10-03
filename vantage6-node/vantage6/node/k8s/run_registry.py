"""Persistent node-local registry of algorithm run attempts.

The registry is the node's source of truth for which *generation* (attempt) of a
run owns which Kubernetes resources. It also carries write-ahead cleanup records
(tombstones) so that resource deletion can be retried after failures, network
outages or node restarts without ever touching a newer generation.

The state is stored as a single JSON file on the task data volume (a PVC) and is
rewritten atomically. All mutating operations are serialized with a reentrant
lock so they are safe to call from the node's socket, queue and polling threads.
"""

import json
import logging
import os
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from vantage6.common import logger_name

REGISTY_FILENAME = ".v6_node_state.json"

# Kill event ids are remembered long enough to survive socket reconnects and pod
# restarts, after which stale entries are garbage collected.
KILL_EVENT_TTL_SECONDS = 24 * 3600

# Cap the logs persisted on a cleanup record to avoid unbounded registry growth.
MAX_PERSISTED_LOG_CHARS = 50_000

ACTIVE_STATE_RUNNING = "running"
ACTIVE_STATE_KILLING = "killing"

CLEANUP_REASON_KILLED = "killed"
CLEANUP_REASON_COMPLETED = "completed"
CLEANUP_REASON_SHUTDOWN = "shutdown"
CLEANUP_REASON_RECONCILE = "reconcile"


@dataclass
class ActiveRun:
    """A run generation that currently owns Kubernetes resources."""

    run_id: int
    attempt_tag: str
    job_name: str
    task_id: int
    parent_id: int | None
    init_org_id: int
    state: str = ACTIVE_STATE_RUNNING
    created_at: float = field(default_factory=time.time)


@dataclass
class CleanupRecord:
    """
    Write-ahead tombstone pinning the exact resources of one run generation.

    The record is created *before* deletion starts and removed only after every
    pinned resource is gone (404 counts as gone) and any pending HQ log update has
    been delivered. Because it pins the generation-specific job name, retrying it
    can never delete resources belonging to a newer generation of the same run.
    """

    run_id: int
    attempt_tag: str | None
    job_name: str
    secret_name: str
    docker_secret_name: str | None
    reason: str
    task_id: int | None = None
    parent_id: int | None = None
    logs: str = ""
    logs_delivered: bool = False
    killed_reported: bool = False
    retries: int = 0
    last_error: str = ""
    created_at: float = field(default_factory=time.time)


class RunRegistry:
    """Persistent registry of active run attempts and pending cleanups."""

    def __init__(self, task_dir: str | Path, task_dir_extension: str | None = None):
        self.log = logging.getLogger(logger_name(__name__))
        base = Path(task_dir)
        if task_dir_extension:
            base = base / task_dir_extension
        self.path = base / REGISTY_FILENAME
        self._lock = threading.RLock()

        self.counter = 0
        self.active: dict[int, ActiveRun] = {}
        # keyed by job_name, which is unique per generation
        self.cleanups: dict[str, CleanupRecord] = {}
        self.processed_events: dict[str, float] = {}
        self.last_killall_ts: float | None = None

        self._load()

    # ------------------------------------------------------------------
    # attempt generation
    # ------------------------------------------------------------------
    def next_attempt_tag(self) -> str:
        """Reserve and persist a new monotonically increasing generation tag."""
        with self._lock:
            self.counter += 1
            tag = _int_to_tag(self.counter)
            self._save_locked()
            return tag

    def ensure_counter_at_least(self, value: int) -> None:
        """
        Advance the attempt counter so it is never reused.

        Used after rebuilding state from Kubernetes objects whose tags may have
        been issued by a registry instance whose state was lost.
        """
        with self._lock:
            if value > self.counter:
                self.counter = value
                self._save_locked()

    # ------------------------------------------------------------------
    # active runs
    # ------------------------------------------------------------------
    def activate(self, active_run: ActiveRun) -> None:
        """Register a run generation as active."""
        with self._lock:
            self.active[active_run.run_id] = active_run
            self._save_locked()

    def get_active(self, run_id: int) -> ActiveRun | None:
        with self._lock:
            return self.active.get(run_id)

    def list_active(self) -> list[ActiveRun]:
        with self._lock:
            return list(self.active.values())

    def mark_killing(self, run_id: int) -> ActiveRun | None:
        """Mark the current generation of a run as being killed."""
        with self._lock:
            active_run = self.active.get(run_id)
            if active_run is not None:
                active_run.state = ACTIVE_STATE_KILLING
                self._save_locked()
            return active_run

    def deactivate(self, run_id: int) -> None:
        """Remove a run from the active map."""
        with self._lock:
            if run_id in self.active:
                del self.active[run_id]
                self._save_locked()

    # ------------------------------------------------------------------
    # cleanup tombstones
    # ------------------------------------------------------------------
    def add_cleanup(self, record: CleanupRecord) -> bool:
        """
        Add a cleanup tombstone.

        Returns False if a tombstone for the same (generation-specific) job name
        already exists, in which case the existing record is kept.
        """
        with self._lock:
            if record.job_name in self.cleanups:
                return False
            record.logs = record.logs[:MAX_PERSISTED_LOG_CHARS]
            self.cleanups[record.job_name] = record
            self._save_locked()
            return True

    def get_cleanup(self, job_name: str) -> CleanupRecord | None:
        with self._lock:
            return self.cleanups.get(job_name)

    def cleanup_exists_for_run(self, run_id: int) -> bool:
        with self._lock:
            return any(r.run_id == run_id for r in self.cleanups.values())

    def list_cleanups(self) -> list[CleanupRecord]:
        with self._lock:
            return list(self.cleanups.values())

    def cleanup_job_names(self) -> set[str]:
        with self._lock:
            return set(self.cleanups)

    def record_retry(self, job_name: str, error: str) -> None:
        with self._lock:
            record = self.cleanups.get(job_name)
            if record is None:
                return
            record.retries += 1
            record.last_error = error[:500]
            self._save_locked()

    def mark_logs_delivered(self, job_name: str) -> None:
        with self._lock:
            record = self.cleanups.get(job_name)
            if record is None:
                return
            record.logs_delivered = True
            record.logs = ""
            self._save_locked()

    def mark_killed_reported(self, job_name: str) -> None:
        with self._lock:
            record = self.cleanups.get(job_name)
            if record is None:
                return
            record.killed_reported = True
            self._save_locked()

    def remove_cleanup(self, job_name: str) -> None:
        with self._lock:
            if job_name in self.cleanups:
                del self.cleanups[job_name]
                self._save_locked()

    def due_kill_reports(self) -> list[CleanupRecord]:
        """User-killed generations whose terminal feedback has not been emitted.

        Shutdown kills are excluded: their runs are expected to be picked up and
        re-executed when the node comes back, so they must never be reported as
        KILLED to HQ.
        """
        with self._lock:
            return [
                record
                for record in self.cleanups.values()
                if record.reason == CLEANUP_REASON_KILLED and not record.killed_reported
            ]

    # ------------------------------------------------------------------
    # kill event deduplication
    # ------------------------------------------------------------------
    def note_kill_event(self, event_id: str | None) -> bool:
        """
        Remember a processed kill event id.

        Returns True if the event is new and should be processed, False if it was
        processed before. Events without an id (legacy senders) are always
        processed.
        """
        if not event_id:
            return True
        with self._lock:
            now = time.time()
            self._prune_events_locked(now)
            if event_id in self.processed_events:
                return False
            self.processed_events[event_id] = now
            self._save_locked()
            return True

    def is_stale_kill_all(self, issued_at: float | None) -> bool:
        """
        Check whether a kill-all instruction is older than the last one seen.

        Instructions without a timestamp (legacy senders) are never considered
        stale.
        """
        if issued_at is None:
            return False
        with self._lock:
            if self.last_killall_ts is not None and issued_at < self.last_killall_ts:
                return True
            self.last_killall_ts = issued_at
            self._save_locked()
            return False

    def _prune_events_locked(self, now: float) -> None:
        stale = [
            event_id
            for event_id, seen_at in self.processed_events.items()
            if now - seen_at > KILL_EVENT_TTL_SECONDS
        ]
        for event_id in stale:
            del self.processed_events[event_id]

    # ------------------------------------------------------------------
    # persistence
    # ------------------------------------------------------------------
    def _load(self) -> None:
        with self._lock:
            if not self.path.exists():
                return
            try:
                data = json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError):
                self.log.exception(
                    "Run registry at %s is corrupt; it will be rebuilt from the "
                    "Kubernetes cluster state.",
                    self.path,
                )
                self._quarantine_locked()
                return
            try:
                self.counter = int(data.get("counter", 0))
                self.active = {
                    int(run_id): ActiveRun(**payload)
                    for run_id, payload in data.get("active", {}).items()
                }
                self.cleanups = {
                    job_name: CleanupRecord(**payload)
                    for job_name, payload in data.get("cleanups", {}).items()
                }
                self.processed_events = data.get("events", {})
                self.last_killall_ts = data.get("last_killall_ts")
            except (TypeError, AttributeError):
                self.log.exception(
                    "Run registry at %s has an unexpected schema; it will be "
                    "rebuilt from the Kubernetes cluster state.",
                    self.path,
                )
                self._quarantine_locked()

    def _quarantine_locked(self) -> None:
        try:
            os.rename(
                self.path,
                self.path.with_name(f"{self.path.name}.corrupt-{int(time.time())}"),
            )
        except OSError:
            self.log.exception("Could not quarantine corrupt run registry")
        self.counter = 0
        self.active = {}
        self.cleanups = {}
        self.processed_events = {}
        self.last_killall_ts = None

    def _save_locked(self) -> None:
        payload = {
            "counter": self.counter,
            "active": {str(run_id): asdict(run) for run_id, run in self.active.items()},
            "cleanups": {
                job_name: asdict(record) for job_name, record in self.cleanups.items()
            },
            "events": self.processed_events,
            "last_killall_ts": self.last_killall_ts,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_name(f"{self.path.name}.tmp")
        try:
            tmp_path.write_text(json.dumps(payload))
            os.replace(tmp_path, self.path)
        except OSError:
            self.log.exception("Failed to persist run registry to %s", self.path)
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass


def _int_to_tag(value: int) -> str:
    """Render a positive integer as a short lowercase base36 tag."""
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    chars = []
    while value:
        value, remainder = divmod(value, 36)
        chars.append(digits[remainder])
    return "g" + "".join(reversed(chars))
