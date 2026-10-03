"""
Tests for the terminal-state guard in the node -> HQ algorithm_status_change
socket handler.

Once a run is terminal (killed, completed, crashed, ...), late or duplicated
callbacks must be ignored: they must neither be re-broadcast nor release
dependent tasks. Callbacks for unknown runs must be ignored without raising.
"""

import datetime
import logging
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from vantage6.common.enum import RunStatus

from vantage6.hq.model import Collaboration, Organization, Run, Task
from vantage6.hq.model.base import Database
from vantage6.hq.websockets import DefaultSocketNamespace


class TestAlgorithmStatusChangeGuard(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        Database().connect("sqlite://", allow_drop_all=True)

    @classmethod
    def tearDownClass(cls):
        Database().clear_data()
        Database().close()

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["SECRET_KEY"] = "test"

        self.org = Organization(name="ws-guard-org")
        self.collab = Collaboration(name="ws-guard-collab", organizations=[self.org])
        self.collab.save()
        self.task = Task(
            name="ws-guard-task",
            image="image",
            collaboration=self.collab,
            init_org=self.org,
            job_id=42,
        )
        self.task.save()

    def tearDown(self):
        Database().clear_data()

    def _handler(self):
        handler = DefaultSocketNamespace.__new__(DefaultSocketNamespace)
        handler.log = logging.getLogger("test-ws-guard")
        handler.socketio = MagicMock()
        return handler

    def _payload(self, run_id, status):
        return {
            "run_id": run_id,
            "task_id": self.task.id,
            "collaboration_id": self.collab.id,
            "status": status,
            "node_id": 1,
            "organization_id": self.org.id,
            "parent_id": None,
        }

    def _emit_guard(self, handler, payload):
        # socket.io's attribute-style session is not available outside a real
        # socket connection; the node identity check is exercised elsewhere.
        handler._is_node = MagicMock(return_value=True)
        with (
            self.app.test_request_context(),
            patch("vantage6.hq.websockets.emit") as emit_mock,
        ):
            handler.on_algorithm_status_change(payload)
        return emit_mock

    def test_late_completed_after_kill_is_ignored(self):
        run = Run(
            organization=self.org,
            task=self.task,
            status=RunStatus.KILLED.value,
            finished_at=datetime.datetime.now(datetime.UTC),
        )
        run.save()

        emit_mock = self._emit_guard(
            self._handler(), self._payload(run.id, RunStatus.COMPLETED.value)
        )
        emit_mock.assert_not_called()
        # the kill must remain the single terminal state
        self.assertEqual(Run.get(run.id).status, RunStatus.KILLED.value)

    def test_late_crashed_after_completed_is_ignored(self):
        run = Run(
            organization=self.org,
            task=self.task,
            status=RunStatus.COMPLETED.value,
            finished_at=datetime.datetime.now(datetime.UTC),
        )
        run.save()

        emit_mock = self._emit_guard(
            self._handler(), self._payload(run.id, RunStatus.CRASHED.value)
        )
        emit_mock.assert_not_called()

    def test_duplicate_killed_callback_is_ignored(self):
        run = Run(
            organization=self.org,
            task=self.task,
            status=RunStatus.KILLED.value,
            finished_at=datetime.datetime.now(datetime.UTC),
        )
        run.save()

        emit_mock = self._emit_guard(
            self._handler(), self._payload(run.id, RunStatus.KILLED.value)
        )
        emit_mock.assert_not_called()

    def test_callback_for_unknown_run_is_ignored(self):
        emit_mock = self._emit_guard(
            self._handler(), self._payload(999999, RunStatus.COMPLETED.value)
        )
        emit_mock.assert_not_called()

    def test_active_callback_for_open_run_is_broadcast(self):
        run = Run(
            organization=self.org,
            task=self.task,
            status=RunStatus.ACTIVE.value,
            finished_at=None,
        )
        run.save()

        emit_mock = self._emit_guard(
            self._handler(), self._payload(run.id, RunStatus.ACTIVE.value)
        )
        self.assertEqual(emit_mock.call_count, 1)
        self.assertEqual(emit_mock.call_args.args[0], "algorithm_status_change")


if __name__ == "__main__":
    unittest.main()
