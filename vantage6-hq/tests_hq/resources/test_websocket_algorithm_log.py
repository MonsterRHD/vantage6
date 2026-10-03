"""
Tests for the algorithm log websocket handler.

The handler is cursor-aware: newer nodes send a per-run line sequence number
(``seq``) and HQ acknowledges how many lines it persisted, so that duplicates
are ignored, gaps trigger a resend request, late lines after completion are
discarded, and events from older nodes (without ``seq``) keep working.
"""

import datetime
from unittest import TestCase
from unittest.mock import Mock, patch

from vantage6.common.enum import AlgorithmStepType, RunStatus

from vantage6.hq.model import Collaboration, Organization, Run, Task
from vantage6.hq.model.base import Database
from vantage6.hq.websockets import DefaultSocketNamespace


class TestAlgorithmLogHandler(TestCase):
    @classmethod
    def setUpClass(cls):
        Database().connect("sqlite://", allow_drop_all=True)

    @classmethod
    def tearDownClass(cls):
        Database().clear_data()
        Database().close()

    def setUp(self):
        org = Organization()
        col = Collaboration(organizations=[org])
        task = Task(collaboration=col, image="some-image", init_org=org)
        task.save()
        self.run = Run(
            task=task,
            organization=org,
            status=RunStatus.ACTIVE.value,
            action=AlgorithmStepType.CENTRAL_COMPUTE.value,
            log=None,
            log_seq=0,
        )
        self.run.save()
        self.namespace = DefaultSocketNamespace(
            "/tasks", socketio=Mock(), metrics=Mock()
        )
        self._patches = [
            patch.object(DefaultSocketNamespace, "_is_node", return_value=True),
            patch("vantage6.hq.websockets.emit"),
        ]
        self.emit_mock = self._patches[1].start()
        self._patches[0].start()

    def tearDown(self):
        for patcher in self._patches:
            patcher.stop()

    def _handle(self, **overrides):
        data = {
            "collaboration_id": 1,
            "run_id": self.run.id,
            "task_id": self.run.task_id,
            "log": "a line",
        }
        data.update(overrides)
        response = self.namespace.on_algorithm_log(data)
        return response, self.emit_mock

    def test_legacy_node_without_seq_appends(self):
        response, emit_mock = self._handle(seq=None, log="legacy line")

        self.assertEqual(response, {"status": "ok"})
        run = Run.get(self.run.id)
        self.assertEqual(run.log, "legacy line")
        self.assertEqual(run.log_seq, 0)
        # broadcast keeps the old wire format (no seq field)
        broadcast = emit_mock.call_args.args[0]
        self.assertEqual(broadcast, "algorithm_log")
        self.assertNotIn("seq", emit_mock.call_args.args[1])

    def test_new_node_appends_and_advances_cursor(self):
        response, emit_mock = self._handle(seq=0)
        self.assertEqual(response, {"status": "ok", "ack_seq": 1})

        run = Run.get(self.run.id)
        self.assertEqual(run.log, "a line")
        self.assertEqual(run.log_seq, 1)
        self.assertEqual(emit_mock.call_args.args[1]["seq"], 0)

    def test_duplicate_seq_is_not_stored_or_broadcast(self):
        self._handle(seq=0, log="first")
        response, emit_mock = self._handle(seq=0, log="first")

        self.assertEqual(response, {"status": "ok", "ack_seq": 1})
        run = Run.get(self.run.id)
        self.assertEqual(run.log, "first")
        self.assertEqual(run.log_seq, 1)
        # only the first line was broadcast
        self.assertEqual(emit_mock.call_count, 1)

    def test_lines_are_newline_separated(self):
        self._handle(seq=0, log="a")
        self._handle(seq=1, log="b")

        run = Run.get(self.run.id)
        self.assertEqual(run.log, "a\nb")
        self.assertEqual(run.log_seq, 2)

    def test_gap_requests_resend_from_last_confirmed(self):
        self._handle(seq=0, log="a")
        self.emit_mock.reset_mock()
        response, emit_mock = self._handle(seq=5, log="f")

        self.assertEqual(response, {"status": "resend", "ack_seq": 1})
        run = Run.get(self.run.id)
        self.assertEqual(run.log, "a")
        self.assertEqual(run.log_seq, 1)
        emit_mock.assert_not_called()

    def test_empty_line_advances_cursor_without_mutating_log(self):
        self._handle(seq=0, log="a")
        response, _ = self._handle(seq=1, log="")

        self.assertEqual(response, {"status": "ok", "ack_seq": 2})
        run = Run.get(self.run.id)
        self.assertEqual(run.log, "a")
        self.assertEqual(run.log_seq, 2)

    def test_events_after_completion_are_discarded_but_acknowledged(self):
        self.run.log = "complete log"
        self.run.log_seq = 3
        self.run.finished_at = datetime.datetime.now(datetime.UTC)
        self.run.save()

        response, emit_mock = self._handle(seq=3, log="late tail line")

        self.assertEqual(response, {"status": "finalized", "ack_seq": 3})
        run = Run.get(self.run.id)
        self.assertEqual(run.log, "complete log")
        self.assertEqual(run.log_seq, 3)
        emit_mock.assert_not_called()

    def test_unknown_run_returns_error(self):
        response, emit_mock = self._handle(run_id=999_999, seq=0)
        self.assertEqual(response["status"], "error")
        emit_mock.assert_not_called()

    def test_invalid_seq_returns_error(self):
        response, emit_mock = self._handle(seq=-1)
        self.assertEqual(response["status"], "error")
        emit_mock.assert_not_called()
