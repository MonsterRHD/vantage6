import threading
import unittest
from unittest.mock import Mock, patch

from vantage6.common.enum import RunStatus

from vantage6.node import Node
from vantage6.node.k8s.container_manager import (
    ContainerManager,
    LogStreamState,
    _LogStreamer,
)
from vantage6.node.k8s.data_classes import KilledRun, Result


def _make_stream_manager(share_algorithm_logs=True, pod_lines=None):
    manager = Mock()
    manager.share_algorithm_logs = share_algorithm_logs
    manager.log = Mock()
    manager.client.collaboration_id = 7
    manager.task_namespace = "test-namespace"
    manager.socket_io = Mock()
    manager.socket_io.connected = True

    def socket_emit(event, data, namespace=None, callback=None):
        if callback is None:
            manager.socket_io.emit(event, data, namespace=namespace)
        else:
            manager.socket_io.emit(
                event, data, namespace=namespace, callback=callback
            )

    manager.socket_emit.side_effect = socket_emit
    pod = Mock()
    pod.metadata.name = "pod-1"
    pod.metadata.creation_timestamp = "2024-01-01T00:00:00Z"
    manager.core_api.list_namespaced_pod.return_value.items = [pod]
    return manager


def _ok_ack(payload):
    return {"status": "ok", "ack_seq": payload["seq"] + 1}


def _watch_returning(lines):
    fake_watch = Mock()
    fake_watch.stream.return_value = iter(lines)
    return fake_watch


class TestLogStreamer(unittest.TestCase):
    def _make_streamer(self, manager=None, start_seq=0):
        manager = manager or _make_stream_manager()
        streamer = _LogStreamer(
            manager=manager,
            run_id=42,
            container_name="vantage6-run-42",
            task_id=99,
            start_seq=start_seq,
        )
        streamer.ack_timeout = 0.05
        streamer.send_retry_seconds = 0
        streamer.watch_retry_seconds = 0
        streamer.disconnect_poll_seconds = 0
        streamer.no_pod_retries = 2
        streamer.legacy_ack_timeouts = 2
        return streamer, manager

    def test_disabled_does_not_emit(self):
        manager = _make_stream_manager(share_algorithm_logs=False)
        streamer, manager = self._make_streamer(manager)
        streamer.run()
        manager.socket_io.emit.assert_not_called()
        self.assertEqual(streamer.state, LogStreamState.STOPPED)

    def test_emits_with_seq_and_advances_cursor_on_ack(self):
        streamer, manager = self._make_streamer()

        def emit_side(event, payload, namespace=None, callback=None):
            self.assertEqual(event, "algorithm_log")
            self.assertEqual(namespace, "/tasks")
            callback(_ok_ack(payload))

        manager.socket_io.emit.side_effect = emit_side
        with patch(
            "vantage6.node.k8s.container_manager.watch.Watch",
            return_value=_watch_returning(["hello"]),
        ):
            streamer.run()

        self.assertEqual(streamer.acked_count, 1)
        self.assertEqual(streamer.pending_count, 0)
        self.assertEqual(streamer.state, LogStreamState.STOPPED)
        manager.socket_io.emit.assert_called_once()
        payload = manager.socket_io.emit.call_args.args[1]
        self.assertEqual(payload["seq"], 0)
        self.assertEqual(payload["log"], "hello")
        self.assertEqual(payload["run_id"], 42)

    def test_watch_interruption_retries_without_resending_confirmed_line(self):
        streamer, manager = self._make_streamer()

        def emit_side(event, payload, namespace=None, callback=None):
            callback(_ok_ack(payload))

        manager.socket_io.emit.side_effect = emit_side
        good_watch = _watch_returning(["hello"])
        with patch(
            "vantage6.node.k8s.container_manager.watch.Watch",
            side_effect=[
                _failing_watch(ConnectionError("connection reset")),
                good_watch,
            ],
        ):
            streamer.run()

        # the line was delivered exactly once despite the watch restart
        self.assertEqual(manager.socket_io.emit.call_count, 1)
        self.assertEqual(streamer.acked_count, 1)
        self.assertTrue(
            any("k8s_watch_retry" in str(c) for c in manager.log.warning.call_args_list)
        )

    def test_socket_disconnect_parks_and_resends_after_reconnect(self):
        streamer, manager = self._make_streamer()
        streamer.disconnect_poll_seconds = 0.01
        socket = manager.socket_io
        socket.connected = False

        def emit_side(event, payload, namespace=None, callback=None):
            callback(_ok_ack(payload))

        socket.emit.side_effect = emit_side

        def flip_connected(*args, **kwargs):
            socket.connected = True

        # become connected right after the first park
        manager.log.warning.side_effect = flip_connected

        with patch(
            "vantage6.node.k8s.container_manager.watch.Watch",
            return_value=_watch_returning(["hello"]),
        ):
            streamer.run()

        self.assertEqual(socket.emit.call_count, 1)
        self.assertEqual(streamer.acked_count, 1)
        self.assertTrue(
            any(
                "disconnected_retry" in str(c)
                for c in manager.log.warning.call_args_list
            )
        )

    def test_send_failure_enters_send_retry_state_and_resends(self):
        streamer, manager = self._make_streamer()

        def emit_side(event, payload, namespace=None, callback=None):
            count = manager.socket_io.emit.call_count
            if count == 1:
                raise ConnectionError("send failed")
            callback(_ok_ack(payload))

        manager.socket_io.emit.side_effect = emit_side
        with patch(
            "vantage6.node.k8s.container_manager.watch.Watch",
            return_value=_watch_returning(["hello"]),
        ):
            streamer.run()

        self.assertEqual(manager.socket_io.emit.call_count, 2)
        payloads = [
            call.args[1] for call in manager.socket_io.emit.call_args_list
        ]
        self.assertEqual([p["seq"] for p in payloads], [0, 0])
        self.assertEqual(streamer.acked_count, 1)
        self.assertTrue(
            any(
                "send_failed_retry" in str(c)
                for c in manager.log.warning.call_args_list
            )
        )

    def test_missing_acknowledgement_falls_back_to_legacy_mode(self):
        streamer, manager = self._make_streamer()
        streamer.ack_timeout = 0.01

        with patch(
            "vantage6.node.k8s.container_manager.watch.Watch",
            return_value=_watch_returning(["hello"]),
        ):
            streamer.run()

        # two acknowledged attempts time out, then one legacy emit without
        # callback; the cursor still advances in legacy mode
        self.assertGreaterEqual(manager.socket_io.emit.call_count, 3)
        last_call = manager.socket_io.emit.call_args_list[-1]
        self.assertNotIn("callback", last_call.kwargs)
        self.assertTrue(streamer._legacy)
        self.assertEqual(streamer.state, LogStreamState.STOPPED)
        self.assertEqual(streamer.acked_count, 1)
        self.assertTrue(
            any("legacy" in str(c).lower() for c in manager.log.warning.call_args_list)
        )

    def test_resend_request_restarts_watch_and_replays_tail(self):
        streamer, manager = self._make_streamer()
        # the complete pod log used to locate the pod containing seq 0
        manager.core_api.read_namespaced_pod_log.return_value = "a\nb"

        def emit_side(event, payload, namespace=None, callback=None):
            count = manager.socket_io.emit.call_count
            if count == 1:
                callback({"status": "resend", "ack_seq": 0})
            else:
                callback(_ok_ack(payload))

        manager.socket_io.emit.side_effect = emit_side
        # first watch replays line 0 and HQ asks for resend; second watch
        # re-reads the pod from the start and finds a second line
        with patch(
            "vantage6.node.k8s.container_manager.watch.Watch",
            side_effect=[
                _watch_returning(["a"]),
                _watch_returning(["a", "b"]),
            ],
        ):
            streamer.run()

        payloads = [
            call.args[1] for call in manager.socket_io.emit.call_args_list
        ]
        self.assertEqual([p["seq"] for p in payloads], [0, 0, 1])
        self.assertEqual([p["log"] for p in payloads], ["a", "a", "b"])
        self.assertEqual(streamer.acked_count, 2)

    def test_resume_skips_lines_already_confirmed_at_hq(self):
        streamer, manager = self._make_streamer(start_seq=1)

        def emit_side(event, payload, namespace=None, callback=None):
            callback(_ok_ack(payload))

        manager.socket_io.emit.side_effect = emit_side
        with patch(
            "vantage6.node.k8s.container_manager.watch.Watch",
            return_value=_watch_returning(["a", "b"]),
        ):
            streamer.run()

        payloads = [
            call.args[1] for call in manager.socket_io.emit.call_args_list
        ]
        self.assertEqual([p["seq"] for p in payloads], [1])
        self.assertEqual([p["log"] for p in payloads], ["b"])
        self.assertEqual(streamer.acked_count, 2)

    def test_finalized_ack_ends_the_stream(self):
        streamer, manager = self._make_streamer()

        def emit_side(event, payload, namespace=None, callback=None):
            callback({"status": "finalized", "ack_seq": 1})

        manager.socket_io.emit.side_effect = emit_side
        with patch(
            "vantage6.node.k8s.container_manager.watch.Watch",
            return_value=_watch_returning(["hello"]),
        ):
            streamer.run()

        self.assertEqual(streamer.state, LogStreamState.FINALIZED)
        self.assertTrue(streamer._finalized)

    def test_stop_interrupts_parked_stream(self):
        streamer, manager = self._make_streamer()
        streamer.no_pod_retries = 10_000
        streamer.watch_retry_seconds = 0.01
        manager.core_api.list_namespaced_pod.return_value.items = []

        thread = threading.Thread(target=streamer.run)
        streamer.thread = thread
        thread.start()
        try:
            self.assertTrue(thread.is_alive())
            self.assertEqual(streamer.state, LogStreamState.NO_POD_RETRY)
        finally:
            streamer.stop(timeout=2)
        self.assertFalse(thread.is_alive())


def _failing_watch(exc):
    fake_watch = Mock()

    def _raise(*args, **kwargs):
        raise exc

    fake_watch.stream.side_effect = _raise
    return fake_watch


class TestContainerManagerLogRegistry(unittest.TestCase):
    def _make_manager(self, share_algorithm_logs=True):
        manager = ContainerManager.__new__(ContainerManager)
        manager.log = Mock()
        manager.share_algorithm_logs = share_algorithm_logs
        manager.socket_io = Mock()
        manager.client = Mock()
        manager.client.collaboration_id = 7
        manager.task_namespace = "test-namespace"
        manager.core_api = Mock()
        manager._log_streamers = {}
        manager._log_streamers_lock = threading.Lock()
        manager._socket_emit_lock = threading.Lock()
        return manager

    def test_disabled_does_not_register_stream(self):
        manager = self._make_manager(share_algorithm_logs=False)
        run_io = Mock(run_id=42, container_name="vantage6-run-42")
        manager._stream_logs(run_io=run_io, task_id=99)
        self.assertEqual(manager.get_log_stream_states(), {})

    def test_duplicate_stream_is_not_started_twice(self):
        manager = self._make_manager()
        started = threading.Event()

        def blocking_run(self):
            started.set()
            self._stop_event.wait(2)

        try:
            with patch.object(_LogStreamer, "run", blocking_run):
                manager.reattach_log_stream(run_id=1, task_id=2, start_seq=0)
                self.assertTrue(started.wait(2))
                manager.reattach_log_stream(run_id=1, task_id=2, start_seq=0)
                self.assertEqual(list(manager.get_log_stream_states()), [1])
        finally:
            manager.stop_log_stream(1)

    def test_stop_unknown_run_is_a_noop(self):
        manager = self._make_manager()
        manager.stop_log_stream(999)


class TestFinishedTaskLogSharing(unittest.TestCase):
    def _make_node(self, share_algorithm_logs: bool) -> Node:
        node = Node.__new__(Node)
        node.log = Mock()
        node.config = {"share_algorithm_logs": share_algorithm_logs}
        node.k8s_container_manager = Mock()
        node.k8s_container_manager.process_next_completed_run.return_value = Result(
            run_id=1,
            task_id=2,
            logs="sensitive algorithm logs",
            data="output-data",
            status=RunStatus.COMPLETED,
            parent_id=None,
            init_org_id=5,
        )
        node.client = Mock()
        node.client.whoami = Mock(id_=1, organization_id=1)
        node.client.collaboration_id = 1
        node.socketIO = Mock()
        return node

    def _run_one_iteration(self, node: Node) -> None:
        # __send_updates_finished_tasks loops forever with a trailing
        # time.sleep(1); make sleep raise so the loop stops after one pass.
        with (
            patch("vantage6.node.time.sleep", side_effect=InterruptedError),
            self.assertRaises(InterruptedError),
        ):
            node._Node__send_updates_finished_tasks()

    def test_logs_redacted_when_sharing_disabled(self):
        node = self._make_node(share_algorithm_logs=False)
        self._run_one_iteration(node)

        patch_call = node.client.run.patch.call_args
        self.assertEqual(
            patch_call.kwargs["data"]["log"],
            "Node does not allow sharing algorithm logs",
        )

    def test_logs_shared_when_sharing_enabled(self):
        node = self._make_node(share_algorithm_logs=True)
        self._run_one_iteration(node)

        patch_call = node.client.run.patch.call_args
        self.assertEqual(patch_call.kwargs["data"]["log"], "sensitive algorithm logs")

    def test_empty_logs_are_sent_as_empty_string(self):
        node = self._make_node(share_algorithm_logs=True)
        node.k8s_container_manager.process_next_completed_run.return_value = Result(
            run_id=1,
            task_id=2,
            logs="",
            data="output-data",
            status=RunStatus.COMPLETED,
            parent_id=None,
            init_org_id=5,
        )
        self._run_one_iteration(node)

        patch_call = node.client.run.patch.call_args
        self.assertEqual(patch_call.kwargs["data"]["log"], "")


class TestKillContainersLogSharing(unittest.TestCase):
    def _make_node(self, share_algorithm_logs: bool) -> Node:
        node = Node.__new__(Node)
        node.log = Mock()
        node.config = {"share_algorithm_logs": share_algorithm_logs}
        node.client = Mock()
        node.client.collaboration_id = 7
        node.client.whoami = Mock(id_=1)
        node.k8s_container_manager = Mock()
        node.k8s_container_manager.kill_algorithm_runs.return_value = [
            KilledRun(run_id=1, task_id=2, parent_id=None, logs="sensitive kill logs")
        ]
        return node

    def test_kill_logs_not_sent_when_sharing_disabled(self):
        node = self._make_node(share_algorithm_logs=False)
        node.kill_containers({"collaboration_id": 7})

        node.client.run.patch.assert_not_called()

    def test_kill_logs_sent_when_sharing_enabled(self):
        node = self._make_node(share_algorithm_logs=True)
        node.kill_containers({"collaboration_id": 7})

        node.client.run.patch.assert_called_once_with(
            id_=1, data={"log": "sensitive kill logs"}
        )


if __name__ == "__main__":
    unittest.main()
