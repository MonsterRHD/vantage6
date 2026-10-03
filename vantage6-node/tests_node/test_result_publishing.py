import unittest
from unittest.mock import Mock, patch

from vantage6.common.enum import RunStatus

from vantage6.node import Node
from vantage6.node.k8s.container_manager import ContainerManager
from vantage6.node.k8s.data_classes import Result


def _make_result(run_id=1):
    return Result(
        run_id=run_id,
        task_id=2,
        logs="logs",
        data=b"data",
        status=RunStatus.COMPLETED,
        parent_id=None,
        init_org_id=5,
    )


class TestAcknowledgeFinishedRun(unittest.TestCase):
    def _make_manager(self):
        manager = ContainerManager.__new__(ContainerManager)
        manager.log = Mock()
        manager.num_active_tasks = 3
        manager._finalized_runs = set()
        manager.task_namespace = "ns"
        manager.core_api = Mock()
        manager.batch_api = Mock()
        return manager

    def test_acknowledgement_decrements_and_deletes_once(self):
        manager = self._make_manager()
        with patch(
            "vantage6.node.k8s.container_manager.delete_run_related_pods"
        ) as delete:
            manager.acknowledge_finished_run(7)
            manager.acknowledge_finished_run(7)

        self.assertEqual(manager.num_active_tasks, 2)
        delete.assert_called_once()
        self.assertIn(7, manager._finalized_runs)

    def test_cleanup_failure_does_not_raise(self):
        manager = self._make_manager()
        with patch(
            "vantage6.node.k8s.container_manager.delete_run_related_pods",
            side_effect=RuntimeError,
        ):
            manager.acknowledge_finished_run(7)

        self.assertEqual(manager.num_active_tasks, 2)


class TestReportThenAcknowledgeOrdering(unittest.TestCase):
    def _make_node(self):
        node = Node.__new__(Node)
        node.log = Mock()
        node.config = {"share_algorithm_logs": True}
        node.k8s_container_manager = Mock()
        node.k8s_container_manager.process_next_completed_run.return_value = (
            _make_result()
        )
        node.client = Mock()
        node.client.whoami = Mock(id_=1, organization_id=1)
        node.client.collaboration_id = 1
        node.socketIO = Mock()
        node.socketIO.namespaces = {"/tasks": True}
        return node

    def _run_one_iteration(self, node):
        with (
            patch("vantage6.node.time.sleep", side_effect=InterruptedError),
            self.assertRaises(InterruptedError),
        ):
            node._Node__send_updates_finished_tasks()

    def test_job_acknowledged_after_successful_report(self):
        node = self._make_node()
        self._run_one_iteration(node)

        node.client.run.patch.assert_called_once()
        node.k8s_container_manager.acknowledge_finished_run.assert_called_once_with(1)

    def test_job_not_acknowledged_when_report_fails(self):
        node = self._make_node()
        node.client.run.patch.side_effect = ConnectionError

        self._run_one_iteration(node)

        node.client.run.patch.assert_called_once()
        node.k8s_container_manager.acknowledge_finished_run.assert_not_called()


class TestKillFinalizedRun(unittest.TestCase):
    def _annotations(self, run_id):
        return {
            "run_id": str(run_id),
            "task_id": "2",
            "task_parent_id": "",
            "init_org_id": "5",
            "action": "federated_compute",
            "session_id": "3",
            "df_name": "",
            "df_id": "",
            "df_label": "",
        }

    def setUp(self):
        self._patches = [
            patch("vantage6.node.k8s.run_io.TASK_FILES_ROOT", self._tmp()),
            patch("vantage6.node.k8s.session_manager.TASK_FILES_ROOT", self._tmp()),
        ]
        for patcher in self._patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    @staticmethod
    def _tmp():
        import tempfile

        return tempfile.mkdtemp()

    def _make_manager(self, run_ids):
        manager = ContainerManager.__new__(ContainerManager)
        manager.log = Mock()
        manager.num_active_tasks = len(run_ids)
        manager._finalized_runs = set()
        manager.task_namespace = "ns"
        manager.core_api = Mock()
        manager.batch_api = Mock()
        manager.client = Mock()
        manager.ctx = Mock()
        manager.ctx.config = {}
        manager.task_job_label_selector = "node_id=1"
        jobs = []
        for run_id in run_ids:
            job = Mock()
            job.metadata.annotations = self._annotations(run_id)
            jobs.append(job)
        manager.batch_api.list_namespaced_job.return_value.items = jobs
        manager._get_logs = Mock(return_value="logs")
        return manager, jobs

    def test_finalized_run_is_not_killed(self):
        manager, _ = self._make_manager([7, 8])
        manager._finalized_runs.add(7)

        with patch(
            "vantage6.node.k8s.container_manager.delete_run_related_pods"
        ) as delete:
            killed = manager._kill_algorithm_runs()

        killed_ids = {run.run_id for run in killed}
        self.assertEqual(killed_ids, {8})
        deleted_run_ids = {call.kwargs["run_id"] for call in delete.call_args_list}
        self.assertEqual(deleted_run_ids, {8})
        # only the actually killed run is counted as inactive
        self.assertEqual(manager.num_active_tasks, 1)

    def test_acknowledged_run_is_not_killed_twice(self):
        manager, _ = self._make_manager([7])

        with patch(
            "vantage6.node.k8s.container_manager.delete_run_related_pods"
        ) as delete:
            manager.acknowledge_finished_run(7)
            killed = manager._kill_algorithm_runs()

        self.assertEqual(killed, [])
        delete.assert_called_once()
        self.assertEqual(manager.num_active_tasks, 0)


if __name__ == "__main__":
    unittest.main()
