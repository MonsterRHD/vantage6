"""
Tests for generation-aware cancellation and cleanup in ContainerManager.

Coverage:
- a kill is bound to the current generation and never deletes a newer one
- duplicated kill events are dropped and KILLED is reported once
- failed deletions leave a retryable tombstone that succeeds later
- the completion poller never reports an outcome for a killed generation
- runs no longer open at HQ are reclaimed on reconciliation
- legacy (untagged) jobs are still killed
"""

import tempfile
import unittest
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException

from vantage6.common.enum import RunStatus

from vantage6.node.k8s import run_io as run_io_mod, session_manager as session_mod
from vantage6.node.k8s.container_manager import (
    ANN_ATTEMPT_TAG,
    ANN_DOCKER_LOGIN_SECRET,
    LABEL_ATTEMPT_TAG,
    LABEL_NODE_ID,
    LABEL_RUN_ID,
    ContainerManager,
)
from vantage6.node.k8s.data_classes import KilledRun, ToBeKilled
from vantage6.node.k8s.run_registry import (
    CLEANUP_REASON_KILLED,
    ActiveRun,
    CleanupRecord,
    RunRegistry,
)


def _job(name: str, run_id: int = 10, tag: str | None = "g1"):
    annotations = {
        "run_id": str(run_id),
        "task_id": "1",
        "task_parent_id": "",
        "init_org_id": "2",
        "action": "data_extraction",
        "session_id": "1",
        "df_name": "",
        "df_id": "",
        "df_label": "",
        ANN_ATTEMPT_TAG: tag or "",
        ANN_DOCKER_LOGIN_SECRET: f"docker-login-secret-run-id-{run_id}-{tag}"
        if tag
        else "",
    }
    labels = {LABEL_NODE_ID: "node-a", LABEL_RUN_ID: str(run_id)}
    if tag:
        labels[LABEL_ATTEMPT_TAG] = tag
    job = MagicMock()
    job.metadata.name = name
    job.metadata.annotations = annotations
    job.metadata.labels = labels
    job.status.succeeded = 0
    job.status.failed = 0
    return job


def _active(run_id=10, tag="g1"):
    return ActiveRun(
        run_id=run_id,
        attempt_tag=tag,
        job_name=f"vantage6-run-{run_id}-{tag}",
        task_id=1,
        parent_id=None,
        init_org_id=2,
    )


class GenerationTestBase(unittest.TestCase):
    def setUp(self):
        self.manager = ContainerManager.__new__(ContainerManager)
        self.manager.log = MagicMock()
        self.manager.task_namespace = "tasks"
        self.manager.task_job_labels = {LABEL_NODE_ID: "node-a"}
        self.manager.task_job_label_selector = "node_id=node-a"
        self.manager.num_active_tasks = 1
        self.manager.share_algorithm_logs = False
        self.manager._task_dir_extension = None

        self.client = MagicMock()
        self.client.whoami.organization_id = 3
        self.manager.client = self.client

        self.batch_api = MagicMock()
        self.core_api = MagicMock()
        self.core_api.list_namespaced_pod.return_value.items = []
        self.manager.batch_api = self.batch_api
        self.manager.core_api = self.core_api

        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        # redirect the on-host task root so RunIO/SessionFileManager can create
        # their directories in this sandbox instead of /app/tasks
        for module in (session_mod, run_io_mod):
            patcher = patch.object(module, "TASK_FILES_ROOT", self._tmp.name)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.manager.registry = RunRegistry(self._tmp.name)
        # counter starts at 1 so g1 exists
        self.manager.registry.next_attempt_tag()

    def _set_jobs(self, *jobs):
        self.batch_api.list_namespaced_job.return_value.items = list(jobs)

        def _read(name, namespace):
            match = next((j for j in jobs if j.metadata.name == name), None)
            if match is None:
                raise ApiException(status=404)
            return match

        self.batch_api.read_namespaced_job.side_effect = _read

    def _deleted_job_names(self):
        return [
            call.kwargs.get("name")
            for call in self.batch_api.delete_namespaced_job.call_args_list
        ]

    def _deleted_secret_names(self):
        return [
            call.kwargs.get("name")
            for call in self.core_api.delete_namespaced_secret.call_args_list
        ]


class TestKillGenerationMatching(GenerationTestBase):
    def test_kill_pins_and_deletes_current_generation(self):
        self.manager.registry.activate(_active())
        self._set_jobs(_job("vantage6-run-10-g1"))

        killed = self.manager.kill_algorithm_runs(
            [ToBeKilled(task_id=1, run_id=10, organization_id=3)], event_id="e1"
        )

        self.assertEqual(len(killed), 1)
        self.assertEqual(killed[0].attempt_tag, "g1")
        self.assertEqual(self._deleted_job_names(), ["vantage6-run-10-g1"])
        self.assertIn("vantage6-run-10-g1", self._deleted_secret_names())
        self.assertIn("docker-login-secret-run-id-10-g1", self._deleted_secret_names())
        self.assertIsNone(self.manager.registry.get_active(10))
        record = self.manager.registry.get_cleanup("vantage6-run-10-g1")
        self.assertIsNotNone(record)
        self.assertEqual(record.reason, CLEANUP_REASON_KILLED)

    def test_old_generation_tombstone_never_deletes_new_generation(self):
        # two generations exist; the node knows g2 as current
        g1 = _job("vantage6-run-10-g1")
        g2 = _job("vantage6-run-10-g2", tag="g2")
        self._set_jobs(g1, g2)
        self.manager.registry.next_attempt_tag()
        self.manager.registry.activate(_active(tag="g2"))

        # g1 cleanup is a stale tombstone carried over from an earlier kill
        record_g1 = CleanupRecord(
            run_id=10,
            attempt_tag="g1",
            job_name="vantage6-run-10-g1",
            secret_name="vantage6-run-10-g1",
            docker_secret_name="docker-login-secret-run-id-10-g1",
            reason="killed",
        )
        self.manager.registry.add_cleanup(record_g1)

        self.manager._delete_generation(record_g1)

        self.assertEqual(self._deleted_job_names(), ["vantage6-run-10-g1"])
        self.assertNotIn("vantage6-run-10-g2", self._deleted_job_names())

    def test_duplicate_kill_event_is_dropped(self):
        self.manager.registry.activate(_active())
        self._set_jobs(_job("vantage6-run-10-g1"))
        kill_list = [ToBeKilled(task_id=1, run_id=10, organization_id=3)]

        first = self.manager.kill_algorithm_runs(kill_list, event_id="e1")
        second = self.manager.kill_algorithm_runs(kill_list, event_id="e1")

        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(self._deleted_job_names().count("vantage6-run-10-g1"), 1)

    def test_failed_deletion_is_retried_via_tombstone(self):
        self.manager.registry.activate(_active())
        self._set_jobs(_job("vantage6-run-10-g1"))
        self.batch_api.delete_namespaced_job.side_effect = ApiException(status=500)

        killed = self.manager.kill_algorithm_runs(
            [ToBeKilled(task_id=1, run_id=10, organization_id=3)], event_id="e1"
        )
        self.assertEqual(len(killed), 1)

        record = self.manager.registry.get_cleanup("vantage6-run-10-g1")
        self.assertIsNotNone(record)
        self.assertEqual(record.retries, 1)

        # deletion recovers on the next reconciliation round
        self.batch_api.delete_namespaced_job.side_effect = None
        self.manager._process_cleanups()
        self.assertIsNone(self.manager.registry.get_cleanup("vantage6-run-10-g1"))

    def test_legacy_job_without_tag_is_killed(self):
        legacy = _job("vantage6-run-10", tag=None)
        self._set_jobs(legacy)

        killed = self.manager.kill_algorithm_runs(
            [ToBeKilled(task_id=1, run_id=10, organization_id=3)], event_id="legacy"
        )

        self.assertEqual(len(killed), 1)
        self.assertIsNone(killed[0].attempt_tag)
        self.assertEqual(self._deleted_job_names(), ["vantage6-run-10"])
        self.assertIn("docker-login-secret-run-id-10", self._deleted_secret_names())


class TestCompletionPollerGuard(GenerationTestBase):
    def _finished(self, name, run_id):
        job = _job(name, run_id=run_id, tag="g1")
        job.status.succeeded = 1
        return job

    def test_killed_generation_is_never_reported_as_finished(self):
        killed_job = self._finished("vantage6-run-10-g1", 10)
        other_job = self._finished("vantage6-run-11-g1", 11)
        self._set_jobs(killed_job, other_job)

        # run 10 is already registered for cleanup (kill won the race)
        self.manager.registry.add_cleanup(
            CleanupRecord(
                run_id=10,
                attempt_tag="g1",
                job_name="vantage6-run-10-g1",
                secret_name="vantage6-run-10-g1",
                docker_secret_name=None,
                reason="killed",
            )
        )

        run_io_11 = MagicMock()
        run_io_11.run_id = 11
        run_io_11.attempt_tag = "g1"
        run_io_11.container_name = "vantage6-run-11-g1"
        run_io_11.docker_login_secret_name = "docker-login-secret-run-id-11-g1"
        run_io_11.process_output.return_value = (b"output", RunStatus.COMPLETED)
        self.manager._get_logs = MagicMock(return_value="logs")

        with patch(
            "vantage6.node.k8s.container_manager.RunIO.from_dict",
            return_value=run_io_11,
        ):
            result = self.manager.process_next_completed_run()

        self.assertEqual(result.run_id, 11)
        # the killed generation's tombstone is untouched and no CRASHED/completed
        # result for run 10 was produced
        self.assertIsNotNone(self.manager.registry.get_cleanup("vantage6-run-10-g1"))


class TestReconcileWithHQ(GenerationTestBase):
    def test_non_open_run_resources_are_reclaimed(self):
        self._set_jobs(_job("vantage6-run-10-g1"))
        self.manager.registry.activate(_active())

        self.manager.reconcile_with_hq(open_run_ids=set())

        self.assertEqual(self._deleted_job_names(), ["vantage6-run-10-g1"])
        self.assertIsNotNone(self.manager.registry.get_cleanup("vantage6-run-10-g1"))
        self.assertIsNone(self.manager.registry.get_active(10))

    def test_open_run_resources_are_left_alone(self):
        self._set_jobs(_job("vantage6-run-10-g1"))
        self.manager.registry.activate(_active())

        self.manager.reconcile_with_hq(open_run_ids={10})

        self.assertEqual(self._deleted_job_names(), [])
        self.assertIsNotNone(self.manager.registry.get_active(10))

    def test_kill_ack_flushed_once(self):
        self._set_jobs()
        self.manager.registry.add_cleanup(
            CleanupRecord(
                run_id=10,
                attempt_tag="g1",
                job_name="vantage6-run-10-g1",
                secret_name="vantage6-run-10-g1",
                docker_secret_name=None,
                reason="killed",
                task_id=1,
                parent_id=None,
                killed_reported=False,
            )
        )

        first = self.manager.reconcile_with_hq(open_run_ids=set())
        second = self.manager.reconcile_with_hq(open_run_ids=set())

        self.assertEqual(len(first), 1)
        self.assertIsInstance(first[0], KilledRun)
        self.assertEqual(second, [])


class TestRunIOGenerationNames(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        for module in (session_mod, run_io_mod):
            patcher = patch.object(module, "TASK_FILES_ROOT", self._tmp.name)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _run_io(self, tag):
        from vantage6.common.enum import AlgorithmStepType

        from vantage6.node.k8s.run_io import RunIO

        return RunIO(
            run_id=10,
            session_id=1,
            action=AlgorithmStepType.DATA_EXTRACTION,
            client=MagicMock(),
            attempt_tag=tag,
        )

    def test_generation_suffix_on_names_and_io_folder(self):
        run_io = self._run_io("g2")
        self.assertEqual(run_io.container_name, "vantage6-run-10-g2")
        self.assertEqual(
            run_io.docker_login_secret_name, "docker-login-secret-run-id-10-g2"
        )
        self.assertTrue(run_io.run_folder.endswith("10/g2"))

    def test_legacy_names_without_tag(self):
        run_io = self._run_io(None)
        self.assertEqual(run_io.container_name, "vantage6-run-10")
        self.assertEqual(
            run_io.docker_login_secret_name, "docker-login-secret-run-id-10"
        )
        self.assertTrue(run_io.run_folder.endswith("10"))
        self.assertFalse(run_io.run_folder.endswith("10/None"))

    def test_io_files_are_generation_scoped(self):
        run_io = self._run_io("g3")
        input_rel, output_rel = run_io.create_files(b"in", b"out")
        self.assertEqual(input_rel, "10/g3/input")
        self.assertEqual(output_rel, "10/g3/output")


if __name__ == "__main__":
    unittest.main()
