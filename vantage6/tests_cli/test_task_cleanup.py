import unittest
from unittest.mock import MagicMock

from kubernetes.client.rest import ApiException

from vantage6.cli.node.common.task_cleanup import (
    ResourceDeletionResult,
    delete_run_generation_resources,
    delete_run_related_pods,
)


def _api(status: int | None = None):
    return ApiException(status=status)


class TestDeleteRunGenerationResources(unittest.TestCase):
    def setUp(self):
        self.core_api = MagicMock()
        self.batch_api = MagicMock()
        self.pod = MagicMock()
        self.pod.metadata.name = "pod-1"
        self.core_api.list_namespaced_pod.return_value.items = [self.pod]

    def _delete(self, **kwargs):
        defaults = {
            "job_name": "vantage6-run-10-g2",
            "namespace": "tasks",
            "core_api": self.core_api,
            "batch_api": self.batch_api,
            "docker_secret_name": "docker-login-secret-run-id-10-g2",
        }
        defaults.update(kwargs)
        return delete_run_generation_resources(**defaults)

    def test_deletes_exact_generation_resources(self):
        result = self._delete()

        self.batch_api.delete_namespaced_job.assert_called_once_with(
            name="vantage6-run-10-g2", namespace="tasks"
        )
        self.core_api.list_namespaced_pod.assert_called_once_with(
            "tasks", label_selector="job-name=vantage6-run-10-g2"
        )
        self.core_api.delete_namespaced_pod.assert_called_once_with(
            name="pod-1", namespace="tasks"
        )
        secret_names = [
            call.kwargs["name"]
            for call in self.core_api.delete_namespaced_secret.call_args_list
        ]
        self.assertIn("vantage6-run-10-g2", secret_names)
        self.assertIn("docker-login-secret-run-id-10-g2", secret_names)
        self.assertTrue(result.all_succeeded)

    def test_404_counts_as_succeeded(self):
        self.batch_api.delete_namespaced_job.side_effect = _api(404)
        self.core_api.delete_namespaced_pod.side_effect = _api(404)
        self.core_api.delete_namespaced_secret.side_effect = _api(404)

        result = self._delete()
        self.assertTrue(result.all_succeeded)

    def test_job_deletion_failure_is_reported(self):
        self.batch_api.delete_namespaced_job.side_effect = _api(500)
        result = self._delete()
        self.assertFalse(result.job)
        self.assertFalse(result.all_succeeded)
        # other resources are still attempted so partial failures are observable
        self.assertTrue(result.secret)

    def test_pod_list_404_is_succeeded(self):
        self.core_api.list_namespaced_pod.side_effect = _api(404)
        result = self._delete()
        self.assertTrue(result.pods)

    def test_missing_docker_secret_name_skips_legacy_secret(self):
        result = delete_run_generation_resources(
            job_name="vantage6-run-10-g2",
            namespace="tasks",
            core_api=self.core_api,
            batch_api=self.batch_api,
        )
        secret_names = [
            call.kwargs["name"]
            for call in self.core_api.delete_namespaced_secret.call_args_list
        ]
        self.assertEqual(secret_names, ["vantage6-run-10-g2"])
        self.assertTrue(result.docker_secret)
        self.assertIsInstance(result, ResourceDeletionResult)


class TestLegacyWrapper(unittest.TestCase):
    def test_legacy_naming(self):
        core_api = MagicMock()
        batch_api = MagicMock()
        core_api.list_namespaced_pod.return_value.items = []

        delete_run_related_pods(
            run_id=10,
            container_name="vantage6-run-10",
            namespace="tasks",
            core_api=core_api,
            batch_api=batch_api,
        )

        batch_api.delete_namespaced_job.assert_called_once_with(
            name="vantage6-run-10", namespace="tasks"
        )
        secret_names = [
            call.kwargs["name"]
            for call in core_api.delete_namespaced_secret.call_args_list
        ]
        self.assertIn("vantage6-run-10", secret_names)
        self.assertIn("docker-login-secret-run-id-10", secret_names)


if __name__ == "__main__":
    unittest.main()
