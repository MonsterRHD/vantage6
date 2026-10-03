import json
import tempfile
import unittest
from pathlib import Path

from vantage6.node.k8s.run_registry import (
    ACTIVE_STATE_KILLING,
    ActiveRun,
    CleanupRecord,
    RunRegistry,
    _int_to_tag,
)


def _active(run_id=10, tag="g1"):
    return ActiveRun(
        run_id=run_id,
        attempt_tag=tag,
        job_name=f"vantage6-run-{run_id}-{tag}",
        task_id=1,
        parent_id=None,
        init_org_id=2,
    )


def _cleanup(run_id=10, tag="g1"):
    return CleanupRecord(
        run_id=run_id,
        attempt_tag=tag,
        job_name=f"vantage6-run-{run_id}-{tag}",
        secret_name=f"vantage6-run-{run_id}-{tag}",
        docker_secret_name=f"docker-login-secret-run-id-{run_id}-{tag}",
        reason="killed",
        task_id=1,
    )


class TestAttemptTags(unittest.TestCase):
    def test_tags_monotonic_and_base36(self):
        self.assertEqual(_int_to_tag(1), "g1")
        self.assertEqual(_int_to_tag(36), "g10")
        self.assertEqual(_int_to_tag(37), "g11")

    def test_counter_persists_across_instances(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            tag_1 = registry.next_attempt_tag()
            tag_2 = registry.next_attempt_tag()
            self.assertEqual(tag_1, "g1")
            self.assertEqual(tag_2, "g2")

            reloaded = RunRegistry(tmp)
            self.assertEqual(reloaded.next_attempt_tag(), "g3")

    def test_counter_survives_dev_extension_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            RunRegistry(tmp, task_dir_extension="dev").next_attempt_tag()
            self.assertTrue((Path(tmp) / "dev" / ".v6_node_state.json").exists())


class TestActiveRuns(unittest.TestCase):
    def test_activate_get_mark_killing(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            registry.activate(_active())
            self.assertEqual(registry.get_active(10).attempt_tag, "g1")

            marked = registry.mark_killing(10)
            self.assertEqual(marked.state, ACTIVE_STATE_KILLING)

            reloaded = RunRegistry(tmp)
            self.assertEqual(reloaded.get_active(10).state, ACTIVE_STATE_KILLING)

    def test_mark_killing_unknown_run_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(RunRegistry(tmp).mark_killing(999))

    def test_deactivate(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            registry.activate(_active())
            registry.deactivate(10)
            self.assertIsNone(registry.get_active(10))


class TestCleanupRecords(unittest.TestCase):
    def test_add_is_idempotent_per_job_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            self.assertTrue(registry.add_cleanup(_cleanup(tag="g1")))
            self.assertFalse(registry.add_cleanup(_cleanup(tag="g1")))
            self.assertEqual(len(registry.list_cleanups()), 1)

    def test_generations_kept_distinct(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            registry.add_cleanup(_cleanup(tag="g1"))
            registry.add_cleanup(_cleanup(tag="g2"))
            self.assertEqual(len(registry.list_cleanups()), 2)
            self.assertTrue(registry.cleanup_exists_for_run(10))
            self.assertIsNone(registry.get_cleanup("vantage6-run-10-g3"))

    def test_retry_and_remove_persist(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            registry.add_cleanup(_cleanup())
            registry.record_retry("vantage6-run-10-g1", "boom")
            registry.remove_cleanup("vantage6-run-10-g1")
            self.assertEqual(registry.list_cleanups(), [])

            reloaded = RunRegistry(tmp)
            self.assertEqual(reloaded.list_cleanups(), [])

    def test_retries_survive_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            registry.add_cleanup(_cleanup())
            registry.record_retry("vantage6-run-10-g1", "boom")

            reloaded = RunRegistry(tmp)
            record = reloaded.get_cleanup("vantage6-run-10-g1")
            self.assertEqual(record.retries, 1)
            self.assertEqual(record.last_error, "boom")

    def test_logs_delivered_clears_logs(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            record = _cleanup()
            record.logs = "some logs"
            registry.add_cleanup(record)
            registry.mark_logs_delivered("vantage6-run-10-g1")
            self.assertEqual(registry.get_cleanup("vantage6-run-10-g1").logs, "")
            self.assertTrue(registry.get_cleanup("vantage6-run-10-g1").logs_delivered)

    def test_due_kill_reports_only_unreported_kills(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            killed = _cleanup(tag="g1")
            completed = _cleanup(tag="g2")
            completed.reason = "completed"
            registry.add_cleanup(killed)
            registry.add_cleanup(completed)
            self.assertEqual(len(registry.due_kill_reports()), 1)
            registry.mark_killed_reported("vantage6-run-10-g1")
            self.assertEqual(registry.due_kill_reports(), [])


class TestKillEventDeduplication(unittest.TestCase):
    def test_event_id_dedup_persists(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            self.assertTrue(registry.note_kill_event("evt-1"))
            self.assertFalse(registry.note_kill_event("evt-1"))

            reloaded = RunRegistry(tmp)
            self.assertFalse(reloaded.note_kill_event("evt-1"))
            self.assertTrue(reloaded.note_kill_event("evt-2"))

    def test_legacy_event_without_id_always_processed(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            self.assertTrue(registry.note_kill_event(None))
            self.assertTrue(registry.note_kill_event(None))

    def test_kill_all_timestamp_ordering(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            self.assertFalse(registry.is_stale_kill_all(100.0))
            self.assertTrue(registry.is_stale_kill_all(99.0))
            self.assertFalse(registry.is_stale_kill_all(101.0))

    def test_legacy_kill_all_without_timestamp_not_stale(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            registry.is_stale_kill_all(100.0)
            self.assertFalse(registry.is_stale_kill_all(None))


class TestCorruptRegistry(unittest.TestCase):
    def test_corrupt_file_is_quarantined_and_rebuilt(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / ".v6_node_state.json"
            state_file.write_text("not json {{{")

            registry = RunRegistry(tmp)
            self.assertEqual(registry.counter, 0)
            self.assertEqual(registry.list_cleanups(), [])
            self.assertTrue(
                any(
                    p.name.startswith(".v6_node_state.json.corrupt-")
                    for p in Path(tmp).iterdir()
                )
            )

            registry.next_attempt_tag()
            reloaded = RunRegistry(tmp)
            self.assertEqual(reloaded.next_attempt_tag(), "g2")

    def test_persisted_payload_is_valid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            registry = RunRegistry(tmp)
            registry.activate(_active())
            registry.add_cleanup(_cleanup())
            data = json.loads((Path(tmp) / ".v6_node_state.json").read_text())
            self.assertIn("10", data["active"])
            self.assertIn("vantage6-run-10-g1", data["cleanups"])


if __name__ == "__main__":
    unittest.main()
