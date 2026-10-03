import datetime
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from vantage6.common.globals import SESSION_STATE_FILENAME

from vantage6.node.k8s import session_manager as sm_module
from vantage6.node.k8s.session_manager import SessionFileManager


class SessionManagerTestBase(unittest.TestCase):
    def setUp(self):
        import tempfile

        self.tmp = tempfile.mkdtemp()
        self._patches = [
            patch("vantage6.node.k8s.session_manager.TASK_FILES_ROOT", self.tmp),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def _session_dir(self, session_id: int = 1) -> Path:
        return Path(self.tmp) / "sessions" / f"session{session_id:09d}"

    def _state_df(self, manager: SessionFileManager):
        return pq.read_table(manager.session_state_file).to_pandas()


class TestAtomicDataframeWrite(SessionManagerTestBase):
    def test_dataframe_file_is_published_atomically(self):
        manager = SessionFileManager(1)
        table = pa.table({"a": [1, 2, 3]})

        manager.write_dataframe_file(table, "df")

        target = self._session_dir() / "df.parquet"
        self.assertTrue(target.exists())
        self.assertEqual(pq.read_table(target).to_pydict(), table.to_pydict())
        self.assertEqual(list(self._session_dir().glob("*.tmp")), [])

    def test_failed_write_keeps_previous_valid_file(self):
        manager = SessionFileManager(1)
        table_v1 = pa.table({"a": [1]})
        manager.write_dataframe_file(table_v1, "df")
        target = self._session_dir() / "df.parquet"

        def failing_write(*args, **kwargs):
            raise OSError("disk full")

        with (
            patch.object(sm_module.pq, "write_table", side_effect=failing_write),
            self.assertRaises(OSError),
        ):
            manager.write_dataframe_file(pa.table({"a": [2]}), "df")

        # the previous valid file is untouched and no temp file is visible
        self.assertEqual(pq.read_table(target).to_pydict(), table_v1.to_pydict())
        self.assertEqual(list(self._session_dir().glob("*.tmp")), [])

    def test_temp_files_are_not_discovered_as_dataframes(self):
        manager = SessionFileManager(1)
        manager.write_dataframe_file(pa.table({"a": [1]}), "df")

        with (
            patch.object(sm_module.pq, "write_table", side_effect=OSError),
            self.assertRaises(OSError),
        ):
            manager.write_dataframe_file(pa.table({"a": [2]}), "other")

        # dataframe discovery globs only *.parquet, so failed temp files and
        # the state file are invisible as dataframes
        parquet_files = {f.name for f in self._session_dir().glob("*.parquet")}
        self.assertEqual(
            parquet_files,
            {"df.parquet", f"{SESSION_STATE_FILENAME}.parquet"},
        )


class TestStateFileIdempotency(SessionManagerTestBase):
    def test_entry_deduplicated_per_run(self):
        manager = SessionFileManager(1)

        manager.update_state_file(
            action="data_extraction",
            message="Session updated.",
            filename="df.parquet",
            df_name="df",
            run_id=11,
        )
        # reprocessing the same run must not append a second entry
        manager.update_state_file(
            action="data_extraction",
            message="Session updated.",
            filename="df.parquet",
            df_name="df",
            run_id=11,
        )

        state = self._state_df(manager)
        self.assertEqual(len(state[state["run_id"] == 11]), 1)

    def test_different_runs_get_separate_entries(self):
        manager = SessionFileManager(1)
        for run_id in (11, 12):
            manager.update_state_file(
                action="preprocessing",
                message="Session updated.",
                run_id=run_id,
            )

        state = self._state_df(manager)
        self.assertEqual(sorted(state["run_id"].dropna().tolist()), [11, 12])

    def test_entries_without_run_id_always_appended(self):
        manager = SessionFileManager(1)
        manager.update_state_file(action="delete", message="Dataframe deleted")
        manager.update_state_file(action="delete", message="Dataframe deleted")

        state = self._state_df(manager)
        deletes = state[state["action"] == "delete"]
        self.assertEqual(len(deletes), 2)

    def test_old_state_file_without_run_id_column(self):
        # simulate a state file created by an older node version
        session_dir = self._session_dir(7)
        session_dir.mkdir(parents=True)
        old_state = pa.table(
            {
                "action": ["data_extraction"],
                "file": ["df.parquet"],
                "timestamp": [datetime.datetime.now(datetime.UTC)],
                "message": ["Session updated."],
                "dataframe": ["df"],
            }
        )
        state_path = session_dir / f"{SESSION_STATE_FILENAME}.parquet"
        pq.write_table(old_state, state_path)

        manager = SessionFileManager(7)
        manager.update_state_file(
            action="preprocessing",
            message="Session updated.",
            run_id=21,
        )
        manager.update_state_file(
            action="preprocessing",
            message="Session updated.",
            run_id=21,
        )

        state = self._state_df(manager)
        self.assertEqual(len(state), 2)
        self.assertTrue(bool(pd.isna(state["run_id"].iloc[0])))
        self.assertEqual(int(state["run_id"].iloc[1]), 21)


class TestRecoverTempFiles(SessionManagerTestBase):
    def test_stale_temp_files_removed(self):
        session_dir = self._session_dir(1)
        session_dir.mkdir(parents=True)
        stale = session_dir / ".df.parquet.abc123.tmp"
        stale.write_bytes(b"partial parquet")
        valid = session_dir / "df.parquet"
        valid.write_bytes(b"valid")

        removed = SessionFileManager.recover_temp_files()

        self.assertEqual(removed, [stale])
        self.assertFalse(stale.exists())
        self.assertTrue(valid.exists())

    def test_recover_with_task_dir_extension(self):
        extension_dir = Path(self.tmp) / "dev" / "sessions" / "session000000003"
        extension_dir.mkdir(parents=True)
        stale = extension_dir / ".df.parquet.fff.tmp"
        stale.write_bytes(b"x")

        removed = SessionFileManager.recover_temp_files(task_dir_extension="dev")

        self.assertEqual(removed, [stale])
        self.assertFalse(stale.exists())

    def test_recover_without_sessions_root(self):
        self.assertEqual(SessionFileManager.recover_temp_files(), [])


class TestDeleteDataframeFile(SessionManagerTestBase):
    def test_delete_removes_file_and_records_state(self):
        manager = SessionFileManager(1)
        manager.write_dataframe_file(pa.table({"a": [1]}), "df")
        target = self._session_dir() / "df.parquet"
        self.assertTrue(target.exists())

        manager.delete_dataframe_file("df")

        self.assertFalse(target.exists())
        state = self._state_df(manager)
        self.assertIn("delete", set(state["action"]))


if __name__ == "__main__":
    unittest.main()
