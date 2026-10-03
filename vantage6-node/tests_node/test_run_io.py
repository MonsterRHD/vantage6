import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from vantage6.common.enum import AlgorithmStepType, RunStatus

from vantage6.node.k8s.run_io import RunIO
from vantage6.node.k8s.session_manager import TEMP_FILE_SUFFIX, SessionFileManager


class RunIOTestBase(unittest.TestCase):
    RUN_ID = 1
    SESSION_ID = 2
    DF_ID = 10

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        for target in (
            "vantage6.node.k8s.run_io.TASK_FILES_ROOT",
            "vantage6.node.k8s.session_manager.TASK_FILES_ROOT",
        ):
            patcher = patch(target, self.tmp)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = Mock()
        self.run_folder = os.path.join(self.tmp, str(self.RUN_ID))
        os.makedirs(self.run_folder)

    def _make_run_io(self, action, df_name="df"):
        details = (
            {"name": df_name, "id": self.DF_ID, "db_label": "label"}
            if df_name is not None
            else None
        )
        return RunIO(
            run_id=self.RUN_ID,
            session_id=self.SESSION_ID,
            action=action,
            client=self.client,
            dataframe_details=details,
        )

    def _write_output(self, payload):
        mode = "wb" if isinstance(payload, bytes) else "w"
        with open(os.path.join(self.run_folder, "output"), mode) as fp:
            fp.write(payload)

    def _write_output_parquet(self, table):
        pq.write_table(table, os.path.join(self.run_folder, "output"))

    def _state_df(self, run_io: RunIO):
        return pq.read_table(run_io.session_file_manager.session_state_file).to_pandas()

    def _session_df(self, run_io: RunIO, name="df"):
        path = os.path.join(
            run_io.session_file_manager.local_session_folder, f"{name}.parquet"
        )
        return pq.read_table(path).to_pydict()

    def _state_rows_for_run(self, run_io: RunIO):
        state = self._state_df(run_io)
        return state[state["run_id"] == self.RUN_ID]


class TestExtractionProcessing(RunIOTestBase):
    def test_completed_publishes_dataframe_and_columns(self):
        run_io = self._make_run_io(AlgorithmStepType.DATA_EXTRACTION)
        table = pa.table({"a": [1, 2], "b": ["x", "y"]})
        self._write_output_parquet(table)

        data, status = run_io.process_output()

        self.assertEqual(status, RunStatus.COMPLETED)
        self.assertEqual(data, b"")
        self.assertEqual(self._session_df(run_io), table.to_pydict())
        self.client.column.post.assert_called_once_with(
            self.DF_ID,
            [
                {"name": "a", "dtype": "int64"},
                {"name": "b", "dtype": "string"},
            ],
        )
        self.assertEqual(len(self._state_rows_for_run(run_io)), 1)

    def test_corrupt_output_is_failure_without_completion(self):
        run_io = self._make_run_io(AlgorithmStepType.PREPROCESSING)
        self._write_output(b"this is not a parquet file")

        data, status = run_io.process_output()

        self.assertEqual(data, b"")
        self.assertEqual(status, RunStatus.UNEXPECTED_OUTPUT)
        self.client.column.post.assert_not_called()

    def test_missing_dataframe_details_is_failed(self):
        run_io = self._make_run_io(AlgorithmStepType.DATA_EXTRACTION, df_name=None)
        self._write_output_parquet(pa.table({"a": [1]}))

        _, status = run_io.process_output()

        self.assertEqual(status, RunStatus.FAILED)
        self.client.column.post.assert_not_called()

    def test_reprocessing_same_run_is_idempotent(self):
        run_io = self._make_run_io(AlgorithmStepType.DATA_EXTRACTION)
        table = pa.table({"a": [1, 2]})
        self._write_output_parquet(table)

        first_data, first_status = run_io.process_output()
        second_data, second_status = run_io.process_output()

        self.assertEqual((first_data, first_status), (b"", RunStatus.COMPLETED))
        self.assertEqual((second_data, second_status), (b"", RunStatus.COMPLETED))
        # the session state contains a single entry for this run ...
        self.assertEqual(len(self._state_rows_for_run(run_io)), 1)
        # ... while the (idempotent) column upload is attempted for every try
        self.assertEqual(self.client.column.post.call_count, 2)
        # and the published dataframe is still valid
        self.assertEqual(self._session_df(run_io), table.to_pydict())

    def test_column_upload_failure_is_retryable_without_duplicate_state(self):
        run_io = self._make_run_io(AlgorithmStepType.DATA_EXTRACTION)
        self._write_output_parquet(pa.table({"a": [1]}))
        self.client.column.post.side_effect = ConnectionError("HQ down")

        with self.assertRaises(ConnectionError):
            run_io.process_output()

        # the state entry was already recorded but no completion was sent yet
        self.assertEqual(len(self._state_rows_for_run(run_io)), 1)

        # HQ is back: the run is reprocessed
        self.client.column.post.side_effect = None
        data, status = run_io.process_output()
        self.assertEqual((data, status), (b"", RunStatus.COMPLETED))
        self.assertEqual(len(self._state_rows_for_run(run_io)), 1)
        self.assertEqual(self.client.column.post.call_count, 2)

    def test_dataframe_write_failure_keeps_previous_result(self):
        run_io = self._make_run_io(AlgorithmStepType.DATA_EXTRACTION)
        self._write_output_parquet(pa.table({"a": [1]}))
        _, status = run_io.process_output()
        self.assertEqual(status, RunStatus.COMPLETED)
        valid = self._session_df(run_io)
        self.client.column.post.reset_mock()

        with patch.object(
            SessionFileManager, "write_dataframe_file", side_effect=OSError
        ):
            _, status = run_io.process_output()

        self.assertEqual(status, RunStatus.FAILED)
        self.client.column.post.assert_not_called()
        # previous valid dataframe is still in place
        self.assertEqual(self._session_df(run_io), valid)

    def test_state_write_failure_is_not_reported_completed(self):
        run_io = self._make_run_io(AlgorithmStepType.DATA_EXTRACTION)
        self._write_output_parquet(pa.table({"a": [1]}))

        with patch.object(SessionFileManager, "update_state_file", side_effect=OSError):
            _, status = run_io.process_output()

        self.assertEqual(status, RunStatus.FAILED)
        self.client.column.post.assert_not_called()


class TestComputeProcessing(RunIOTestBase):
    def test_plain_result_compatible(self):
        run_io = self._make_run_io(AlgorithmStepType.FEDERATED_COMPUTE)
        result = b"serialized-algorithm-result"
        self._write_output(result)

        data, status = run_io.process_output()

        self.assertEqual(status, RunStatus.COMPLETED)
        self.assertEqual(data, result)
        self.assertEqual(len(self._state_rows_for_run(run_io)), 1)

    def test_plain_result_reprocessing_does_not_duplicate_state(self):
        run_io = self._make_run_io(AlgorithmStepType.CENTRAL_COMPUTE)
        self._write_output(b"result")

        run_io.process_output()
        data, status = run_io.process_output()

        self.assertEqual((data, status), (b"result", RunStatus.COMPLETED))
        self.assertEqual(len(self._state_rows_for_run(run_io)), 1)

    def test_state_failure_does_not_block_result_delivery(self):
        run_io = self._make_run_io(AlgorithmStepType.FEDERATED_COMPUTE)
        self._write_output(b"result")

        with patch.object(SessionFileManager, "update_state_file", side_effect=OSError):
            data, status = run_io.process_output()

        self.assertEqual((data, status), (b"result", RunStatus.COMPLETED))


class TestNoTempFilesLeft(RunIOTestBase):
    def test_no_tmp_files_after_extraction(self):
        run_io = self._make_run_io(AlgorithmStepType.DATA_EXTRACTION)
        self._write_output_parquet(pa.table({"a": [1]}))
        run_io.process_output()

        tmp_files = [
            f
            for f in os.listdir(run_io.session_file_manager.local_session_folder)
            if f.endswith(TEMP_FILE_SUFFIX)
        ]
        self.assertEqual(tmp_files, [])


if __name__ == "__main__":
    unittest.main()
