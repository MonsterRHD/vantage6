import datetime
import logging
import os
import threading
import uuid
from pathlib import Path
from typing import ClassVar

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from vantage6.common import logger_name
from vantage6.common.globals import SESSION_STATE_FILENAME

from vantage6.node.globals import TASK_FILES_ROOT

log = logging.getLogger(logger_name(__name__))

# Files are fully written to a temp file with this suffix and only renamed to
# their final name once the write is complete and flushed. Readers therefore
# never observe a half-written parquet file. The suffix also keeps temp files
# from being picked up by the ``*.parquet`` dataframe discovery glob.
TEMP_FILE_SUFFIX = ".tmp"
TEMP_FILE_GLOB = f"*{TEMP_FILE_SUFFIX}"


def _fsync_directory(folder: Path) -> None:
    """Best-effort persistence of a rename in ``folder`` to disk."""
    try:
        dir_fd = os.open(folder, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        # some filesystems do not support fsync on directories
        pass
    finally:
        os.close(dir_fd)


def atomic_write_parquet(table: pa.Table, target: Path | str) -> None:
    """
    Write a parquet table to ``target`` atomically.

    The table is written and flushed to a temp file next to the target and is
    made visible only afterwards through an atomic rename. If writing is
    interrupted or fails, the previous target file (if any) is left untouched
    and the temp file is removed.

    Parameters
    ----------
    table: pa.Table
        Table to write
    target: Path | str
        Final destination of the parquet file
    """
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_file = target.parent / f".{target.name}.{uuid.uuid4().hex}{TEMP_FILE_SUFFIX}"
    try:
        with open(temp_file, "wb") as fp:
            pq.write_table(table, fp)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(temp_file, target)
        _fsync_directory(target.parent)
    except BaseException:
        # also clean up on KeyboardInterrupt/SystemExit so an interrupted
        # publish never leaves a visible temp file behind
        temp_file.unlink(missing_ok=True)
        raise


class SessionFileManager:
    """
    Class to manage session state on the node.
    """

    # per-session locks serializing the read-modify-write of session files
    # within this node process
    _locks_guard = threading.Lock()
    _session_locks: ClassVar[dict[str, threading.Lock]] = {}

    @classmethod
    def _session_lock(cls, local_session_folder: str) -> threading.Lock:
        with cls._locks_guard:
            return cls._session_locks.setdefault(local_session_folder, threading.Lock())

    @classmethod
    def recover_temp_files(cls, task_dir_extension: str | None = None) -> list[Path]:
        """
        Remove temp files left behind by interrupted parquet writes.

        Called at node startup before any run is processed. A temp file can
        only exist when the node died while writing; the corresponding final
        file was never replaced and the run is reprocessed from its retained
        k8s job and output file, so removing the temp file cannot discard
        committed data.

        Parameters
        ----------
        task_dir_extension: str, optional
            Extension of the task directory used in development.

        Returns
        -------
        list[Path]
            The temp files that were removed.
        """
        root = Path(TASK_FILES_ROOT)
        if task_dir_extension:
            root = root / task_dir_extension
        sessions_root = root / "sessions"
        if not sessions_root.is_dir():
            return []

        removed: list[Path] = []
        for session_dir in sessions_root.glob("session*"):
            if not session_dir.is_dir():
                continue
            for temp_file in session_dir.glob(TEMP_FILE_GLOB):
                try:
                    temp_file.unlink()
                    removed.append(temp_file)
                    log.warning(
                        "Removed incomplete session file '%s' left behind by an "
                        "interrupted write",
                        temp_file,
                    )
                except OSError:
                    log.exception("Could not remove temp file '%s'", temp_file)
        return removed

    def __init__(self, session_id: str, task_dir_extension: str | None = None):
        """
        Parameters
        ----------
        session_id: str
            ID of the session
        task_dir_extension: str, optional
            Extension to the directory to put the run files in. This is used to prevent
            that nodes use the same task directory in a development environment.
            Defaults to None.
        """
        self.log = logging.getLogger(logger_name(__name__))
        self.session_id = session_id

        self.session_name = f"session{self.session_id:09d}"
        self.session_state_file_name = f"{SESSION_STATE_FILENAME}.parquet"

        self.session_folder = os.path.join("sessions", self.session_name)
        if task_dir_extension:
            self.local_session_folder = os.path.join(
                TASK_FILES_ROOT, task_dir_extension, self.session_folder
            )
        else:
            self.local_session_folder = os.path.join(
                TASK_FILES_ROOT, self.session_folder
            )
        os.makedirs(self.local_session_folder, exist_ok=True)
        self.session_state_file = os.path.join(
            self.local_session_folder, self.session_state_file_name
        )
        self._file_lock = self._session_lock(self.local_session_folder)

        if not Path(self.session_state_file).exists():
            with self._file_lock:
                if not Path(self.session_state_file).exists():
                    self._create_session_state_file(self.session_id)

    def update_state_file(
        self,
        action: str,
        message: str,
        filename: str = "",
        df_name: str = "",
        run_id: int | None = None,
    ) -> None:
        """
        Update the session state file with the current action, file and message

        The update is idempotent per run: when ``run_id`` is provided and an
        entry for that run is already present (e.g. because the result is
        reprocessed after a failed upload to HQ), no duplicate entry is
        appended.

        Parameters
        ----------
        action: str
            Action that was performed
        message: str
            Message to be added to the state file
        filename: str, optional
            File resulting from the action
        df_name: str, optional
            Dataframe name that was updated. Some actions on the session are not
            related to a specific dataframe, so this parameter is optional.
        run_id: int, optional
            ID of the run that performed the action. Used to skip duplicate
            entries when a run is processed more than once.

        Raises
        ------
        OSError, pyarrow.ArrowException
            If the state file cannot be read or written atomically
        """
        self.log.debug(
            "Update session state file for action '%s' on dataframe '%s' ",
            action,
            df_name,
        )
        with self._file_lock:
            state = pq.read_table(self.session_state_file).to_pandas()

            if "run_id" in state.columns:
                state["run_id"] = state["run_id"].astype("Int64")
                if run_id is not None and (state["run_id"] == run_id).any():
                    self.log.debug(
                        "Session state already contains an entry for run %s, "
                        "not appending a duplicate entry",
                        run_id,
                    )
                    return
            else:
                # state file written by an older node version
                state["run_id"] = pd.Series([pd.NA] * len(state), dtype="Int64")

            new_row = pd.DataFrame(
                [
                    {
                        "action": action,
                        "file": filename,
                        "timestamp": datetime.datetime.now(datetime.UTC),
                        "message": message,
                        "dataframe": df_name,
                        "run_id": pd.NA if run_id is None else run_id,
                    }
                ]
            )
            state = pd.concat([state, new_row], ignore_index=True)
            state["run_id"] = state["run_id"].astype("Int64")

            atomic_write_parquet(
                pa.Table.from_pandas(state), Path(self.session_state_file)
            )

    def write_dataframe_file(self, algo_result: pa.Table, df_name: str) -> None:
        """
        Write the session dataframe to a parquet file.

        The file is published atomically: a previously written valid file for
        this dataframe stays available to readers until the new file has been
        completely written and renamed into place.

        Parameters
        ----------
        algo_result: pa.Table
            Table with the results of the algorithm
        df_name: str
            Name of the dataframe to update

        Raises
        ------
        OSError, pyarrow.ArrowException
            If the parquet file cannot be written
        """
        target = Path(self.local_session_folder) / f"{df_name}.parquet"
        with self._file_lock:
            atomic_write_parquet(algo_result, target)

    def delete_dataframe_file(self, df_name: str) -> None:
        """
        Delete the session dataframe file.
        """
        df_path = Path(self.local_session_folder) / f"{df_name}.parquet"
        if not df_path.exists():
            self.log.warning("Dataframe file '%s' does not exist", df_path)
            return

        try:
            df_path.unlink()
            self.log.info("Dataframe file '%s' deleted", df_path)
        except Exception:
            self.log.exception("Error deleting dataframe file '%s'", df_path)

        # update session state to include the deletion; the dataframe itself
        # is already gone, so a failing state update must not block the
        # deletion acknowledgement
        try:
            self.update_state_file(
                action="delete",
                message="Dataframe deleted",
                filename=f"{df_name}.parquet",
                df_name=df_name,
            )
        except Exception:
            self.log.exception("Error recording dataframe deletion in session state")

    def _create_session_state_file(self, session_id: int) -> str:
        """
        Create a file to store the state of the session.

        Parameters
        ----------
        session_id: int
            ID of the session

        Returns
        -------
        str
            Path to the session state file
        """
        self.log.debug(f"Creating session state file for session_id={session_id}")
        session_state = pa.table(
            {
                "action": [""],
                "file": [self.session_state_file_name],
                "timestamp": [datetime.datetime.now(datetime.UTC)],
                "message": ["Created this session file."],
                "dataframe": [""],
                "run_id": pa.array([None], type=pa.int64()),
            }
        )
        atomic_write_parquet(session_state, Path(self.session_state_file))
        return self.session_state_file
