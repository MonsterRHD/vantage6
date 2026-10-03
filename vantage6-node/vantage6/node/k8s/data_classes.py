from typing import NamedTuple

from vantage6.common.enum import RunStatus


class Result(NamedTuple):
    """
    Data class to store the result of the docker image.

    Attributes
    ----------
    run_id: int
        ID of the current algorithm run
    task_id: int
        ID of the task the run belongs to
    logs: str
        Logs attached to current algorithm run
    data: str
        Output data of the algorithm
    status: RunStatus
        Status of the algorithm run
    parent_id: int | None
        ID of the parent task, if any
    init_org_id: int
        ID of the organization that initiated the task. Required to encrypt the
        result for them.
    attempt_tag: str | None
        Generation tag of the run attempt that produced this result
    """

    run_id: int
    task_id: int
    logs: str
    data: str
    status: RunStatus
    parent_id: int | None
    init_org_id: int
    attempt_tag: str | None = None


# Taken from docker_manager.py
class ToBeKilled(NamedTuple):
    """Data class to store which tasks should be killed"""

    task_id: int
    run_id: int
    organization_id: int


# Taken from docker_manager.py
class KilledRun(NamedTuple):
    """Data class to store which algorithms have been killed"""

    run_id: int
    task_id: int
    parent_id: int | None
    logs: str
    attempt_tag: str | None = None
