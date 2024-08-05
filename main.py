import logging
from typing import Any
from uuid import UUID

from fastapi import FastAPI

logger = logging.getLogger(__name__)

app = FastAPI()


# states:
# 'PENDING'  # Task is waiting for execution or unknown. Any task id that’s not known is implied to be in the pending state.
# 'STARTED'  # Task has been started. Not reported by default, to enable please see app.Task.track_started.
# 'SUCCESS'  # Task has been successfully executed.
# 'FAILURE'  # Task execution resulted in failure.
# 'RETRY'  # Task is being retried.
# 'REVOKED'  # Task has been revoked.


# TODO: use example of `https://github.com/mher/flower/blob/master/flower/api/tasks.py` to manipulate celery tasks info

class TaskInfo:
    uuid: UUID
    name: str
    args: list[Any]
    kwargs: dict[str, Any]
    state: str
    link: str


class ErrorInfo:
    code: int
    msg: str


class TaskNotExistErrorInfo(ErrorInfo):
    def __init__(self) -> None:
        super().__init__()

        self.code = 0
        self.msg = "the task doesn't exist"
        # can be add args field to store infarmation about tash id


class TaskInProcessErrorInfo(ErrorInfo):
    def __init__(self) -> None:
        super().__init__()

        self.code = 1
        self.msg = "task is in process"
        # can be add args field to store infarmation about tash id


class TaskNotInProcessErrorInfo(ErrorInfo):
    def __init__(self) -> None:
        super().__init__()

        self.code = 2
        self.msg = "task is not in process"
        # can be add args field to store infarmation about tash id


tasks: dict[UUID, TaskInfo] = {}


@app.post("/create-test-task/")
def create_task(name: str, args, kwargs) -> TaskInfo:
    # TODO: register task
    raise NotImplementedError


def cancel_task(id_: UUID) -> None:
    # TODO: cancel task
    ...


def delete_task(id_: UUID) -> None:
    # TODO: cancel task
    ...


# create
@app.post("/tasks/")
def create_tasks(ids: list[UUID]) -> dict[UUID, TaskInfo | ErrorInfo]:
    res: dict[UUID, TaskInfo | ErrorInfo] = {}
    for id_ in set(ids):
        if id_ not in tasks:
            res[id_] = TaskNotExistErrorInfo()
        elif tasks[id_].state not in ["FINISHED", "REVORKED"]:
            res[id_] = TaskInProcessErrorInfo()
        else:
            task_template = tasks[id_]
            task = create_task(
                task_template.name, task_template.args, task_template.kwargs
            )
            tasks[task.uuid] = task

    return res


# read (get tasks information)
@app.get("/tasks/")
def read_tasks(ids: list[UUID] | None = None) -> dict[UUID, TaskInfo | ErrorInfo]:
    if ids is None:
        return tasks

    res = {id_: tasks[id_] if id_ in tasks else TaskNotExistErrorInfo() for id_ in ids}
    return res


# update (cancel tasks)
@app.patch("/tasks/")
def update_tasks(ids: list[UUID] | None = None) -> dict[UUID, ErrorInfo]:
    if ids is None:
        running_task_ids = [
            id_ for id_ in tasks if tasks[id_].state in ["PENDING", "STARTED", "RETRY"]
        ]
        for id_ in running_task_ids:
            cancel_task(id_)
        return {}  # it would be good to return state and finish time; does they finish during http request?

    res: dict[UUID, ErrorInfo] = {}
    for id_ in set(ids):
        if id_ not in tasks:
            res[id_] = TaskNotExistErrorInfo()
        elif tasks[id_].state in ["SUCCESS", "FAILURE", "REVORKED"]:
            res[id_] = TaskNotInProcessErrorInfo()
        else:
            cancel_task(id_)

    return res


# delete
@app.delete("/tasks/")
def delete_tasks(ids: list[UUID] | None = None) -> dict[UUID, ErrorInfo]:
    if ids is None:
        for id_ in list(tasks.keys()):
            cancel_task(id_)
        return {}

    res: dict[UUID, ErrorInfo] = {}
    for id_ in set(ids):
        if id_ not in tasks:
            res[id_] = TaskNotExistErrorInfo()
        else:
            # TODO: delete the task
            delete_task(id_)

    return res
