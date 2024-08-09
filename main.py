from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Any
from uuid import uuid1

from fastapi import Body, FastAPI, Query
from pydantic import BaseModel

from tasks import factory as tasks_factory

logger = logging.getLogger(__name__)

app = FastAPI()


# states:
# 'PENDING'  # Task is waiting for execution or unknown. Any task id that’s not known is implied to be in the pending state.
# 'STARTED'  # Task has been started. Not reported by default, to enable please see app.Task.track_started.
# 'SUCCESS'  # Task has been successfully executed.
# 'FAILURE'  # Task execution resulted in failure.
# 'RETRY'  # Task is being retried.
# 'REVOKED'  # Task has been revoked.


class TaskInfo(BaseModel):
    uuid: str
    name: str
    kwargs: dict[str, Any]
    state: str
    result: Any | None = None


class TaskData:
    uuid: str
    task: asyncio.Task
    kwargs: dict[str, Any]

    @property
    def state_result(self) -> tuple[str, Any]:
        result = None
        try:
            result = self.task.result()
            state = "SUCCESS"
        except asyncio.CancelledError:
            state = "REVOKED"
        except asyncio.InvalidStateError:
            state = "STARTED"
        except Exception:
            state = "FAILURE"

        return state, result

    def to_task_info(self) -> TaskInfo:
        task_info = TaskInfo(
            uuid=self.uuid,
            name=self.task.get_name(),
            kwargs=self.kwargs,
            state="PENDING",
        )
        task_info.state, task_info.result = self.state_result

        return task_info


class ErrorInfo(BaseModel):
    code: int
    msg: str


class TaskNotExistErrorInfo(ErrorInfo):
    def __init__(self) -> None:
        super().__init__(code=0, msg="the task doesn't exist")
        # can be add args field to store infarmation about tash id


class TaskInProcessErrorInfo(ErrorInfo):
    def __init__(self) -> None:
        super().__init__(code=1, msg="task is in process")
        # can be add args field to store infarmation about tash id


class TaskNotInProcessErrorInfo(ErrorInfo):
    def __init__(self) -> None:
        super().__init__(code=2, msg="task is not in process")
        # can be add args field to store infarmation about tash id


tasks: dict[str, TaskData] = {}


def _create_task(name: str, *args, **kwargs) -> str:
    task_data = TaskData()
    task_data.uuid = str(uuid1())
    task_data.task = tasks_factory[name](**kwargs)  # TODO: try/except ('name doesn't exist', 'kwargs is wrong')
    task_data.kwargs = kwargs

    tasks[task_data.uuid] = task_data

    return task_data.uuid


def _cancel_task(id_: str) -> None:
    tasks[id_].task.cancel()


def _delete_task(id_: str) -> None:
    if not tasks[id_].task.done():
        tasks[id_].task.cancel()
    del tasks[id_]


@app.post("/create-task/")
async def create_task(params: Annotated[dict[str, Any], Body()]) -> TaskInfo:
    name = params.pop("name")
    uuid = _create_task(name, **params)
    return tasks[uuid].to_task_info()


# create
@app.post("/tasks/")
async def create_tasks(
    ids: Annotated[list[str], Query()],
) -> dict[str, TaskInfo | ErrorInfo]:
    res: dict[str, TaskInfo | ErrorInfo] = {}
    for id_ in set(ids):
        if id_ not in tasks:
            res[id_] = TaskNotExistErrorInfo()
        elif tasks[id_].state_result[0] not in ["SUCCESS", "REVOKED"]:
            res[id_] = TaskInProcessErrorInfo()
        else:
            task_data_template = tasks[id_]
            uuid = _create_task(
                task_data_template.task.get_name(),
                **task_data_template.kwargs,
            )
            res[id_] = tasks[uuid].to_task_info()

    return res


# read (get tasks information)
@app.get("/tasks/")
async def read_tasks(
    ids: Annotated[list[str] | None, Query()] = None,
) -> dict[str, TaskInfo | ErrorInfo]:
    if ids is None:
        return {
            task_uuid: task_data.to_task_info()
            for task_uuid, task_data in tasks.items()
        }

    res: dict[str, TaskInfo | ErrorInfo] = {
        id_: tasks[id_].to_task_info() if id_ in tasks else TaskNotExistErrorInfo()
        for id_ in ids
    }
    return res


# update (cancel tasks)
@app.patch("/tasks/")
async def update_tasks(
    ids: Annotated[list[str] | None, Query()] = None,
) -> dict[str, ErrorInfo]:
    if ids is None:
        running_task_ids = [
            id_
            for id_ in tasks
            if tasks[id_].state_result[0] in ["PENDING", "STARTED", "RETRY"]
        ]
        for id_ in running_task_ids:
            _cancel_task(id_)
        return {}  # it would be good to return state and finish time; does they finish during http request?

    res: dict[str, ErrorInfo] = {}
    for id_ in set(ids):
        if id_ not in tasks:
            res[id_] = TaskNotExistErrorInfo()
        elif tasks[id_].state_result[0] in ["SUCCESS", "FAILURE", "REVOKED"]:
            res[id_] = TaskNotInProcessErrorInfo()
        else:
            _cancel_task(id_)

    return res


# delete
@app.delete("/tasks/")
async def delete_tasks(
    ids: Annotated[list[str] | None, Query()] = None,
) -> dict[str, ErrorInfo]:
    if ids is None:
        for id_ in list(tasks.keys()):
            _cancel_task(id_)
        return {}

    res: dict[str, ErrorInfo] = {}
    for id_ in set(ids):
        if id_ not in tasks:
            res[id_] = TaskNotExistErrorInfo()
        else:
            _delete_task(id_)

    return res
