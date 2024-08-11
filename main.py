from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Annotated, Any
from uuid import uuid1

from fastapi import FastAPI, Query
from pydantic import BaseModel

from tasks import factory as tasks_factory

logger = logging.getLogger(__name__)

app = FastAPI()


class TaskData:
    uuid: str
    task: asyncio.Task
    kwargs: dict[str, Any]
    start_date: datetime
    end_date: datetime | None

    def __init__(self) -> None:
        self.start_date = datetime.now()
        self.end_date = None

    # states:
    # 'PENDING'  # Task is waiting for execution or unknown. Any task id that’s not known is implied to be in the pending state.
    # 'STARTED'  # Task has been started. Not reported by default, to enable please see app.Task.track_started.
    # 'SUCCESS'  # Task has been successfully executed.
    # 'FAILURE'  # Task execution resulted in failure.
    # 'RETRY'  # Task is being retried.
    # 'REVOKED'  # Task has been revoked.

    def is_in_process(self) -> bool:
        return self.state_result[0] in ["PENDING", "STARTED", "RETRY"]

    def is_not_in_process(self) -> bool:
        return self.state_result[0] in ["SUCCESS", "FAILURE", "REVOKED"]

    @property
    def name(self) -> str:
        return self.task.get_name()

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


tasks: dict[str, TaskData] = {}


class TaskInfo(BaseModel):
    uuid: str
    name: str
    kwargs: dict[str, Any]
    start_date: datetime
    end_date: datetime | None
    state: str
    result: Any | None = None

    @staticmethod
    def from_task_data(task_data: TaskData) -> TaskInfo:
        task_info = TaskInfo(
            uuid=task_data.uuid,
            name=task_data.task.get_name(),
            kwargs=task_data.kwargs,
            start_date=task_data.start_date,
            end_date=task_data.end_date,
            state="PENDING",
        )
        task_info.state, task_info.result = task_data.state_result

        return task_info


def _get_task_info(id_: str) -> TaskInfo:
    return TaskInfo.from_task_data(tasks[id_])


class ErrorInfo(BaseModel):
    uuid: str
    error_code: int
    error_msg: str

    @staticmethod
    def not_exist_data(uuid: str):
        return ErrorInfo(
            uuid=uuid,
            error_code=0,
            error_msg="the task does not exist",
        )

    @staticmethod
    def in_process_data(uuid: str):
        return ErrorInfo(
            uuid=uuid,
            error_code=0,
            error_msg="task is in process",
        )

    @staticmethod
    def not_in_process_data(uuid: str):
        return ErrorInfo(
            uuid=uuid,
            error_code=0,
            error_msg="task is not in process",
        )


def _create_task(name: str, **kwargs) -> str:
    duplicated_task_data = next(
        (
            x
            for x in tasks.values()
            if x.name == name and x.kwargs == kwargs and x.is_in_process()
        ),
        None,
    )  # it can be O(1) if use something like index precalculated before
    if duplicated_task_data is not None:
        return duplicated_task_data.uuid

    task = tasks_factory[name](**kwargs)

    task_data = TaskData()
    task_data.uuid = str(uuid1())
    task_data.task = task
    task_data.kwargs = kwargs

    tasks[task_data.uuid] = task_data

    def finish_callback(_: Any) -> None:
        task_data.end_date = datetime.now()
        # can be run signal about data changed

    task.add_done_callback(finish_callback)

    return task_data.uuid


def _cancel_task(id_: str) -> None:
    tasks[id_].task.cancel()
    tasks[id_].end_date = datetime.now()


def _delete_task(id_: str) -> None:
    if not tasks[id_].task.done():
        tasks[id_].task.cancel()
    del tasks[id_]


@app.post("/sleep-task-creator/")
async def create_sleep_task(delay: Annotated[float, Query()]) -> TaskInfo:
    uuid = _create_task("sleep_task", delay=delay)
    return TaskInfo.from_task_data(tasks[uuid])


@app.post("/tasks/")
async def recreate_tasks(
    ids: Annotated[list[str], Query()],
) -> list[TaskInfo | ErrorInfo]:
    res: list[TaskInfo | ErrorInfo] = []
    for id_ in set(ids):
        if id_ not in tasks:
            res.append(ErrorInfo.not_exist_data(id_))
            continue
        if tasks[id_].is_in_process():
            res.append(ErrorInfo.in_process_data(id_))
            continue

        task_data_template = tasks[id_]
        uuid = _create_task(
            task_data_template.name,
            **task_data_template.kwargs,
        )
        res.append(_get_task_info(uuid))

    return res


@app.get("/tasks/")
async def read_tasks(
    ids: Annotated[list[str] | None, Query()] = None,
) -> list[TaskInfo | ErrorInfo]:
    if ids is None:
        return [_get_task_info(uuid) for uuid in tasks]

    res: list[TaskInfo | ErrorInfo] = [
        _get_task_info(id_) if id_ in tasks else ErrorInfo.not_exist_data(id_)
        for id_ in ids
    ]
    return res


@app.patch("/tasks/")
async def cancel_tasks(
    ids: Annotated[list[str] | None, Query()] = None,
) -> list[ErrorInfo]:
    if ids is None:
        running_task_ids = [id_ for id_ in tasks if tasks[id_].is_not_in_process()]
        for id_ in running_task_ids:
            _cancel_task(id_)
        return []  # it would be good to return state and finish time; is they finished during http request?

    res: list[ErrorInfo] = []
    for id_ in set(ids):
        if id_ not in tasks:
            res.append(ErrorInfo.not_exist_data(id_))
            continue
        if tasks[id_].is_not_in_process():
            res.append(ErrorInfo.not_in_process_data(id_))
            continue

        _cancel_task(id_)

    return res


@app.delete("/tasks/")
async def delete_tasks(
    ids: Annotated[list[str] | None, Query()] = None,
) -> list[ErrorInfo]:
    if ids is None:
        for id_ in list(tasks.keys()):
            _cancel_task(id_)
        return []

    res: list[ErrorInfo] = []
    for id_ in set(ids):
        if id_ not in tasks:
            res.append(ErrorInfo.not_exist_data(id_))
            continue

        _delete_task(id_)

    return res
