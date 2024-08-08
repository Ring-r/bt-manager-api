import asyncio
import logging
import random
from typing import Annotated, Any
from uuid import uuid1

from fastapi import FastAPI, Query
from pydantic import BaseModel

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


@app.post("/create-test-task/")
async def create_task(name: str, delay: float | None = None) -> TaskInfo:
    if delay is None:
        delay = random.randint(1, 1000)

    task_data = TaskData()
    task_data.uuid = str(uuid1())
    task_data.task = asyncio.create_task(asyncio.sleep(delay), name=name)
    task_data.kwargs = {"delay": delay}

    tasks[task_data.uuid] = task_data

    return task_data.to_task_info()


def cancel_task(id_: str) -> None:
    tasks[id_].task.cancel()


def delete_task(id_: str) -> None:
    if not tasks[id_].task.done():
        tasks[id_].task.cancel()
    del tasks[id_]


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
            task_data_clone = tasks[id_]
            task = await create_task(
                task_data_clone.task.get_name(),
                **task_data_clone.kwargs,
            )
            res[id_] = task

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
            cancel_task(id_)
        return {}  # it would be good to return state and finish time; does they finish during http request?

    res: dict[str, ErrorInfo] = {}
    for id_ in set(ids):
        if id_ not in tasks:
            res[id_] = TaskNotExistErrorInfo()
        elif tasks[id_].state_result[0] in ["SUCCESS", "FAILURE", "REVOKED"]:
            res[id_] = TaskNotInProcessErrorInfo()
        else:
            cancel_task(id_)

    return res


# delete
@app.delete("/tasks/")
async def delete_tasks(
    ids: Annotated[list[str] | None, Query()] = None,
) -> dict[str, ErrorInfo]:
    if ids is None:
        for id_ in list(tasks.keys()):
            cancel_task(id_)
        return {}

    res: dict[str, ErrorInfo] = {}
    for id_ in set(ids):
        if id_ not in tasks:
            res[id_] = TaskNotExistErrorInfo()
        else:
            delete_task(id_)

    return res
