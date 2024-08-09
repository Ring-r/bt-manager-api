from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable

logger = logging.getLogger(__name__)


def sleep_10_task(*args, **kwargs) -> asyncio.Task:
    """Create and return an asyncio task that sleeps for 10 seconds.

    This function creates an asyncio task that runs `asyncio.sleep(10)` and assigns
    the task a name based on the function's name (`sleep_10_task`).

    Parameters
    ----------
    *args
        Variable positional arguments to be passed to the task.
    **kwargs
        Variable keyword arguments to be passed to the task.

    Returns
    -------
    asyncio.Task
        The created asyncio task that will sleep for 10 seconds.

    Example
    -------
    >>> task = sleep_10_task()
    >>> task.get_name()
    'sleep_10_task'

    """
    return asyncio.create_task(asyncio.sleep(10), name=sleep_10_task.__name__)


def sleep_100_task(*args, **kwargs) -> asyncio.Task:
    """Create and return an asyncio task that sleeps for 100 seconds.

    This function creates an asyncio task that runs `asyncio.sleep(10)` and assigns
    the task a name based on the function's name (`sleep_100_task`).

    Parameters
    ----------
    *args
        Variable positional arguments to be passed to the task.
    **kwargs
        Variable keyword arguments to be passed to the task.

    Returns
    -------
    asyncio.Task
        The created asyncio task that will sleep for 100 seconds.

    Example
    -------
    >>> task = sleep_100_task()
    >>> task.get_name()
    'sleep_100_task'

    """
    return asyncio.create_task(asyncio.sleep(100), name=sleep_100_task.__name__)


def sleep_1000_task(*args, **kwargs) -> asyncio.Task:
    """Create and return an asyncio task that sleeps for 1000 seconds.

    This function creates an asyncio task that runs `asyncio.sleep(10)` and assigns
    the task a name based on the function's name (`sleep_1000_task`).

    Parameters
    ----------
    *args
        Variable positional arguments to be passed to the task.
    **kwargs
        Variable keyword arguments to be passed to the task.

    Returns
    -------
    asyncio.Task
        The created asyncio task that will sleep for 1000 seconds.

    Example
    -------
    >>> task = sleep_1000_task()
    >>> task.get_name()
    'sleep_1000_task'

    """
    return asyncio.create_task(asyncio.sleep(1000), name=sleep_1000_task.__name__)


factory: dict[str, Callable] = {
    sleep_10_task.__name__: sleep_10_task,
    sleep_100_task.__name__: sleep_100_task,
    sleep_1000_task.__name__: sleep_1000_task,
}

logger.warning(json.dumps({task_name: "" for task_name in factory}, indent=2))
