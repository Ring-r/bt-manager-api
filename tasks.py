from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable

logger = logging.getLogger(__name__)


factory: dict[str, Callable[..., asyncio.Task]] = {}


def add_to_factory(name: str | None = None):
    def decorator(func):
        factory[name if name is not None else func.__name__] = func
        return func

    return decorator


@add_to_factory()
def sleep_task(delay: float) -> asyncio.Task:
    return asyncio.create_task(asyncio.sleep(delay), name=sleep_task.__name__)


logger.warning(json.dumps({task_name: "" for task_name in factory}, indent=2))
