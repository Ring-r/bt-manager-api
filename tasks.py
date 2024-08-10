from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable

logger = logging.getLogger(__name__)


def sleep_task(delay: float) -> asyncio.Task:
    return asyncio.create_task(asyncio.sleep(delay), name=sleep_task.__name__)


factory: dict[str, Callable] = {
    sleep_task.__name__: sleep_task,
}

logger.warning(json.dumps({task_name: "" for task_name in factory}, indent=2))
