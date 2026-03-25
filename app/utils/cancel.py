"""
Task cancellation via Redis flags.

Workers poll these keys to detect cancellation requests and stop cleanly.
The API sets the flag; workers delete it on exit (normal, error, or cancel).

Key format: task_cancel:{task_id}  (TTL: 1 hour as a safety net)
"""
import redis as redis_lib
from config import settings

_CANCEL_TTL = 3600  # seconds — ensures keys don't accumulate if a worker crashes


def _sync_client() -> redis_lib.Redis:
    return redis_lib.from_url(settings.REDIS_URL, decode_responses=True)


def set_cancel_flag(task_id: int) -> None:
    with _sync_client() as r:
        r.set(f"task_cancel:{task_id}", "1", ex=_CANCEL_TTL)


def check_cancel_flag(task_id: int) -> bool:
    with _sync_client() as r:
        return r.exists(f"task_cancel:{task_id}") == 1


def clear_cancel_flag(task_id: int) -> None:
    with _sync_client() as r:
        r.delete(f"task_cancel:{task_id}")


# Async version for use in the FastAPI process
async def async_set_cancel_flag(task_id: int) -> None:
    import redis.asyncio as aioredis
    async with aioredis.from_url(settings.REDIS_URL, decode_responses=True) as r:
        await r.set(f"task_cancel:{task_id}", "1", ex=_CANCEL_TTL)
