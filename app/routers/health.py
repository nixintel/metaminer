import subprocess
from fastapi import APIRouter, Response
from sqlalchemy import text
from app.database import AsyncSessionLocal
import redis.asyncio as aioredis
from config import settings

router = APIRouter(tags=["health"])


@router.get("/healthcheck")
async def healthcheck(response: Response):
    status = {}

    # Database check
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        status["db"] = "ok"
    except Exception as e:
        status["db"] = f"error: {e}"

    # Redis check
    try:
        r = aioredis.from_url(settings.REDIS_URL, socket_timeout=3)
        await r.ping()
        await r.aclose()
        status["redis"] = "ok"
    except Exception as e:
        status["redis"] = f"error: {e}"

    # exiftool check
    try:
        result = subprocess.run(
            [settings.EXIFTOOL_PATH, "-ver"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        status["exiftool"] = f"ok (v{result.stdout.strip()})" if result.returncode == 0 else "error"
    except Exception as e:
        status["exiftool"] = f"error: {e}"

    all_ok = all(v == "ok" or v.startswith("ok") for v in status.values())
    status["status"] = "ok" if all_ok else "degraded"

    if not all_ok:
        response.status_code = 503

    return status
