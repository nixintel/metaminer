import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging

from config import settings
from app.database import init_db
from app.utils.logging_config import configure_logging, drain_log_queue
from app.routers import health, projects, submissions, crawl, tasks, metadata, logs, scheduled_crawls
import app.models.scheduled_crawl  # ensure table is created by init_db()


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    await init_db()
    # Ensure temp and retained dirs exist
    settings.TEMP_DIR.mkdir(parents=True, exist_ok=True)
    settings.RETAINED_FILES_DIR.mkdir(parents=True, exist_ok=True)
    logging.getLogger("metaminer").info("Metaminer API started")
    drain_task = asyncio.create_task(drain_log_queue())
    yield
    logging.getLogger("metaminer").info("Metaminer API shutting down")
    drain_task.cancel()
    await asyncio.gather(drain_task, return_exceptions=True)


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    docs_url=settings.DOCS_URL,
    redoc_url=settings.REDOC_URL,
    lifespan=lifespan,
)

# Healthcheck at root level (not under /api/v1)
app.include_router(health.router)

# API routes
prefix = settings.API_PREFIX
app.include_router(projects.router, prefix=prefix)
app.include_router(submissions.router, prefix=prefix)
app.include_router(crawl.router, prefix=prefix)
app.include_router(tasks.router, prefix=prefix)
app.include_router(metadata.router, prefix=prefix)
app.include_router(logs.router, prefix=prefix)
app.include_router(scheduled_crawls.router, prefix=prefix)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.getLogger("metaminer").error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})
