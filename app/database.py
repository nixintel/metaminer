from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from config import settings


class Base(DeclarativeBase):
    pass


engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


_MIGRATIONS = [
    # Added in v0.2: HTTP change-detection headers for crawl deduplication
    "ALTER TABLE file_submissions ADD COLUMN IF NOT EXISTS http_etag VARCHAR(512)",
    "ALTER TABLE file_submissions ADD COLUMN IF NOT EXISTS http_last_modified VARCHAR(128)",
    # Added in v0.3: Scrapy JOBDIR path for mid-crawl resume
    "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS crawl_jobdir TEXT",
    # Added in v0.4: per-crawl cross-domain link following toggle
    "ALTER TABLE scheduled_crawls ADD COLUMN IF NOT EXISTS allow_cross_domain BOOLEAN NOT NULL DEFAULT FALSE",
    # Added in v0.5: indices on log_entries filter columns (task_id, submission_id)
    # to avoid full table scans when viewing logs for a specific task or submission.
    "CREATE INDEX IF NOT EXISTS ix_log_entries_task_id ON log_entries (task_id)",
    "CREATE INDEX IF NOT EXISTS ix_log_entries_submission_id ON log_entries (submission_id)",
]


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        for stmt in _MIGRATIONS:
            await conn.execute(text(stmt))


async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def make_task_session_factory():
    """
    Create a fresh async engine + session factory for use inside Celery tasks.

    asyncpg's connection pool is bound to the event loop it was created on.
    Each call to asyncio.run() in a Celery worker creates a new loop and then
    destroys it, so the module-level engine/AsyncSessionLocal cannot be safely
    reused across task executions in the same worker process.

    Callers MUST await engine.dispose() when done (use a try/finally block).
    Returns (engine, session_factory).

    Pool sizing: a crawl task runs up to CRAWL_URL_CONCURRENCY URLs at once, each
    holding a short-lived session per in-flight file, plus the progress committer.
    Size the pool to that peak (+2 headroom) so concurrent URLs never block on
    connection checkout. Note total DB connections across the system are roughly
    CRAWL_WORKER_COUNT × pool_size + the manual workers + the API engine — keep the
    sum under Postgres max_connections.
    """
    pool_size = max(5, settings.CRAWL_URL_CONCURRENCY + 2)
    task_engine = create_async_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=2,
    )
    session_factory = async_sessionmaker(
        task_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return task_engine, session_factory
