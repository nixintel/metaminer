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
    """
    task_engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
    session_factory = async_sessionmaker(
        task_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return task_engine, session_factory
