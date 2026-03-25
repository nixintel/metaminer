"""
Shared fixtures for integration tests.

Requires a running Postgres instance with a 'metaminer_test' database.

Create the test database once:
    docker compose exec postgres psql -U metaminer -c "CREATE DATABASE metaminer_test;"

Then run:
    TEST_DATABASE_URL=postgresql+asyncpg://metaminer:metaminer@postgres:5432/metaminer_test pytest -m integration

Or point at a different instance:
    TEST_DATABASE_URL=postgresql+asyncpg://user:pass@host/dbname pytest -m integration
"""
import os
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

# Import all models so Base.metadata knows about every table
import app.models.project
import app.models.task
import app.models.file_submission
import app.models.metadata_record
import app.models.log_entry
import app.models.scheduled_crawl
from app.database import Base

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://metaminer:metaminer@localhost:5432/metaminer_test",
)


@pytest.fixture
async def test_engine():
    """
    Creates a fresh async engine for each test.
    Tables are created before the test and dropped afterwards,
    keeping everything in the same function-scoped event loop.
    """
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
async def db(test_engine):
    """
    Yields an AsyncSession for a single test.
    SQLAlchemy auto-begins a transaction on first use; we roll it back
    after each test so tests never leave data behind.

    Note: call `await db.flush()` inside tests (not `await db.commit()`)
    to make newly added objects visible to queries in the same session.
    """
    async with AsyncSession(test_engine, expire_on_commit=False) as session:
        yield session
        await session.rollback()
