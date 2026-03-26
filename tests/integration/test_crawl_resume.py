"""
Integration tests for crawl task resume via Scrapy JOBDIR.

What these tests do
-------------------
They verify the Task model's crawl_jobdir column is correctly persisted,
and that the migration creates the column in the real schema.

The column lifecycle (saved on start, cleared on success, preserved on
failure) depends on the Celery+multiprocessing+asyncio stack and is best
verified by running a real crawl.  These tests cover the data-layer
contract: the column exists, stores values, and can be nulled.

Run with:
    pytest tests/integration/test_crawl_resume.py
"""
import pytest

pytestmark = pytest.mark.integration

from app.models.project import Project
from app.models.task import Task


@pytest.fixture
async def project(db):
    p = Project(name="Resume Test Project")
    db.add(p)
    await db.flush()
    return p


class TestCrawlJobdirColumn:
    async def test_crawl_jobdir_defaults_to_none(self, db, project):
        task = Task(project_id=project.id, task_type="crawl", status="pending")
        db.add(task)
        await db.flush()
        assert task.crawl_jobdir is None

    async def test_crawl_jobdir_can_be_stored_and_retrieved(self, db, project):
        jobdir = "/app/data/crawl_jobs/task_42"
        task = Task(
            project_id=project.id,
            task_type="crawl",
            status="running",
            crawl_jobdir=jobdir,
        )
        db.add(task)
        await db.flush()

        await db.refresh(task)
        assert task.crawl_jobdir == jobdir

    async def test_crawl_jobdir_can_be_cleared_to_none(self, db, project):
        # Simulates what happens on successful task completion
        task = Task(
            project_id=project.id,
            task_type="crawl",
            status="running",
            crawl_jobdir="/app/data/crawl_jobs/task_1",
        )
        db.add(task)
        await db.flush()

        task.crawl_jobdir = None
        await db.flush()
        await db.refresh(task)
        assert task.crawl_jobdir is None

    async def test_crawl_jobdir_preserved_when_status_is_failed(self, db, project):
        # On failure the jobdir should remain set so a re-queued task can resume
        jobdir = "/app/data/crawl_jobs/task_7"
        task = Task(
            project_id=project.id,
            task_type="crawl",
            status="failed",
            crawl_jobdir=jobdir,
            error_message="Scrapy process error: connection refused",
        )
        db.add(task)
        await db.flush()

        await db.refresh(task)
        assert task.crawl_jobdir == jobdir
        assert task.status == "failed"

    async def test_non_crawl_task_crawl_jobdir_is_none(self, db, project):
        # crawl_jobdir is only meaningful for crawl tasks; bulk tasks leave it null
        task = Task(project_id=project.id, task_type="bulk", status="pending")
        db.add(task)
        await db.flush()
        assert task.crawl_jobdir is None

    async def test_crawl_jobdir_column_accepts_long_path(self, db, project):
        # Column is TEXT (unlimited) — long paths should round-trip fine
        long_path = "/app/data/crawl_jobs/" + "x" * 500
        task = Task(
            project_id=project.id,
            task_type="crawl",
            status="running",
            crawl_jobdir=long_path,
        )
        db.add(task)
        await db.flush()
        await db.refresh(task)
        assert task.crawl_jobdir == long_path
