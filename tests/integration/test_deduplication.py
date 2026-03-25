"""
Integration tests for duplicate/change-detection logic.

What these tests do
-------------------
They insert real rows into a test Postgres database and call the same
functions that the production crawl pipeline calls.  This verifies that:

  - _is_duplicate() correctly detects a file that was already processed
    in the same project but NOT in a different project.

  - _should_skip_crawl_file() applies the ETag → Last-Modified → hash
    priority chain correctly: if the server says nothing has changed we
    skip the file; if anything differs we re-process it.

Why these can't be unit tests
------------------------------
Both functions issue SQL queries.  You could mock the session, but then
you'd be testing the mock rather than the actual query logic.  Running
against a real database verifies the SQL, the column names, and the
index behaviour all at once.

Run with:
    pytest tests/integration/test_deduplication.py
"""
import pytest

pytestmark = pytest.mark.integration

from app.models.project import Project
from app.models.file_submission import FileSubmission
from app.services.metadata_service import _is_duplicate
from app.crawler.download_manager import _should_skip_crawl_file

# A valid-length SHA-256 hex string (64 chars) used as test data
HASH_A = "a" * 64
HASH_B = "b" * 64


@pytest.fixture
async def project(db):
    p = Project(name="Dedup Test Project")
    db.add(p)
    await db.flush()
    return p


@pytest.fixture
async def other_project(db):
    p = Project(name="Other Project")
    db.add(p)
    await db.flush()
    return p


# ---------------------------------------------------------------------------
# _is_duplicate
# ---------------------------------------------------------------------------

class TestIsDuplicate:
    async def test_no_prior_submission_returns_false(self, db, project):
        result = await _is_duplicate(db, project.id, HASH_A)
        assert result is False

    async def test_same_hash_same_project_returns_true(self, db, project):
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="report.pdf",
            file_hash_sha256=HASH_A,
            submission_mode="single",
        ))
        await db.flush()
        assert await _is_duplicate(db, project.id, HASH_A) is True

    async def test_same_hash_different_project_returns_false(self, db, project, other_project):
        db.add(FileSubmission(
            project_id=other_project.id,
            original_filename="report.pdf",
            file_hash_sha256=HASH_A,
            submission_mode="single",
        ))
        await db.flush()
        # Same hash but belongs to a different project — not a duplicate here
        assert await _is_duplicate(db, project.id, HASH_A) is False

    async def test_different_hash_same_project_returns_false(self, db, project):
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="report.pdf",
            file_hash_sha256=HASH_A,
            submission_mode="single",
        ))
        await db.flush()
        assert await _is_duplicate(db, project.id, HASH_B) is False


# ---------------------------------------------------------------------------
# _should_skip_crawl_file
# ---------------------------------------------------------------------------

URL = "https://example.com/document.pdf"


class TestShouldSkipCrawlFile:
    async def test_no_prior_record_returns_false(self, db, project):
        result = await _should_skip_crawl_file(
            db, project.id, URL, HASH_A, etag="abc", last_modified=None, deduplicate=True
        )
        assert result is False

    async def test_deduplicate_false_always_returns_false(self, db, project):
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="document.pdf",
            source_url=URL,
            http_etag="abc",
            file_hash_sha256=HASH_A,
            submission_mode="crawl",
        ))
        await db.flush()
        result = await _should_skip_crawl_file(
            db, project.id, URL, HASH_A, etag="abc", last_modified=None, deduplicate=False
        )
        assert result is False

    async def test_etag_match_returns_true(self, db, project):
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="document.pdf",
            source_url=URL,
            http_etag="etag-v1",
            file_hash_sha256=HASH_A,
            submission_mode="crawl",
        ))
        await db.flush()
        result = await _should_skip_crawl_file(
            db, project.id, URL, HASH_A, etag="etag-v1", last_modified=None, deduplicate=True
        )
        assert result is True

    async def test_etag_mismatch_returns_false(self, db, project):
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="document.pdf",
            source_url=URL,
            http_etag="etag-v1",
            file_hash_sha256=HASH_A,
            submission_mode="crawl",
        ))
        await db.flush()
        result = await _should_skip_crawl_file(
            db, project.id, URL, HASH_B, etag="etag-v2", last_modified=None, deduplicate=True
        )
        assert result is False

    async def test_etag_takes_priority_over_last_modified(self, db, project):
        # ETag differs but Last-Modified matches — ETag wins, should NOT skip
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="document.pdf",
            source_url=URL,
            http_etag="etag-old",
            http_last_modified="Mon, 01 Jan 2024 00:00:00 GMT",
            file_hash_sha256=HASH_A,
            submission_mode="crawl",
        ))
        await db.flush()
        result = await _should_skip_crawl_file(
            db, project.id, URL, HASH_A,
            etag="etag-new",
            last_modified="Mon, 01 Jan 2024 00:00:00 GMT",
            deduplicate=True,
        )
        assert result is False

    async def test_last_modified_match_returns_true_when_no_etag(self, db, project):
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="document.pdf",
            source_url=URL,
            http_last_modified="Mon, 01 Jan 2024 00:00:00 GMT",
            file_hash_sha256=HASH_A,
            submission_mode="crawl",
        ))
        await db.flush()
        result = await _should_skip_crawl_file(
            db, project.id, URL, HASH_A,
            etag=None,
            last_modified="Mon, 01 Jan 2024 00:00:00 GMT",
            deduplicate=True,
        )
        assert result is True

    async def test_hash_fallback_match_returns_true(self, db, project):
        # No ETag or Last-Modified on either side — fall back to hash comparison
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="document.pdf",
            source_url=URL,
            file_hash_sha256=HASH_A,
            submission_mode="crawl",
        ))
        await db.flush()
        result = await _should_skip_crawl_file(
            db, project.id, URL, HASH_A, etag=None, last_modified=None, deduplicate=True
        )
        assert result is True

    async def test_hash_fallback_mismatch_returns_false(self, db, project):
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="document.pdf",
            source_url=URL,
            file_hash_sha256=HASH_A,
            submission_mode="crawl",
        ))
        await db.flush()
        result = await _should_skip_crawl_file(
            db, project.id, URL, HASH_B, etag=None, last_modified=None, deduplicate=True
        )
        assert result is False

    async def test_no_comparators_returns_false(self, db, project):
        # Record exists but has no ETag, Last-Modified, or hash stored
        db.add(FileSubmission(
            project_id=project.id,
            original_filename="document.pdf",
            source_url=URL,
            submission_mode="crawl",
        ))
        await db.flush()
        result = await _should_skip_crawl_file(
            db, project.id, URL, file_hash=None, etag=None, last_modified=None, deduplicate=True
        )
        assert result is False
