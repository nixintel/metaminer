"""
Unit tests for multi-URL crawl functionality.

Covers:
  - CrawlSubmit schema: URL list validation (empty, whitespace, stripping, ordering)
  - run_crawl_task: multi-URL aggregation, per-URL retry logic, cancellation
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pydantic import ValidationError

pytestmark = pytest.mark.unit


# ─────────────────────────────────────────────────────────────────────────────
# CrawlSubmit schema
# ─────────────────────────────────────────────────────────────────────────────

class TestCrawlSubmitSchema:
    def _make(self, **overrides):
        from app.schemas.crawl import CrawlSubmit
        defaults = {"project_id": 1, "urls": ["https://example.com"]}
        return CrawlSubmit(**{**defaults, **overrides})

    def test_single_url_accepted(self):
        obj = self._make(urls=["https://example.com"])
        assert obj.urls == ["https://example.com"]

    def test_multiple_urls_accepted(self):
        urls = ["https://a.com", "https://b.com", "https://c.com"]
        assert self._make(urls=urls).urls == urls

    def test_url_order_preserved(self):
        urls = ["https://third.com", "https://first.com", "https://second.com"]
        assert self._make(urls=urls).urls == urls

    def test_empty_list_raises(self):
        with pytest.raises(ValidationError, match="At least one URL is required"):
            self._make(urls=[])

    def test_whitespace_only_url_raises(self):
        with pytest.raises(ValidationError, match="At least one URL is required"):
            self._make(urls=["   "])

    def test_all_blank_urls_raises(self):
        with pytest.raises(ValidationError, match="At least one URL is required"):
            self._make(urls=["", "  ", "\t"])

    def test_leading_trailing_whitespace_stripped(self):
        obj = self._make(urls=["  https://example.com  "])
        assert obj.urls == ["https://example.com"]

    def test_blank_urls_filtered_when_valid_ones_present(self):
        obj = self._make(urls=["", "https://a.com", "   ", "https://b.com"])
        assert obj.urls == ["https://a.com", "https://b.com"]

    def test_multiple_urls_each_stripped_independently(self):
        obj = self._make(urls=[" https://a.com ", " https://b.com "])
        assert obj.urls == ["https://a.com", "https://b.com"]

    def test_defaults_are_correct(self):
        obj = self._make()
        assert obj.deduplicate is True
        assert obj.full_download is False
        assert obj.retain_files is False
        assert obj.depth_limit is None
        assert obj.allowed_file_types is None
        assert obj.robotstxt_obey is None
        assert obj.crawl_images is None
        assert obj.allow_cross_domain is None


# ─────────────────────────────────────────────────────────────────────────────
# Multi-URL task loop
# ─────────────────────────────────────────────────────────────────────────────

def _result(files_seen=5, processed=3, errors=0, skipped=2, failure_count=0, failed_urls=None):
    return {
        "files_seen": files_seen,
        "processed": processed,
        "errors": errors,
        "skipped": skipped,
        "failure_count": failure_count,
        "failed_urls": failed_urls or [],
    }


class TestMultiUrlTaskLoop:
    """
    Tests for the multi-URL loop inside run_crawl_task._run.

    Heavy dependencies are mocked:
      _crawl_one_url, make_task_session_factory, check_cancel_flag,
      clear_cancel_flag, asyncio.sleep, config.settings
    """

    def _make_db_task(self):
        t = MagicMock()
        t.id = 1
        t.status = "pending"
        t.files_found = 0
        t.files_processed = 0
        t.skipped_duplicates = 0
        t.crawl_failures = 0
        t.crawl_jobdir = None
        t.error_message = None
        t.crawl_errors = None
        return t

    def _make_session_factory(self, db_task):
        mock_db = AsyncMock()
        mock_db.get = AsyncMock(return_value=db_task)
        mock_db.commit = AsyncMock()

        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=mock_db)
        ctx.__aexit__ = AsyncMock(return_value=None)

        SessionLocal = MagicMock(return_value=ctx)
        engine = AsyncMock()
        engine.dispose = AsyncMock()
        return engine, SessionLocal

    def _run(self, urls, side_effects, cancel_sequence=None):
        """
        Run run_crawl_task.apply() with controlled _crawl_one_url results.

        side_effects: iterable of return dicts or Exceptions, consumed in
                      order across all _crawl_one_url calls (including retries).
        cancel_sequence: iterable of bools fed to check_cancel_flag in order;
                         defaults to always-False.
        """
        db_task = self._make_db_task()
        engine, SessionLocal = self._make_session_factory(db_task)

        effects = iter(side_effects)
        cancel_iter = iter(cancel_sequence or [])

        async def fake_crawl(**_kwargs):
            v = next(effects)
            if isinstance(v, Exception):
                raise v
            return v

        def fake_check_cancel(_task_id):
            return next(cancel_iter, False)

        with (
            patch("app.workers.crawl_tasks._crawl_one_url", side_effect=fake_crawl),
            patch("app.database.make_task_session_factory", return_value=(engine, SessionLocal)),
            patch("app.workers.crawl_tasks.check_cancel_flag", side_effect=fake_check_cancel),
            patch("app.workers.crawl_tasks.clear_cancel_flag"),
            patch("asyncio.sleep", new=AsyncMock()),
            patch("config.settings", MagicMock()),
        ):
            from app.workers.crawl_tasks import run_crawl_task
            run_crawl_task.apply(args=[1, 10, urls])

        return db_task

    # ── Status outcomes ───────────────────────────────────────────────────────

    def test_single_url_success_sets_completed(self):
        task = self._run(["https://a.com"], [_result()])
        assert task.status == "completed"

    def test_all_urls_succeed_sets_completed(self):
        task = self._run(
            ["https://a.com", "https://b.com", "https://c.com"],
            [_result(), _result(), _result()],
        )
        assert task.status == "completed"

    def test_url_exhausted_after_all_retries_sets_failed(self):
        # PER_URL_RETRIES=2 means 3 total attempts; all fail
        task = self._run(
            ["https://bad.com"],
            [RuntimeError("timeout"), RuntimeError("timeout"), RuntimeError("timeout")],
        )
        assert task.status == "failed"

    def test_partial_failure_sets_failed_status(self):
        task = self._run(
            ["https://ok.com", "https://bad.com"],
            [
                _result(),
                RuntimeError("x"), RuntimeError("x"), RuntimeError("x"),
            ],
        )
        assert task.status == "failed"

    def test_cancellation_sets_cancelled_status(self):
        # check_cancel_flag call order for 2 URLs, first URL succeeds:
        #   call 0: outer loop before URL 0 → False (proceed)
        #   call 1: inner retry loop before attempt 0 → False (proceed)
        #   URL 0 crawl succeeds, DB updated
        #   call 2: outer loop before URL 1 → True (cancel)
        task = self._run(
            ["https://a.com", "https://b.com"],
            [_result()],
            cancel_sequence=[False, False, True],
        )
        assert task.status == "cancelled"

    # ── Stat aggregation ──────────────────────────────────────────────────────

    def test_stats_summed_across_urls(self):
        task = self._run(
            ["https://a.com", "https://b.com"],
            [
                _result(files_seen=10, processed=8, skipped=2, failure_count=1),
                _result(files_seen=6,  processed=5, skipped=1, failure_count=0),
            ],
        )
        assert task.files_found == 16
        assert task.files_processed == 13
        assert task.skipped_duplicates == 3
        assert task.crawl_failures == 1

    def test_failed_url_appears_in_error_message(self):
        task = self._run(
            ["https://bad.com"],
            [RuntimeError("x"), RuntimeError("x"), RuntimeError("x")],
        )
        assert "https://bad.com" in task.error_message

    def test_file_processing_errors_reported(self):
        task = self._run(
            ["https://a.com"],
            [_result(files_seen=5, processed=3, errors=2, skipped=0)],
        )
        assert task.status == "failed"
        assert "file(s) failed" in task.error_message

    # ── Retry logic ───────────────────────────────────────────────────────────

    def test_retry_succeeds_on_second_attempt(self):
        task = self._run(
            ["https://a.com"],
            [RuntimeError("transient"), _result(files_seen=5, processed=5)],
        )
        assert task.status == "completed"
        assert task.files_found == 5

    def test_two_failures_then_success_within_retry_budget(self):
        # PER_URL_RETRIES=2, so up to 3 total attempts; 2 fail then 1 succeeds
        task = self._run(
            ["https://a.com"],
            [RuntimeError("x"), RuntimeError("x"), _result(files_seen=3, processed=3)],
        )
        assert task.status == "completed"
        assert task.files_found == 3

    def test_successful_retry_not_counted_as_error(self):
        """A URL that fails once but succeeds on retry must not appear in error_message."""
        task = self._run(
            ["https://a.com"],
            [RuntimeError("transient"), _result()],
        )
        assert task.error_message is None

    # ── URL index forwarded correctly ─────────────────────────────────────────

    def test_url_idx_increments_per_url(self):
        calls = []

        async def capturing(**kwargs):
            calls.append((kwargs["url"], kwargs["url_idx"]))
            return _result()

        db_task = self._make_db_task()
        engine, SessionLocal = self._make_session_factory(db_task)

        with (
            patch("app.workers.crawl_tasks._crawl_one_url", side_effect=capturing),
            patch("app.database.make_task_session_factory", return_value=(engine, SessionLocal)),
            patch("app.workers.crawl_tasks.check_cancel_flag", return_value=False),
            patch("app.workers.crawl_tasks.clear_cancel_flag"),
            patch("asyncio.sleep", new=AsyncMock()),
            patch("config.settings", MagicMock()),
        ):
            from app.workers.crawl_tasks import run_crawl_task
            run_crawl_task.apply(args=[1, 10, ["https://first.com", "https://second.com"]])

        assert calls == [("https://first.com", 0), ("https://second.com", 1)]
