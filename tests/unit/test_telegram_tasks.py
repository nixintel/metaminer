"""
Unit tests for the Telegram scraper module.

Covers:
  - Celery task decorator configuration (acks_late, max_retries, queue)
  - Celery routing for telegram tasks and scheduler
  - TaskResponse.telegram_channel computed field
  - TelegramScrapeSubmit and ScheduledTelegramScrapeCreate schema validation

No Telegram API, database, or network access required.
"""
import json
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone

from app.workers.telegram_tasks import run_telegram_task
from app.workers.celery_app import celery_app
from app.schemas.task import TaskResponse
from app.schemas.telegram import (
    TelegramScrapeSubmit,
    ScheduledTelegramScrapeCreate,
    ScheduledTelegramScrapeUpdate,
    ScheduledTelegramScrapeResponse,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Celery task configuration
# ---------------------------------------------------------------------------

class TestTelegramTaskConfig:
    def test_acks_late_is_true(self):
        assert run_telegram_task.acks_late is True

    def test_max_retries_is_three(self):
        assert run_telegram_task.max_retries == 3

    def test_task_name(self):
        assert run_telegram_task.name == "metaminer.telegram_task"

    def test_default_retry_delay(self):
        assert run_telegram_task.default_retry_delay == 60


class TestCeleryRouting:
    def test_telegram_task_routes_to_telegram_queue(self):
        routes = celery_app.conf.task_routes
        assert routes["metaminer.telegram_task"]["queue"] == "telegram"

    def test_dispatch_telegram_scrapes_routes_to_maintenance_queue(self):
        routes = celery_app.conf.task_routes
        assert routes["metaminer.dispatch_scheduled_telegram_scrapes"]["queue"] == "maintenance"

    def test_telegram_queue_is_defined(self):
        queue_names = {q.name for q in celery_app.conf.task_queues}
        assert "telegram" in queue_names

    def test_dispatch_telegram_scrapes_in_beat_schedule(self):
        beat = celery_app.conf.beat_schedule
        task_names = {entry["task"] for entry in beat.values()}
        assert "metaminer.dispatch_scheduled_telegram_scrapes" in task_names

    def test_dispatch_telegram_scrapes_beat_interval_is_60s(self):
        beat = celery_app.conf.beat_schedule
        entry = next(
            e for e in beat.values()
            if e["task"] == "metaminer.dispatch_scheduled_telegram_scrapes"
        )
        assert entry["schedule"] == 60


# ---------------------------------------------------------------------------
# TaskResponse.telegram_channel computed field
# ---------------------------------------------------------------------------

def _make_task_response(**overrides) -> TaskResponse:
    defaults = dict(
        id=1,
        project_id=1,
        task_type="telegram",
        status="pending",
        celery_task_id=None,
        config_json=None,
        files_found=0,
        files_processed=0,
        crawl_failures=0,
        crawl_errors=None,
        skipped_duplicates=0,
        error_message=None,
        created_at=datetime.now(timezone.utc),
        started_at=None,
        completed_at=None,
    )
    defaults.update(overrides)
    return TaskResponse(**defaults)


class TestTaskResponseTelegramChannel:
    def test_channel_extracted_from_config_json(self):
        resp = _make_task_response(
            config_json=json.dumps({"channel": "@testchannel", "max_files": 50})
        )
        assert resp.telegram_channel == "@testchannel"

    def test_channel_is_none_when_config_json_is_none(self):
        resp = _make_task_response(config_json=None)
        assert resp.telegram_channel is None

    def test_channel_is_none_when_key_absent_from_config(self):
        resp = _make_task_response(
            config_json=json.dumps({"url": "https://example.com"})
        )
        assert resp.telegram_channel is None

    def test_channel_is_none_when_config_json_is_invalid(self):
        resp = _make_task_response(config_json="not valid json {{")
        assert resp.telegram_channel is None

    def test_crawl_url_is_none_for_telegram_task(self):
        # telegram tasks have channel, not url
        resp = _make_task_response(
            config_json=json.dumps({"channel": "@testchannel"})
        )
        assert resp.crawl_url is None

    def test_channel_with_at_prefix_preserved(self):
        resp = _make_task_response(
            config_json=json.dumps({"channel": "@my_channel_123"})
        )
        assert resp.telegram_channel == "@my_channel_123"


# ---------------------------------------------------------------------------
# TelegramScrapeSubmit schema
# ---------------------------------------------------------------------------

class TestTelegramScrapeSubmit:
    def test_minimal_valid_submission(self):
        body = TelegramScrapeSubmit(project_id=1, channel="@testchannel")
        assert body.project_id == 1
        assert body.channel == "@testchannel"

    def test_defaults_are_none_or_false(self):
        body = TelegramScrapeSubmit(project_id=1, channel="@chan")
        assert body.allowed_file_types is None
        assert body.max_file_size_mb is None
        assert body.max_files is None
        assert body.date_from is None
        assert body.date_to is None
        assert body.retain_files is False
        assert body.deduplicate is True
        assert body.pdf_mode is None

    def test_all_fields_accepted(self):
        now = datetime.now(timezone.utc)
        body = TelegramScrapeSubmit(
            project_id=2,
            channel="t.me/somechannel",
            allowed_file_types=["pdf", "docx"],
            max_file_size_mb=50,
            max_files=200,
            date_from=now,
            date_to=now,
            retain_files=True,
            deduplicate=False,
            pdf_mode=True,
        )
        assert body.allowed_file_types == ["pdf", "docx"]
        assert body.max_file_size_mb == 50
        assert body.max_files == 200
        assert body.retain_files is True
        assert body.pdf_mode is True


# ---------------------------------------------------------------------------
# ScheduledTelegramScrapeCreate schema
# ---------------------------------------------------------------------------

class TestScheduledTelegramScrapeCreate:
    def test_valid_schedule(self):
        s = ScheduledTelegramScrapeCreate(
            project_id=1,
            channel="@channel",
            frequency_seconds=3600,
        )
        assert s.frequency_seconds == 3600

    def test_frequency_below_60_raises(self):
        with pytest.raises(Exception):
            ScheduledTelegramScrapeCreate(
                project_id=1,
                channel="@channel",
                frequency_seconds=59,
            )

    def test_frequency_of_zero_raises(self):
        with pytest.raises(Exception):
            ScheduledTelegramScrapeCreate(
                project_id=1,
                channel="@channel",
                frequency_seconds=0,
            )

    def test_frequency_exactly_60_is_valid(self):
        s = ScheduledTelegramScrapeCreate(
            project_id=1,
            channel="@channel",
            frequency_seconds=60,
        )
        assert s.frequency_seconds == 60

    def test_defaults(self):
        s = ScheduledTelegramScrapeCreate(
            project_id=1,
            channel="@channel",
            frequency_seconds=86400,
        )
        assert s.retain_files is False
        assert s.deduplicate is True
        assert s.pdf_mode is None
        assert s.max_files is None
        assert s.max_file_size_mb is None
        assert s.date_range_days is None


class TestScheduledTelegramScrapeUpdate:
    def test_all_fields_optional(self):
        # An empty update is valid
        u = ScheduledTelegramScrapeUpdate()
        assert u.is_active is None
        assert u.frequency_seconds is None

    def test_frequency_below_60_raises_on_update(self):
        with pytest.raises(Exception):
            ScheduledTelegramScrapeUpdate(frequency_seconds=30)

    def test_frequency_none_is_valid(self):
        u = ScheduledTelegramScrapeUpdate(frequency_seconds=None)
        assert u.frequency_seconds is None

    def test_partial_update_sets_only_specified_fields(self):
        u = ScheduledTelegramScrapeUpdate(is_active=False)
        assert u.is_active is False
        assert u.frequency_seconds is None
        assert u.pdf_mode is None
