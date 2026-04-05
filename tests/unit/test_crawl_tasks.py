"""
Unit tests for crawl task resume changes.

Covers:
  - Celery app-level acks_late / reject_on_worker_lost config
  - run_crawl_task decorator attributes (acks_late, max_retries)
  - _run_scrapy_in_process: JOBDIR included when jobdir is provided, absent when None

These tests mock Scrapy entirely so no Scrapy installation is required.
"""
import queue
import pytest
from unittest.mock import MagicMock, patch

from app.workers.crawl_tasks import _run_scrapy_in_process, run_crawl_task
from app.workers.celery_app import celery_app

pytestmark = pytest.mark.unit


class TestCeleryAppConfig:
    def test_task_acks_late_is_true(self):
        assert celery_app.conf.task_acks_late is True

    def test_task_reject_on_worker_lost_is_true(self):
        assert celery_app.conf.task_reject_on_worker_lost is True


class TestCrawlTaskDecorator:
    def test_acks_late_is_true(self):
        assert run_crawl_task.acks_late is True

    def test_max_retries_is_three(self):
        assert run_crawl_task.max_retries == 3


class TestRunScrapyInProcessJobdir:
    """
    Verify that JOBDIR is correctly included in or excluded from the Scrapy
    process settings dict depending on whether jobdir is provided.

    CrawlerProcess is mocked so we can capture the settings argument without
    starting a real crawl.
    """

    def _invoke(self, tmp_path, jobdir=None):
        """
        Call _run_scrapy_in_process with all Scrapy dependencies mocked.
        Returns the settings dict that was passed to CrawlerProcess.
        """
        captured = {}
        result_q = queue.SimpleQueue()

        class FakeSpider:
            """Minimal subclass-able stand-in for MetaminerSpider."""
            name = "fake"
            def closed(self, reason): pass

        class CapturingCrawlerProcess:
            def __init__(self, settings):
                captured["settings"] = dict(settings)
            def crawl(self, *args, **kwargs): pass
            def start(self): pass

        mock_scrapy_crawler = MagicMock()
        mock_scrapy_crawler.CrawlerProcess = CapturingCrawlerProcess

        mock_spider_module = MagicMock()
        mock_spider_module.MetaminerSpider = FakeSpider

        mock_cfg = MagicMock()
        mock_cfg.CRAWLER_DEPTH_LIMIT = 2
        mock_cfg.CRAWLER_DOWNLOAD_TIMEOUT = 30
        mock_cfg.CRAWLER_DOWNLOAD_DELAY = 0
        mock_cfg.CRAWLER_AUTOTHROTTLE_ENABLED = True
        mock_cfg.CRAWLER_AUTOTHROTTLE_MAX_DELAY = 10.0
        mock_cfg.CRAWLER_CONCURRENT_REQUESTS = 4
        mock_cfg.CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN = 2
        mock_cfg.CRAWLER_ROBOTSTXT_OBEY = True
        mock_cfg.CRAWLER_USER_AGENT = "TestAgent/1.0"
        mock_cfg.CRAWLER_RETRY_ENABLED = True
        mock_cfg.CRAWLER_RETRY_TIMES = 3
        mock_cfg.CRAWLER_RETRY_HTTP_CODES = [500, 502, 503]
        mock_cfg.CRAWLER_RETRY_PRIORITY_ADJUST = -1
        mock_cfg.CRAWLER_RETRY_ON_TIMEOUT = True
        mock_cfg.CRAWLER_HTTPERROR_ALLOW_ALL = False
        mock_cfg.CRAWLER_HANDLE_HTTPSTATUS_LIST = []
        mock_cfg.CRAWLER_DOWNLOAD_FAIL_ON_DATALOSS = False
        mock_cfg.CRAWLER_PARTIAL_DOWNLOAD_SIZE_MB = 10
        mock_cfg.CRAWLER_PROXY = None
        mock_cfg.CRAWLER_FOLLOW_IMAGE_TAGS = False
        mock_cfg.CRAWLER_ALLOW_CROSS_DOMAIN = False

        with (
            patch.dict("sys.modules", {
                "scrapy": MagicMock(),
                "scrapy.crawler": mock_scrapy_crawler,
                "app.crawler.scrapy_crawler": mock_spider_module,
            }),
            patch("config.settings", mock_cfg),
        ):
            _run_scrapy_in_process(
                start_url="https://example.com",
                allowed_file_types=None,
                depth_limit=None,
                full_download=False,
                output_dir=str(tmp_path),
                result_queue=result_q,
                jobdir=jobdir,
            )

        return captured.get("settings", {})

    def test_jobdir_present_in_scrapy_settings_when_provided(self, tmp_path):
        jobdir = str(tmp_path / "jobdir")
        settings = self._invoke(tmp_path, jobdir=jobdir)
        assert settings.get("JOBDIR") == jobdir

    def test_jobdir_absent_from_scrapy_settings_when_none(self, tmp_path):
        settings = self._invoke(tmp_path, jobdir=None)
        assert "JOBDIR" not in settings

    def test_depth_limit_falls_back_to_config_when_not_specified(self, tmp_path):
        settings = self._invoke(tmp_path, jobdir=None)
        # depth_limit=None → should use cfg.CRAWLER_DEPTH_LIMIT (2 from mock)
        assert settings["DEPTH_LIMIT"] == 2

    def test_depth_limit_overrides_config_when_specified(self, tmp_path):
        # Need to call _run_scrapy_in_process with depth_limit explicitly
        captured = {}
        result_q = queue.SimpleQueue()

        class FakeSpider:
            name = "fake"
            def closed(self, reason): pass

        class CapturingCrawlerProcess:
            def __init__(self, settings):
                captured["settings"] = dict(settings)
            def crawl(self, *args, **kwargs): pass
            def start(self): pass

        mock_scrapy_crawler = MagicMock()
        mock_scrapy_crawler.CrawlerProcess = CapturingCrawlerProcess
        mock_spider_module = MagicMock()
        mock_spider_module.MetaminerSpider = FakeSpider
        mock_cfg = MagicMock()
        mock_cfg.CRAWLER_DEPTH_LIMIT = 2
        mock_cfg.CRAWLER_PROXY = None
        mock_cfg.CRAWLER_FOLLOW_IMAGE_TAGS = False
        mock_cfg.CRAWLER_ALLOW_CROSS_DOMAIN = False

        with (
            patch.dict("sys.modules", {
                "scrapy": MagicMock(),
                "scrapy.crawler": mock_scrapy_crawler,
                "app.crawler.scrapy_crawler": mock_spider_module,
            }),
            patch("config.settings", mock_cfg),
        ):
            _run_scrapy_in_process(
                start_url="https://example.com",
                allowed_file_types=None,
                depth_limit=5,
                full_download=False,
                output_dir=str(tmp_path),
                result_queue=result_q,
            )

        assert captured["settings"]["DEPTH_LIMIT"] == 5
