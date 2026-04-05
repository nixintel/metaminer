"""
Celery task for web crawling.

Scrapy's Twisted reactor cannot be restarted once stopped, so each crawl task
runs Scrapy in a fresh subprocess via multiprocessing.Process to avoid
'reactor already started' errors across multiple task executions.
"""
import asyncio
import logging
import multiprocessing
import time
from datetime import datetime, timezone
from pathlib import Path

from app.workers.celery_app import celery_app
from app.utils.cancel import check_cancel_flag, clear_cancel_flag

logger = logging.getLogger("metaminer.crawl_tasks")


def _run_scrapy_in_process(
    start_url: str,
    allowed_file_types: list[str] | None,
    depth_limit: int | None,
    full_download: bool,
    output_dir: str,
    result_queue: multiprocessing.Queue,
    robotstxt_obey: bool | None = None,
    crawl_images: bool | None = None,
    jobdir: str | None = None,
    allow_cross_domain: bool | None = None,
):
    """Runs inside a child process. Puts result dict into result_queue on completion."""
    try:
        from scrapy.crawler import CrawlerProcess
        from app.crawler.scrapy_crawler import MetaminerSpider
        from config import settings as cfg
        from urllib.parse import urlparse, urlunparse

        # Build the full Scrapy settings dict here so they are actually applied.
        # custom_settings on the spider class is read by Scrapy BEFORE __init__,
        # so instance-level overrides in __init__ are silently ignored.
        # Passing settings to CrawlerProcess applies them at project priority (20),
        # which is lower than spider custom_settings (30) — middlewares in
        # custom_settings still win — but higher than Scrapy defaults (0).
        process_settings = {
            **( {"JOBDIR": jobdir} if jobdir else {} ),
            "DEPTH_LIMIT": depth_limit if depth_limit is not None else cfg.CRAWLER_DEPTH_LIMIT,
            "DOWNLOAD_TIMEOUT": cfg.CRAWLER_DOWNLOAD_TIMEOUT,
            "DOWNLOAD_DELAY": cfg.CRAWLER_DOWNLOAD_DELAY,
            "AUTOTHROTTLE_ENABLED": cfg.CRAWLER_AUTOTHROTTLE_ENABLED,
            "AUTOTHROTTLE_MAX_DELAY": cfg.CRAWLER_AUTOTHROTTLE_MAX_DELAY,
            "CONCURRENT_REQUESTS": cfg.CRAWLER_CONCURRENT_REQUESTS,
            "CONCURRENT_REQUESTS_PER_DOMAIN": cfg.CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN,
            "ROBOTSTXT_OBEY": robotstxt_obey if robotstxt_obey is not None else cfg.CRAWLER_ROBOTSTXT_OBEY,
            "USER_AGENT": cfg.CRAWLER_USER_AGENT,
            "RETRY_ENABLED": cfg.CRAWLER_RETRY_ENABLED,
            "RETRY_TIMES": cfg.CRAWLER_RETRY_TIMES,
            "RETRY_HTTP_CODES": cfg.CRAWLER_RETRY_HTTP_CODES,
            "RETRY_PRIORITY_ADJUST": cfg.CRAWLER_RETRY_PRIORITY_ADJUST,
            "RETRY_ON_TIMEOUT": cfg.CRAWLER_RETRY_ON_TIMEOUT,
            "HTTPERROR_ALLOW_ALL": cfg.CRAWLER_HTTPERROR_ALLOW_ALL,
            "HANDLE_HTTPSTATUS_LIST": cfg.CRAWLER_HANDLE_HTTPSTATUS_LIST,
            "DOWNLOAD_FAIL_ON_DATALOSS": cfg.CRAWLER_DOWNLOAD_FAIL_ON_DATALOSS,
            "PARTIAL_DOWNLOAD_ENABLED": not full_download,
            "PARTIAL_DOWNLOAD_SIZE_MB": cfg.CRAWLER_PARTIAL_DOWNLOAD_SIZE_MB,
            "LOG_LEVEL": "INFO",
        }

        if cfg.CRAWLER_PROXY:
            proxy_url = cfg.CRAWLER_PROXY
            if cfg.CRAWLER_PROXY_USERNAME and "://" in proxy_url:
                p = urlparse(proxy_url)
                proxy_url = urlunparse(p._replace(
                    netloc=f"{cfg.CRAWLER_PROXY_USERNAME}:{cfg.CRAWLER_PROXY_PASSWORD}@{p.hostname}:{p.port}"
                ))
            process_settings["HTTPPROXY_ENABLED"] = True
            process_settings["HTTP_PROXY"] = proxy_url
            process_settings["HTTPS_PROXY"] = proxy_url

        class _TrackingSpider(MetaminerSpider):
            def closed(self, reason):
                super().closed(reason)  # emits {"type":"done",...} on self._result_queue

        process = CrawlerProcess(settings=process_settings)
        follow_images = crawl_images if crawl_images is not None else cfg.CRAWLER_FOLLOW_IMAGE_TAGS
        cross_domain = allow_cross_domain if allow_cross_domain is not None else cfg.CRAWLER_ALLOW_CROSS_DOMAIN
        process.crawl(
            _TrackingSpider,
            start_url=start_url,
            allowed_file_types=allowed_file_types,
            full_download=full_download,
            output_dir=Path(output_dir),
            crawl_images=follow_images,
            allow_cross_domain=cross_domain,
            result_queue=result_queue,
        )
        process.start()
    except Exception as e:
        result_queue.put({"error": str(e)})


@celery_app.task(bind=True, name="metaminer.crawl_task", queue="crawl",
                 acks_late=True, max_retries=3, default_retry_delay=60)
def run_crawl_task(
    self,
    task_id: int,
    project_id: int,
    url: str,
    depth_limit: int | None = None,
    allowed_file_types: list[str] | None = None,
    full_download: bool = False,
    retain_files: bool = False,
    deduplicate: bool = True,
    robotstxt_obey: bool | None = None,
    crawl_images: bool | None = None,
    allow_cross_domain: bool | None = None,
):
    from config import settings

    async def _run():
        import queue as _queue
        from app.database import make_task_session_factory
        from app.models.task import Task
        from app.crawler.download_manager import process_one_crawl_file

        logger.info(
            "Crawl task starting | task_id=%d | url=%s | depth_limit=%s | "
            "file_types=%s | deduplicate=%s | retain_files=%s | full_download=%s",
            task_id, url, depth_limit, allowed_file_types,
            deduplicate, retain_files, full_download,
        )

        task_engine, SessionLocal = make_task_session_factory()
        try:
            async with SessionLocal() as db:
                task = await db.get(Task, task_id)
                if not task:
                    logger.error("Crawl task not found in DB | task_id=%d", task_id)
                    return
                if task.status in ("completed", "failed", "cancelled"):
                    logger.warning(
                        "Crawl task %d already in terminal state (%s), discarding stale message",
                        task_id, task.status,
                    )
                    return
                task.status = "running"
                task.started_at = datetime.now(timezone.utc)
                # Persist jobdir path so it survives worker crashes — Scrapy can
                # resume from this dir if the task is re-queued after a crash.
                jobdir = str(settings.TEMP_DIR.parent / "crawl_jobs" / f"task_{task_id}")
                task.crawl_jobdir = jobdir
                await db.commit()

            Path(jobdir).mkdir(parents=True, exist_ok=True)
            resuming = any(Path(jobdir).iterdir()) if Path(jobdir).exists() else False
            logger.info(
                "%s Scrapy job | task_id=%d | jobdir=%s",
                "Resuming" if resuming else "Starting", task_id, jobdir,
            )

            # Run Scrapy in an isolated child process.
            # Use 'spawn' (not 'fork') so the child starts with a clean asyncio
            # state — forking inside asyncio.run() hands the child a half-open
            # event loop that Scrapy/Twisted can't use, causing an instant exit.
            ctx = multiprocessing.get_context("spawn")
            result_queue: multiprocessing.Queue = ctx.Queue()
            output_dir = str(settings.TEMP_DIR / f"crawl_{task_id}")
            Path(output_dir).mkdir(parents=True, exist_ok=True)

            logger.info(
                "Launching Scrapy subprocess | task_id=%d | url=%s | output_dir=%s",
                task_id, url, output_dir,
            )

            proc = ctx.Process(
                target=_run_scrapy_in_process,
                args=(url, allowed_file_types, depth_limit, full_download, output_dir, result_queue, robotstxt_obey, crawl_images, jobdir, allow_cross_domain),
                daemon=False,
            )
            proc.start()

            # Stream results from the queue as files are downloaded.
            # Per-file events ("type":"file") are processed immediately;
            # the "done" event signals the crawl is complete.
            files_seen = 0
            processed = 0
            errors = 0
            skipped_duplicates = 0
            crawl_done = False
            crawl_done_msg: dict = {}
            cancelled = False

            async def _handle_file_msg(msg: dict):
                nonlocal processed, errors, skipped_duplicates, files_seen
                files_seen += 1
                outcome = await process_one_crawl_file(
                    file_path=msg["path"],
                    source_url=msg["source_url"],
                    etag=msg["etag"],
                    last_modified=msg["last_modified"],
                    project_id=project_id,
                    task_id=task_id,
                    retain_files=retain_files,
                    pdf_mode=None,
                    deduplicate=deduplicate,
                    session_factory=SessionLocal,
                )
                if outcome == "processed":
                    processed += 1
                elif outcome == "skipped":
                    skipped_duplicates += 1
                else:
                    errors += 1

                # Write incremental progress every 10 files
                if (processed + skipped_duplicates) % 10 == 0:
                    async with SessionLocal() as db:
                        t = await db.get(Task, task_id)
                        if t:
                            t.files_processed = processed
                            t.skipped_duplicates = skipped_duplicates
                            await db.commit()

            while True:
                # Drain all messages currently available without blocking
                while True:
                    try:
                        msg = result_queue.get_nowait()
                    except _queue.Empty:
                        break

                    if msg.get("type") == "file":
                        await _handle_file_msg(msg)
                    elif msg.get("type") == "done":
                        crawl_done = True
                        crawl_done_msg = msg
                    elif "error" in msg:
                        raise RuntimeError(f"Scrapy process error: {msg['error']}")

                if crawl_done and not proc.is_alive():
                    break

                if check_cancel_flag(task_id):
                    logger.info(
                        "Cancellation requested | task_id=%d | terminating subprocess",
                        task_id,
                    )
                    proc.terminate()
                    proc.join(timeout=10)
                    if proc.is_alive():
                        logger.warning(
                            "Subprocess did not exit after SIGTERM, sending SIGKILL | task_id=%d",
                            task_id,
                        )
                        proc.kill()
                        proc.join(timeout=5)
                    cancelled = True
                    break

                time.sleep(1)

            logger.info(
                "Scrapy subprocess exited | task_id=%d | exit_code=%s | cancelled=%s",
                task_id, proc.exitcode, cancelled,
            )

            if cancelled:
                async with SessionLocal() as db:
                    task = await db.get(Task, task_id)
                    if task:
                        task.status = "cancelled"
                        task.crawl_jobdir = None
                        task.completed_at = datetime.now(timezone.utc)
                        await db.commit()
                import shutil; shutil.rmtree(jobdir, ignore_errors=True)
                return

            # Final drain: process any messages that arrived between the last
            # get_nowait() call and the subprocess exit.
            while True:
                try:
                    msg = result_queue.get_nowait()
                except _queue.Empty:
                    break
                if msg.get("type") == "file":
                    await _handle_file_msg(msg)
                elif msg.get("type") == "done":
                    crawl_done = True
                    crawl_done_msg = msg
                elif "error" in msg:
                    raise RuntimeError(f"Scrapy process error: {msg['error']}")

            failure_count = crawl_done_msg.get("failure_count", 0)

            logger.info(
                "Scrapy crawl complete | task_id=%d | url=%s | "
                "files_seen=%d | request_failures=%d | closed_reason=%s",
                task_id, url, files_seen,
                failure_count, crawl_done_msg.get("closed_reason"),
            )

            if crawl_done_msg.get("failed_urls"):
                for item in crawl_done_msg["failed_urls"]:
                    logger.warning(
                        "Crawl request failure | task_id=%d | url=%s | error=%s",
                        task_id, item.get("url"), item.get("error"),
                    )

            final_status = "completed" if errors == 0 else "completed_with_errors"
            logger.info(
                "Crawl task complete | task_id=%d | url=%s | status=%s | "
                "files_seen=%d | processed=%d | skipped=%d | errors=%d",
                task_id, url, final_status,
                files_seen, processed, skipped_duplicates, errors,
            )

            async with SessionLocal() as db:
                task = await db.get(Task, task_id)
                if task:
                    task.status = "completed" if errors == 0 else "failed"
                    task.files_found = files_seen
                    task.files_processed = processed
                    task.skipped_duplicates = skipped_duplicates
                    task.crawl_failures = failure_count
                    task.completed_at = datetime.now(timezone.utc)
                    if errors:
                        task.error_message = f"{errors} file(s) failed to process"
                    if crawl_done_msg.get("failed_urls"):
                        previous = task.crawl_errors or ""
                        task.crawl_errors = ", ".join(filter(None, [previous, str(crawl_done_msg.get("failed_urls"))]))
                    if errors == 0:
                        # Crawl completed successfully — jobdir no longer needed
                        task.crawl_jobdir = None
                    await db.commit()

            if errors == 0:
                import shutil; shutil.rmtree(jobdir, ignore_errors=True)
        finally:
            clear_cancel_flag(task_id)
            await task_engine.dispose()
            # Remove the per-crawl temp directory. Use rmtree rather than rmdir
            # because retain_file() copies (not moves) files, so originals can
            # remain when retain_files=True; the retained copy is already safe.
            try:
                import shutil
                shutil.rmtree(output_dir, ignore_errors=True)
            except Exception:
                pass

    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error(f"Crawl task {task_id} failed: {e}", exc_info=True)

        async def _mark_failed():
            from app.database import make_task_session_factory
            from app.models.task import Task
            task_engine, SessionLocal = make_task_session_factory()
            try:
                async with SessionLocal() as db:
                    task = await db.get(Task, task_id)
                    if task:
                        task.status = "failed"
                        task.error_message = str(e)
                        task.completed_at = datetime.now(timezone.utc)
                        await db.commit()
            finally:
                await task_engine.dispose()

        asyncio.run(_mark_failed())
        raise
