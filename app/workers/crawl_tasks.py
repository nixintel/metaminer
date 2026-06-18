"""
Celery task for web crawling.

Scrapy's Twisted reactor cannot be restarted once stopped, so each crawl task
runs Scrapy in a fresh subprocess via multiprocessing.Process to avoid
'reactor already started' errors across multiple task executions.

Multi-URL support: the task accepts a list of start URLs and crawls up to
CRAWL_URL_CONCURRENCY of them concurrently (bounded by an asyncio.Semaphore),
each in its own isolated Scrapy subprocess. If a URL fails, it is retried up to
PER_URL_RETRIES times with a short delay; that delay yields to sibling URLs
rather than blocking them. After all retries are exhausted the failure is logged
and recorded, while other URLs continue. The overall task only enters a hard
failure state if an infrastructure-level exception occurs (database unreachable,
etc.).

A single debounced committer task writes live progress to the Task row so the
concurrent URL coroutines never clobber each other's counters.
"""
import asyncio
import logging
import multiprocessing
from datetime import datetime, timezone
from pathlib import Path

from app.workers.celery_app import celery_app
from app.utils.cancel import check_cancel_flag, clear_cancel_flag

logger = logging.getLogger("metaminer.crawl_tasks")

PER_URL_RETRIES = 2        # additional attempts after the first failure
PER_URL_RETRY_DELAY = 30   # seconds between per-URL retry attempts


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
    task_id: int | None = None,
):
    """Runs inside a child process. Puts result dict into result_queue on completion."""
    try:
        from scrapy.crawler import CrawlerProcess
        from app.crawler.scrapy_crawler import MetaminerSpider
        from config import settings as cfg
        from urllib.parse import urlparse, urlunparse

        # This is a fresh spawned process — Celery's worker_process_init logging setup
        # did not run here, so the spider's own logs would never reach the DB. Attach a
        # Redis log handler (WARNING+, to bound volume) that stamps task_id on every
        # record, so crawl failures (e.g. blocked start URL) are visible in the task's logs.
        if task_id is not None:
            try:
                import logging as _logging
                from app.utils.logging_config import RedisQueueHandler

                class _TaskIdFilter(_logging.Filter):
                    def filter(self, record):
                        record.task_id = task_id
                        return True

                _h = RedisQueueHandler()
                _h.setLevel(_logging.WARNING)
                _h.setFormatter(_logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"
                ))
                _h.addFilter(_TaskIdFilter())
                _logging.getLogger().addHandler(_h)
                _logging.getLogger().setLevel(_logging.INFO)
            except Exception:
                pass

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
                super().closed(reason)

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


class _TaskCancelled(Exception):
    """Raised inside _crawl_one_url when a cancellation flag is detected."""
    pass


class _StartUrlError(Exception):
    """Raised when the start URL never returned a successful response.

    retryable=False for permanent failures (e.g. DNS resolution failure, connection
    refused) that won't recover on retry, so the caller can fail fast instead of
    burning the per-URL retry ladder.
    """
    def __init__(self, message: str, retryable: bool = True):
        super().__init__(message)
        self.retryable = retryable


# Substrings in a start-URL failure reason that indicate a permanent (non-retryable)
# error — retrying the whole crawl won't help.
_PERMANENT_START_ERRORS = (
    "DNS lookup failed",
    "CannotResolveHost",
    "Connection was refused",
    "ConnectionRefused",
    "No route to host",
)


class _Progress:
    """Live, task-wide progress counters shared across concurrent URL crawls.

    asyncio is single-threaded, so plain ``+=`` between awaits is atomic and no
    lock is needed. These drive the debounced DB committer; the authoritative
    final counts are aggregated from each URL's returned stats.
    """
    __slots__ = ("files_seen", "processed", "skipped", "errors")

    def __init__(self):
        self.files_seen = 0
        self.processed = 0
        self.skipped = 0
        self.errors = 0


async def _write_progress(shared: "_Progress", SessionLocal, task_id: int):
    """Write the current shared counters to the Task row (one transaction)."""
    from app.models.task import Task
    async with SessionLocal() as db:
        t = await db.get(Task, task_id)
        if t:
            t.files_found = shared.files_seen
            t.files_processed = shared.processed
            t.skipped_duplicates = shared.skipped
            await db.commit()


async def _progress_committer(shared: "_Progress", SessionLocal, task_id: int):
    """Single writer of live progress to the Task row.

    Loops until cancelled, committing the shared counters every ~2s. Having one
    committer (rather than each URL coroutine writing its own local counts)
    prevents concurrent URLs from clobbering each other's progress.
    """
    while True:
        await asyncio.sleep(2)
        try:
            await _write_progress(shared, SessionLocal, task_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("Progress committer write failed | task_id=%d | error=%s", task_id, e)


async def _crawl_one_url(
    *,
    url: str,
    url_idx: int,
    task_id: int,
    project_id: int,
    depth_limit,
    allowed_file_types,
    full_download,
    retain_files,
    deduplicate,
    robotstxt_obey,
    crawl_images,
    allow_cross_domain,
    SessionLocal,
    settings,
    shared: "_Progress",
    active_filters=None,
) -> dict:
    """
    Run Scrapy for a single URL.
    Returns a stats dict on success.
    Raises _TaskCancelled if the task cancellation flag is set.
    Raises on any other crawl failure so the caller can retry.
    """
    import queue as _queue
    from app.crawler.download_manager import process_one_crawl_file

    jobdir = str(settings.TEMP_DIR.parent / "crawl_jobs" / f"task_{task_id}_url_{url_idx}")
    output_dir = str(settings.TEMP_DIR / f"crawl_{task_id}_url_{url_idx}")
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(jobdir).mkdir(parents=True, exist_ok=True)

    resuming = any(Path(jobdir).iterdir()) if Path(jobdir).exists() else False
    logger.info(
        "%s Scrapy job | task_id=%d | url_idx=%d | url=%s | jobdir=%s",
        "Resuming" if resuming else "Starting", task_id, url_idx, url, jobdir,
    )

    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()

    logger.info(
        "Launching Scrapy subprocess | task_id=%d | url=%s | output_dir=%s",
        task_id, url, output_dir,
    )

    proc = ctx.Process(
        target=_run_scrapy_in_process,
        args=(url, allowed_file_types, depth_limit, full_download, output_dir,
              result_queue, robotstxt_obey, crawl_images, jobdir, allow_cross_domain,
              task_id),
        daemon=False,
    )
    proc.start()

    files_seen = 0
    processed = 0
    errors = 0
    skipped_duplicates = 0
    crawl_done = False
    crawl_done_msg: dict = {}

    async def _handle_file_msg(msg: dict):
        nonlocal processed, errors, skipped_duplicates, files_seen
        files_seen += 1
        shared.files_seen += 1
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
            active_filters=active_filters,
        )
        if outcome == "processed":
            processed += 1
            shared.processed += 1
        elif outcome == "skipped":
            skipped_duplicates += 1
            shared.skipped += 1
        else:
            errors += 1
            shared.errors += 1
        # Live progress is written to the DB by the single _progress_committer
        # task from the shared counters — no per-URL commit here, which would
        # clobber sibling URLs' progress under concurrency.

    try:
        while True:
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
                raise _TaskCancelled()

            await asyncio.sleep(1)

        logger.info(
            "Scrapy subprocess exited | task_id=%d | url=%s | exit_code=%s",
            task_id, url, proc.exitcode,
        )

        # Final drain: process any messages that arrived between the last
        # get_nowait() and subprocess exit.
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

    finally:
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
        import shutil
        shutil.rmtree(output_dir, ignore_errors=True)

    failure_count = crawl_done_msg.get("failure_count", 0)

    if crawl_done_msg.get("failed_urls"):
        for item in crawl_done_msg["failed_urls"]:
            logger.warning(
                "Crawl request failure | task_id=%d | url=%s | error=%s",
                task_id, item.get("url"), item.get("error"),
                extra={"task_id": task_id},
            )

    logger.info(
        "URL crawl complete | task_id=%d | url=%s | "
        "files_seen=%d | request_failures=%d | closed_reason=%s",
        task_id, url, files_seen,
        failure_count, crawl_done_msg.get("closed_reason"),
        extra={"task_id": task_id},
    )

    import shutil
    shutil.rmtree(jobdir, ignore_errors=True)

    # If the start URL itself never returned a successful response (blocked/geoblocked,
    # 403/451, connection reset, timeout), the crawl couldn't begin. Raise so the caller
    # retries and ultimately records the URL as failed — instead of a silent "completed".
    if not crawl_done_msg.get("start_url_succeeded", True):
        failed = crawl_done_msg.get("failed_urls", [])
        reason = "; ".join(
            f"{i.get('url')}: {i.get('error')}" for i in failed
        ) or (
            f"start URL did not return a successful response "
            f"(closed_reason={crawl_done_msg.get('closed_reason')}; possible block/geoblock/timeout)"
        )
        # Permanent failures (DNS NXDOMAIN, connection refused) won't recover on retry —
        # fail fast rather than re-running the whole crawl 3× with 30s gaps.
        retryable = not any(s in reason for s in _PERMANENT_START_ERRORS)
        raise _StartUrlError(f"Start URL failed: {reason}", retryable=retryable)

    return {
        "files_seen": files_seen,
        "processed": processed,
        "errors": errors,
        "skipped": skipped_duplicates,
        "failure_count": failure_count,
        "failed_urls": crawl_done_msg.get("failed_urls", []),
    }


@celery_app.task(bind=True, name="metaminer.crawl_task", queue="crawl",
                 acks_late=True, max_retries=3, default_retry_delay=60)
def run_crawl_task(
    self,
    task_id: int,
    project_id: int,
    urls: list[str],
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
        from app.database import make_task_session_factory
        from app.models.task import Task

        logger.info(
            "Crawl task starting | task_id=%d | urls=%s | depth_limit=%s | "
            "file_types=%s | deduplicate=%s | retain_files=%s | full_download=%s",
            task_id, urls, depth_limit, allowed_file_types,
            deduplicate, retain_files, full_download,
            extra={"task_id": task_id},
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
                task.crawl_jobdir = str(
                    settings.TEMP_DIR.parent / "crawl_jobs" / f"task_{task_id}"
                )
                await db.commit()

            shared = _Progress()
            cancelled = False
            sem = asyncio.Semaphore(max(1, settings.CRAWL_URL_CONCURRENCY))

            # Load active auto-tagging filters once for the whole crawl (project + globals).
            # A filter created mid-crawl won't be picked up here; the backfill task covers that.
            from app.services.filter_service import load_active_filters
            async with SessionLocal() as db:
                active_filters = await load_active_filters(db, project_id)
            logger.info(
                "Loaded %d active filter(s) for auto-tagging | task_id=%d",
                len(active_filters), task_id,
            )

            # Per-URL coroutine: owns its retry loop so a retry delay yields to
            # sibling URLs instead of blocking them. Returns a tagged stats dict
            # rather than mutating outer counters, so concurrent URLs can't race.
            async def _crawl_url_with_retries(url_idx: int, url: str) -> dict:
                async with sem:
                    logger.info(
                        "Processing URL %d/%d | task_id=%d | url=%s",
                        url_idx + 1, len(urls), task_id, url,
                    )
                    for attempt in range(PER_URL_RETRIES + 1):
                        if check_cancel_flag(task_id):
                            return {"status": "cancelled", "url": url}
                        try:
                            result = await _crawl_one_url(
                                url=url,
                                url_idx=url_idx,
                                task_id=task_id,
                                project_id=project_id,
                                depth_limit=depth_limit,
                                allowed_file_types=allowed_file_types,
                                full_download=full_download,
                                retain_files=retain_files,
                                deduplicate=deduplicate,
                                robotstxt_obey=robotstxt_obey,
                                crawl_images=crawl_images,
                                allow_cross_domain=allow_cross_domain,
                                SessionLocal=SessionLocal,
                                settings=settings,
                                shared=shared,
                                active_filters=active_filters,
                            )
                            return {"status": "ok", "url": url, **result}
                        except _TaskCancelled:
                            return {"status": "cancelled", "url": url}
                        except Exception as e:
                            # Permanent start-URL failures (DNS/connection refused) won't
                            # recover — don't waste the retry ladder on them.
                            permanent = isinstance(e, _StartUrlError) and not e.retryable
                            if permanent:
                                logger.error(
                                    "URL crawl failed (permanent, not retrying) | "
                                    "task_id=%d | url=%s | error=%s",
                                    task_id, url, e,
                                    extra={"task_id": task_id},
                                )
                                return {"status": "exhausted", "url": url, "error": str(e)}
                            if attempt < PER_URL_RETRIES:
                                logger.warning(
                                    "URL crawl failed (attempt %d/%d), retrying in %ds | "
                                    "task_id=%d | url=%s | error=%s",
                                    attempt + 1, PER_URL_RETRIES + 1,
                                    PER_URL_RETRY_DELAY, task_id, url, e,
                                    extra={"task_id": task_id},
                                )
                                await asyncio.sleep(PER_URL_RETRY_DELAY)
                            else:
                                logger.error(
                                    "URL crawl failed after %d attempt(s), skipping | "
                                    "task_id=%d | url=%s | error=%s",
                                    PER_URL_RETRIES + 1, task_id, url, e,
                                    exc_info=True,
                                    extra={"task_id": task_id},
                                )
                                return {"status": "exhausted", "url": url, "error": str(e)}

            logger.info(
                "Crawling %d URL(s) with url-concurrency=%d | task_id=%d",
                len(urls), min(len(urls), max(1, settings.CRAWL_URL_CONCURRENCY)), task_id,
            )

            # Single live-progress writer (see _progress_committer) plus the
            # bounded fan-out. return_exceptions=True keeps one bad URL from
            # cancelling its siblings and leaking their subprocesses.
            committer = asyncio.create_task(
                _progress_committer(shared, SessionLocal, task_id)
            )
            try:
                results = await asyncio.gather(
                    *[_crawl_url_with_retries(i, u) for i, u in enumerate(urls)],
                    return_exceptions=True,
                )
            finally:
                committer.cancel()
                try:
                    await committer
                except asyncio.CancelledError:
                    pass

            # Aggregate authoritative final counts from each URL's returned stats.
            total_files_seen = 0
            total_processed = 0
            total_errors = 0
            total_skipped = 0
            total_failure_count = 0
            all_failed_url_items: list[dict] = []
            urls_exhausted: list[dict] = []

            for res in results:
                if isinstance(res, BaseException):
                    # An exception escaped the per-URL coroutine despite its
                    # own handling — record it so the failure is never silent.
                    logger.error(
                        "Unexpected error in URL crawl coroutine | task_id=%d | error=%s",
                        task_id, res, exc_info=res,
                    )
                    urls_exhausted.append({"url": "<unknown>", "error": str(res)})
                    continue
                status = res.get("status")
                if status == "cancelled":
                    cancelled = True
                    continue
                if status == "exhausted":
                    urls_exhausted.append({"url": res["url"], "error": res.get("error", "")})
                    continue
                # status == "ok"
                total_files_seen += res["files_seen"]
                total_processed += res["processed"]
                total_errors += res["errors"]
                total_skipped += res["skipped"]
                total_failure_count += res["failure_count"]
                all_failed_url_items.extend(res["failed_urls"])

            # ── Terminal state ──────────────────────────────────────────────
            if cancelled:
                async with SessionLocal() as db:
                    task = await db.get(Task, task_id)
                    if task:
                        task.status = "cancelled"
                        task.crawl_jobdir = None
                        task.completed_at = datetime.now(timezone.utc)
                        await db.commit()
                return

            has_any_error = bool(urls_exhausted) or total_errors > 0
            final_status = "failed" if has_any_error else "completed"

            error_parts = []
            if urls_exhausted:
                failed_list = ", ".join(item["url"] for item in urls_exhausted)
                error_parts.append(f"URLs that failed all retries: {failed_list}")
            if total_errors:
                error_parts.append(f"{total_errors} file(s) failed to process")

            _log = logger.error if final_status == "failed" else logger.info
            _log(
                "Crawl task %s | task_id=%d | status=%s | urls=%d | "
                "files_seen=%d | processed=%d | skipped=%d | "
                "file_errors=%d | urls_failed=%d%s",
                "FAILED" if final_status == "failed" else "complete",
                task_id, final_status, len(urls),
                total_files_seen, total_processed, total_skipped,
                total_errors, len(urls_exhausted),
                ("; " + "; ".join(error_parts)) if error_parts else "",
                extra={"task_id": task_id},
            )

            async with SessionLocal() as db:
                task = await db.get(Task, task_id)
                if task:
                    task.status = final_status
                    task.files_found = total_files_seen
                    task.files_processed = total_processed
                    task.skipped_duplicates = total_skipped
                    task.crawl_failures = total_failure_count
                    task.completed_at = datetime.now(timezone.utc)
                    task.crawl_jobdir = None
                    if error_parts:
                        task.error_message = "; ".join(error_parts)
                    combined_failures = all_failed_url_items + urls_exhausted
                    if combined_failures:
                        task.crawl_errors = str(combined_failures)
                    await db.commit()

        finally:
            clear_cancel_flag(task_id)
            await task_engine.dispose()

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
