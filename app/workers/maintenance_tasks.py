"""
Periodic maintenance tasks run by Celery beat (optional) or called at startup.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from app.workers.celery_app import celery_app

logger = logging.getLogger("metaminer.maintenance")


@celery_app.task(name="metaminer.dispatch_scheduled_crawls", queue="maintenance")
def dispatch_scheduled_crawls():
    """
    Runs every 60 seconds. Finds active scheduled crawls whose next_run_at is
    due, dispatches a crawl task for each, then advances next_run_at.
    Scheduled crawls always use deduplicate=True so only new files are processed.
    """
    async def _run():
        from app.database import make_task_session_factory
        from app.models.scheduled_crawl import ScheduledCrawl
        from app.workers.crawl_tasks import run_crawl_task
        from app.models.task import Task
        from sqlalchemy import select
        import json

        now = datetime.now(timezone.utc)
        task_engine, SessionLocal = make_task_session_factory()
        try:
            async with SessionLocal() as db:
                result = await db.execute(
                    select(ScheduledCrawl).where(
                        ScheduledCrawl.is_active == True,
                        ScheduledCrawl.next_run_at <= now,
                    )
                )
                due = result.scalars().all()

            for schedule in due:
                allowed_file_types = None
                if schedule.allowed_file_types:
                    try:
                        allowed_file_types = json.loads(schedule.allowed_file_types)
                    except Exception:
                        pass

                # Create a Task record so the crawl is tracked in the UI
                async with SessionLocal() as db:
                    import json as _json
                    task = Task(
                        project_id=schedule.project_id,
                        task_type="crawl",
                        config_json=_json.dumps({
                            "url": schedule.url,
                            "depth_limit": schedule.depth_limit,
                            "allowed_file_types": allowed_file_types,
                            "full_download": schedule.full_download,
                            "retain_files": schedule.retain_files,
                            "crawl_images": schedule.crawl_images,
                            "robotstxt_obey": schedule.robotstxt_obey,
                            "allow_cross_domain": schedule.allow_cross_domain,
                            "scheduled_crawl_id": schedule.id,
                        }),
                    )
                    db.add(task)
                    await db.flush()
                    task_id = task.id
                    celery_result = run_crawl_task.delay(
                        task_id,
                        schedule.project_id,
                        schedule.url,
                        schedule.depth_limit,
                        allowed_file_types,
                        schedule.full_download,
                        schedule.retain_files,
                        True,  # deduplicate — always True for scheduled crawls
                        schedule.robotstxt_obey,
                        schedule.crawl_images,
                        schedule.allow_cross_domain,
                    )
                    task.celery_task_id = celery_result.id
                    await db.commit()

                # Advance the schedule
                async with SessionLocal() as db:
                    s = await db.get(ScheduledCrawl, schedule.id)
                    if s:
                        s.last_run_at = now
                        s.next_run_at = now + timedelta(seconds=s.frequency_seconds)
                        await db.commit()

                logger.info(
                    "Dispatched scheduled crawl | schedule_id=%d | url=%s | "
                    "task_id=%d | next_run_at=%s",
                    schedule.id, schedule.url, task_id,
                    now + timedelta(seconds=schedule.frequency_seconds),
                )
        finally:
            await task_engine.dispose()

    asyncio.run(_run())


@celery_app.task(name="metaminer.purge_old_logs", queue="maintenance")
def purge_old_logs():
    from config import settings

    async def _run():
        from app.database import AsyncSessionLocal
        from app.models.log_entry import LogEntry
        from sqlalchemy import delete

        cutoff = datetime.now(timezone.utc) - timedelta(days=settings.LOG_DB_RETENTION_DAYS)
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                delete(LogEntry).where(LogEntry.created_at < cutoff)
            )
            await db.commit()
            logger.info(f"Purged {result.rowcount} log entries older than {settings.LOG_DB_RETENTION_DAYS} days")

    asyncio.run(_run())


@celery_app.task(name="metaminer.cleanup_temp_files", queue="maintenance")
def cleanup_temp_files():
    from config import settings
    from app.services.file_service import cleanup_temp_older_than

    cleanup_temp_older_than(settings.TEMP_FILE_TTL_HOURS)
    logger.info(f"Cleaned up temp files older than {settings.TEMP_FILE_TTL_HOURS} hour(s)")
