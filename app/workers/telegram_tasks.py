"""
Celery task for Telegram channel scraping.

Unlike the web crawler, Telethon is asyncio-native and does not use Twisted,
so no subprocess isolation is required — the task runs entirely within
asyncio.run() in the solo Celery worker process.
"""
import asyncio
import logging
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path

from app.workers.celery_app import celery_app
from app.utils.cancel import check_cancel_flag, clear_cancel_flag

logger = logging.getLogger("metaminer.telegram_tasks")


@celery_app.task(
    bind=True,
    name="metaminer.telegram_task",
    queue="telegram",
    acks_late=True,
    max_retries=3,
    default_retry_delay=60,
)
def run_telegram_task(
    self,
    task_id: int,
    project_id: int,
    channel: str,
    allowed_file_types: list[str] | None = None,
    max_file_size_mb: int | None = None,
    max_files: int | None = None,
    date_from_iso: str | None = None,
    date_to_iso: str | None = None,
    retain_files: bool = False,
    deduplicate: bool = True,
    pdf_mode: bool | None = None,
):
    from config import settings

    async def _run():
        from app.database import make_task_session_factory
        from app.models.task import Task
        from app.services.metadata_service import process_single_file
        from app.services.telegram_service import get_credentials, check_session, make_client

        # Resolve defaults
        _max_file_size_mb = max_file_size_mb if max_file_size_mb is not None else settings.TELEGRAM_MAX_FILE_SIZE_MB
        _max_files = max_files if max_files is not None else settings.TELEGRAM_MAX_FILES
        _allowed_types = set(t.lower() for t in (allowed_file_types or settings.TELEGRAM_ALLOWED_FILE_TYPES))

        now = datetime.now(timezone.utc)
        _date_to = datetime.fromisoformat(date_to_iso) if date_to_iso else now
        _date_from = (
            datetime.fromisoformat(date_from_iso)
            if date_from_iso
            else now - timedelta(days=settings.TELEGRAM_DATE_RANGE_DAYS)
        )

        logger.info(
            "Telegram task starting | task_id=%d | channel=%s | date_from=%s | date_to=%s | "
            "max_files=%d | file_types=%s",
            task_id, channel, _date_from.isoformat(), _date_to.isoformat(),
            _max_files, sorted(_allowed_types),
        )

        task_engine, SessionLocal = make_task_session_factory()
        output_dir = settings.TEMP_DIR / f"telegram_{task_id}"
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            async with SessionLocal() as db:
                task = await db.get(Task, task_id)
                if not task:
                    logger.error("Telegram task not found in DB | task_id=%d", task_id)
                    return
                if task.status in ("completed", "failed", "cancelled"):
                    logger.warning(
                        "Telegram task %d already in terminal state (%s), discarding stale message",
                        task_id, task.status,
                    )
                    return
                task.status = "running"
                task.started_at = datetime.now(timezone.utc)
                await db.commit()

            # --- Credential and session checks ---
            async with SessionLocal() as db:
                creds = await get_credentials(db)

            if not creds:
                raise RuntimeError(
                    "No Telegram credentials configured. Set TELEGRAM_API_ID/TELEGRAM_API_HASH "
                    "in .env or POST credentials to /api/v1/telegram/credentials."
                )

            if not check_session():
                raise RuntimeError(
                    f"Telegram session file not found at {settings.TELEGRAM_SESSION_PATH}. "
                    "Authenticate first via POST /api/v1/telegram/auth/start and /auth/verify."
                )

            api_id, api_hash = creds
            processed = 0
            skipped_duplicates = 0
            files_seen = 0
            cancelled = False

            client = make_client(api_id, api_hash)
            async with client:
                logger.info("Telegram client connected | task_id=%d | channel=%s", task_id, channel)

                async for message in client.iter_messages(
                    channel,
                    reverse=True,
                    offset_date=_date_from,
                ):
                    # Stop if we've passed the end of the date range
                    if message.date and message.date.replace(tzinfo=timezone.utc) > _date_to:
                        break

                    # Skip non-file messages
                    if not message.document:
                        continue

                    files_seen += 1

                    # --- Extension filter ---
                    fname = (
                        getattr(message.file, "name", None)
                        or f"tg_{message.id}{getattr(message.file, 'ext', '')}"
                    )
                    ext = Path(fname).suffix.lstrip(".").lower()
                    if ext not in _allowed_types:
                        logger.debug(
                            "Skipping file (type not in allowed list) | task_id=%d | file=%s | ext=%s",
                            task_id, fname, ext,
                        )
                        continue

                    # --- Size filter ---
                    file_size_bytes = message.document.size or 0
                    if file_size_bytes > _max_file_size_mb * 1024 * 1024:
                        logger.debug(
                            "Skipping file (too large) | task_id=%d | file=%s | size_mb=%.1f",
                            task_id, fname, file_size_bytes / 1024 / 1024,
                        )
                        continue

                    # --- Download ---
                    dest = output_dir / fname
                    # Avoid clobbering if the same filename appears multiple times
                    if dest.exists():
                        dest = output_dir / f"{message.id}_{fname}"

                    logger.info(
                        "Downloading | task_id=%d | message_id=%d | file=%s | size_mb=%.1f",
                        task_id, message.id, fname, file_size_bytes / 1024 / 1024,
                    )
                    await client.download_media(message, file=str(dest))

                    # --- Metadata extraction ---
                    channel_clean = channel.lstrip("@")
                    source_url = f"https://t.me/{channel_clean}/{message.id}"

                    async with SessionLocal() as db:
                        result = await process_single_file(
                            db=db,
                            project_id=project_id,
                            file_path=str(dest),
                            retain_file_opt=retain_files,
                            pdf_mode=pdf_mode,
                            task_id=task_id,
                            submission_mode="telegram",
                            source_url=source_url,
                        )

                    if result["skipped_duplicate"]:
                        skipped_duplicates += 1
                    else:
                        processed += 1

                    # Remove temp file unless retained (retain_file() already copied it)
                    if not retain_files and dest.exists():
                        dest.unlink(missing_ok=True)

                    # Progress update every 10 files
                    if (processed + skipped_duplicates) % 10 == 0:
                        async with SessionLocal() as db:
                            t = await db.get(Task, task_id)
                            if t:
                                t.files_found = files_seen
                                t.files_processed = processed
                                t.skipped_duplicates = skipped_duplicates
                                await db.commit()

                    # Cancellation check
                    if check_cancel_flag(task_id):
                        logger.info("Cancellation requested | task_id=%d", task_id)
                        cancelled = True
                        break

                    # Max files reached
                    if (processed + skipped_duplicates) >= _max_files:
                        logger.info(
                            "Max files reached | task_id=%d | limit=%d", task_id, _max_files
                        )
                        break

            if cancelled:
                async with SessionLocal() as db:
                    t = await db.get(Task, task_id)
                    if t:
                        t.status = "cancelled"
                        t.files_found = files_seen
                        t.files_processed = processed
                        t.skipped_duplicates = skipped_duplicates
                        t.completed_at = datetime.now(timezone.utc)
                        await db.commit()
                return

            logger.info(
                "Telegram task complete | task_id=%d | channel=%s | "
                "files_seen=%d | processed=%d | skipped=%d",
                task_id, channel, files_seen, processed, skipped_duplicates,
            )

            async with SessionLocal() as db:
                t = await db.get(Task, task_id)
                if t:
                    t.status = "completed"
                    t.files_found = files_seen
                    t.files_processed = processed
                    t.skipped_duplicates = skipped_duplicates
                    t.completed_at = datetime.now(timezone.utc)
                    await db.commit()

        finally:
            clear_cancel_flag(task_id)
            await task_engine.dispose()
            shutil.rmtree(output_dir, ignore_errors=True)

    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error("Telegram task %d failed: %s", task_id, e, exc_info=True)

        async def _mark_failed():
            from app.database import make_task_session_factory
            from app.models.task import Task
            task_engine, SessionLocal = make_task_session_factory()
            try:
                async with SessionLocal() as db:
                    t = await db.get(Task, task_id)
                    if t:
                        t.status = "failed"
                        t.error_message = str(e)
                        t.completed_at = datetime.now(timezone.utc)
                        await db.commit()
            finally:
                await task_engine.dispose()

        asyncio.run(_mark_failed())
        raise
