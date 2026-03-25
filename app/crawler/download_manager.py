"""
Runs after a Scrapy crawl completes. Processes each downloaded file
through metadata_service and optionally retains or deletes it.
"""
import logging
from pathlib import Path
from sqlalchemy import select
from app.models.file_submission import FileSubmission
from app.services.metadata_service import process_single_file
from app.services.file_service import delete_file_safe

logger = logging.getLogger("metaminer.download_manager")


async def _should_skip_crawl_file(
    db,
    project_id: int,
    source_url: str | None,
    file_hash: str | None,
    etag: str | None,
    last_modified: str | None,
    deduplicate: bool,
) -> bool:
    """
    Decide whether this file has already been processed and can be skipped.

    Change-detection priority:
      1. ETag — most reliable; server-authoritative, independent of download size
      2. Last-Modified — reliable when present
      3. Hash — fallback; unreliable for partial downloads if the file changes
         beyond the partial boundary, or if the partial limit changes between runs

    Returns True (skip) only when we can confirm the file has NOT changed.
    When in doubt, returns False so the file is re-processed.
    """
    if not deduplicate or not source_url:
        return False

    existing = await db.execute(
        select(FileSubmission)
        .where(
            FileSubmission.project_id == project_id,
            FileSubmission.source_url == source_url,
        )
        .order_by(FileSubmission.submitted_at.desc())
        .limit(1)
    )
    record = existing.scalars().first()
    if not record:
        return False  # Never seen this URL → always process

    # ETag comparison (strip quotes for safety — servers vary in quoting style)
    if etag and record.http_etag:
        matched = etag.strip('"') == record.http_etag.strip('"')
        logger.debug(
            "Dedup via ETag | url=%s | current=%s | stored=%s | matched=%s",
            source_url, etag, record.http_etag, matched,
        )
        return matched

    # Last-Modified comparison
    if last_modified and record.http_last_modified:
        matched = last_modified == record.http_last_modified
        logger.debug(
            "Dedup via Last-Modified | url=%s | current=%s | stored=%s | matched=%s",
            source_url, last_modified, record.http_last_modified, matched,
        )
        return matched

    # Hash fallback — only reliable for full downloads or stable partial size
    if file_hash and record.file_hash_sha256:
        matched = file_hash == record.file_hash_sha256
        logger.debug(
            "Dedup via hash (fallback) | url=%s | matched=%s",
            source_url, matched,
        )
        return matched

    return False  # Cannot determine → process to be safe


async def process_downloaded_files(
    downloaded_files: list[str],
    source_urls: dict[str, str],
    project_id: int,
    task_id: int,
    retain_files: bool,
    pdf_mode: bool | None,
    response_headers: dict[str, dict] | None = None,
    deduplicate: bool = True,
    session_factory=None,
) -> tuple[int, int, int]:
    """
    Process all downloaded files. Returns (processed_count, error_count, skipped_duplicates).

    session_factory must be provided by Celery task callers (a per-task factory
    created via make_task_session_factory()) to avoid asyncpg event-loop mismatch
    errors across task executions in the same worker process.
    """
    from app.utils.helpers import sha256_file

    if session_factory is None:
        from app.database import AsyncSessionLocal
        session_factory = AsyncSessionLocal

    if response_headers is None:
        response_headers = {}

    logger.info(
        "Batch started | task_id=%d | project_id=%d | files=%d | "
        "deduplicate=%s | retain=%s",
        task_id, project_id, len(downloaded_files), deduplicate, retain_files,
    )

    processed = 0
    errors = 0
    skipped_duplicates = 0

    for i, file_path in enumerate(downloaded_files, 1):
        source_url = source_urls.get(file_path)
        headers = response_headers.get(file_path, {})
        etag = headers.get("etag")
        last_modified = headers.get("last_modified")
        filename = Path(file_path).name
        file_exists = Path(file_path).exists()
        file_hash = sha256_file(file_path) if file_exists else None

        logger.info(
            "File %d/%d | file=%s | source_url=%s | exists=%s | "
            "etag=%s | last_modified=%s",
            i, len(downloaded_files), filename, source_url, file_exists,
            etag or "<none>", last_modified or "<none>",
        )

        if not file_exists:
            logger.warning(
                "File missing on disk, skipping | file=%s | source_url=%s",
                filename, source_url,
            )
            errors += 1
            continue

        # Check for duplicates before processing
        async with session_factory() as db:
            should_skip = await _should_skip_crawl_file(
                db, project_id, source_url, file_hash, etag, last_modified, deduplicate
            )
            if should_skip:
                logger.info(
                    "Skipped (unchanged) | file=%s | source_url=%s",
                    filename, source_url,
                )
                skipped_duplicates += 1
                if not retain_files:
                    delete_file_safe(file_path)
                continue

        logger.info(
            "Extracting metadata | file=%s | source_url=%s",
            filename, source_url,
        )

        try:
            async with session_factory() as db:
                result = await process_single_file(
                    db=db,
                    project_id=project_id,
                    file_path=file_path,
                    retain_file_opt=retain_files,
                    pdf_mode=pdf_mode,
                    task_id=task_id,
                    submission_mode="crawl",
                    source_url=source_url,
                    http_etag=etag,
                    http_last_modified=last_modified,
                )
                await db.commit()
            logger.info(
                "Metadata saved | file=%s | submission_id=%s | records=%d | source_url=%s",
                filename,
                result.get("submission_id"),
                result.get("records_created", 0),
                source_url,
            )
            processed += 1
        except Exception as e:
            logger.error(
                "Metadata extraction failed | file=%s | source_url=%s | error=%s",
                filename, source_url, e,
                exc_info=True,
            )
            errors += 1
        finally:
            if not retain_files:
                delete_file_safe(file_path)

    logger.info(
        "Batch complete | task_id=%d | project_id=%d | "
        "processed=%d | skipped=%d | errors=%d",
        task_id, project_id, processed, skipped_duplicates, errors,
    )

    return processed, errors, skipped_duplicates
