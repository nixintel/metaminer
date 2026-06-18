"""
On-demand backfill: apply auto-tagging filters to already-ingested metadata.

Triggered when a user creates/edits a filter and opts to check existing data, scoped to
a project or the whole database. Additive: only flips interesting False->True (the
`interesting = false` WHERE clause), so manual marks and prior auto-tags are preserved
and re-runs are idempotent/resumable.

The async core (`backfill_scan`) takes a session factory so it can be tested against a
test DB; the Celery task wraps it with make_task_session_factory + asyncio.run.
"""
import asyncio
import json
import logging
from datetime import datetime, timezone

from app.workers.celery_app import celery_app

logger = logging.getLogger("metaminer.filter_tasks")

_BATCH_SIZE = 500
# Backstop against an unbounded scan; if hit we stop and log loudly (no silent truncation).
_MAX_ROWS = 1_000_000


def _has_field_filter(fs) -> bool:
    return any(f.filter_type == "exif_field" for f in fs.filters)


def _parse(raw_json_str: str):
    try:
        return json.loads(raw_json_str)
    except Exception:
        return {}


async def backfill_scan(SessionLocal, task_id: int, filter_ids: list[int] | None,
                        project_id: int | None) -> tuple[int, int]:
    """Scan existing metadata and auto-tag matches. Returns (scanned, flagged).

    Updates the Task row to running/completed with progress counters. Keyset pagination
    (mr.id) is concurrency-safe under live ingestion; commits per batch keep locks short
    and make the run resumable.
    """
    from sqlalchemy import select
    from app.models.task import Task
    from app.models.metadata_record import MetadataRecord
    from app.models.file_submission import FileSubmission
    from app.models.filter_criteria import FilterCriteria
    from app.services.filter_service import compile_filters, FilterSet

    # Mark running (skip if the task was already finalized/cancelled).
    async with SessionLocal() as db:
        task = await db.get(Task, task_id)
        if not task or task.status in ("completed", "failed", "cancelled"):
            return (0, 0)
        task.status = "running"
        task.started_at = datetime.now(timezone.utc)
        await db.commit()

    # Load target filters once.
    async with SessionLocal() as db:
        stmt = select(FilterCriteria)
        if filter_ids:
            stmt = stmt.where(FilterCriteria.id.in_(filter_ids))
        else:
            stmt = stmt.where(FilterCriteria.is_active.is_(True))
        all_compiled = compile_filters((await db.execute(stmt)).scalars().all()).filters
    logger.info(
        "Filter backfill starting | task_id=%d | filters=%d | project_id=%s",
        task_id, len(all_compiled), project_id,
    )

    # Per-project FilterSet cache: a record in project P is matched only against filters
    # that are global (project_id is None) or scoped to P.
    _cache: dict[int | None, FilterSet] = {}

    def _filterset_for(pid):
        if pid not in _cache:
            _cache[pid] = FilterSet(
                [f for f in all_compiled if f.project_id is None or f.project_id == pid]
            )
        return _cache[pid]

    scanned = 0
    flagged = 0
    last_id = 0
    capped = False

    while True:
        async with SessionLocal() as db:
            q = (
                select(MetadataRecord, FileSubmission.source_url, FileSubmission.project_id)
                .join(FileSubmission, MetadataRecord.submission_id == FileSubmission.id)
                .where(MetadataRecord.interesting.is_(False))
                .where(MetadataRecord.id > last_id)
            )
            if project_id is not None:
                q = q.where(FileSubmission.project_id == project_id)
            q = q.order_by(MetadataRecord.id.asc()).limit(_BATCH_SIZE)
            rows = (await db.execute(q)).all()
            if not rows:
                break

            for record, source_url, rec_project_id in rows:
                fs = _filterset_for(rec_project_id)
                if fs:
                    # Only parse exif JSON when a field filter needs it.
                    exif = _parse(record.raw_json) if _has_field_filter(fs) else None
                    matched, reason = fs.evaluate(source_url, record.raw_json, exif)
                    if matched:
                        record.interesting = True
                        record.interesting_reason = reason
                        flagged += 1
                scanned += 1
            last_id = rows[-1][0].id
            await db.commit()

        async with SessionLocal() as db:
            t = await db.get(Task, task_id)
            if t:
                t.files_found = scanned
                t.files_processed = flagged
                await db.commit()
        logger.info(
            "Backfill progress | task_id=%d | scanned=%d | flagged=%d | last_id=%d",
            task_id, scanned, flagged, last_id,
        )

        if scanned >= _MAX_ROWS:
            capped = True
            logger.warning(
                "Backfill cap reached (%d rows) | task_id=%d | scanned=%d | flagged=%d | "
                "remaining rows NOT processed — re-run to continue",
                _MAX_ROWS, task_id, scanned, flagged,
            )
            break

    async with SessionLocal() as db:
        t = await db.get(Task, task_id)
        if t:
            t.status = "completed"
            t.files_found = scanned
            t.files_processed = flagged
            t.completed_at = datetime.now(timezone.utc)
            if capped:
                t.error_message = f"Stopped at safety cap of {_MAX_ROWS} rows; re-run to continue."
            await db.commit()
    logger.info(
        "Filter backfill complete | task_id=%d | scanned=%d | flagged=%d | capped=%s",
        task_id, scanned, flagged, capped,
    )
    return (scanned, flagged)


@celery_app.task(bind=True, name="metaminer.filter_backfill", queue="manual",
                 acks_late=True, max_retries=0)
def run_filter_backfill(self, task_id: int, filter_ids: list[int] | None = None,
                        project_id: int | None = None):
    """Celery entrypoint: run backfill_scan against the live DB."""
    async def _run():
        from app.database import make_task_session_factory
        task_engine, SessionLocal = make_task_session_factory()
        try:
            await backfill_scan(SessionLocal, task_id, filter_ids, project_id)
        finally:
            await task_engine.dispose()

    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error("Filter backfill %d failed: %s", task_id, e, exc_info=True)

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
