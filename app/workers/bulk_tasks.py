import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

from app.workers.celery_app import celery_app
from app.utils.cancel import check_cancel_flag, clear_cancel_flag

logger = logging.getLogger("metaminer.bulk_tasks")


def _get_all_files(paths: list[str]) -> list[Path]:
    files = []
    for p in paths:
        path = Path(p)
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(f for f in path.rglob("*") if f.is_file())
        else:
            logger.warning(f"Path not found or not accessible: {p}")
    return files


@celery_app.task(bind=True, name="metaminer.bulk_task", queue="bulk")
def run_bulk_task(
    self,
    task_id: int,
    paths: list[str],
    project_id: int,
    retain_files: bool = False,
    pdf_mode: bool | None = None,
):
    async def _run():
        from app.database import make_task_session_factory
        from app.models.task import Task
        from app.services.metadata_service import process_single_file

        task_engine, SessionLocal = make_task_session_factory()
        try:
            async with SessionLocal() as db:
                task = await db.get(Task, task_id)
                if not task:
                    logger.error(f"Task {task_id} not found")
                    return
                if task.status in ("completed", "failed", "cancelled"):
                    logger.warning(
                        "Bulk task %d already in terminal state (%s), discarding stale message",
                        task_id, task.status,
                    )
                    return

                task.status = "running"
                task.started_at = datetime.now(timezone.utc)
                await db.commit()

            all_files = _get_all_files(paths)

            async with SessionLocal() as db:
                task = await db.get(Task, task_id)
                task.files_found = len(all_files)
                await db.commit()

            processed = 0
            for file_path in all_files:
                if check_cancel_flag(task_id):
                    logger.info(
                        "Bulk task cancelled | task_id=%d | processed=%d/%d",
                        task_id, processed, len(all_files),
                    )
                    async with SessionLocal() as db:
                        task = await db.get(Task, task_id)
                        if task:
                            task.status = "cancelled"
                            task.files_processed = processed
                            task.completed_at = datetime.now(timezone.utc)
                            await db.commit()
                    clear_cancel_flag(task_id)
                    return

                try:
                    async with SessionLocal() as db:
                        await process_single_file(
                            db=db,
                            project_id=project_id,
                            file_path=str(file_path),
                            retain_file_opt=retain_files,
                            pdf_mode=pdf_mode,
                            task_id=task_id,
                            submission_mode="bulk",
                        )
                        await db.commit()
                    processed += 1
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {e}")

                # Update progress every 10 files
                if processed % 10 == 0:
                    async with SessionLocal() as db:
                        task = await db.get(Task, task_id)
                        if task:
                            task.files_processed = processed
                            await db.commit()

            async with SessionLocal() as db:
                task = await db.get(Task, task_id)
                if task:
                    task.status = "completed"
                    task.files_processed = processed
                    task.completed_at = datetime.now(timezone.utc)
                    await db.commit()

            logger.info(f"Bulk task {task_id} complete: {processed}/{len(all_files)} files processed")
        finally:
            clear_cancel_flag(task_id)
            await task_engine.dispose()

    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error(f"Bulk task {task_id} failed: {e}", exc_info=True)

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
