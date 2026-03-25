from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.schemas.submission import SingleFileSubmit, BulkSubmit, SubmissionResponse
from app.schemas.task import TaskResponse
from app.models.project import Project
from app.models.task import Task
from app.services.metadata_service import process_single_file
import json

router = APIRouter(prefix="/submissions", tags=["submissions"])


@router.post("/single", response_model=SubmissionResponse, status_code=201)
async def submit_single_file(body: SingleFileSubmit, db: AsyncSession = Depends(get_db)):
    project = await db.get(Project, body.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    result = await process_single_file(
        db=db,
        project_id=body.project_id,
        file_path=body.file_path,
        retain_file_opt=body.retain_file,
        pdf_mode=body.pdf_mode,
    )
    return result


@router.post("/bulk", response_model=TaskResponse, status_code=202)
async def submit_bulk(body: BulkSubmit, db: AsyncSession = Depends(get_db)):
    from app.workers.bulk_tasks import run_bulk_task

    project = await db.get(Project, body.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    task = Task(
        project_id=body.project_id,
        task_type="bulk",
        config_json=json.dumps({
            "paths": body.paths,
            "retain_files": body.retain_files,
            "pdf_mode": body.pdf_mode,
        }),
    )
    db.add(task)
    await db.flush()
    await db.refresh(task)

    celery_result = run_bulk_task.delay(task.id, body.paths, body.project_id, body.retain_files, body.pdf_mode)
    task.celery_task_id = celery_result.id
    await db.flush()

    return task
