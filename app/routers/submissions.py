from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.schemas.submission import ManualSubmit
from app.schemas.task import TaskResponse
from app.models.project import Project
from app.models.task import Task
import json

router = APIRouter(prefix="/submissions", tags=["submissions"])


@router.post("/manual", response_model=TaskResponse, status_code=202)
async def submit_manual(body: ManualSubmit, db: AsyncSession = Depends(get_db)):
    from app.workers.manual_tasks import run_manual_task

    project = await db.get(Project, body.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    task = Task(
        project_id=body.project_id,
        task_type="manual",
        config_json=json.dumps({
            "paths": body.paths,
            "retain_files": body.retain_files,
            "pdf_mode": body.pdf_mode,
        }),
    )
    db.add(task)
    await db.flush()
    await db.refresh(task)

    celery_result = run_manual_task.delay(task.id, body.paths, body.project_id, body.retain_files, body.pdf_mode)
    task.celery_task_id = celery_result.id
    await db.flush()

    return task
