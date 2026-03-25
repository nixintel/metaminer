from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db
from app.schemas.crawl import CrawlSubmit
from app.schemas.task import TaskResponse
from app.models.project import Project
from app.models.task import Task
import json

router = APIRouter(prefix="/crawl", tags=["crawl"])


@router.post("", response_model=TaskResponse, status_code=202)
async def submit_crawl(body: CrawlSubmit, db: AsyncSession = Depends(get_db)):
    from app.workers.crawl_tasks import run_crawl_task

    project = await db.get(Project, body.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    task = Task(
        project_id=body.project_id,
        task_type="crawl",
        config_json=json.dumps({
            "url": body.url,
            "depth_limit": body.depth_limit,
            "allowed_file_types": body.allowed_file_types,
            "full_download": body.full_download,
            "retain_files": body.retain_files,
            "deduplicate": body.deduplicate,
            "robotstxt_obey": body.robotstxt_obey,
            "crawl_images": body.crawl_images,
        }),
    )
    db.add(task)
    await db.flush()
    await db.refresh(task)

    celery_result = run_crawl_task.delay(
        task.id,
        body.project_id,
        body.url,
        body.depth_limit,
        body.allowed_file_types,
        body.full_download,
        body.retain_files,
        body.deduplicate,
        body.robotstxt_obey,
        body.crawl_images,
    )
    task.celery_task_id = celery_result.id
    await db.flush()

    return task
