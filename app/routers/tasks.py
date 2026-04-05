from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.database import get_db
from app.models.task import Task
from app.schemas.task import TaskResponse

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.get("/summary", response_model=dict)
async def task_summary(
    project_id: int | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Get aggregated task counts by status for a project or all projects."""
    q = select(Task.status, func.count(Task.id).label("count"))
    
    if project_id is not None:
        q = q.where(Task.project_id == project_id)
    
    q = q.group_by(Task.status)
    result = await db.execute(q)
    
    # Initialize counters with defaults
    summary = {
        "pending": 0,
        "running": 0,
        "completed": 0,
        "failed": 0,
        "cancelled": 0,
    }
    
    # Populate from query results
    for status, count in result:
        if status in summary:
            summary[status] = count
    
    return summary


@router.get("", response_model=list[TaskResponse])
async def list_tasks(
    project_id: int | None = None,
    status: str | None = None,
    task_type: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(Task).order_by(Task.created_at.desc())
    if project_id is not None:
        q = q.where(Task.project_id == project_id)
    if status is not None:
        q = q.where(Task.status == status)
    if task_type is not None:
        q = q.where(Task.task_type == task_type)
    result = await db.execute(q)
    return result.scalars().all()


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(task_id: int, db: AsyncSession = Depends(get_db)):
    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.delete("/{task_id}", status_code=204)
async def cancel_task(task_id: int, db: AsyncSession = Depends(get_db)):
    from app.workers.celery_app import celery_app
    from app.utils.cancel import async_set_cancel_flag

    task = await db.get(Task, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    if task.status in ("completed", "failed", "cancelled"):
        raise HTTPException(status_code=409, detail=f"Task is already {task.status}")

    # Set Redis flag — running workers poll this and stop cleanly
    await async_set_cancel_flag(task_id)

    # For pending tasks, also revoke from the Celery queue (no terminate —
    # that would kill the entire solo worker process)
    if task.celery_task_id and task.status == "pending":
        celery_app.control.revoke(task.celery_task_id)

    task.status = "cancelled"
    task.completed_at = datetime.now(timezone.utc)
    await db.flush()
