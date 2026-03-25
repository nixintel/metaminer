from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Annotated
from datetime import datetime
from app.database import get_db
from app.models.log_entry import LogEntry
from app.schemas.log import LogEntryResponse

router = APIRouter(prefix="/logs", tags=["logs"])


@router.get("", response_model=list[LogEntryResponse])
async def get_logs(
    level: Annotated[str | None, Query(description="DEBUG, INFO, WARNING, ERROR")] = None,
    task_id: Annotated[int | None, Query()] = None,
    submission_id: Annotated[int | None, Query()] = None,
    since: Annotated[datetime | None, Query()] = None,
    until: Annotated[datetime | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    db: AsyncSession = Depends(get_db),
):
    q = select(LogEntry).order_by(LogEntry.created_at.desc())

    if level:
        q = q.where(LogEntry.level == level.upper())
    if task_id is not None:
        q = q.where(LogEntry.task_id == task_id)
    if submission_id is not None:
        q = q.where(LogEntry.submission_id == submission_id)
    if since:
        q = q.where(LogEntry.created_at >= since)
    if until:
        q = q.where(LogEntry.created_at <= until)

    q = q.offset(offset).limit(limit)
    result = await db.execute(q)
    return result.scalars().all()
