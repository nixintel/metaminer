import json
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.database import get_db
from app.models.scheduled_crawl import ScheduledCrawl
from app.models.project import Project
from app.schemas.scheduled_crawl import (
    ScheduledCrawlCreate,
    ScheduledCrawlUpdate,
    ScheduledCrawlResponse,
)

router = APIRouter(prefix="/scheduled-crawls", tags=["scheduled-crawls"])


@router.post("", response_model=ScheduledCrawlResponse, status_code=201)
async def create_scheduled_crawl(
    body: ScheduledCrawlCreate,
    db: AsyncSession = Depends(get_db),
):
    project = await db.get(Project, body.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    schedule = ScheduledCrawl(
        project_id=body.project_id,
        url=body.url,
        frequency_seconds=body.frequency_seconds,
        depth_limit=body.depth_limit,
        allowed_file_types=json.dumps(body.allowed_file_types) if body.allowed_file_types else None,
        full_download=body.full_download,
        retain_files=body.retain_files,
        crawl_images=body.crawl_images,
        robotstxt_obey=body.robotstxt_obey,
        allow_cross_domain=body.allow_cross_domain,
        # next_run_at defaults to now, so it is picked up on the next dispatch tick
        next_run_at=datetime.now(timezone.utc),
    )
    db.add(schedule)
    await db.flush()
    await db.refresh(schedule)
    return ScheduledCrawlResponse.model_validate(schedule)


@router.get("", response_model=list[ScheduledCrawlResponse])
async def list_scheduled_crawls(
    project_id: int | None = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(ScheduledCrawl).order_by(ScheduledCrawl.created_at.desc())
    if project_id is not None:
        q = q.where(ScheduledCrawl.project_id == project_id)
    result = await db.execute(q)
    return [ScheduledCrawlResponse.model_validate(s) for s in result.scalars().all()]


@router.get("/{schedule_id}", response_model=ScheduledCrawlResponse)
async def get_scheduled_crawl(schedule_id: int, db: AsyncSession = Depends(get_db)):
    schedule = await db.get(ScheduledCrawl, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Scheduled crawl not found")
    return ScheduledCrawlResponse.model_validate(schedule)


@router.patch("/{schedule_id}", response_model=ScheduledCrawlResponse)
async def update_scheduled_crawl(
    schedule_id: int,
    body: ScheduledCrawlUpdate,
    db: AsyncSession = Depends(get_db),
):
    schedule = await db.get(ScheduledCrawl, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Scheduled crawl not found")

    for field, value in body.model_dump(exclude_unset=True).items():
        if field == "allowed_file_types":
            setattr(schedule, field, json.dumps(value) if value is not None else None)
        else:
            setattr(schedule, field, value)

    # If frequency changed, recalculate next_run_at from last_run_at (or now)
    if body.frequency_seconds is not None:
        base = schedule.last_run_at or datetime.now(timezone.utc)
        schedule.next_run_at = base + timedelta(seconds=schedule.frequency_seconds)

    await db.flush()
    await db.refresh(schedule)
    return ScheduledCrawlResponse.model_validate(schedule)


@router.delete("/{schedule_id}", status_code=204)
async def delete_scheduled_crawl(schedule_id: int, db: AsyncSession = Depends(get_db)):
    schedule = await db.get(ScheduledCrawl, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Scheduled crawl not found")
    await db.delete(schedule)
