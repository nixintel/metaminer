"""
Telegram scraper API.

Endpoints:
  Immediate scrapes:  POST/GET/DELETE /telegram/scrape[/{task_id}]
  Credentials:        GET/POST/DELETE  /telegram/credentials
  Status:             GET              /telegram/status
  Auth:               POST             /telegram/auth/start|verify
  Scheduled scrapes:  POST/GET/PATCH/DELETE /telegram/scheduled[/{id}]
"""
import json
import logging
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.database import get_db
from app.models.task import Task
from app.models.project import Project
from app.models.telegram_credentials import TelegramCredentials
from app.models.scheduled_telegram_scrape import ScheduledTelegramScrape
from app.schemas.task import TaskResponse
from app.schemas.telegram import (
    TelegramScrapeSubmit,
    TelegramCredentialsCreate,
    TelegramCredentialsResponse,
    TelegramAuthStart,
    TelegramAuthVerify,
    ScheduledTelegramScrapeCreate,
    ScheduledTelegramScrapeUpdate,
    ScheduledTelegramScrapeResponse,
)
from app.services.telegram_service import (
    get_credentials,
    check_session,
    make_client,
    start_auth,
    verify_auth,
)
from config import settings

router = APIRouter(prefix="/telegram", tags=["telegram"])
logger = logging.getLogger("metaminer.telegram_router")


# ---------------------------------------------------------------------------
# Immediate scrape tasks
# ---------------------------------------------------------------------------

@router.post("/scrape", response_model=TaskResponse, status_code=202)
async def submit_telegram_scrape(
    body: TelegramScrapeSubmit,
    db: AsyncSession = Depends(get_db),
):
    """Queue an immediate Telegram channel scrape task."""
    project = await db.get(Project, body.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    creds = await get_credentials(db)
    if not creds:
        raise HTTPException(
            status_code=400,
            detail="No Telegram credentials configured. Set TELEGRAM_API_ID/TELEGRAM_API_HASH "
                   "in .env or POST to /telegram/credentials.",
        )
    if not check_session():
        raise HTTPException(
            status_code=400,
            detail=f"Telegram session file not found at {settings.TELEGRAM_SESSION_PATH}. "
                   "Authenticate via POST /telegram/auth/start then /auth/verify.",
        )

    now = datetime.now(timezone.utc)
    date_from = body.date_from or (now - timedelta(days=settings.TELEGRAM_DATE_RANGE_DAYS))
    date_to = body.date_to or now

    from app.workers.telegram_tasks import run_telegram_task

    task = Task(
        project_id=body.project_id,
        task_type="telegram",
        config_json=json.dumps({
            "channel": body.channel,
            "allowed_file_types": body.allowed_file_types,
            "max_file_size_mb": body.max_file_size_mb,
            "max_files": body.max_files,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
            "retain_files": body.retain_files,
            "deduplicate": body.deduplicate,
            "pdf_mode": body.pdf_mode,
        }),
    )
    db.add(task)
    await db.flush()
    await db.refresh(task)

    celery_result = run_telegram_task.delay(
        task.id,
        body.project_id,
        body.channel,
        body.allowed_file_types,
        body.max_file_size_mb,
        body.max_files,
        date_from.isoformat(),
        date_to.isoformat(),
        body.retain_files,
        body.deduplicate,
        body.pdf_mode,
    )
    task.celery_task_id = celery_result.id
    await db.flush()
    return task


@router.get("/scrape", response_model=list[TaskResponse])
async def list_telegram_scrapes(
    project_id: int | None = None,
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """List telegram scrape tasks, optionally filtered by project and status."""
    q = select(Task).where(Task.task_type == "telegram").order_by(Task.created_at.desc())
    if project_id is not None:
        q = q.where(Task.project_id == project_id)
    if status is not None:
        q = q.where(Task.status == status)
    result = await db.execute(q)
    return result.scalars().all()


@router.get("/scrape/{task_id}", response_model=TaskResponse)
async def get_telegram_scrape(task_id: int, db: AsyncSession = Depends(get_db)):
    """Get a single telegram scrape task."""
    task = await db.get(Task, task_id)
    if not task or task.task_type != "telegram":
        raise HTTPException(status_code=404, detail="Telegram task not found")
    return task


@router.delete("/scrape/{task_id}", status_code=204)
async def cancel_telegram_scrape(task_id: int, db: AsyncSession = Depends(get_db)):
    """Cancel a running or pending telegram scrape task."""
    from app.workers.celery_app import celery_app
    from app.utils.cancel import async_set_cancel_flag

    task = await db.get(Task, task_id)
    if not task or task.task_type != "telegram":
        raise HTTPException(status_code=404, detail="Telegram task not found")
    if task.status in ("completed", "failed", "cancelled"):
        raise HTTPException(status_code=409, detail=f"Task is already {task.status}")

    await async_set_cancel_flag(task_id)

    if task.celery_task_id and task.status == "pending":
        celery_app.control.revoke(task.celery_task_id)

    task.status = "cancelled"
    task.completed_at = datetime.now(timezone.utc)
    await db.flush()


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@router.get("/status")
async def telegram_status(db: AsyncSession = Depends(get_db)):
    """Return whether credentials and session file are configured."""
    creds = await get_credentials(db)
    return {
        "credentials_ok": creds is not None,
        "session_ok": check_session(),
        "session_path": str(settings.TELEGRAM_SESSION_PATH),
    }


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

@router.post("/credentials", response_model=TelegramCredentialsResponse, status_code=201)
async def upsert_credentials(
    body: TelegramCredentialsCreate,
    db: AsyncSession = Depends(get_db),
):
    """Create or replace the stored Telegram API credentials."""
    result = await db.execute(select(TelegramCredentials).limit(1))
    cred = result.scalar_one_or_none()
    if cred:
        cred.api_id = body.api_id
        cred.api_hash = body.api_hash
    else:
        cred = TelegramCredentials(api_id=body.api_id, api_hash=body.api_hash)
        db.add(cred)
    await db.flush()
    await db.refresh(cred)
    return cred


@router.delete("/credentials", status_code=204)
async def delete_credentials(db: AsyncSession = Depends(get_db)):
    """Remove stored Telegram API credentials from the database."""
    result = await db.execute(select(TelegramCredentials))
    for cred in result.scalars().all():
        await db.delete(cred)
    await db.flush()


# ---------------------------------------------------------------------------
# Auth (first-time session creation)
# ---------------------------------------------------------------------------

@router.post("/auth/start")
async def auth_start(body: TelegramAuthStart, db: AsyncSession = Depends(get_db)):
    """
    Begin phone-number authentication. Sends a login code to the Telegram app.
    Requires credentials to already be configured.
    """
    creds = await get_credentials(db)
    if not creds:
        raise HTTPException(status_code=400, detail="No Telegram credentials configured.")

    api_id, api_hash = creds
    settings.TELEGRAM_SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
    client = make_client(api_id, api_hash)
    async with client:
        await start_auth(client, body.phone)

    return {"status": "code_sent", "message": "Check your Telegram app for the login code."}


@router.post("/auth/verify")
async def auth_verify(body: TelegramAuthVerify, db: AsyncSession = Depends(get_db)):
    """
    Complete phone-number authentication using the code sent to the Telegram app.
    Writes the session file to disk. Supports 2FA via the optional password field.
    """
    creds = await get_credentials(db)
    if not creds:
        raise HTTPException(status_code=400, detail="No Telegram credentials configured.")

    api_id, api_hash = creds
    client = make_client(api_id, api_hash)
    try:
        async with client:
            await verify_auth(client, body.phone, body.code, body.password)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "status": "authenticated",
        "session_path": str(settings.TELEGRAM_SESSION_PATH),
    }


# ---------------------------------------------------------------------------
# Scheduled scrapes
# ---------------------------------------------------------------------------

@router.post("/scheduled", response_model=ScheduledTelegramScrapeResponse, status_code=201)
async def create_scheduled_scrape(
    body: ScheduledTelegramScrapeCreate,
    db: AsyncSession = Depends(get_db),
):
    project = await db.get(Project, body.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    schedule = ScheduledTelegramScrape(
        project_id=body.project_id,
        channel=body.channel,
        frequency_seconds=body.frequency_seconds,
        allowed_file_types=json.dumps(body.allowed_file_types) if body.allowed_file_types else None,
        max_file_size_mb=body.max_file_size_mb,
        max_files=body.max_files,
        date_range_days=body.date_range_days,
        pdf_mode=body.pdf_mode,
        retain_files=body.retain_files,
        deduplicate=body.deduplicate,
        next_run_at=datetime.now(timezone.utc),
    )
    db.add(schedule)
    await db.flush()
    await db.refresh(schedule)
    return ScheduledTelegramScrapeResponse.model_validate(schedule)


@router.get("/scheduled", response_model=list[ScheduledTelegramScrapeResponse])
async def list_scheduled_scrapes(
    project_id: int | None = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(ScheduledTelegramScrape).order_by(ScheduledTelegramScrape.created_at.desc())
    if project_id is not None:
        q = q.where(ScheduledTelegramScrape.project_id == project_id)
    result = await db.execute(q)
    return [ScheduledTelegramScrapeResponse.model_validate(s) for s in result.scalars().all()]


@router.get("/scheduled/{schedule_id}", response_model=ScheduledTelegramScrapeResponse)
async def get_scheduled_scrape(schedule_id: int, db: AsyncSession = Depends(get_db)):
    schedule = await db.get(ScheduledTelegramScrape, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Scheduled scrape not found")
    return ScheduledTelegramScrapeResponse.model_validate(schedule)


@router.patch("/scheduled/{schedule_id}", response_model=ScheduledTelegramScrapeResponse)
async def update_scheduled_scrape(
    schedule_id: int,
    body: ScheduledTelegramScrapeUpdate,
    db: AsyncSession = Depends(get_db),
):
    schedule = await db.get(ScheduledTelegramScrape, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Scheduled scrape not found")

    for field, value in body.model_dump(exclude_unset=True).items():
        if field == "allowed_file_types":
            setattr(schedule, field, json.dumps(value) if value is not None else None)
        else:
            setattr(schedule, field, value)

    if body.frequency_seconds is not None:
        base = schedule.last_run_at or datetime.now(timezone.utc)
        schedule.next_run_at = base + timedelta(seconds=schedule.frequency_seconds)

    await db.flush()
    await db.refresh(schedule)
    return ScheduledTelegramScrapeResponse.model_validate(schedule)


@router.delete("/scheduled/{schedule_id}", status_code=204)
async def delete_scheduled_scrape(schedule_id: int, db: AsyncSession = Depends(get_db)):
    schedule = await db.get(ScheduledTelegramScrape, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Scheduled scrape not found")
    await db.delete(schedule)
