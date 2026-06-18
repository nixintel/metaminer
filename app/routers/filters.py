import json
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_

from app.database import get_db
from app.models.filter_criteria import FilterCriteria
from app.models.project import Project
from app.models.task import Task
from app.schemas.filter_criteria import (
    FilterCriteriaCreate,
    FilterCriteriaUpdate,
    FilterCriteriaResponse,
    FilterBackfillRequest,
)
from app.services.filter_service import validate_filter, FilterValidationError

router = APIRouter(prefix="/filters", tags=["filters"])


async def _check_project(db: AsyncSession, project_id: int | None):
    if project_id is not None and not await db.get(Project, project_id):
        raise HTTPException(status_code=404, detail="Project not found")


@router.post("", response_model=FilterCriteriaResponse, status_code=201)
async def create_filter(body: FilterCriteriaCreate, db: AsyncSession = Depends(get_db)):
    await _check_project(db, body.project_id)
    flt = FilterCriteria(
        name=body.name,
        filter_type=body.filter_type,
        value=body.value,  # already validated/normalized by the schema
        project_id=body.project_id,
        is_active=body.is_active,
    )
    db.add(flt)
    await db.flush()
    await db.refresh(flt)
    return FilterCriteriaResponse.model_validate(flt)


@router.get("", response_model=list[FilterCriteriaResponse])
async def list_filters(project_id: int | None = None, db: AsyncSession = Depends(get_db)):
    q = select(FilterCriteria).order_by(FilterCriteria.created_at.desc())
    if project_id is not None:
        # That project's filters PLUS globals.
        q = q.where(or_(FilterCriteria.project_id == project_id, FilterCriteria.project_id.is_(None)))
    result = await db.execute(q)
    return [FilterCriteriaResponse.model_validate(f) for f in result.scalars().all()]


@router.get("/{filter_id}", response_model=FilterCriteriaResponse)
async def get_filter(filter_id: int, db: AsyncSession = Depends(get_db)):
    flt = await db.get(FilterCriteria, filter_id)
    if not flt:
        raise HTTPException(status_code=404, detail="Filter not found")
    return FilterCriteriaResponse.model_validate(flt)


@router.patch("/{filter_id}", response_model=FilterCriteriaResponse)
async def update_filter(
    filter_id: int, body: FilterCriteriaUpdate, db: AsyncSession = Depends(get_db)
):
    flt = await db.get(FilterCriteria, filter_id)
    if not flt:
        raise HTTPException(status_code=404, detail="Filter not found")

    data = body.model_dump(exclude_unset=True)
    if "project_id" in data:
        await _check_project(db, data["project_id"])

    # Re-validate value against the effective type if either changed.
    if "value" in data or "filter_type" in data:
        eff_type = data.get("filter_type", flt.filter_type)
        eff_value = data.get("value", flt.value)
        try:
            data["value"] = validate_filter(eff_type, eff_value)
        except FilterValidationError as e:
            raise HTTPException(status_code=422, detail=str(e))

    for field, value in data.items():
        setattr(flt, field, value)

    await db.flush()
    await db.refresh(flt)
    return FilterCriteriaResponse.model_validate(flt)


@router.delete("/{filter_id}", status_code=204)
async def delete_filter(filter_id: int, db: AsyncSession = Depends(get_db)):
    flt = await db.get(FilterCriteria, filter_id)
    if not flt:
        raise HTTPException(status_code=404, detail="Filter not found")
    await db.delete(flt)


@router.post("/{filter_id}/backfill", status_code=202)
async def backfill_filter(
    filter_id: int, body: FilterBackfillRequest, db: AsyncSession = Depends(get_db)
):
    """Apply this filter to already-ingested metadata. body.project_id = null means whole DB."""
    from app.workers.filter_tasks import run_filter_backfill

    flt = await db.get(FilterCriteria, filter_id)
    if not flt:
        raise HTTPException(status_code=404, detail="Filter not found")
    await _check_project(db, body.project_id)

    # Effective scan scope: explicit request scope, else the filter's own project (None = whole DB).
    scope_project = body.project_id if body.project_id is not None else flt.project_id

    task = Task(
        project_id=scope_project,  # may be None for a whole-DB backfill of a global filter
        task_type="filter_backfill",
        config_json=json.dumps({"filter_id": filter_id, "scope_project_id": scope_project}),
    )
    db.add(task)
    await db.flush()
    await db.refresh(task)

    result = run_filter_backfill.delay(task.id, [filter_id], scope_project)
    task.celery_task_id = result.id
    await db.flush()
    return {"task_id": task.id, "celery_task_id": result.id, "scope_project_id": scope_project}
