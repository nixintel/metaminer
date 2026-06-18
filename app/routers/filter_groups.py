import json
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_

from app.database import get_db
from app.models.filter_group import FilterGroup
from app.models.filter_criteria import FilterCriteria
from app.models.project import Project
from app.models.task import Task
from app.schemas.filter_group import (
    FilterGroupCreate,
    FilterGroupUpdate,
    FilterGroupResponse,
    FilterGroupBackfillRequest,
)

router = APIRouter(prefix="/filter-groups", tags=["filter-groups"])


async def _check_project(db: AsyncSession, project_id: int | None):
    if project_id is not None and not await db.get(Project, project_id):
        raise HTTPException(status_code=404, detail="Project not found")


async def _resolve_members(db: AsyncSession, group_project_id: int | None,
                           filter_ids: list[int]) -> list[FilterCriteria]:
    """Fetch member FilterCriteria and enforce scope-eligibility.

    A project group may contain that project's filters + globals; a global group may
    contain only globals. Raises 404 for unknown ids, 422 for scope violations.
    """
    if not filter_ids:
        return []
    rows = (await db.execute(
        select(FilterCriteria).where(FilterCriteria.id.in_(filter_ids))
    )).scalars().all()
    found = {f.id for f in rows}
    missing = [fid for fid in filter_ids if fid not in found]
    if missing:
        raise HTTPException(status_code=404, detail=f"Unknown filter id(s): {missing}")
    for f in rows:
        if group_project_id is None:
            # Global group: only global filters are eligible.
            if f.project_id is not None:
                raise HTTPException(
                    status_code=422,
                    detail=f"Filter #{f.id} is project-scoped and cannot be added to a global group",
                )
        else:
            # Project group: that project's filters + globals.
            if f.project_id is not None and f.project_id != group_project_id:
                raise HTTPException(
                    status_code=422,
                    detail=f"Filter #{f.id} belongs to a different project and is not eligible",
                )
    return rows


async def _get_group_with_filters(db: AsyncSession, group_id: int) -> FilterGroup | None:
    # select() triggers the selectin load of .filters (async-safe).
    return (await db.execute(
        select(FilterGroup).where(FilterGroup.id == group_id)
    )).scalars().first()


@router.post("", response_model=FilterGroupResponse, status_code=201)
async def create_filter_group(body: FilterGroupCreate, db: AsyncSession = Depends(get_db)):
    await _check_project(db, body.project_id)
    members = await _resolve_members(db, body.project_id, body.filter_ids)
    group = FilterGroup(name=body.name, project_id=body.project_id, is_active=body.is_active)
    group.filters = members
    db.add(group)
    await db.flush()
    await db.refresh(group)
    return FilterGroupResponse.model_validate(group)


@router.get("", response_model=list[FilterGroupResponse])
async def list_filter_groups(project_id: int | None = None, db: AsyncSession = Depends(get_db)):
    q = select(FilterGroup).order_by(FilterGroup.created_at.desc())
    if project_id is not None:
        # That project's groups PLUS globals.
        q = q.where(or_(FilterGroup.project_id == project_id, FilterGroup.project_id.is_(None)))
    result = await db.execute(q)
    return [FilterGroupResponse.model_validate(g) for g in result.scalars().all()]


@router.get("/{group_id}", response_model=FilterGroupResponse)
async def get_filter_group(group_id: int, db: AsyncSession = Depends(get_db)):
    group = await _get_group_with_filters(db, group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Filter group not found")
    return FilterGroupResponse.model_validate(group)


@router.patch("/{group_id}", response_model=FilterGroupResponse)
async def update_filter_group(
    group_id: int, body: FilterGroupUpdate, db: AsyncSession = Depends(get_db)
):
    group = await _get_group_with_filters(db, group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Filter group not found")

    data = body.model_dump(exclude_unset=True)
    if "name" in data:
        group.name = data["name"]
    if "is_active" in data:
        group.is_active = data["is_active"]
    if "filter_ids" in data and data["filter_ids"] is not None:
        # [] clears membership; a list replaces it (scope-validated against the group's project).
        group.filters = await _resolve_members(db, group.project_id, data["filter_ids"])

    await db.flush()
    await db.refresh(group)
    return FilterGroupResponse.model_validate(group)


@router.delete("/{group_id}", status_code=204)
async def delete_filter_group(group_id: int, db: AsyncSession = Depends(get_db)):
    group = await db.get(FilterGroup, group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Filter group not found")
    await db.delete(group)


@router.post("/{group_id}/backfill", status_code=202)
async def backfill_filter_group(
    group_id: int, body: FilterGroupBackfillRequest, db: AsyncSession = Depends(get_db)
):
    """Apply this group's member filters to already-ingested metadata."""
    from app.workers.filter_tasks import run_filter_backfill

    group = await _get_group_with_filters(db, group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Filter group not found")
    await _check_project(db, body.project_id)

    member_ids = [f.id for f in group.filters]
    if not member_ids:
        # Refuse — passing [] would make the worker fall through to "all active filters".
        raise HTTPException(status_code=400, detail="Group has no member filters; nothing to backfill")

    scope_project = body.project_id if body.project_id is not None else group.project_id
    task = Task(
        project_id=scope_project,
        task_type="filter_backfill",
        config_json=json.dumps({"group_id": group_id, "filter_ids": member_ids,
                                "scope_project_id": scope_project}),
    )
    db.add(task)
    await db.flush()
    await db.refresh(task)

    result = run_filter_backfill.delay(task.id, member_ids, scope_project)
    task.celery_task_id = result.id
    await db.flush()
    return {"task_id": task.id, "celery_task_id": result.id, "scope_project_id": scope_project}
