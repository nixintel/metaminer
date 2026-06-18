from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated
from datetime import datetime
from app.database import get_db
from app.services.query_service import (
    query_metadata,
    query_metadata_tree,
    get_metadata_by_id,
    set_metadata_interesting,
)
from app.schemas.metadata import QueryRequest, MetadataRecordUpdate

router = APIRouter(prefix="/metadata", tags=["metadata"])


@router.get("")
async def search_metadata(
    project_id: Annotated[int | None, Query()] = None,
    task_id: Annotated[int | None, Query()] = None,
    file_type: Annotated[str | None, Query()] = None,
    file_type__in: Annotated[str | None, Query(description="Comma-separated list, e.g. PDF,DOCX")] = None,
    author: Annotated[str | None, Query()] = None,
    title: Annotated[str | None, Query()] = None,
    creator_tool: Annotated[str | None, Query()] = None,
    producer: Annotated[str | None, Query()] = None,
    mime_type: Annotated[str | None, Query()] = None,
    pdf_variant: Annotated[str | None, Query(description="original or rollback")] = None,
    submission_mode: Annotated[str | None, Query(description="manual, crawl, or telegram")] = None,
    interesting: Annotated[bool | None, Query(description="If true, only entries marked Interesting")] = None,
    source_url__contains: Annotated[str | None, Query()] = None,
    extracted_after: Annotated[datetime | None, Query()] = None,
    extracted_before: Annotated[datetime | None, Query()] = None,
    q: Annotated[str | None, Query(description="Full-text search across author, title, creator_tool, producer, file_name")] = None,
    raw_contains: Annotated[str | None, Query(description="Search inside raw exiftool JSON")] = None,
    sort_by: Annotated[str, Query()] = "extracted_at",
    order: Annotated[str, Query(description="asc or desc")] = "desc",
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    db: AsyncSession = Depends(get_db),
):
    params = dict(
        project_id=project_id,
        task_id=task_id,
        file_type=file_type,
        file_type__in=file_type__in,
        author=author,
        title=title,
        creator_tool=creator_tool,
        producer=producer,
        mime_type=mime_type,
        pdf_variant=pdf_variant,
        submission_mode=submission_mode,
        interesting=interesting,
        source_url__contains=source_url__contains,
        extracted_after=extracted_after,
        extracted_before=extracted_before,
        q=q,
        raw_contains=raw_contains,
        sort_by=sort_by,
        order=order,
        limit=limit,
        offset=offset,
    )
    return await query_metadata(db, params)


@router.post("/query")
async def query_metadata_post(request: QueryRequest, db: AsyncSession = Depends(get_db)):
    return await query_metadata_tree(db, request.model_dump())


@router.get("/{metadata_id}")
async def get_metadata(metadata_id: int, db: AsyncSession = Depends(get_db)):
    record = await get_metadata_by_id(db, metadata_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Metadata record {metadata_id} not found")
    return record


@router.patch("/{metadata_id}")
async def update_metadata(
    metadata_id: int, body: MetadataRecordUpdate, db: AsyncSession = Depends(get_db)
):
    data = body.model_dump(exclude_unset=True)
    if "interesting" in data:
        record = await set_metadata_interesting(db, metadata_id, data["interesting"])
    else:
        record = await get_metadata_by_id(db, metadata_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Metadata record {metadata_id} not found")
    return record
