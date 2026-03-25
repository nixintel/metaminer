import json
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_
from sqlalchemy.orm import selectinload

from app.models.metadata_record import MetadataRecord
from app.models.file_submission import FileSubmission
from app.models.project import Project

logger = logging.getLogger("metaminer.query_service")

SORTABLE_COLUMNS = {
    "extracted_at": MetadataRecord.extracted_at,
    "file_name": MetadataRecord.file_name,
    "file_type": MetadataRecord.file_type,
    "author": MetadataRecord.author,
    "title": MetadataRecord.title,
    "create_date": MetadataRecord.create_date,
    "modify_date": MetadataRecord.modify_date,
    "submitted_at": FileSubmission.submitted_at,
}


async def query_metadata(db: AsyncSession, params: dict) -> list[dict]:
    q = (
        select(MetadataRecord, FileSubmission, Project)
        .join(FileSubmission, MetadataRecord.submission_id == FileSubmission.id)
        .join(Project, FileSubmission.project_id == Project.id)
    )

    filters = []

    if params.get("project_id"):
        filters.append(FileSubmission.project_id == params["project_id"])

    if params.get("file_type"):
        filters.append(MetadataRecord.file_type.ilike(params["file_type"]))

    if params.get("file_type__in"):
        types = [t.strip().upper() for t in params["file_type__in"].split(",")]
        filters.append(MetadataRecord.file_type.in_(types))

    if params.get("author"):
        filters.append(MetadataRecord.author.ilike(f"%{params['author']}%"))

    if params.get("title"):
        filters.append(MetadataRecord.title.ilike(f"%{params['title']}%"))

    if params.get("creator_tool"):
        filters.append(MetadataRecord.creator_tool.ilike(f"%{params['creator_tool']}%"))

    if params.get("producer"):
        filters.append(MetadataRecord.producer.ilike(f"%{params['producer']}%"))

    if params.get("mime_type"):
        filters.append(MetadataRecord.mime_type == params["mime_type"])

    if params.get("pdf_variant"):
        filters.append(MetadataRecord.pdf_variant == params["pdf_variant"])

    if params.get("submission_mode"):
        filters.append(FileSubmission.submission_mode == params["submission_mode"])

    if params.get("source_url__contains"):
        filters.append(FileSubmission.source_url.ilike(f"%{params['source_url__contains']}%"))

    if params.get("extracted_after"):
        filters.append(MetadataRecord.extracted_at >= params["extracted_after"])

    if params.get("extracted_before"):
        filters.append(MetadataRecord.extracted_at <= params["extracted_before"])

    if params.get("q"):
        term = f"%{params['q']}%"
        filters.append(or_(
            MetadataRecord.author.ilike(term),
            MetadataRecord.title.ilike(term),
            MetadataRecord.creator_tool.ilike(term),
            MetadataRecord.producer.ilike(term),
            MetadataRecord.file_name.ilike(term),
        ))

    if params.get("raw_contains"):
        filters.append(MetadataRecord.raw_json.ilike(f"%{params['raw_contains']}%"))

    if filters:
        q = q.where(*filters)

    # Sorting
    sort_col = SORTABLE_COLUMNS.get(params.get("sort_by", "extracted_at"), MetadataRecord.extracted_at)
    if params.get("order", "desc") == "asc":
        q = q.order_by(sort_col.asc())
    else:
        q = q.order_by(sort_col.desc())

    q = q.offset(params.get("offset", 0)).limit(min(params.get("limit", 50), 500))

    result = await db.execute(q)
    rows = result.all()

    output = []
    for record, submission, project in rows:
        data = {col.name: getattr(record, col.name) for col in MetadataRecord.__table__.columns}
        try:
            data["raw_json"] = json.loads(data["raw_json"])
        except Exception:
            pass
        data["source_url"] = submission.source_url
        data["submission_mode"] = submission.submission_mode
        data["project_id"] = project.id
        data["project_name"] = project.name
        output.append(data)

    return output
