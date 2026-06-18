import json
import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, and_
from sqlalchemy.orm import selectinload

from app.models.metadata_record import MetadataRecord
from app.models.file_submission import FileSubmission
from app.models.project import Project

logger = logging.getLogger("metaminer.query_service")

FILTERABLE_COLUMNS = {
    "file_name": MetadataRecord.file_name,
    "author": MetadataRecord.author,
    "title": MetadataRecord.title,
    "creator_tool": MetadataRecord.creator_tool,
    "producer": MetadataRecord.producer,
    "file_type": MetadataRecord.file_type,
    "file_type_extension": MetadataRecord.file_type_extension,
    "mime_type": MetadataRecord.mime_type,
    "pdf_variant": MetadataRecord.pdf_variant,
    "pdf_version": MetadataRecord.pdf_version,
    "extracted_at": MetadataRecord.extracted_at,
    "create_date": MetadataRecord.create_date,
    "modify_date": MetadataRecord.modify_date,
    "source_url": FileSubmission.source_url,
    "submission_mode": FileSubmission.submission_mode,
}

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


def _serialize_row(record: MetadataRecord, submission: FileSubmission, project: Project) -> dict:
    """Flatten a (record, submission, project) join row into the API response dict."""
    data = {col.name: getattr(record, col.name) for col in MetadataRecord.__table__.columns}
    try:
        data["raw_json"] = json.loads(data["raw_json"])
    except Exception:
        pass
    data["source_url"] = submission.source_url
    data["submission_mode"] = submission.submission_mode
    data["project_id"] = project.id
    data["project_name"] = project.name
    return data


async def get_record_matches(db: AsyncSession, metadata_id: int, interesting_reason: str | None) -> dict:
    """Derive the displayed matches for one record, applying the "only the group(s)" rule.

    Returns {"groups": [{id,name}], "filters": [{id,name}], "manual": bool} where:
      - groups   = groups with >=1 member that matched this record;
      - filters  = matched filters that belong to NO group (shown standalone);
      - manual   = the record was manually flagged.
    Not scoped by is_active or project (a recorded match is historical fact). Detail-only
    (do not call per-row on list pages).
    """
    from app.models.metadata_filter_match import MetadataFilterMatch
    from app.models.filter_criteria import FilterCriteria
    from app.models.filter_group import FilterGroup, filter_group_members

    # Groups with at least one matched member of this record.
    group_rows = (await db.execute(
        select(FilterGroup.id, FilterGroup.name)
        .distinct()
        .join(filter_group_members, filter_group_members.c.group_id == FilterGroup.id)
        .join(MetadataFilterMatch, MetadataFilterMatch.filter_id == filter_group_members.c.filter_id)
        .where(MetadataFilterMatch.metadata_id == metadata_id)
        .order_by(FilterGroup.name)
    )).all()

    # Matched filters that are members of NO group → shown standalone.
    filter_rows = (await db.execute(
        select(FilterCriteria.id, FilterCriteria.name)
        .join(MetadataFilterMatch, MetadataFilterMatch.filter_id == FilterCriteria.id)
        .where(MetadataFilterMatch.metadata_id == metadata_id)
        .where(~select(filter_group_members.c.group_id)
               .where(filter_group_members.c.filter_id == FilterCriteria.id)
               .exists())
        .order_by(FilterCriteria.name)
    )).all()

    return {
        "groups": [{"id": gid, "name": name} for gid, name in group_rows],
        "filters": [{"id": fid, "name": name} for fid, name in filter_rows],
        "manual": interesting_reason == "manual",
    }


async def get_metadata_by_id(db: AsyncSession, metadata_id: int) -> dict | None:
    """Fetch a single metadata record by its primary key, or None if it doesn't exist.

    Enriches the detail payload with the record's matched groups/filters (the metadata
    list/search path intentionally does NOT do this — it would N+1 across large pages)."""
    q = (
        select(MetadataRecord, FileSubmission, Project)
        .join(FileSubmission, MetadataRecord.submission_id == FileSubmission.id)
        .join(Project, FileSubmission.project_id == Project.id)
        .where(MetadataRecord.id == metadata_id)
    )
    row = (await db.execute(q)).first()
    if row is None:
        return None
    data = _serialize_row(*row)
    matches = await get_record_matches(db, metadata_id, data.get("interesting_reason"))
    data["matched_groups"] = matches["groups"]
    data["matched_filters"] = matches["filters"]
    data["matched_manual"] = matches["manual"]
    return data


async def set_metadata_interesting(db: AsyncSession, metadata_id: int, interesting: bool) -> dict | None:
    """Set the 'interesting' flag on a metadata record.

    Returns the context-serialized record (project_name, source_url, …) or None if no
    record with that id exists. Caller's transaction is committed by the get_db dependency.
    """
    record = await db.get(MetadataRecord, metadata_id)
    if record is None:
        return None
    record.interesting = interesting
    # Provenance: a manual mark records "manual"; unmarking clears the reason.
    record.interesting_reason = "manual" if interesting else None
    await db.flush()
    return await get_metadata_by_id(db, metadata_id)


async def query_metadata(db: AsyncSession, params: dict) -> list[dict]:
    q = (
        select(MetadataRecord, FileSubmission, Project)
        .join(FileSubmission, MetadataRecord.submission_id == FileSubmission.id)
        .join(Project, FileSubmission.project_id == Project.id)
    )

    filters = []

    if params.get("project_id"):
        filters.append(FileSubmission.project_id == params["project_id"])

    if params.get("task_id"):
        filters.append(FileSubmission.task_id == params["task_id"])

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

    if params.get("interesting"):
        filters.append(MetadataRecord.interesting.is_(True))

    if params.get("matched_filter_id"):
        # Records that matched a specific single filter (any match, not just the
        # short-circuit winner) — EXISTS against the metadata_filter_matches join table.
        from sqlalchemy import exists
        from app.models.metadata_filter_match import MetadataFilterMatch
        fid = int(params["matched_filter_id"])
        filters.append(
            exists().where(
                and_(
                    MetadataFilterMatch.metadata_id == MetadataRecord.id,
                    MetadataFilterMatch.filter_id == fid,
                )
            )
        )

    if params.get("matched_group_id"):
        # Records that matched ANY member filter of the given group.
        from sqlalchemy import exists
        from app.models.metadata_filter_match import MetadataFilterMatch
        from app.models.filter_group import filter_group_members
        gid = int(params["matched_group_id"])
        filters.append(
            exists().where(
                and_(
                    MetadataFilterMatch.metadata_id == MetadataRecord.id,
                    MetadataFilterMatch.filter_id == filter_group_members.c.filter_id,
                    filter_group_members.c.group_id == gid,
                )
            )
        )

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
            MetadataRecord.raw_json.ilike(term),
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

    return [_serialize_row(record, submission, project) for record, submission, project in rows]


def build_filter_clause(node: dict):
    """Recursively convert a filter tree node to a SQLAlchemy clause."""
    if "field" in node:
        col = FILTERABLE_COLUMNS.get(node["field"])
        if col is None:
            return None
        op, val = node["op"], node["value"]
        if op == "contains":
            return col.ilike(f"%{val}%")
        elif op == "equals":
            return col == val
        elif op == "starts_with":
            return col.ilike(f"{val}%")
        elif op == "before":
            return col <= val
        elif op == "after":
            return col >= val
        elif op == "in":
            return col.in_([v.strip() for v in val.split(",")])
        return None
    else:
        clauses = [build_filter_clause(c) for c in node.get("conditions", [])]
        clauses = [c for c in clauses if c is not None]
        if not clauses:
            return None
        return or_(*clauses) if node.get("operator") == "OR" else and_(*clauses)


async def query_metadata_tree(db: AsyncSession, request: dict) -> list[dict]:
    """Execute a metadata query from a POST filter tree body."""
    stmt = (
        select(MetadataRecord, FileSubmission, Project)
        .join(FileSubmission, MetadataRecord.submission_id == FileSubmission.id)
        .join(Project, FileSubmission.project_id == Project.id)
    )

    clause = build_filter_clause(request)
    if clause is not None:
        stmt = stmt.where(clause)

    sort_col = SORTABLE_COLUMNS.get(request.get("sort_by", "extracted_at"), MetadataRecord.extracted_at)
    if request.get("order", "desc") == "asc":
        stmt = stmt.order_by(sort_col.asc())
    else:
        stmt = stmt.order_by(sort_col.desc())

    stmt = stmt.offset(request.get("offset", 0)).limit(min(request.get("limit", 50), 500))

    result = await db.execute(stmt)
    rows = result.all()

    return [_serialize_row(record, submission, project) for record, submission, project in rows]
