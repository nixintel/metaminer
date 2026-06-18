import asyncio
import json
import logging
from pathlib import Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.file_submission import FileSubmission
from app.models.metadata_record import MetadataRecord
from app.models.metadata_filter_match import MetadataFilterMatch
from app.services.exiftool import extract_metadata, get_exiftool_version, ExiftoolError
from app.services.pdf_service import is_pdf, extract_pdf_both_variants
from app.services.file_service import retain_file
from app.utils.helpers import sha256_file
from config import settings

logger = logging.getLogger("metaminer.metadata_service")

# Fields promoted to metadata_records columns for SQL filtering
PROMOTED_FIELDS = {
    "FileName": "file_name",
    "FileType": "file_type",
    "FileTypeExtension": "file_type_extension",
    "MIMEType": "mime_type",
    "FileSize": "file_size",
    "CreateDate": "create_date",
    "ModifyDate": "modify_date",
    "Author": "author",
    "Title": "title",
    "CreatorTool": "creator_tool",
    "Producer": "producer",
    "PDFVersion": "pdf_version",
}


def _extract_promoted(meta: dict) -> dict:
    """Flatten grouped exiftool JSON and extract promoted fields."""
    flat = {}
    for section in meta.values() if isinstance(meta, dict) else [meta]:
        if isinstance(section, dict):
            flat.update(section)

    result = {}
    for exif_key, col_name in PROMOTED_FIELDS.items():
        val = flat.get(exif_key)
        result[col_name] = str(val) if val is not None else None
    return result


def _make_record(submission_id: int, meta: dict, pdf_variant: str | None, version: str | None) -> MetadataRecord:
    promoted = _extract_promoted(meta)
    return MetadataRecord(
        submission_id=submission_id,
        pdf_variant=pdf_variant,
        raw_json=json.dumps(meta),
        exiftool_version=version,
        **promoted,
    )


def _apply_filters(rec: MetadataRecord, meta: dict, source_url: str | None, active_filters) -> None:
    """Auto-tag a freshly built record, recording EVERY matching single filter.

    Records a metadata_filter_matches row per match (via the cascade relationship, so they
    flush with the record). Sets interesting=True if any matched; sets interesting_reason
    (the first match's descriptor) only when not already set — preserving a "manual" mark.
    """
    if not active_filters:
        return
    matched_ids, first_reason = active_filters.evaluate_all(source_url, rec.raw_json, meta)
    if not matched_ids:
        return
    for fid in matched_ids:
        rec.filter_matches.append(MetadataFilterMatch(filter_id=fid))
    if not rec.interesting:
        rec.interesting = True
    if rec.interesting_reason is None:
        rec.interesting_reason = first_reason
    logger.info(
        "Auto-tagged interesting | submission_id=%s | variant=%s | matched_filters=%d | reason=%s",
        rec.submission_id, rec.pdf_variant, len(matched_ids), first_reason,
    )


async def _is_duplicate(db: AsyncSession, project_id: int, file_hash: str) -> bool:
    result = await db.execute(
        select(FileSubmission.id)
        .where(FileSubmission.project_id == project_id)
        .where(FileSubmission.file_hash_sha256 == file_hash)
        .limit(1)
    )
    return result.scalar() is not None


async def process_single_file(
    db: AsyncSession,
    project_id: int,
    file_path: str,
    retain_file_opt: bool = False,
    pdf_mode: bool | None = None,
    task_id: int | None = None,
    submission_mode: str = "manual",
    source_url: str | None = None,
    http_etag: str | None = None,
    http_last_modified: str | None = None,
    active_filters=None,
) -> dict:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    file_hash = sha256_file(path)
    file_size = path.stat().st_size

    logger.info(
        "Processing file | file=%s | size=%d bytes | project_id=%d | mode=%s | source_url=%s",
        path.name, file_size, project_id, submission_mode, source_url,
    )

    # Dedup check within project
    if await _is_duplicate(db, project_id, file_hash):
        logger.info(
            "Skipped (duplicate in project) | file=%s | project_id=%d",
            path.name, project_id,
        )
        return {
            "submission_id": None,
            "project_id": project_id,
            "original_filename": path.name,
            "submission_mode": submission_mode,
            "records_created": 0,
            "skipped_duplicate": True,
        }

    use_pdf_mode = pdf_mode if pdf_mode is not None else settings.PDF_MODE_ENABLED

    retained_path = None
    if retain_file_opt:
        retained = retain_file(path, project_id, path.name)
        retained_path = str(retained)
        logger.info("File retained | file=%s | retained_path=%s", path.name, retained_path)

    submission = FileSubmission(
        project_id=project_id,
        task_id=task_id,
        original_filename=path.name,
        original_path=str(path),
        source_url=source_url,
        http_etag=http_etag,
        http_last_modified=http_last_modified,
        file_hash_sha256=file_hash,
        file_size_bytes=file_size,
        submission_mode=submission_mode,
        retained=retain_file_opt,
        retained_path=retained_path,
    )
    db.add(submission)
    await db.flush()

    logger.info(
        "Submission created | submission_id=%d | file=%s | hash=%s...",
        submission.id, path.name, file_hash[:12],
    )

    exif_version = await asyncio.to_thread(get_exiftool_version)
    records_created = 0

    # Auto-tagging filters. Callers in hot loops preload these once per task and pass them
    # in; if not provided, load once here (one query for this file) as a safe fallback.
    if active_filters is None:
        from app.services.filter_service import load_active_filters
        active_filters = await load_active_filters(db, project_id)

    if use_pdf_mode and is_pdf(path):
        logger.info(
            "PDF detected | submission_id=%d | file=%s | extracting original + rollback variants",
            submission.id, path.name,
        )
        original_meta, rollback_meta = await asyncio.to_thread(extract_pdf_both_variants, path)
        promoted = _extract_promoted(original_meta)
        submission.mime_type = promoted.get("mime_type")

        rec = _make_record(submission.id, original_meta, "original", exif_version)
        _apply_filters(rec, original_meta, source_url, active_filters)
        db.add(rec)
        records_created += 1
        logger.info(
            "Metadata record saved | submission_id=%d | variant=original | "
            "file_type=%s | mime_type=%s | pdf_version=%s | author=%s | title=%s",
            submission.id,
            promoted.get("file_type"),
            promoted.get("mime_type"),
            promoted.get("pdf_version"),
            promoted.get("author"),
            promoted.get("title"),
        )

        if rollback_meta:
            rec = _make_record(submission.id, rollback_meta, "rollback", exif_version)
            _apply_filters(rec, rollback_meta, source_url, active_filters)
            db.add(rec)
            records_created += 1
            logger.info(
                "Metadata record saved | submission_id=%d | variant=rollback",
                submission.id,
            )
        else:
            logger.info(
                "No rollback variant (no incremental update layers or structural issue) | "
                "submission_id=%d | file=%s",
                submission.id, path.name,
            )
    else:
        logger.info(
            "Running exiftool extraction | submission_id=%d | file=%s | pdf_mode_enabled=%s",
            submission.id, path.name, use_pdf_mode,
        )
        try:
            meta = await asyncio.to_thread(extract_metadata, path)
        except ExiftoolError as e:
            logger.error(
                "exiftool extraction failed | submission_id=%d | file=%s | error=%s",
                submission.id, path.name, e,
            )
            raise

        promoted = _extract_promoted(meta)
        submission.mime_type = promoted.get("mime_type")
        rec = _make_record(submission.id, meta, None, exif_version)
        _apply_filters(rec, meta, source_url, active_filters)
        db.add(rec)
        records_created += 1
        logger.info(
            "Metadata record saved | submission_id=%d | file_type=%s | "
            "mime_type=%s | size=%s | author=%s | title=%s",
            submission.id,
            promoted.get("file_type"),
            promoted.get("mime_type"),
            promoted.get("file_size"),
            promoted.get("author"),
            promoted.get("title"),
        )

    await db.flush()

    logger.info(
        "Complete | submission_id=%d | file=%s | records_created=%d",
        submission.id, path.name, records_created,
    )

    return {
        "submission_id": submission.id,
        "project_id": project_id,
        "original_filename": path.name,
        "submission_mode": submission_mode,
        "records_created": records_created,
        "skipped_duplicate": False,
    }
