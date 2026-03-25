import subprocess
import logging
from pathlib import Path
from app.services.exiftool import extract_metadata, ExiftoolError
from app.services.file_service import make_temp_copy, delete_file_safe
from config import settings

logger = logging.getLogger("metaminer.pdf_service")


def is_pdf(file_path: str | Path) -> bool:
    path = Path(file_path)
    if path.suffix.lower() == ".pdf":
        return True
    # Check magic bytes
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"%PDF"
    except Exception:
        return False


# Patterns in exiftool stderr that indicate a structural PDF defect rather
# than a generic write failure.
_STRUCTURAL_PDF_ERROR_PATTERNS = (
    "invalid xref",
    "invalid pdf",
    "bad xref",
    "corrupted",
    "rebuild",
)


def _is_structural_pdf_error(stderr: str) -> bool:
    lower = stderr.lower()
    return any(pattern in lower for pattern in _STRUCTURAL_PDF_ERROR_PATTERNS)


def extract_pdf_both_variants(file_path: str | Path) -> tuple[dict, dict | None]:
    """
    Extract metadata from a PDF in two passes:
      1. Original: exiftool on the file as-is
      2. Rollback: run `exiftool -PDF-update:all=` then extract from modified copy

    Returns (original_metadata, rollback_metadata).
    rollback_metadata is None if:
      - the rollback command fails (logged as WARNING, with distinction between
        structural PDF defects and other errors), or
      - the rollback was a no-op because the PDF has no incremental update layers
        (logged as DEBUG — this is expected for many PDFs).
    """
    original_meta = extract_metadata(file_path)

    # Work on a temp copy so we never mutate the submitted file
    temp_copy = make_temp_copy(file_path, suffix="_rollback_work")
    # exiftool creates a backup of temp_copy as temp_copy + "_original"
    exiftool_backup = Path(str(temp_copy) + "_original")

    try:
        result = subprocess.run(
            [settings.EXIFTOOL_PATH, "-PDF-update:all=", str(temp_copy)],
            capture_output=True,
            text=True,
            timeout=settings.EXIFTOOL_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            if _is_structural_pdf_error(stderr):
                logger.warning(
                    f"PDF has structural issues (skipping rollback) for {file_path}: {stderr}"
                )
            else:
                logger.warning(
                    f"PDF rollback command failed for {file_path}: {stderr}"
                )
            return original_meta, None

        # exiftool prints "1 image files updated" when it stripped update layers,
        # or "1 image files unchanged" when there was nothing to strip.
        if "image files unchanged" in result.stdout:
            logger.debug(
                f"PDF has no incremental update layers, rollback is a no-op: {file_path}"
            )
            return original_meta, None

        # temp_copy is now the rolled-back version; extract from it
        try:
            rollback_meta = extract_metadata(temp_copy)
        except ExiftoolError as e:
            logger.warning(f"Failed to extract rollback metadata for {file_path}: {e}")
            rollback_meta = None

        return original_meta, rollback_meta

    finally:
        delete_file_safe(temp_copy)
        delete_file_safe(exiftool_backup)
