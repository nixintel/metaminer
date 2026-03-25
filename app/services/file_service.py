import shutil
import logging
from pathlib import Path
from app.utils.helpers import sha256_file
from config import settings

logger = logging.getLogger("metaminer.file_service")


def make_temp_copy(source: str | Path, suffix: str = "") -> Path:
    """Copy a file to the temp directory and return the temp path."""
    source = Path(source)
    settings.TEMP_DIR.mkdir(parents=True, exist_ok=True)
    dest = settings.TEMP_DIR / f"{source.stem}_{sha256_file(source)[:8]}{suffix}{source.suffix}"
    shutil.copy2(source, dest)
    return dest


def retain_file(source: str | Path, project_id: int, original_name: str) -> Path:
    """Copy a file to the retained files directory and return the path."""
    dest_dir = settings.RETAINED_FILES_DIR / str(project_id)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / original_name
    # Avoid overwriting if the same name exists
    if dest.exists():
        stem = Path(original_name).stem
        ext = Path(original_name).suffix
        dest = dest_dir / f"{stem}_{sha256_file(source)[:8]}{ext}"
    shutil.copy2(source, dest)
    return dest


def delete_file_safe(path: str | Path):
    try:
        Path(path).unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Could not delete temp file {path}: {e}")


def cleanup_temp_older_than(hours: int):
    """Remove temp files older than `hours` hours."""
    import time
    cutoff = time.time() - hours * 3600
    for f in settings.TEMP_DIR.iterdir():
        if f.is_file() and f.stat().st_mtime < cutoff:
            delete_file_safe(f)
