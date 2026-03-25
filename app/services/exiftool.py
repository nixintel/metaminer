import json
import logging
import subprocess
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from config import settings

logger = logging.getLogger("metaminer.exiftool")


class ExiftoolError(RuntimeError):
    pass


@retry(
    retry=retry_if_exception_type(ExiftoolError),
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    reraise=True,
)
def extract_metadata(file_path: str | Path) -> dict:
    """Run exiftool on a file and return parsed JSON metadata (first element)."""
    path = str(file_path)
    try:
        result = subprocess.run(
            [settings.EXIFTOOL_PATH, "-json", "-a", "-u", "-g", path],
            capture_output=True,
            text=True,
            timeout=settings.EXIFTOOL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise ExiftoolError(f"exiftool timed out on {path}")
    except FileNotFoundError:
        raise ExiftoolError(
            f"exiftool not found at '{settings.EXIFTOOL_PATH}'. "
            "Ensure it is installed and on PATH."
        )

    if result.returncode != 0:
        raise ExiftoolError(f"exiftool exited {result.returncode}: {result.stderr.strip()}")

    try:
        data = json.loads(result.stdout)
        return data[0] if data else {}
    except (json.JSONDecodeError, IndexError) as e:
        raise ExiftoolError(f"Failed to parse exiftool output: {e}")


def get_exiftool_version() -> str | None:
    try:
        result = subprocess.run(
            [settings.EXIFTOOL_PATH, "-ver"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception:
        return None
