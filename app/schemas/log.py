from datetime import datetime
from pydantic import BaseModel


class LogEntryResponse(BaseModel):
    id: int
    level: str
    logger_name: str
    message: str
    task_id: int | None
    submission_id: int | None
    created_at: datetime

    model_config = {"from_attributes": True}


class CrawlSubmit(BaseModel):
    project_id: int
    url: str
    depth_limit: int | None = None
    allowed_file_types: list[str] | None = None
    full_download: bool = False
    retain_files: bool = False
