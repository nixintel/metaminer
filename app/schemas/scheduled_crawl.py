import json
from datetime import datetime
from pydantic import BaseModel, field_validator


class ScheduledCrawlCreate(BaseModel):
    project_id: int
    url: str
    frequency_seconds: int
    depth_limit: int | None = None
    allowed_file_types: list[str] | None = None
    full_download: bool = False
    retain_files: bool = False
    crawl_images: bool = False
    robotstxt_obey: bool | None = None

    @field_validator("frequency_seconds")
    @classmethod
    def frequency_must_be_positive(cls, v: int) -> int:
        if v < 60:
            raise ValueError("frequency_seconds must be at least 60")
        return v


class ScheduledCrawlUpdate(BaseModel):
    is_active: bool | None = None
    frequency_seconds: int | None = None
    depth_limit: int | None = None
    allowed_file_types: list[str] | None = None
    full_download: bool | None = None
    retain_files: bool | None = None
    crawl_images: bool | None = None
    robotstxt_obey: bool | None = None

    @field_validator("frequency_seconds")
    @classmethod
    def frequency_must_be_positive(cls, v: int | None) -> int | None:
        if v is not None and v < 60:
            raise ValueError("frequency_seconds must be at least 60")
        return v


class ScheduledCrawlResponse(BaseModel):
    id: int
    project_id: int
    url: str
    frequency_seconds: int
    is_active: bool
    depth_limit: int | None
    allowed_file_types: list[str] | None
    full_download: bool
    retain_files: bool
    crawl_images: bool
    robotstxt_obey: bool | None
    last_run_at: datetime | None
    next_run_at: datetime
    created_at: datetime

    model_config = {"from_attributes": True}

    @classmethod
    def model_validate(cls, obj, **kwargs):
        # Deserialise allowed_file_types from the JSON text column
        if hasattr(obj, "allowed_file_types") and isinstance(obj.allowed_file_types, str):
            try:
                obj.allowed_file_types = json.loads(obj.allowed_file_types)
            except Exception:
                obj.allowed_file_types = None
        return super().model_validate(obj, **kwargs)
