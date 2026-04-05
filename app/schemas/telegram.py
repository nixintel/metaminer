import json
from datetime import datetime
from pydantic import BaseModel, field_validator


# ---------------------------------------------------------------------------
# Immediate scrape
# ---------------------------------------------------------------------------

class TelegramScrapeSubmit(BaseModel):
    project_id: int
    channel: str                            # "@channelname", "t.me/channelname", or bare name
    allowed_file_types: list[str] | None = None  # None = use TELEGRAM_ALLOWED_FILE_TYPES
    max_file_size_mb: int | None = None          # None = use TELEGRAM_MAX_FILE_SIZE_MB
    max_files: int | None = None                 # None = use TELEGRAM_MAX_FILES
    date_from: datetime | None = None            # None = now - TELEGRAM_DATE_RANGE_DAYS
    date_to: datetime | None = None              # None = now
    retain_files: bool = False
    deduplicate: bool = True
    pdf_mode: bool | None = None                 # None = use PDF_MODE_ENABLED; True = rollback


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

class TelegramCredentialsCreate(BaseModel):
    api_id: int
    api_hash: str


class TelegramCredentialsResponse(BaseModel):
    id: int
    api_id: int
    created_at: datetime
    updated_at: datetime
    # api_hash intentionally omitted from response

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class TelegramAuthStart(BaseModel):
    phone: str   # E.164 format, e.g. "+447700900000"


class TelegramAuthVerify(BaseModel):
    phone: str
    code: str
    password: str | None = None  # 2FA password if account has it enabled


# ---------------------------------------------------------------------------
# Scheduled scrapes
# ---------------------------------------------------------------------------

class ScheduledTelegramScrapeCreate(BaseModel):
    project_id: int
    channel: str
    frequency_seconds: int
    allowed_file_types: list[str] | None = None
    max_file_size_mb: int | None = None
    max_files: int | None = None
    date_range_days: int | None = None
    pdf_mode: bool | None = None
    retain_files: bool = False
    deduplicate: bool = True

    @field_validator("frequency_seconds")
    @classmethod
    def frequency_must_be_positive(cls, v: int) -> int:
        if v < 60:
            raise ValueError("frequency_seconds must be at least 60")
        return v


class ScheduledTelegramScrapeUpdate(BaseModel):
    is_active: bool | None = None
    frequency_seconds: int | None = None
    allowed_file_types: list[str] | None = None
    max_file_size_mb: int | None = None
    max_files: int | None = None
    date_range_days: int | None = None
    pdf_mode: bool | None = None
    retain_files: bool | None = None
    deduplicate: bool | None = None

    @field_validator("frequency_seconds")
    @classmethod
    def frequency_must_be_positive(cls, v: int | None) -> int | None:
        if v is not None and v < 60:
            raise ValueError("frequency_seconds must be at least 60")
        return v


class ScheduledTelegramScrapeResponse(BaseModel):
    id: int
    project_id: int
    channel: str
    frequency_seconds: int
    is_active: bool
    allowed_file_types: list[str] | None
    max_file_size_mb: int | None
    max_files: int | None
    date_range_days: int | None
    pdf_mode: bool | None
    retain_files: bool
    deduplicate: bool
    last_run_at: datetime | None
    next_run_at: datetime
    created_at: datetime

    model_config = {"from_attributes": True}

    @classmethod
    def model_validate(cls, obj, **kwargs):
        if hasattr(obj, "allowed_file_types") and isinstance(obj.allowed_file_types, str):
            try:
                obj.allowed_file_types = json.loads(obj.allowed_file_types)
            except Exception:
                obj.allowed_file_types = None
        return super().model_validate(obj, **kwargs)
