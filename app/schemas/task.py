import json
from datetime import datetime
from pydantic import BaseModel, computed_field


class TaskResponse(BaseModel):
    id: int
    project_id: int
    task_type: str
    status: str
    celery_task_id: str | None
    config_json: str | None = None
    files_found: int
    files_processed: int
    crawl_failures: int = 0
    crawl_errors: str | None = None
    skipped_duplicates: int = 0
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    @computed_field
    @property
    def crawl_url(self) -> str | None:
        """URL submitted for crawl tasks, extracted from config_json."""
        if self.config_json:
            try:
                return json.loads(self.config_json).get("url")
            except Exception:
                return None
        return None

    @computed_field
    @property
    def telegram_channel(self) -> str | None:
        """Channel submitted for telegram tasks, extracted from config_json."""
        if self.config_json:
            try:
                return json.loads(self.config_json).get("channel")
            except Exception:
                return None
        return None

    model_config = {"from_attributes": True}
