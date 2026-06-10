import json
from datetime import datetime, timezone
from sqlalchemy import String, Text, Integer, Boolean, ForeignKey, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


class ScheduledCrawl(Base):
    __tablename__ = "scheduled_crawls"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    # Stored as a JSON array string e.g. '["https://a.com","https://b.com"]'.
    # Old records may contain a plain URL string — the `urls` property handles both.
    url: Mapped[str] = mapped_column(Text, nullable=False)
    frequency_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Crawl options (mirrors CrawlSubmit)
    depth_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    allowed_file_types: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON array
    full_download: Mapped[bool] = mapped_column(Boolean, default=False)
    retain_files: Mapped[bool] = mapped_column(Boolean, default=False)
    crawl_images: Mapped[bool] = mapped_column(Boolean, default=False)
    robotstxt_obey: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    allow_cross_domain: Mapped[bool] = mapped_column(Boolean, default=False)

    # Schedule state
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    project: Mapped["Project"] = relationship("Project")

    @property
    def urls(self) -> list[str]:
        """Return the list of start URLs. Handles both JSON-array and legacy plain-string format."""
        try:
            parsed = json.loads(self.url)
            if isinstance(parsed, list):
                return [u for u in parsed if u]
        except Exception:
            pass
        return [self.url] if self.url else []
