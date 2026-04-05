from datetime import datetime, timezone
from sqlalchemy import Integer, Text, Boolean, ForeignKey, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


class ScheduledTelegramScrape(Base):
    """
    Recurring Telegram channel scrape schedule.

    channel is scoped to a project — the same channel can exist under multiple
    projects as independent rows with separate metadata collections.
    No unique constraint on (project_id, channel): multiple schedules per
    channel/project are allowed (e.g. different frequency or file type filters).
    """
    __tablename__ = "scheduled_telegram_scrapes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    channel: Mapped[str] = mapped_column(Text, nullable=False)  # e.g. "@channelname"
    frequency_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Scrape options
    allowed_file_types: Mapped[str | None] = mapped_column(Text, nullable=True)  # JSON array
    max_file_size_mb: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_files: Mapped[int | None] = mapped_column(Integer, nullable=True)
    date_range_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pdf_mode: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    retain_files: Mapped[bool] = mapped_column(Boolean, default=False)
    deduplicate: Mapped[bool] = mapped_column(Boolean, default=True)

    # Schedule state
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    project: Mapped["Project"] = relationship("Project")
