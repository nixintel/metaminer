from datetime import datetime, timezone
from sqlalchemy import String, Text, Integer, ForeignKey, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    task_type: Mapped[str] = mapped_column(String(20), nullable=False)  # 'bulk' | 'crawl'
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending"
    )  # pending | running | completed | failed | cancelled
    celery_task_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    config_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    files_found: Mapped[int] = mapped_column(Integer, default=0)
    files_processed: Mapped[int] = mapped_column(Integer, default=0)
    crawl_failures: Mapped[int] = mapped_column(Integer, default=0)
    crawl_errors: Mapped[str | None] = mapped_column(Text, nullable=True)
    skipped_duplicates: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    crawl_jobdir: Mapped[str | None] = mapped_column(Text, nullable=True)  # Scrapy JOBDIR path for mid-crawl resume
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    project: Mapped["Project"] = relationship("Project", back_populates="tasks")
    submissions: Mapped[list["FileSubmission"]] = relationship(
        "FileSubmission", back_populates="task"
    )
    log_entries: Mapped[list["LogEntry"]] = relationship(
        "LogEntry", back_populates="task"
    )
