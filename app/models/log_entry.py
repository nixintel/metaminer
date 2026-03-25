from datetime import datetime, timezone
from sqlalchemy import String, Text, Integer, ForeignKey, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


class LogEntry(Base):
    __tablename__ = "log_entries"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    level: Mapped[str] = mapped_column(String(10), nullable=False)  # DEBUG|INFO|WARNING|ERROR
    logger_name: Mapped[str] = mapped_column(String(255), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    task_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("tasks.id", ondelete="SET NULL"), nullable=True
    )
    submission_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("file_submissions.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, index=True
    )

    task: Mapped["Task | None"] = relationship("Task", back_populates="log_entries")
    submission: Mapped["FileSubmission | None"] = relationship(
        "FileSubmission", back_populates="log_entries"
    )

    __table_args__ = (
        Index("ix_log_entries_level", "level"),
    )
