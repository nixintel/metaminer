from datetime import datetime, timezone
from sqlalchemy import String, Text, Integer, Boolean, ForeignKey, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


class FileSubmission(Base):
    __tablename__ = "file_submissions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    project_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    task_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("tasks.id", ondelete="SET NULL"), nullable=True
    )
    original_filename: Mapped[str] = mapped_column(String(512), nullable=False)
    original_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    http_etag: Mapped[str | None] = mapped_column(String(512), nullable=True)
    http_last_modified: Mapped[str | None] = mapped_column(String(128), nullable=True)
    file_hash_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    file_size_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    submission_mode: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # 'single' | 'bulk' | 'crawl'
    retained: Mapped[bool] = mapped_column(Boolean, default=False)
    retained_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    project: Mapped["Project"] = relationship("Project", back_populates="submissions")
    task: Mapped["Task | None"] = relationship("Task", back_populates="submissions")
    metadata_records: Mapped[list["MetadataRecord"]] = relationship(
        "MetadataRecord", back_populates="submission", cascade="all, delete-orphan"
    )
    log_entries: Mapped[list["LogEntry"]] = relationship(
        "LogEntry", back_populates="submission"
    )

    __table_args__ = (
        Index("ix_file_submissions_crawl_dedup", "project_id", "source_url", "file_hash_sha256"),
        Index("ix_file_submissions_source_url", "project_id", "source_url"),
    )
