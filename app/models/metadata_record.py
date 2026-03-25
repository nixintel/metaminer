from datetime import datetime, timezone
from sqlalchemy import String, Text, Integer, ForeignKey, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


class MetadataRecord(Base):
    __tablename__ = "metadata_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    submission_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("file_submissions.id", ondelete="CASCADE"), nullable=False
    )
    # For PDFs: 'original' (before rollback) or 'rollback' (after -PDF-update:all=)
    # NULL for non-PDF files
    pdf_variant: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Full exiftool JSON output (always present)
    raw_json: Mapped[str] = mapped_column(Text, nullable=False)
    exiftool_version: Mapped[str | None] = mapped_column(String(20), nullable=True)
    extracted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    # Promoted columns for SQL filtering (subset of raw_json fields)
    file_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    file_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    file_type_extension: Mapped[str | None] = mapped_column(String(20), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_size: Mapped[str | None] = mapped_column(String(50), nullable=True)
    create_date: Mapped[str | None] = mapped_column(String(50), nullable=True)
    modify_date: Mapped[str | None] = mapped_column(String(50), nullable=True)
    author: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    creator_tool: Mapped[str | None] = mapped_column(Text, nullable=True)
    producer: Mapped[str | None] = mapped_column(Text, nullable=True)
    pdf_version: Mapped[str | None] = mapped_column(String(20), nullable=True)

    submission: Mapped["FileSubmission"] = relationship(
        "FileSubmission", back_populates="metadata_records"
    )

    __table_args__ = (
        Index("ix_metadata_records_submission_id", "submission_id"),
        Index("ix_metadata_records_extracted_at", "extracted_at"),
        Index("ix_metadata_records_file_type", "file_type"),
    )
