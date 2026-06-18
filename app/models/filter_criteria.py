from datetime import datetime, timezone
from sqlalchemy import String, Text, Integer, Boolean, ForeignKey, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


class FilterCriteria(Base):
    """A user-defined criterion that auto-tags matching metadata as 'interesting'.

    Three types:
      - 'keyword'    : case-insensitive substring match against source_url + raw exif JSON.
      - 'regex'      : regex match against source_url + raw exif JSON.
      - 'exif_field' : flags when the named exif field exists AND is non-empty.

    project_id is nullable: NULL means the filter is global (applies to every project),
    otherwise it applies only to that project's metadata.
    """
    __tablename__ = "filters"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    filter_type: Mapped[str] = mapped_column(String(20), nullable=False)  # keyword | regex | exif_field
    value: Mapped[str] = mapped_column(Text, nullable=False)  # keyword text / regex pattern / exif field name
    project_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)

    project: Mapped["Project | None"] = relationship("Project")

    __table_args__ = (
        # Speeds up load_active_filters (is_active + project scoping).
        Index("ix_filters_project_active", "project_id", "is_active"),
    )
