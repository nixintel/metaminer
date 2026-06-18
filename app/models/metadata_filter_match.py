from datetime import datetime, timezone
from sqlalchemy import Integer, ForeignKey, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


class MetadataFilterMatch(Base):
    """Authoritative record of which single filters matched a metadata record.

    One row per (metadata record, single filter) match. Group matches are derived at
    read time from current group membership. Composite PK gives uniqueness and enables
    idempotent INSERT ... ON CONFLICT DO NOTHING during backfill.
    """
    __tablename__ = "metadata_filter_matches"

    metadata_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("metadata_records.id", ondelete="CASCADE"), primary_key=True
    )
    filter_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("filters.id", ondelete="CASCADE"), primary_key=True
    )
    matched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    __table_args__ = (
        Index("ix_mfm_filter_id", "filter_id"),
    )
