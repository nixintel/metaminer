from datetime import datetime, timezone
from sqlalchemy import String, Integer, Boolean, ForeignKey, DateTime, Index, Table, Column
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


# Many-to-many: a single filter can belong to several groups, and a group bundles
# several filters (OR semantics — a group matches a record if any member matches).
filter_group_members = Table(
    "filter_group_members",
    Base.metadata,
    Column("group_id", ForeignKey("filter_groups.id", ondelete="CASCADE"), primary_key=True),
    Column("filter_id", ForeignKey("filters.id", ondelete="CASCADE"), primary_key=True),
    Index("ix_fgm_filter_id", "filter_id"),
)


class FilterGroup(Base):
    """A named OR-bundle of single filters used to match patterns of behaviour.

    Mirrors FilterCriteria scoping: project_id NULL = global (all projects); otherwise
    applies only to that project's metadata.
    """
    __tablename__ = "filter_groups"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    project_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=True
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)

    project: Mapped["Project | None"] = relationship("Project")
    # selectin: load members without per-attribute lazy IO (async-safe under asyncpg).
    # backref adds FilterCriteria.groups.
    filters: Mapped[list["FilterCriteria"]] = relationship(
        "FilterCriteria",
        secondary=filter_group_members,
        backref="groups",
        lazy="selectin",
    )

    __table_args__ = (
        Index("ix_filter_groups_project_active", "project_id", "is_active"),
    )
