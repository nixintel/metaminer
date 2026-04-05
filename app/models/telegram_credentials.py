from datetime import datetime, timezone
from sqlalchemy import Integer, Text, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


def _now():
    return datetime.now(timezone.utc)


class TelegramCredentials(Base):
    """
    Stores Telegram API credentials as a DB-level alternative to .env vars.
    Treat this as a single-row config table — app always reads the first row.
    api_id / api_hash are obtained from https://my.telegram.org.
    """
    __tablename__ = "telegram_credentials"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    api_id: Mapped[int] = mapped_column(Integer, nullable=False)
    api_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_now, onupdate=_now
    )
