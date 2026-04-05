"""
Telegram service helpers: credential resolution, session management, and auth flow.

Credential priority:
  1. TELEGRAM_API_ID / TELEGRAM_API_HASH env vars (via config.py)
  2. TelegramCredentials row in the database

The anon.session file is created by Telethon on first authentication and reused
thereafter. Its location is configured by TELEGRAM_SESSION_PATH.
"""
import hashlib
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from config import settings

logger = logging.getLogger("metaminer.telegram_service")

_AUTH_TTL = 300  # seconds — phone_code_hash expires after 5 minutes


async def get_credentials(db: AsyncSession) -> tuple[int, str] | None:
    """
    Return (api_id, api_hash) from env vars if set, else from DB, else None.
    """
    if settings.TELEGRAM_API_ID and settings.TELEGRAM_API_HASH:
        return settings.TELEGRAM_API_ID, settings.TELEGRAM_API_HASH

    from app.models.telegram_credentials import TelegramCredentials
    result = await db.execute(select(TelegramCredentials).limit(1))
    cred = result.scalar_one_or_none()
    if cred:
        return cred.api_id, cred.api_hash

    return None


def check_session() -> bool:
    """Return True if the Telethon session file exists on disk."""
    return settings.TELEGRAM_SESSION_PATH.exists()


def make_client(api_id: int, api_hash: str):
    """
    Return a Telethon TelegramClient bound to the configured session path.
    The client is NOT connected — callers should use it as an async context manager.
    """
    from telethon import TelegramClient
    # Telethon appends .session automatically; pass path without the suffix
    session_base = str(settings.TELEGRAM_SESSION_PATH.with_suffix(""))
    return TelegramClient(session_base, api_id, api_hash)


async def start_auth(client, phone: str) -> str:
    """
    Begin phone-number auth. Sends a code to the user's Telegram app.
    Stores the phone_code_hash in Redis and returns it.
    """
    import redis.asyncio as aioredis
    result = await client.send_code_request(phone)
    phone_code_hash = result.phone_code_hash

    # Key by a hash of the phone number so the plain number isn't stored
    key = _auth_redis_key(phone)
    async with aioredis.from_url(settings.REDIS_URL, decode_responses=True) as r:
        await r.set(key, phone_code_hash, ex=_AUTH_TTL)

    logger.info("Telegram auth code sent | phone_suffix=...%s", phone[-4:])
    return phone_code_hash


async def verify_auth(client, phone: str, code: str, password: str | None = None) -> None:
    """
    Complete phone-number auth using the code sent to the user's Telegram app.
    Optionally handles 2FA via password. Writes the session file to disk.
    """
    import redis.asyncio as aioredis
    from telethon.errors import SessionPasswordNeededError

    key = _auth_redis_key(phone)
    async with aioredis.from_url(settings.REDIS_URL, decode_responses=True) as r:
        phone_code_hash = await r.get(key)

    if not phone_code_hash:
        raise ValueError("Auth session expired or not started. Call /auth/start first.")

    try:
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
    except SessionPasswordNeededError:
        if not password:
            raise ValueError("This account has 2FA enabled. Provide the 'password' field.")
        await client.sign_in(password=password)

    # Clean up the temporary Redis key
    async with aioredis.from_url(settings.REDIS_URL, decode_responses=True) as r:
        await r.delete(key)

    logger.info("Telegram auth complete | session written to %s", settings.TELEGRAM_SESSION_PATH)


def _auth_redis_key(phone: str) -> str:
    phone_hash = hashlib.sha256(phone.encode()).hexdigest()[:16]
    return f"telegram_auth_hash:{phone_hash}"
