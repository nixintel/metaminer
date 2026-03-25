import logging
import logging.handlers
from pathlib import Path
from config import settings

# Redis key for the cross-process log queue
LOG_QUEUE_KEY = "metaminer:log_queue"
# Cap the queue so a stalled API doesn't exhaust Redis memory
LOG_QUEUE_MAX = 50_000

# Module-level sync Redis client for workers (lazy-initialised per process)
_redis_log_client = None


def _get_redis_log_client():
    global _redis_log_client
    if _redis_log_client is None:
        import redis as redis_lib
        _redis_log_client = redis_lib.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=1,
            socket_connect_timeout=1,
        )
    return _redis_log_client


class DBHandler(logging.Handler):
    """Writes log records to the log_entries table via the async session (API process only)."""

    def emit(self, record: logging.LogRecord):
        try:
            from app.database import AsyncSessionLocal
            from app.models.log_entry import LogEntry
            import asyncio

            entry = LogEntry(
                level=record.levelname,
                logger_name=record.name,
                message=self.format(record),
            )

            if hasattr(record, "task_id"):
                entry.task_id = record.task_id
            if hasattr(record, "submission_id"):
                entry.submission_id = record.submission_id

            async def _write():
                async with AsyncSessionLocal() as session:
                    session.add(entry)
                    await session.commit()

            try:
                loop = asyncio.get_running_loop()
                loop.create_task(_write())
            except RuntimeError:
                asyncio.run(_write())
        except Exception:
            self.handleError(record)


class RedisQueueHandler(logging.Handler):
    """
    Serialises log records to a Redis list.  Usable in any process — no event
    loop required.  The API process drains the queue via drain_log_queue().
    """

    def emit(self, record: logging.LogRecord):
        try:
            import json
            from datetime import datetime, timezone

            data = json.dumps({
                "level": record.levelname,
                "logger_name": record.name,
                "message": self.format(record),
                "task_id": getattr(record, "task_id", None),
                "submission_id": getattr(record, "submission_id", None),
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
            r = _get_redis_log_client()
            pipe = r.pipeline()
            pipe.lpush(LOG_QUEUE_KEY, data)
            # Trim so the queue never grows beyond LOG_QUEUE_MAX entries
            pipe.ltrim(LOG_QUEUE_KEY, 0, LOG_QUEUE_MAX - 1)
            pipe.execute()
        except Exception:
            self.handleError(record)


async def drain_log_queue():
    """
    Long-running coroutine — call as an asyncio background task in the API
    lifespan.  Every 2 s it pops up to 100 records from the Redis log queue
    and batch-inserts them into log_entries.
    """
    import asyncio
    import json
    from datetime import datetime, timezone

    import redis as redis_lib

    from app.database import AsyncSessionLocal
    from app.models.log_entry import LogEntry

    logger = logging.getLogger("metaminer.log_drain")
    r = redis_lib.Redis.from_url(settings.REDIS_URL, decode_responses=True)

    while True:
        try:
            await asyncio.sleep(2)

            # Pop up to 100 items from the right of the list (FIFO — workers lpush)
            pipe = r.pipeline()
            for _ in range(100):
                pipe.rpop(LOG_QUEUE_KEY)
            raw_items = [item for item in pipe.execute() if item is not None]

            if not raw_items:
                continue

            records = []
            for raw in raw_items:
                try:
                    d = json.loads(raw)
                    records.append(LogEntry(
                        level=d["level"],
                        logger_name=d["logger_name"],
                        message=d["message"],
                        task_id=d.get("task_id"),
                        submission_id=d.get("submission_id"),
                        created_at=datetime.fromisoformat(d["created_at"]),
                    ))
                except Exception as exc:
                    logger.debug("Skipping malformed log queue entry: %s", exc)

            if records:
                async with AsyncSessionLocal() as session:
                    session.add_all(records)
                    await session.commit()

        except asyncio.CancelledError:
            break
        except Exception as exc:
            # Log at DEBUG so we don't risk infinite recursion through the DB handler
            logger.debug("Log drain error: %s", exc)


def configure_logging(enable_db_handler: bool = True):
    log_dir = settings.LOG_FILE.parent
    log_dir.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Rotating file handler
    file_handler = logging.handlers.RotatingFileHandler(
        settings.LOG_FILE,
        maxBytes=settings.LOG_MAX_FILE_SIZE_MB * 1024 * 1024,
        backupCount=settings.LOG_BACKUP_COUNT,
    )
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    if enable_db_handler:
        # API process: write directly to DB via the async session (INFO+)
        db_handler = DBHandler()
        db_handler.setLevel(logging.INFO)
        db_handler.setFormatter(formatter)
        root.addHandler(db_handler)
    else:
        # Worker processes: push to Redis queue; API drain task writes to DB
        redis_handler = RedisQueueHandler()
        redis_handler.setLevel(logging.INFO)
        redis_handler.setFormatter(formatter)
        root.addHandler(redis_handler)

    # Quieten noisy third-party loggers
    # scrapy is kept at INFO so spider self.logger.info() calls are visible
    logging.getLogger("scrapy").setLevel(logging.INFO)
    logging.getLogger("twisted").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
