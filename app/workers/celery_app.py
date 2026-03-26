from celery import Celery
from celery.signals import worker_process_init
from kombu import Queue
from config import settings


@worker_process_init.connect
def init_worker_logging(**kwargs):
    from app.utils.logging_config import configure_logging
    configure_logging(enable_db_handler=False)

celery_app = Celery(
    "metaminer",
    broker=settings.REDIS_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.workers.bulk_tasks",
        "app.workers.crawl_tasks",
        "app.workers.maintenance_tasks",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,             # Acknowledge only after completion — re-queues on worker crash
    task_reject_on_worker_lost=True, # Return message to queue if the worker process is killed
    worker_prefetch_multiplier=1,  # One task at a time per worker slot (long-running tasks)
    worker_heartbeat_timeout=300,  # solo pool blocks heartbeats during crawl; suppress false drift warnings

    # --- Queue definitions ---
    # bulk:        file metadata extraction (prefork workers, CPU/IO parallel)
    # crawl:       web crawling via Scrapy (solo workers, one crawl per process)
    # maintenance: periodic housekeeping (runs on bulk workers, lightweight)
    task_queues=(
        Queue("bulk"),
        Queue("crawl"),
        Queue("maintenance"),
    ),
    task_default_queue="bulk",

    # Explicit routing so task dispatch never relies on caller remembering the queue name
    task_routes={
        "metaminer.bulk_task":          {"queue": "bulk"},
        "metaminer.crawl_task":         {"queue": "crawl"},
        "metaminer.purge_old_logs":              {"queue": "maintenance"},
        "metaminer.cleanup_temp_files":          {"queue": "maintenance"},
        "metaminer.dispatch_scheduled_crawls":   {"queue": "maintenance"},
    },

    beat_schedule={
        "dispatch-scheduled-crawls": {
            "task": "metaminer.dispatch_scheduled_crawls",
            "schedule": 60,  # every minute — checks for due scheduled crawls
        },
        "purge-old-logs-daily": {
            "task": "metaminer.purge_old_logs",
            "schedule": 86400,  # every 24 hours
        },
        "cleanup-temp-files-hourly": {
            "task": "metaminer.cleanup_temp_files",
            "schedule": 3600,  # every hour
        },
    },
)
