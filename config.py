from pydantic_settings import BaseSettings
from pydantic import Field, model_validator
from pathlib import Path


# Preset user agents selectable via CRAWLER_USER_AGENT_PRESET.
# Set CRAWLER_USER_AGENT to a custom string to override the preset entirely.
USER_AGENT_PRESETS: dict[str, str] = {
    "chrome-windows": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "chrome-mac": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "chrome-linux": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "firefox-windows": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) "
        "Gecko/20100101 Firefox/126.0"
    ),
    "firefox-mac": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:126.0) "
        "Gecko/20100101 Firefox/126.0"
    ),
    "safari-mac": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4.1 Safari/605.1.15"
    ),
    "edge-windows": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"
    ),
    "googlebot": (
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
}


class Settings(BaseSettings):
    # --- Application ---
    APP_NAME: str = "metaminer"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False

    # --- Database ---
    DATABASE_URL: str = "postgresql+asyncpg://metaminer:metaminer@postgres:5432/metaminer"

    # --- Redis / Celery ---
    REDIS_URL: str = "redis://redis:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://redis:6379/1"
    CELERY_TASK_CONCURRENCY: int = 4

    # --- File Retention ---
    RETAIN_FILES: bool = False
    RETAINED_FILES_DIR: Path = Path("/app/data/retained_files")
    TEMP_DIR: Path = Path("/app/data/temp")
    TEMP_FILE_TTL_HOURS: int = 1

    # --- PDF Mode ---
    PDF_MODE_ENABLED: bool = True

    # --- exiftool ---
    EXIFTOOL_PATH: str = "exiftool"
    EXIFTOOL_TIMEOUT_SECONDS: int = 30

    # --- Crawler (Scrapy) ---
    CRAWLER_DEPTH_LIMIT: int = 3
    CRAWLER_DOWNLOAD_DELAY: float = 1.0
    CRAWLER_AUTOTHROTTLE_ENABLED: bool = True
    CRAWLER_AUTOTHROTTLE_MAX_DELAY: float = 10.0
    CRAWLER_CONCURRENT_REQUESTS: int = 8
    CRAWLER_CONCURRENT_REQUESTS_PER_DOMAIN: int = 4
    CRAWLER_ROBOTSTXT_OBEY: bool = True
    # Select a preset by name (see USER_AGENT_PRESETS above).
    # Set CRAWLER_USER_AGENT to a non-empty string to override the preset entirely.
    CRAWLER_USER_AGENT_PRESET: str = "chrome-windows"
    CRAWLER_USER_AGENT: str = ""  # resolved from preset at startup if left empty

    @model_validator(mode="after")
    def resolve_user_agent(self):
        if not self.CRAWLER_USER_AGENT:
            self.CRAWLER_USER_AGENT = USER_AGENT_PRESETS.get(
                self.CRAWLER_USER_AGENT_PRESET,
                USER_AGENT_PRESETS["chrome-windows"],
            )
        return self
    CRAWLER_ALLOWED_FILE_TYPES: list[str] = Field(
        default=[
            # Documents
            "pdf", "doc", "docx", "odt", "rtf",
            # Spreadsheets
            "xls", "xlsx", "ods", "csv",
            # Presentations
            "ppt", "pptx", "odp",
            # Images
            "jpg", "jpeg", "png", "gif", "tiff", "webp",
            # Audio / video
            "mp3", "mp4",
            # Data / text
            "txt", "json", "xml",
            # Archives
            "zip", "rar",
        ]
    )
    CRAWLER_MAX_FILE_SIZE_MB: int = 100

    # --- Retry / timeout ---
    CRAWLER_DOWNLOAD_TIMEOUT: int = 30
    CRAWLER_RETRY_ENABLED: bool = True
    CRAWLER_RETRY_TIMES: int = 3
    CRAWLER_RETRY_HTTP_CODES: list[int] = [500, 502, 503, 504, 522, 524, 408]
    CRAWLER_RETRY_PRIORITY_ADJUST: int = -1
    CRAWLER_RETRY_ON_TIMEOUT: bool = True
    CRAWLER_HTTPERROR_ALLOW_ALL: bool = False
    CRAWLER_HANDLE_HTTPSTATUS_LIST: list[int] = [200, 206]
    CRAWLER_DOWNLOAD_FAIL_ON_DATALOSS: bool = False

    # --- Proxy ---
    # Leave CRAWLER_PROXY blank to disable. Supports http/https/socks5.
    # Examples: http://proxy.example.com:8080
    #           socks5://user:pass@proxy.example.com:1080
    CRAWLER_PROXY: str = ""
    CRAWLER_PROXY_USERNAME: str = ""  # optional, if not embedded in the URL
    CRAWLER_PROXY_PASSWORD: str = ""  # optional, if not embedded in the URL

    # Follow <img src> in addition to <a href> to discover images embedded in pages.
    # Disabled by default — enabling it causes the crawler to request every image
    # on every HTML page (thumbnails, icons, etc.) which increases request volume.
    CRAWLER_FOLLOW_IMAGE_TAGS: bool = False

    # Partial download: fetch only first N MB to extract header metadata (faster)
    CRAWLER_PARTIAL_DOWNLOAD_ENABLED: bool = True
    CRAWLER_PARTIAL_DOWNLOAD_SIZE_MB: int = 10
    CRAWLER_FULL_DOWNLOAD_ENABLED: bool = True

    # --- Logging ---
    LOG_LEVEL: str = "INFO"
    LOG_FILE: Path = Path("/app/logs/metaminer.log")
    LOG_RETENTION_DAYS: int = 30
    LOG_DB_RETENTION_DAYS: int = 90
    LOG_MAX_FILE_SIZE_MB: int = 50
    LOG_BACKUP_COUNT: int = 5

    # --- API ---
    API_PREFIX: str = "/api/v1"
    DOCS_URL: str = "/docs"
    REDOC_URL: str = "/redoc"

    # --- Frontend ---
    FRONTEND_PORT: int = 5000
    API_BASE_URL: str = "http://api:8000"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
