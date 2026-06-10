from pydantic import BaseModel, field_validator


class CrawlSubmit(BaseModel):
    project_id: int
    urls: list[str]
    depth_limit: int | None = None
    allowed_file_types: list[str] | None = None
    full_download: bool = False
    retain_files: bool = False
    deduplicate: bool = True
    robotstxt_obey: bool | None = None      # None = use CRAWLER_ROBOTSTXT_OBEY from config
    crawl_images: bool | None = None        # None = use CRAWLER_FOLLOW_IMAGE_TAGS from config
    allow_cross_domain: bool | None = None  # None = use CRAWLER_ALLOW_CROSS_DOMAIN from config

    @field_validator("urls")
    @classmethod
    def urls_must_not_be_empty(cls, v: list[str]) -> list[str]:
        v = [u.strip() for u in v if u.strip()]
        if not v:
            raise ValueError("At least one URL is required")
        return v
