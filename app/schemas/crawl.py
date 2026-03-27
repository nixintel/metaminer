from pydantic import BaseModel


class CrawlSubmit(BaseModel):
    project_id: int
    url: str
    depth_limit: int | None = None
    allowed_file_types: list[str] | None = None
    full_download: bool = False
    retain_files: bool = False
    deduplicate: bool = True
    robotstxt_obey: bool | None = None      # None = use CRAWLER_ROBOTSTXT_OBEY from config
    crawl_images: bool | None = None        # None = use CRAWLER_FOLLOW_IMAGE_TAGS from config
    allow_cross_domain: bool | None = None  # None = use CRAWLER_ALLOW_CROSS_DOMAIN from config
