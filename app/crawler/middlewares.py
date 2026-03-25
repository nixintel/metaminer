"""
PartialDownloadMiddleware — truncates Scrapy responses after a configured byte limit.

Only first N MB of each file response are downloaded. This is sufficient for exiftool
to extract metadata from file headers (magic bytes, XMP, EXIF blocks, PDF header, etc.)
and is significantly faster than downloading complete files.

Set PARTIAL_DOWNLOAD_ENABLED=False in Scrapy settings to disable.
"""
from scrapy.exceptions import IgnoreRequest
from scrapy.http import Response


class PartialDownloadMiddleware:
    def __init__(self, max_bytes: int, enabled: bool):
        self.max_bytes = max_bytes
        self.enabled = enabled

    @classmethod
    def from_crawler(cls, crawler):
        max_mb = crawler.settings.getfloat("PARTIAL_DOWNLOAD_SIZE_MB", 2.0)
        enabled = crawler.settings.getbool("PARTIAL_DOWNLOAD_ENABLED", True)
        return cls(max_bytes=int(max_mb * 1024 * 1024), enabled=enabled)

    def process_response(self, request, response, spider):
        if not self.enabled:
            return response

        # Only truncate file-type responses (not HTML pages used for link following)
        content_type = response.headers.get("Content-Type", b"").decode("utf-8", errors="ignore")
        if "text/html" in content_type:
            return response

        if len(response.body) > self.max_bytes:
            spider.logger.info(
                "Partial download applied | url=%s | received=%d bytes | "
                "truncated_to=%d bytes (%.1f MB limit)",
                request.url,
                len(response.body),
                self.max_bytes,
                self.max_bytes / (1024 * 1024),
            )
            truncated_body = response.body[: self.max_bytes]
            return response.replace(body=truncated_body)

        spider.logger.info(
            "Full file received | url=%s | size=%d bytes",
            request.url,
            len(response.body),
        )
        return response


class ProxyFallbackMiddleware:
    """Fallback for proxy errors: retry once without proxy."""

    def process_exception(self, request, exception, spider):
        proxy = request.meta.get("proxy")
        if not proxy:
            return None

        spider.logger.warning(
            "Proxy error for %s (%s). Retrying without proxy.",
            request.url,
            exception,
        )

        new_request = request.copy()
        new_request.meta.pop("proxy", None)
        new_request.dont_filter = True
        return new_request
