"""
MetaminerSpider — crawls a URL, discovers links, downloads target file types,
and saves them to a temp directory for metadata extraction.

Each downloaded file path is added to spider.downloaded_files for the caller to process.
"""
from pathlib import Path
from urllib.parse import urlparse
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from config import settings


class MetaminerSpider(CrawlSpider):
    name = "metaminer"

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "app.crawler.middlewares.ProxyFallbackMiddleware": 740,
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
            "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 750,
            "app.crawler.middlewares.PartialDownloadMiddleware": 900,
        },
    }

    def __init__(
        self,
        start_url: str,
        allowed_file_types: list[str] | None = None,
        full_download: bool = False,
        output_dir: Path | None = None,
        crawl_images: bool = False,
        result_queue=None,
        *args,
        **kwargs,
    ):
        self.start_urls = [start_url]
        parsed = urlparse(start_url)
        self.start_domain = parsed.netloc

        # Do NOT set allowed_domains — Scrapy's OffsiteMiddleware would silently
        # drop requests to any other domain, including CDN/asset hosts where files
        # are typically served (e.g. assets.publishing.service.gov.uk for gov.uk).
        # Scope is controlled by DEPTH_LIMIT instead (passed via CrawlerProcess).

        self.target_extensions = set(
            e.lower().lstrip(".")
            for e in (allowed_file_types or settings.CRAWLER_ALLOWED_FILE_TYPES)
        )
        self.output_dir = output_dir or settings.TEMP_DIR
        self.downloaded_files: list[str] = []
        self.source_urls: dict[str, str] = {}       # local_path -> source_url
        self.response_headers: dict[str, dict] = {} # local_path -> {etag, last_modified}
        self.failed_urls: list[dict[str, str]] = []
        self.failure_count: int = 0
        self.full_download = full_download
        self._result_queue = result_queue  # optional streaming queue

        # CrawlSpider._compile_rules() reads self.rules inside super().__init__(),
        # so setting the instance attribute here overrides the class-level default.
        # When crawl_images=True, also extract <img src> to discover embedded images.
        if crawl_images:
            self.rules = (
                Rule(
                    LinkExtractor(
                        deny_extensions=[],
                        tags=["a", "area", "img"],
                        attrs=["href", "src"],
                    ),
                    callback="parse_link",
                    follow=True,
                    errback="handle_error",
                    process_links="log_discovered_file_links",
                ),
            )

        super().__init__(*args, **kwargs)

    # deny_extensions=[] overrides Scrapy's built-in deny list, which blocks pdf,
    # doc, docx, xls, xlsx, ppt, pptx, mp3, mp4, zip, rar and many more by default.
    # We follow all links and filter by target_extensions in parse_link instead.
    rules = (
        Rule(
            LinkExtractor(deny_extensions=[]),
            callback="parse_link",
            follow=True,
            errback="handle_error",
            process_links="log_discovered_file_links",
        ),
    )

    def log_discovered_file_links(self, links):
        """
        Called by the Rule once per page response, with all links extracted from that page.
        Logs every target-extension file URL found, including its domain (cross-domain links
        are flagged explicitly since they were silently dropped in older versions).
        """
        file_links = [
            link for link in links
            if Path(urlparse(link.url).path.lower()).suffix.lstrip(".") in self.target_extensions
        ]

        if file_links:
            self.logger.info(
                "Page scan found %d target file(s) | queuing downloads:",
                len(file_links),
            )
            for link in file_links:
                ext = Path(urlparse(link.url).path.lower()).suffix.lstrip(".")
                link_domain = urlparse(link.url).netloc
                cross_domain = link_domain != self.start_domain
                self.logger.info(
                    "  -> File queued | ext=.%s | cross-domain=%s | url=%s | text=%s",
                    ext,
                    cross_domain,
                    link.url,
                    link.text.strip() if link.text else "<no text>",
                )
        else:
            self.logger.info("Page scan found 0 target files on this page")

        return links

    def start_requests(self):
        # Settings are applied via CrawlerProcess; read them back from the
        # crawler settings object so the log reflects what Scrapy is actually using.
        self.logger.info(
            "Crawl starting | url=%s | start_domain=%s | depth_limit=%s | "
            "file_types=%s | partial_download=%s | autothrottle=%s",
            self.start_urls[0],
            self.start_domain,
            self.crawler.settings.getint("DEPTH_LIMIT"),
            sorted(self.target_extensions),
            self.crawler.settings.getbool("PARTIAL_DOWNLOAD_ENABLED"),
            self.crawler.settings.getbool("AUTOTHROTTLE_ENABLED"),
        )
        yield from super().start_requests()

    def parse_start_url(self, response):
        content_type = (
            response.headers.get("Content-Type", b"")
            .decode("utf-8", errors="ignore")
            .split(";")[0]
            .strip()
        )
        self.logger.info(
            "Start URL response | status=%d | content-type=%s | url=%s",
            response.status, content_type, response.url,
        )
        return []

    def handle_error(self, failure):
        url = failure.request.url if failure.request else "<unknown>"
        msg = repr(failure.value)
        self.logger.warning(
            "Request failed (retries exhausted) | url=%s | error=%s", url, msg
        )
        self.failed_urls.append({"url": url, "error": msg})
        self.failure_count += 1
        return None

    def parse_link(self, response):
        url = response.url
        content_type = (
            response.headers.get("Content-Type", b"")
            .decode("utf-8", errors="ignore")
            .split(";")[0]
            .strip()
        )
        parsed = urlparse(url)
        path = parsed.path.lower()
        ext = Path(path).suffix.lstrip(".")

        self.logger.info(
            "Visited | status=%d | content-type=%s | ext=%s | url=%s",
            response.status,
            content_type,
            f".{ext}" if ext else "<none>",
            url,
        )

        if ext not in self.target_extensions:
            # HTML pages and other navigation links — skipped silently at DEBUG
            # to avoid flooding logs. File-type skips are logged at INFO.
            if ext in ("html", "htm", ""):
                self.logger.debug("Skipped (HTML/navigation) | url=%s", url)
            else:
                self.logger.info(
                    "Skipped (extension not in target list) | ext=.%s | "
                    "target_types=%s | url=%s",
                    ext,
                    sorted(self.target_extensions),
                    url,
                )
            return

        # Check file size limit
        content_length = response.headers.get("Content-Length")
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > settings.CRAWLER_MAX_FILE_SIZE_MB:
                self.logger.info(
                    "Skipped (oversized) | size=%.1f MB | limit=%d MB | url=%s",
                    size_mb, settings.CRAWLER_MAX_FILE_SIZE_MB, url,
                )
                return

        self.logger.info("File candidate | ext=.%s | url=%s", ext, url)

        # Save to temp dir
        raw_name = Path(parsed.path).name or "download"
        stem = Path(raw_name).stem[:200]  # guard against OS 255-byte filename limit
        suffix = Path(raw_name).suffix
        filename = stem + suffix
        dest = self.output_dir / filename
        # Avoid collisions
        counter = 1
        while dest.exists():
            dest = self.output_dir / f"{Path(filename).stem}_{counter}{Path(filename).suffix}"
            counter += 1

        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(response.body)

        self.downloaded_files.append(str(dest))
        self.source_urls[str(dest)] = url

        # Capture HTTP change-detection headers for deduplication
        etag = response.headers.get("ETag", b"").decode("utf-8", errors="ignore").strip().strip('"') or None
        last_modified = response.headers.get("Last-Modified", b"").decode("utf-8", errors="ignore").strip() or None
        self.response_headers[str(dest)] = {"etag": etag, "last_modified": last_modified}

        if self._result_queue is not None:
            self._result_queue.put({
                "type": "file",
                "path": str(dest),
                "source_url": url,
                "etag": etag,
                "last_modified": last_modified,
            })

        self.logger.info(
            "File saved | url=%s | dest=%s | bytes_written=%d | "
            "partial=%s | total_downloaded=%d",
            url,
            dest,
            len(response.body),
            self.custom_settings.get("PARTIAL_DOWNLOAD_ENABLED"),
            len(self.downloaded_files),
        )

    def closed(self, reason):
        self.logger.info(
            "Crawl finished | reason=%s | files_downloaded=%d | "
            "request_failures=%d | start_url=%s",
            reason,
            len(self.downloaded_files),
            self.failure_count,
            self.start_urls[0],
        )
        if self._result_queue is not None:
            self._result_queue.put({
                "type": "done",
                "failed_urls": self.failed_urls,
                "failure_count": self.failure_count,
                "closed_reason": str(reason),
            })
