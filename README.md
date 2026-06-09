# Metaminer

Metaminer is a metadata extraction service that processes file uploads and web crawls. Users can submit files to extract metadata and save it to a database. The crawl mode uses Scrapy to crawl a target website and discover files likely to contain metadata. All extracted metadata is saved to a database. Crawling tasks can be on-demand or scheduled. Scheduled tasks revisit a website periodically but will only add new metadata not previously collected.

Crawl options are highly configurable and can be tweaked to optimise collection. See API docs for more info.

Metaminer also builds in a PDF rollback mode. For some PDF documents Exiftool can rollback changes and recover previously hidden metadata. Metaminer will always attempt to do this by default but it can be disabled.

It uses FastAPI for the REST interface, SQLAlchemy for database access, Celery + Redis for async task processing, and Scrapy for web crawl file discovery and download. A Flask web GUI is available for all core functionality.

> **Note:** The Telegram scraping feature is not yet functional. The API endpoints and UI are present but the integration is incomplete.

---

## Deploying with Docker Compose

### Prerequisites

- Docker and Docker Compose installed
- Git

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/nixintel/metaminer
cd metaminer

# 2. Create your environment file
cp .env.example .env
# Edit .env if you need to change any defaults (database credentials, crawler settings, etc.)

# 3. Build and start all services
docker compose up --build -d

# 4. Verify everything is running
docker compose ps
curl http://localhost:8000/healthcheck
```

The web GUI is available at `http://localhost:5000`.

The API is available at `http://localhost:8000`. Swagger UI is at `http://localhost:8000/docs` and ReDoc at `http://localhost:8000/redoc`.

To stop the stack:
```bash
docker compose down
```

To stop and remove all data volumes:
```bash
docker compose down -v
```

---

## API Overview

Base API prefix: `/api/v1`
Interactive docs: `/docs` (Swagger UI) and `/redoc`

---

### Health

**`GET /healthcheck`**

Checks database, Redis, and exiftool availability. Returns `503` if any service is unavailable.

```bash
curl http://localhost:8000/healthcheck
```

Response:
```json
{"db": "ok", "redis": "ok", "exiftool": "ok", "status": "ok"}
```

---

### Projects

| Method | Path | Description |
| ------ | ---- | ----------- |
| `POST` | `/api/v1/projects` | Create a project — returns `201` |
| `GET` | `/api/v1/projects` | List all projects |
| `GET` | `/api/v1/projects/{project_id}` | Get a project by ID |
| `PATCH` | `/api/v1/projects/{project_id}` | Update name or description |
| `DELETE` | `/api/v1/projects/{project_id}` | Delete a project — returns `204` |

**Create a project:**
```bash
curl -X POST http://localhost:8000/api/v1/projects \
  -H 'Content-Type: application/json' \
  -d '{"name": "My Project", "description": "optional"}'
```

---

### Submissions

Submit files for metadata extraction. Files and directories are processed as a background task.

| Method | Path | Description |
| ------ | ---- | ----------- |
| `POST` | `/api/v1/submissions/manual` | Enqueue batch extraction across files/directories — async, returns `202` |

**Request body:**
```json
{
  "project_id": 1,
  "paths": ["/app/data/folder", "/app/data/extra.xlsx"],
  "retain_files": false,
  "pdf_mode": true
}
```

- `paths`: list of file or directory paths; directories are walked recursively
- `retain_files`: copy files into the project's retained storage after extraction
- `pdf_mode`: run exiftool rollback extraction on PDFs in addition to standard extraction

Returns a task ID to poll for progress.

```bash
curl -X POST http://localhost:8000/api/v1/submissions/manual \
  -H 'Content-Type: application/json' \
  -d '{"project_id": 1, "paths": ["/app/data/report.pdf"], "retain_files": false, "pdf_mode": true}'
```

---

### Crawl

Crawl a website and extract metadata from all discovered files.

**`POST /api/v1/crawl`** — enqueues an async task, returns `202`

Request body:
```json
{
  "project_id": 1,
  "url": "https://example.com",
  "depth_limit": 3,
  "allowed_file_types": ["pdf", "docx", "xlsx"],
  "full_download": false,
  "retain_files": false,
  "deduplicate": true,
  "robotstxt_obey": true,
  "crawl_images": false,
  "allow_cross_domain": false
}
```

| Field | Default | Description |
| ----- | ------- | ----------- |
| `depth_limit` | 3 | How many link-hops from the start URL to follow |
| `allowed_file_types` | all | File extensions to download (e.g. `["pdf", "docx"]`) |
| `full_download` | `false` | Download complete files; `false` uses partial download for metadata only |
| `retain_files` | `false` | Keep downloaded files in project storage |
| `deduplicate` | `true` | Skip files already processed in this project (matched by URL + ETag/hash) |
| `robotstxt_obey` | `true` | Respect `robots.txt` |
| `crawl_images` | `false` | Include image files in crawl |
| `allow_cross_domain` | `false` | Follow links to external domains |

```bash
curl -X POST http://localhost:8000/api/v1/crawl \
  -H 'Content-Type: application/json' \
  -d '{"project_id": 1, "url": "https://example.com", "depth_limit": 2, "allowed_file_types": ["pdf"], "deduplicate": true}'
```

---

### Scheduled Crawls

Run a crawl automatically on a recurring interval.

| Method | Path | Description |
| ------ | ---- | ----------- |
| `POST` | `/api/v1/scheduled-crawls` | Create a scheduled crawl — returns `201` |
| `GET` | `/api/v1/scheduled-crawls` | List all scheduled crawls (filter: `?project_id=`) |
| `GET` | `/api/v1/scheduled-crawls/{schedule_id}` | Get a scheduled crawl by ID |
| `PATCH` | `/api/v1/scheduled-crawls/{schedule_id}` | Update settings or toggle `is_active` |
| `DELETE` | `/api/v1/scheduled-crawls/{schedule_id}` | Delete a scheduled crawl — returns `204` |

**Create body:**
```json
{
  "project_id": 1,
  "url": "https://example.com",
  "frequency_seconds": 86400,
  "depth_limit": 3,
  "allowed_file_types": ["pdf"],
  "full_download": false,
  "retain_files": false,
  "crawl_images": false,
  "robotstxt_obey": true
}
```

`frequency_seconds` minimum is `60`. The response includes `last_run_at` and `next_run_at`.

**Pause a schedule:**
```bash
curl -X PATCH http://localhost:8000/api/v1/scheduled-crawls/1 \
  -H 'Content-Type: application/json' \
  -d '{"is_active": false}'
```

---

### Tasks

Monitor and cancel async tasks (manual submissions and crawls).

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/api/v1/tasks` | List tasks (filter: `?project_id=`, `?status=`, `?task_type=`) |
| `GET` | `/api/v1/tasks/summary` | Aggregated counts by status (filter: `?project_id=`) |
| `GET` | `/api/v1/tasks/{task_id}` | Get a task by ID |
| `DELETE` | `/api/v1/tasks/{task_id}` | Cancel a pending or running task — returns `409` if already terminal |

Task status values: `pending`, `running`, `completed`, `failed`, `cancelled`

Task type values: `manual`, `crawl`, `telegram`

Task response includes: `files_found`, `files_processed`, `crawl_failures`, `crawl_errors`, `skipped_duplicates`, `error_message`, `created_at`, `started_at`, `completed_at`

```bash
# List running tasks for a project
curl "http://localhost:8000/api/v1/tasks?project_id=1&status=running"

# Get task counts for a dashboard
curl "http://localhost:8000/api/v1/tasks/summary?project_id=1"

# Cancel a task
curl -X DELETE http://localhost:8000/api/v1/tasks/42
```

---

### Metadata

Search and filter extracted metadata records.

**`GET /api/v1/metadata`**

All parameters are optional.

| Parameter | Description |
| --------- | ----------- |
| `project_id` | Filter by project |
| `task_id` | Filter by task — returns only records from files submitted by that task |
| `file_type` | Exact match (case-insensitive), e.g. `PDF` |
| `file_type__in` | Comma-separated list, e.g. `PDF,DOCX,XLSX` |
| `author` | Partial match (case-insensitive) |
| `title` | Partial match (case-insensitive) |
| `creator_tool` | Partial match (case-insensitive) |
| `producer` | Partial match (case-insensitive) |
| `mime_type` | Exact match |
| `pdf_variant` | `original` or `rollback` |
| `submission_mode` | `manual` or `crawl` |
| `source_url__contains` | Substring match on the source URL |
| `extracted_after` | ISO 8601 datetime — records extracted after this time |
| `extracted_before` | ISO 8601 datetime — records extracted before this time |
| `q` | Full-text search across author, title, creator_tool, producer, file name, and raw exiftool JSON |
| `raw_contains` | Substring search inside the raw exiftool JSON |
| `sort_by` | Field to sort by (default: `extracted_at`) |
| `order` | `asc` or `desc` (default: `desc`) |
| `limit` | Results per page, 1–500 (default: `50`) |
| `offset` | Pagination offset (default: `0`) |

```bash
# PDFs authored by Alice in project 1
curl "http://localhost:8000/api/v1/metadata?project_id=1&file_type=PDF&author=alice"

# Files from a crawl, paginated
curl "http://localhost:8000/api/v1/metadata?project_id=1&submission_mode=crawl&limit=20&offset=40"

# Full-text search
curl "http://localhost:8000/api/v1/metadata?project_id=1&q=annual+report"

# All metadata from a specific task
curl "http://localhost:8000/api/v1/metadata?task_id=42"
```

**`POST /api/v1/metadata/query`**

Accepts a nested JSON filter tree supporting `AND`/`OR` logic across multiple fields.

```json
{
  "operator": "AND",
  "conditions": [
    {"field": "author", "op": "contains", "value": "Smith"},
    {
      "operator": "OR",
      "conditions": [
        {"field": "title", "op": "contains", "value": "report"},
        {"field": "file_type", "op": "equals", "value": "PDF"}
      ]
    }
  ],
  "sort_by": "extracted_at",
  "order": "desc",
  "limit": 50,
  "offset": 0
}
```

Supported operators: `contains`, `equals`, `starts_with`, `before`, `after`, `in`

---

### Logs

**`GET /api/v1/logs`**

| Parameter | Description |
| --------- | ----------- |
| `level` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| `task_id` | Filter by task |
| `submission_id` | Filter by submission |
| `since` | ISO 8601 datetime |
| `until` | ISO 8601 datetime |
| `limit` | 1–1000 (default: `100`) |
| `offset` | Pagination offset (default: `0`) |

```bash
curl "http://localhost:8000/api/v1/logs?level=ERROR&limit=50"
```

---

## Running Tests

**Unit tests** (no infrastructure required):

```bash
docker compose exec api pytest -m unit -v
```

**Integration tests** (requires the postgres service to be running):

```bash
# Create the test database once
docker compose exec postgres psql -U metaminer -c "CREATE DATABASE metaminer_test;"

# Run integration tests
docker compose exec -e TEST_DATABASE_URL=postgresql+asyncpg://metaminer:metaminer@postgres:5432/metaminer_test api pytest -m integration -v
```

**All tests:**

```bash
docker compose exec -e TEST_DATABASE_URL=postgresql+asyncpg://metaminer:metaminer@postgres:5432/metaminer_test api pytest -v
```
