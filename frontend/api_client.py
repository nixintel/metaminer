"""Synchronous HTTP client for the Metaminer FastAPI backend."""
import os
import httpx

_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000").rstrip("/")
_API = _BASE_URL + "/api/v1"
_TIMEOUT = 30


def _get(path, **params):
    params = {k: v for k, v in params.items() if v is not None and v != ""}
    r = httpx.get(f"{_API}{path}", params=params, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _post(path, json=None):
    r = httpx.post(f"{_API}{path}", json=json, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _patch(path, json=None):
    r = httpx.patch(f"{_API}{path}", json=json, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _delete(path):
    r = httpx.delete(f"{_API}{path}", timeout=_TIMEOUT)
    r.raise_for_status()
    return r.status_code


# ── Health ────────────────────────────────────────────────────────────────────

def get_health():
    r = httpx.get(f"{_BASE_URL}/healthcheck", timeout=10)
    r.raise_for_status()
    return r.json()


# ── Projects ──────────────────────────────────────────────────────────────────

def list_projects():
    return _get("/projects")

def get_project(pid):
    return _get(f"/projects/{pid}")

def create_project(name, description=None):
    return _post("/projects", json={"name": name, "description": description})

def update_project(pid, **kwargs):
    return _patch(f"/projects/{pid}", json={k: v for k, v in kwargs.items()})

def delete_project(pid):
    return _delete(f"/projects/{pid}")


# ── Submissions ───────────────────────────────────────────────────────────────

def submit_manual(project_id, paths, retain_files=False, pdf_mode=False):
    return _post("/submissions/manual", json={
        "project_id": project_id,
        "paths": paths,
        "retain_files": retain_files,
        "pdf_mode": pdf_mode,
    })


# ── Crawl ─────────────────────────────────────────────────────────────────────

def submit_crawl(project_id, urls, depth_limit=None, allowed_file_types=None,
                 full_download=False, retain_files=False, deduplicate=True,
                 robotstxt_obey=True, crawl_images=False, allow_cross_domain=False):
    return _post("/crawl", json={
        "project_id": project_id,
        "urls": urls if isinstance(urls, list) else [urls],
        "depth_limit": depth_limit,
        "allowed_file_types": allowed_file_types or [],
        "full_download": full_download,
        "retain_files": retain_files,
        "deduplicate": deduplicate,
        "robotstxt_obey": robotstxt_obey,
        "crawl_images": crawl_images,
        "allow_cross_domain": allow_cross_domain,
    })


# ── Tasks ─────────────────────────────────────────────────────────────────────

def get_task_summary(project_id=None):
    return _get("/tasks/summary", project_id=project_id)

def list_tasks(project_id=None, status=None, task_type=None):
    return _get("/tasks", project_id=project_id, status=status, task_type=task_type)

def get_task(tid):
    return _get(f"/tasks/{tid}")

def cancel_task(tid):
    return _delete(f"/tasks/{tid}")


# ── Metadata ──────────────────────────────────────────────────────────────────

def search_metadata(**kwargs):
    return _get("/metadata", **kwargs)

def get_metadata(rid):
    return _get(f"/metadata/{rid}")

def set_metadata_interesting(rid, interesting):
    return _patch(f"/metadata/{rid}", json={"interesting": interesting})

def delete_metadata(rid):
    return _delete(f"/metadata/{rid}")

def query_metadata_tree(body):
    r = httpx.post(f"{_API}/metadata/query", json=body, timeout=_TIMEOUT)
    r.raise_for_status()
    return r.json()


# ── Filters (auto-tagging criteria) ─────────────────────────────────────────────

def list_filters(project_id=None):
    return _get("/filters", project_id=project_id)

def get_filter(fid):
    return _get(f"/filters/{fid}")

def create_filter(**kwargs):
    return _post("/filters", json=kwargs)

def update_filter(fid, **kwargs):
    return _patch(f"/filters/{fid}", json={k: v for k, v in kwargs.items() if v is not None})

def delete_filter(fid):
    return _delete(f"/filters/{fid}")

def backfill_filter(fid, project_id=None):
    return _post(f"/filters/{fid}/backfill", json={"project_id": project_id})


# ── Filter groups (OR-bundles of single filters) ─────────────────────────────────

def list_filter_groups(project_id=None):
    return _get("/filter-groups", project_id=project_id)

def get_filter_group(gid):
    return _get(f"/filter-groups/{gid}")

def create_filter_group(**kwargs):
    return _post("/filter-groups", json=kwargs)

def update_filter_group(gid, **kwargs):
    # Keep filter_ids even when [] (means "clear membership"); only drop None values.
    return _patch(f"/filter-groups/{gid}", json={k: v for k, v in kwargs.items() if v is not None})

def delete_filter_group(gid):
    return _delete(f"/filter-groups/{gid}")

def backfill_filter_group(gid, project_id=None):
    return _post(f"/filter-groups/{gid}/backfill", json={"project_id": project_id})


# ── Logs ──────────────────────────────────────────────────────────────────────

def get_logs(level=None, task_id=None, submission_id=None,
             since=None, until=None, limit=100, offset=0):
    return _get("/logs", level=level, task_id=task_id, submission_id=submission_id,
                since=since, until=until, limit=limit, offset=offset)


# ── Scheduled crawls ──────────────────────────────────────────────────────────

def list_scheduled_crawls(project_id=None):
    return _get("/scheduled-crawls", project_id=project_id)

def get_scheduled_crawl(sid):
    return _get(f"/scheduled-crawls/{sid}")

def create_scheduled_crawl(urls, **kwargs):
    return _post("/scheduled-crawls", json={"urls": urls if isinstance(urls, list) else [urls], **kwargs})

def update_scheduled_crawl(sid, **kwargs):
    payload = {k: v for k, v in kwargs.items() if v is not None}
    return _patch(f"/scheduled-crawls/{sid}", json=payload)

def delete_scheduled_crawl(sid):
    return _delete(f"/scheduled-crawls/{sid}")


# ── Telegram ──────────────────────────────────────────────────────────────────

def get_telegram_status():
    return _get("/telegram/status")

def upsert_telegram_credentials(api_id, api_hash):
    return _post("/telegram/credentials", json={"api_id": api_id, "api_hash": api_hash})

def delete_telegram_credentials():
    return _delete("/telegram/credentials")

def telegram_auth_start(phone):
    return _post("/telegram/auth/start", json={"phone": phone})

def telegram_auth_verify(phone, code, password=None):
    payload = {"phone": phone, "code": code}
    if password:
        payload["password"] = password
    return _post("/telegram/auth/verify", json=payload)

def list_telegram_scrapes(project_id=None, status=None):
    return _get("/telegram/scrape", project_id=project_id, status=status)

def get_telegram_scrape(tid):
    return _get(f"/telegram/scrape/{tid}")

def submit_telegram_scrape(**kwargs):
    return _post("/telegram/scrape", json=kwargs)

def cancel_telegram_scrape(tid):
    return _delete(f"/telegram/scrape/{tid}")

def list_scheduled_telegram_scrapes(project_id=None):
    return _get("/telegram/scheduled", project_id=project_id)

def create_scheduled_telegram_scrape(**kwargs):
    return _post("/telegram/scheduled", json=kwargs)

def update_scheduled_telegram_scrape(sid, **kwargs):
    payload = {k: v for k, v in kwargs.items() if v is not None}
    return _patch(f"/telegram/scheduled/{sid}", json=payload)

def delete_scheduled_telegram_scrape(sid):
    return _delete(f"/telegram/scheduled/{sid}")
