import csv
import io
import json
import logging
import os
import uuid
from urllib.parse import urlencode

import httpx
from flask import Flask, Response, flash, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

from frontend import api_client

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-prod")
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500 MB upload limit

TEMP_DIR = os.environ.get("TEMP_DIR", "/app/data/temp")

logger = logging.getLogger("metaminer.frontend")


def _api_error(e: Exception, action: str = "complete this action") -> str:
    if isinstance(e, httpx.HTTPStatusError):
        try:
            detail = e.response.json().get("detail", str(e))
        except Exception:
            detail = str(e)
        return f"API error: {detail}"
    return f"Could not {action}: {e}"


def _projects():
    try:
        return api_client.list_projects()
    except Exception:
        return []


def _save_upload(file) -> str:
    """Save an uploaded FileStorage object to the shared temp dir. Returns the saved path."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    ext = os.path.splitext(secure_filename(file.filename))[1]
    filename = f"{uuid.uuid4().hex}{ext}"
    path = os.path.join(TEMP_DIR, filename)
    file.save(path)
    return path


# ── CSV export helpers ──────────────────────────────────────────────────────

# Core columns, in export order. id / project_name / source_url lead; the rest
# of the promoted metadata fields follow. Flattened exiftool columns are appended
# after these by _records_to_csv().
CORE_CSV_FIELDS = [
    "id", "project_name", "source_url",
    "file_name", "file_type", "mime_type", "file_size",
    "author", "title", "creator_tool", "producer", "pdf_version",
    "create_date", "modify_date", "extracted_at", "submission_mode",
]

# Backend caps limit at 500; we page through with offset to export everything.
_EXPORT_PAGE_SIZE = 500
_EXPORT_MAX_PAGES = 200  # safety guard (~100k rows) — logged, never silent


def _flatten_json(obj, prefix=""):
    """Recursively flatten a nested exiftool dict into {Group:Key: value}.

    Grouped output ({"PDF": {"Author": …}}) becomes "PDF:Author"; flat output
    ({"Author": …}) stays "Author". Lists are JSON-encoded so each field stays a
    single column.
    """
    out = {}
    if not isinstance(obj, dict):
        return out
    for k, v in obj.items():
        key = f"{prefix}:{k}" if prefix else str(k)
        if isinstance(v, dict):
            out.update(_flatten_json(v, key))
        elif isinstance(v, list):
            out[key] = json.dumps(v, ensure_ascii=False)
        else:
            out[key] = v
    return out


def _records_to_csv(records) -> str:
    """Build a CSV string from metadata records.

    Columns: CORE_CSV_FIELDS, then every flattened exiftool field (union across
    all rows, sorted). A record whose raw_json failed to parse upstream (arrives
    as a string instead of a dict) is flagged in an `exif_parse_error` column
    rather than having its exif data silently dropped.
    """
    flattened = []      # one flattened-exif dict per record, parallel to `records`
    exif_keys = set()
    for r in records:
        raw = r.get("raw_json")
        if isinstance(raw, dict):
            flat = _flatten_json(raw)
        elif isinstance(raw, str) and raw.strip():
            flat = {"exif_parse_error": "raw_json could not be parsed by the backend"}
        else:
            flat = {}
        flattened.append(flat)
        exif_keys.update(flat.keys())

    has_error_col = "exif_parse_error" in exif_keys
    error_col = ["exif_parse_error"] if has_error_col else []
    # Exclude core names defensively so DictWriter never sees a duplicate column.
    exif_cols = sorted(
        k for k in exif_keys
        if k != "exif_parse_error" and k not in CORE_CSV_FIELDS
    )
    fieldnames = CORE_CSV_FIELDS + error_col + exif_cols

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r, flat in zip(records, flattened):
        row = {f: r.get(f, "") for f in CORE_CSV_FIELDS}
        row.update({k: ("" if v is None else v) for k, v in flat.items()})
        writer.writerow(row)
    return output.getvalue()


def _paginate(fetch_page):
    """Fetch every matching row by paging through the backend.

    `fetch_page(offset, limit)` returns a list of records. Stops once a page comes
    back shorter than the page size. Bounded by _EXPORT_MAX_PAGES; if the cap is
    hit a warning is logged so truncation is never silent.
    """
    records = []
    offset = 0
    for _ in range(_EXPORT_MAX_PAGES):
        batch = fetch_page(offset, _EXPORT_PAGE_SIZE)
        records.extend(batch)
        if len(batch) < _EXPORT_PAGE_SIZE:
            return records
        offset += _EXPORT_PAGE_SIZE
    logger.warning(
        "Export pagination hit the %d-page cap (%d rows accumulated); export may be truncated.",
        _EXPORT_MAX_PAGES, len(records),
    )
    return records


# ── On-page pagination (visible results) ────────────────────────────────────

# Page-size choices offered in the UI. "all" shows every matching row on one page.
PAGE_SIZE_OPTIONS = ["10", "50", "100", "500", "1000", "all"]
DEFAULT_PAGE_SIZE = "50"


def _parse_page_size(raw):
    """'all' -> None (no per-page cap); otherwise a positive int (default 50)."""
    if raw == "all":
        return None
    try:
        n = int(raw)
        return n if n > 0 else 50
    except (TypeError, ValueError):
        return 50


def _fetch_window(fetch_page, offset, count):
    """Fetch up to `count` rows starting at `offset`, chunked into <=500 backend
    calls (the API caps limit at 500). `count=None` fetches everything from offset.

    Returns (records, has_next). has_next is True when more rows likely exist past
    the returned window (i.e. the window filled completely on a chunk boundary).
    """
    records = []
    cur = offset
    for _ in range(_EXPORT_MAX_PAGES):
        if count is None:
            lim = _EXPORT_PAGE_SIZE
        else:
            lim = min(_EXPORT_PAGE_SIZE, count - len(records))
            if lim <= 0:
                return records, True   # window filled on a chunk boundary; more may exist
        batch = fetch_page(cur, lim)
        records.extend(batch)
        cur += len(batch)
        if len(batch) < lim:
            return records, False      # backend exhausted — definitely no next page
    logger.warning("Result window hit the %d-page cap at offset %d.", _EXPORT_MAX_PAGES, offset)
    return records, True


def _get_pager(page, page_size_raw, count, offset, records, has_next):
    """Pager context for GET (keyword/advanced) results — navigation via hx-get
    links that carry the current search params (minus page)."""
    base = {k: v for k, v in request.args.items() if k != "page"}
    return {
        "mode": "get",
        "page": page,
        "page_size": page_size_raw,
        "base_qs": urlencode(base),
        "has_prev": count is not None and page > 0,
        "has_next": count is not None and has_next,
        "start": offset + 1 if records else 0,
        "end": offset + len(records),
    }


# ── Healthcheck ───────────────────────────────────────────────────────────────

@app.get("/healthcheck")
def healthcheck():
    return {"status": "ok", "service": "metaminer-frontend"}


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.get("/")
def dashboard():
    health = {}
    summary = {}
    recent_tasks = []
    try:
        h = api_client.get_health()
        health = {
            "database": h.get("db", "unknown"),
            "redis": h.get("redis", "unknown"),
            "exiftool": h.get("exiftool", "unknown"),
        }
    except Exception as e:
        flash(f"Could not reach API: {e}", "error")
    try:
        summary = api_client.get_task_summary()
    except Exception:
        pass
    try:
        recent_tasks = api_client.list_tasks()[:10]
    except Exception:
        pass
    return render_template("dashboard.html", health=health, summary=summary, recent_tasks=recent_tasks)


# ── Projects ──────────────────────────────────────────────────────────────────

@app.get("/projects")
def projects_list():
    projects = []
    try:
        projects = api_client.list_projects()
    except Exception as e:
        flash(_api_error(e, "load projects"), "error")
    return render_template("projects/list.html", projects=projects)


@app.get("/projects/new")
def project_new():
    return render_template("projects/form.html", project=None,
                           action=url_for("project_create"))


@app.post("/projects/new")
def project_create():
    name = request.form.get("name", "").strip()
    description = request.form.get("description", "").strip() or None
    if not name:
        flash("Name is required.", "error")
        return redirect(url_for("project_new"))
    try:
        api_client.create_project(name=name, description=description)
        flash(f"Project '{name}' created.", "success")
    except Exception as e:
        flash(_api_error(e, "create project"), "error")
    return redirect(url_for("projects_list"))


@app.get("/projects/<int:pid>/edit")
def project_edit(pid):
    try:
        project = api_client.get_project(pid)
    except Exception as e:
        flash(_api_error(e, "load project"), "error")
        return redirect(url_for("projects_list"))
    return render_template("projects/form.html", project=project,
                           action=url_for("project_update", pid=pid))


@app.post("/projects/<int:pid>/edit")
def project_update(pid):
    name = request.form.get("name", "").strip()
    description = request.form.get("description", "").strip() or None
    if not name:
        flash("Name is required.", "error")
        return redirect(url_for("project_edit", pid=pid))
    try:
        api_client.update_project(pid, name=name, description=description)
        flash("Project updated.", "success")
    except Exception as e:
        flash(_api_error(e, "update project"), "error")
    return redirect(url_for("projects_list"))


@app.post("/projects/<int:pid>/delete")
def project_delete(pid):
    try:
        api_client.delete_project(pid)
        flash("Project deleted.", "success")
    except Exception as e:
        flash(_api_error(e, "delete project"), "error")
    return redirect(url_for("projects_list"))


# ── Submit: Manual ────────────────────────────────────────────────────────────

@app.get("/submit/manual")
def submit_manual():
    default_project_id = request.args.get("project_id", type=int)
    return render_template("submit/manual.html", projects=_projects(),
                           default_project_id=default_project_id)


@app.post("/submit/manual")
def submit_manual_post():
    project_id = request.form.get("project_id", type=int)
    retain_files = bool(request.form.get("retain_files"))
    pdf_mode = bool(request.form.get("pdf_mode"))
    all_uploads = [f for f in request.files.getlist("files") if f and f.filename]
    if not project_id:
        flash("Project is required.", "error")
        return redirect(url_for("submit_manual"))
    if not all_uploads:
        flash("Please select at least one file.", "error")
        return redirect(url_for("submit_manual"))
    try:
        paths = [_save_upload(f) for f in all_uploads]
        task = api_client.submit_manual(project_id, paths, retain_files, pdf_mode)
        flash(f"Manual job queued with {len(paths)} file(s). Task ID: {task.get('id')}", "success")
        return redirect(url_for("task_detail", tid=task.get("id")))
    except Exception as e:
        flash(_api_error(e, "submit files"), "error")
    return redirect(url_for("submit_manual"))


# ── Submit: Crawl ─────────────────────────────────────────────────────────────

@app.get("/submit/crawl")
def submit_crawl():
    project_id = request.args.get("project_id", "")
    return render_template("submit/crawl.html", projects=_projects(),
                           form={"project_id": str(project_id)})


@app.post("/submit/crawl")
def submit_crawl_post():
    project_id = request.form.get("project_id", type=int)
    depth_limit = request.form.get("depth_limit", type=int)
    file_types = request.form.getlist("allowed_file_types")
    extra = [t.strip().upper() for t in request.form.get("extra_types", "").split(",") if t.strip()]
    all_types = list(set(file_types + extra))

    urls_raw = request.form.get("urls", "")
    urls = [u.strip() for u in urls_raw.splitlines() if u.strip()]

    # Capture all submitted values so we can re-populate the form on error
    form = {
        "project_id": str(project_id or ""),
        "urls": urls_raw,
        "depth_limit": str(depth_limit or ""),
        "allowed_file_types": file_types,
        "extra_types": request.form.get("extra_types", ""),
        "full_download": bool(request.form.get("full_download")),
        "retain_files": bool(request.form.get("retain_files")),
        "deduplicate": bool(request.form.get("deduplicate")),
        "robotstxt_obey": bool(request.form.get("robotstxt_obey")),
        "crawl_images": bool(request.form.get("crawl_images")),
        "allow_cross_domain": bool(request.form.get("allow_cross_domain")),
    }

    if not project_id or not urls:
        flash("Project and at least one URL are required.", "error")
        return render_template("submit/crawl.html", projects=_projects(), form=form)
    try:
        task = api_client.submit_crawl(
            project_id=project_id,
            urls=urls,
            depth_limit=depth_limit,
            allowed_file_types=all_types or None,
            full_download=form["full_download"],
            retain_files=form["retain_files"],
            deduplicate=form["deduplicate"],
            robotstxt_obey=form["robotstxt_obey"],
            crawl_images=form["crawl_images"],
            allow_cross_domain=form["allow_cross_domain"],
        )
        flash(f"Crawl queued. Task ID: {task.get('id')}", "success")
        return redirect(url_for("task_detail", tid=task.get("id")))
    except Exception as e:
        flash(_api_error(e, "submit crawl"), "error")
    return render_template("submit/crawl.html", projects=_projects(), form=form)


# ── Tasks ─────────────────────────────────────────────────────────────────────

@app.get("/tasks")
def tasks_list():
    project_id = request.args.get("project_id", type=int)
    status = request.args.get("status") or None
    task_type = request.args.get("task_type") or None
    offset = request.args.get("offset", 0, type=int)
    tasks = []
    try:
        tasks = api_client.list_tasks(project_id=project_id, status=status, task_type=task_type)
    except Exception as e:
        flash(_api_error(e, "load tasks"), "error")
    return render_template("tasks/list.html", tasks=tasks, projects=_projects())


@app.get("/tasks/<int:tid>")
def task_detail(tid):
    try:
        task = api_client.get_task(tid)
    except Exception as e:
        flash(_api_error(e, "load task"), "error")
        return redirect(url_for("tasks_list"))
    logs = []
    try:
        logs = api_client.get_logs(task_id=tid, limit=200)
    except Exception:
        pass
    return render_template("tasks/detail.html", task=task, logs=logs)


@app.post("/tasks/<int:tid>/cancel")
def task_cancel(tid):
    try:
        api_client.cancel_task(tid)
        flash("Task cancellation requested.", "success")
    except Exception as e:
        flash(_api_error(e, "cancel task"), "error")
    return redirect(url_for("task_detail", tid=tid))


@app.get("/tasks/<int:tid>/_progress")
def task_progress(tid):
    try:
        task = api_client.get_task(tid)
    except Exception:
        return "<div id='task-progress'>Error loading status.</div>"
    return render_template("tasks/_progress.html", task=task)


# ── Metadata ──────────────────────────────────────────────────────────────────

@app.get("/metadata")
def metadata_search():
    mode = request.args.get("mode", "keyword")
    projects = _projects()
    records = []
    searched = bool(request.args)
    page_size_raw = request.args.get("page_size", DEFAULT_PAGE_SIZE)
    page = request.args.get("page", 0, type=int)
    pager = None

    if searched and mode != "query":
        try:
            kwargs = {k: v for k, v in request.args.items()
                      if v and k not in ("mode", "page", "page_size", "limit", "offset")}
            count = _parse_page_size(page_size_raw)
            offset = page * count if count is not None else 0
            records, has_next = _fetch_window(
                lambda off, lim: api_client.search_metadata(**kwargs, offset=off, limit=lim),
                offset, count,
            )
            pager = _get_pager(page, page_size_raw, count, offset, records, has_next)
        except Exception as e:
            flash(_api_error(e, "search metadata"), "error")

    # Query builder default state
    qb_fields  = [request.args.get(f"qb_field_{i}", "")  for i in range(5)]
    qb_ops     = [request.args.get(f"qb_op_{i}", "contains") for i in range(5)]
    qb_values  = [request.args.get(f"qb_value_{i}", "") for i in range(5)]
    qb_operator = request.args.get("qb_operator", "AND")
    qb_page_size = request.args.get("qb_page_size", DEFAULT_PAGE_SIZE)

    export_href = url_for("metadata_export") + "?" + request.query_string.decode()

    return render_template(
        "metadata/search.html",
        mode=mode, projects=projects, records=records,
        searched=searched, export_href=export_href, pager=pager,
        page_size=page_size_raw, page_size_options=PAGE_SIZE_OPTIONS,
        qb_fields=qb_fields, qb_ops=qb_ops, qb_values=qb_values,
        qb_operator=qb_operator, qb_page_size=qb_page_size,
    )


@app.get("/metadata/results")
def metadata_results():
    """HTMX partial — returns just the results table for one page of results."""
    records = []
    searched = bool(request.args)
    page_size_raw = request.args.get("page_size", DEFAULT_PAGE_SIZE)
    page = request.args.get("page", 0, type=int)
    pager = None
    if searched:
        try:
            kwargs = {k: v for k, v in request.args.items()
                      if v and k not in ("mode", "page", "page_size", "limit", "offset")}
            count = _parse_page_size(page_size_raw)
            offset = page * count if count is not None else 0
            records, has_next = _fetch_window(
                lambda off, lim: api_client.search_metadata(**kwargs, offset=off, limit=lim),
                offset, count,
            )
            pager = _get_pager(page, page_size_raw, count, offset, records, has_next)
        except Exception as e:
            return f'<div id="results-container"><p class="flash flash-error">{_api_error(e)}</p></div>'
    export_href = url_for("metadata_export") + "?" + request.query_string.decode()
    return render_template("metadata/_results.html", records=records,
                           searched=searched, export_href=export_href, pager=pager)


@app.post("/metadata/query-results")
def metadata_query_results():
    """HTMX partial — executes a query builder POST and returns one page of results."""
    operator = request.form.get("qb_operator", "AND")
    page_size_raw = request.form.get("qb_page_size", DEFAULT_PAGE_SIZE)
    page = request.form.get("qb_page", 0, type=int)
    conditions = []
    for i in range(5):
        field = request.form.get(f"qb_field_{i}", "")
        op    = request.form.get(f"qb_op_{i}", "contains")
        value = request.form.get(f"qb_value_{i}", "")
        if field and value:
            conditions.append({"field": field, "op": op, "value": value})

    if not conditions:
        return '<div id="results-container"><p>Add at least one condition.</p></div>'

    count = _parse_page_size(page_size_raw)
    offset = page * count if count is not None else 0

    def fetch_page(off, lim):
        body = {"operator": operator, "conditions": conditions, "limit": lim, "offset": off}
        return api_client.query_metadata_tree(body)

    records = []
    try:
        records, has_next = _fetch_window(fetch_page, offset, count)
    except Exception as e:
        return f'<div id="results-container"><p class="flash flash-error">{_api_error(e)}</p></div>'
    export_form = {"operator": operator, "conditions": conditions}
    pager = {
        "mode": "post",
        "page": page,
        "page_size": page_size_raw,
        "operator": operator,
        "conditions": conditions,
        "has_prev": count is not None and page > 0,
        "has_next": count is not None and has_next,
        "start": offset + 1 if records else 0,
        "end": offset + len(records),
    }
    return render_template("metadata/_results.html", records=records, pager=pager,
                           searched=True, export_form=export_form)


@app.get("/metadata/export")
def metadata_export():
    """Download ALL matching keyword/advanced search results as CSV (paginated)."""
    kwargs = {k: v for k, v in request.args.items()
              if v and k not in ("mode", "page", "page_size", "limit", "offset")}
    try:
        records = _paginate(
            lambda off, lim: api_client.search_metadata(**kwargs, offset=off, limit=lim)
        )
    except Exception as e:
        flash(_api_error(e, "export"), "error")
        return redirect(url_for("metadata_search"))

    return Response(
        _records_to_csv(records),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=metadata.csv"},
    )


@app.post("/metadata/export-query")
def metadata_export_query():
    """Download ALL matching query-builder results as CSV (paginated)."""
    operator = request.form.get("qb_operator", "AND")
    conditions = []
    for i in range(5):
        field = request.form.get(f"qb_field_{i}", "")
        op    = request.form.get(f"qb_op_{i}", "contains")
        value = request.form.get(f"qb_value_{i}", "")
        if field and value:
            conditions.append({"field": field, "op": op, "value": value})

    if not conditions:
        flash("Add at least one condition before exporting.", "error")
        return redirect(url_for("metadata_search", mode="query"))

    def fetch_page(off, lim):
        body = {"operator": operator, "conditions": conditions, "limit": lim, "offset": off}
        return api_client.query_metadata_tree(body)

    try:
        records = _paginate(fetch_page)
    except Exception as e:
        flash(_api_error(e, "export"), "error")
        return redirect(url_for("metadata_search", mode="query"))

    return Response(
        _records_to_csv(records),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=metadata.csv"},
    )


@app.get("/metadata/<int:rid>")
def metadata_detail(rid):
    try:
        record = api_client.get_metadata(rid)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            flash(f"Record #{rid} not found.", "error")
        else:
            flash(_api_error(e, "load record"), "error")
        return redirect(url_for("metadata_search"))
    except Exception as e:
        flash(_api_error(e, "load record"), "error")
        return redirect(url_for("metadata_search"))
    return render_template("metadata/detail.html", record=record)


# ── Logs ──────────────────────────────────────────────────────────────────────

@app.get("/logs")
def logs_list():
    level = request.args.get("level") or None
    task_id = request.args.get("task_id", type=int)
    submission_id = request.args.get("submission_id", type=int)
    since = request.args.get("since") or None
    until = request.args.get("until") or None
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)
    logs = []
    try:
        logs = api_client.get_logs(level=level, task_id=task_id,
                                   submission_id=submission_id, since=since,
                                   until=until, limit=limit, offset=offset)
    except Exception as e:
        flash(_api_error(e, "load logs"), "error")
    return render_template("logs/list.html", logs=logs)


# ── Scheduled Crawls ──────────────────────────────────────────────────────────

@app.get("/scheduled-crawls")
def scheduled_crawls_list():
    schedules = []
    try:
        schedules = api_client.list_scheduled_crawls()
    except Exception as e:
        flash(_api_error(e, "load schedules"), "error")
    return render_template("scheduled_crawls/list.html", schedules=schedules)


@app.get("/scheduled-crawls/new")
def scheduled_crawl_new():
    return render_template("scheduled_crawls/form.html", schedule=None,
                           projects=_projects(), action=url_for("scheduled_crawl_create"))


@app.post("/scheduled-crawls/new")
def scheduled_crawl_create():
    try:
        types_raw = request.form.get("allowed_file_types", "")
        types = [t.strip().upper() for t in types_raw.split(",") if t.strip()]
        urls = [u.strip() for u in request.form.get("urls", "").splitlines() if u.strip()]
        if not urls:
            flash("At least one URL is required.", "error")
            return redirect(url_for("scheduled_crawl_new"))
        api_client.create_scheduled_crawl(
            urls=urls,
            project_id=int(request.form["project_id"]),
            frequency_seconds=int(request.form["frequency_seconds"]),
            depth_limit=request.form.get("depth_limit", type=int),
            allowed_file_types=types or None,
            full_download=bool(request.form.get("full_download")),
            retain_files=bool(request.form.get("retain_files")),
            crawl_images=bool(request.form.get("crawl_images")),
            robotstxt_obey=bool(request.form.get("robotstxt_obey")),
            allow_cross_domain=bool(request.form.get("allow_cross_domain")),
        )
        flash("Scheduled crawl created.", "success")
    except Exception as e:
        flash(_api_error(e, "create schedule"), "error")
    return redirect(url_for("scheduled_crawls_list"))


@app.get("/scheduled-crawls/<int:sid>/edit")
def scheduled_crawl_edit(sid):
    try:
        schedule = api_client.get_scheduled_crawl(sid)
    except Exception as e:
        flash(_api_error(e, "load schedule"), "error")
        return redirect(url_for("scheduled_crawls_list"))
    return render_template("scheduled_crawls/form.html", schedule=schedule,
                           projects=_projects(), action=url_for("scheduled_crawl_update", sid=sid))


@app.post("/scheduled-crawls/<int:sid>/edit")
def scheduled_crawl_update(sid):
    try:
        types_raw = request.form.get("allowed_file_types", "")
        types = [t.strip().upper() for t in types_raw.split(",") if t.strip()]
        urls = [u.strip() for u in request.form.get("urls", "").splitlines() if u.strip()]
        if not urls:
            flash("At least one URL is required.", "error")
            return redirect(url_for("scheduled_crawl_edit", sid=sid))
        api_client.update_scheduled_crawl(
            sid,
            urls=urls,
            frequency_seconds=int(request.form["frequency_seconds"]),
            depth_limit=request.form.get("depth_limit", type=int),
            allowed_file_types=types or None,
            full_download=bool(request.form.get("full_download")),
            retain_files=bool(request.form.get("retain_files")),
            crawl_images=bool(request.form.get("crawl_images")),
            robotstxt_obey=bool(request.form.get("robotstxt_obey")),
            allow_cross_domain=bool(request.form.get("allow_cross_domain")),
            is_active=bool(request.form.get("is_active")),
        )
        flash("Schedule updated.", "success")
    except Exception as e:
        flash(_api_error(e, "update schedule"), "error")
    return redirect(url_for("scheduled_crawls_list"))


@app.post("/scheduled-crawls/<int:sid>/delete")
def scheduled_crawl_delete(sid):
    try:
        api_client.delete_scheduled_crawl(sid)
        flash("Schedule deleted.", "success")
    except Exception as e:
        flash(_api_error(e, "delete schedule"), "error")
    return redirect(url_for("scheduled_crawls_list"))


@app.post("/scheduled-crawls/<int:sid>/toggle")
def scheduled_crawl_toggle(sid):
    try:
        sc = api_client.get_scheduled_crawl(sid)
        api_client.update_scheduled_crawl(sid, is_active=not sc["is_active"])
    except Exception as e:
        flash(_api_error(e, "toggle schedule"), "error")
    return redirect(url_for("scheduled_crawls_list"))


# ── Telegram ──────────────────────────────────────────────────────────────────

def _telegram_page(**kwargs):
    status = {"credentials_ok": False, "session_ok": False}
    scrapes = []
    scheduled_scrapes = []
    try:
        status = api_client.get_telegram_status()
    except Exception:
        pass
    try:
        scrapes = api_client.list_telegram_scrapes()
    except Exception:
        pass
    try:
        scheduled_scrapes = api_client.list_scheduled_telegram_scrapes()
    except Exception:
        pass
    return render_template(
        "telegram/index.html",
        status=status,
        scrapes=scrapes,
        scheduled_scrapes=scheduled_scrapes,
        projects=_projects(),
        **kwargs,
    )


@app.get("/telegram")
def telegram_index():
    auth_step = session.get("tg_auth_step")
    auth_phone = session.get("tg_auth_phone")
    return _telegram_page(auth_step=auth_step, auth_phone=auth_phone)


@app.post("/telegram/credentials")
def telegram_save_credentials():
    api_id = request.form.get("api_id", "").strip()
    api_hash = request.form.get("api_hash", "").strip()
    if not api_id or not api_hash:
        flash("API ID and API Hash are required.", "error")
    else:
        try:
            api_client.upsert_telegram_credentials(api_id, api_hash)
            flash("Credentials saved.", "success")
        except Exception as e:
            flash(_api_error(e, "save credentials"), "error")
    return redirect(url_for("telegram_index"))


@app.post("/telegram/credentials/delete")
def telegram_remove_credentials():
    try:
        api_client.delete_telegram_credentials()
        flash("Credentials removed.", "success")
    except Exception as e:
        flash(_api_error(e, "remove credentials"), "error")
    return redirect(url_for("telegram_index"))


@app.post("/telegram/auth/start")
def telegram_auth_start_post():
    phone = request.form.get("phone", "").strip()
    if not phone:
        flash("Phone number is required.", "error")
        return redirect(url_for("telegram_index"))
    try:
        api_client.telegram_auth_start(phone)
        session["tg_auth_step"] = "verify"
        session["tg_auth_phone"] = phone
        flash(f"Code sent to {phone}.", "success")
    except Exception as e:
        flash(_api_error(e, "start auth"), "error")
    return redirect(url_for("telegram_index"))


@app.post("/telegram/auth/verify")
def telegram_auth_verify_post():
    phone = request.form.get("phone", "").strip()
    code = request.form.get("code", "").strip()
    password = request.form.get("password", "").strip() or None
    try:
        api_client.telegram_auth_verify(phone, code, password)
        session.pop("tg_auth_step", None)
        session.pop("tg_auth_phone", None)
        flash("Authenticated successfully.", "success")
    except Exception as e:
        flash(_api_error(e, "verify code"), "error")
    return redirect(url_for("telegram_index"))


@app.post("/telegram/scrape")
def telegram_submit_scrape_post():
    try:
        task = api_client.submit_telegram_scrape(
            project_id=int(request.form["project_id"]),
            channel=request.form["channel"],
            max_files=request.form.get("max_files", type=int),
            max_file_size_mb=request.form.get("max_file_size_mb", type=int),
            date_from=request.form.get("date_from") or None,
            date_to=request.form.get("date_to") or None,
            retain_files=bool(request.form.get("retain_files")),
            deduplicate=bool(request.form.get("deduplicate")),
            pdf_mode=bool(request.form.get("pdf_mode")),
        )
        flash(f"Scrape queued. Task ID: {task.get('id')}", "success")
        return redirect(url_for("task_detail", tid=task.get("id")))
    except Exception as e:
        flash(_api_error(e, "submit scrape"), "error")
    return redirect(url_for("telegram_index"))


@app.post("/telegram/scrape/<int:tid>/cancel")
def telegram_cancel_scrape(tid):
    try:
        api_client.cancel_telegram_scrape(tid)
        flash("Scrape cancellation requested.", "success")
    except Exception as e:
        flash(_api_error(e, "cancel scrape"), "error")
    return redirect(url_for("telegram_index"))


@app.post("/telegram/scheduled")
def telegram_create_scheduled():
    try:
        api_client.create_scheduled_telegram_scrape(
            project_id=int(request.form["project_id"]),
            channel=request.form["channel"],
            frequency_seconds=int(request.form["frequency_seconds"]),
            max_files=request.form.get("max_files", type=int),
            date_range_days=request.form.get("date_range_days", type=int),
            retain_files=bool(request.form.get("retain_files")),
            deduplicate=bool(request.form.get("deduplicate")),
            pdf_mode=bool(request.form.get("pdf_mode")),
        )
        flash("Scheduled scrape created.", "success")
    except Exception as e:
        flash(_api_error(e, "create schedule"), "error")
    return redirect(url_for("telegram_index"))


@app.post("/telegram/scheduled/<int:sid>/toggle")
def telegram_toggle_scheduled(sid):
    try:
        ss = api_client.list_scheduled_telegram_scrapes()
        rec = next((s for s in ss if s["id"] == sid), None)
        if rec:
            api_client.update_scheduled_telegram_scrape(sid, is_active=not rec["is_active"])
    except Exception as e:
        flash(_api_error(e, "toggle schedule"), "error")
    return redirect(url_for("telegram_index"))


@app.post("/telegram/scheduled/<int:sid>/delete")
def telegram_delete_scheduled(sid):
    try:
        api_client.delete_scheduled_telegram_scrape(sid)
        flash("Schedule deleted.", "success")
    except Exception as e:
        flash(_api_error(e, "delete schedule"), "error")
    return redirect(url_for("telegram_index"))
