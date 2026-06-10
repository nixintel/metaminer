import csv
import io
import os
import uuid

import httpx
from flask import Flask, Response, flash, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

from frontend import api_client

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-in-prod")
app.config["MAX_CONTENT_LENGTH"] = 500 * 1024 * 1024  # 500 MB upload limit

TEMP_DIR = os.environ.get("TEMP_DIR", "/app/data/temp")


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
    return render_template("submit/manual.html", projects=_projects())


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
    return render_template("submit/crawl.html", projects=_projects(), form={})


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
    limit = request.args.get("limit", 50, type=int)

    if searched and mode != "query":
        try:
            kwargs = {k: v for k, v in request.args.items()
                      if k not in ("mode",) and v}
            records = api_client.search_metadata(**kwargs)
        except Exception as e:
            flash(_api_error(e, "search metadata"), "error")

    # Query builder default state
    qb_fields  = [request.args.get(f"qb_field_{i}", "")  for i in range(5)]
    qb_ops     = [request.args.get(f"qb_op_{i}", "contains") for i in range(5)]
    qb_values  = [request.args.get(f"qb_value_{i}", "") for i in range(5)]
    qb_operator = request.args.get("qb_operator", "AND")
    qb_limit    = request.args.get("qb_limit", "50")

    return render_template(
        "metadata/search.html",
        mode=mode, projects=projects, records=records,
        searched=searched, limit=limit,
        qb_fields=qb_fields, qb_ops=qb_ops, qb_values=qb_values,
        qb_operator=qb_operator, qb_limit=qb_limit,
    )


@app.get("/metadata/results")
def metadata_results():
    """HTMX partial — returns just the results table."""
    records = []
    searched = bool(request.args)
    limit = request.args.get("limit", 50, type=int)
    if searched:
        try:
            kwargs = {k: v for k, v in request.args.items() if v and k != "mode"}
            records = api_client.search_metadata(**kwargs)
        except Exception as e:
            return f'<div id="results-container"><p class="flash flash-error">{_api_error(e)}</p></div>'
    return render_template("metadata/_results.html", records=records,
                           searched=searched, limit=limit)


@app.post("/metadata/query-results")
def metadata_query_results():
    """HTMX partial — executes a query builder POST and returns the results table."""
    operator = request.form.get("qb_operator", "AND")
    limit = request.form.get("qb_limit", 50, type=int)
    conditions = []
    for i in range(5):
        field = request.form.get(f"qb_field_{i}", "")
        op    = request.form.get(f"qb_op_{i}", "contains")
        value = request.form.get(f"qb_value_{i}", "")
        if field and value:
            conditions.append({"field": field, "op": op, "value": value})

    if not conditions:
        return '<div id="results-container"><p>Add at least one condition.</p></div>'

    body = {"operator": operator, "conditions": conditions, "limit": limit}
    records = []
    try:
        records = api_client.query_metadata_tree(body)
    except Exception as e:
        return f'<div id="results-container"><p class="flash flash-error">{_api_error(e)}</p></div>'
    return render_template("metadata/_results.html", records=records,
                           searched=True, limit=limit)


@app.get("/metadata/export")
def metadata_export():
    """Download current search results as CSV (limit 500)."""
    kwargs = {k: v for k, v in request.args.items() if v and k != "mode"}
    kwargs["limit"] = 500
    try:
        records = api_client.search_metadata(**kwargs)
    except Exception as e:
        flash(_api_error(e, "export"), "error")
        return redirect(url_for("metadata_search"))

    fields = ["id", "file_name", "file_type", "mime_type", "file_size",
              "author", "title", "creator_tool", "producer", "pdf_version",
              "create_date", "modify_date", "extracted_at",
              "source_url", "submission_mode", "project_name"]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for r in records:
        writer.writerow({f: r.get(f, "") for f in fields})

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=metadata.csv"},
    )


@app.get("/metadata/<int:rid>")
def metadata_detail(rid):
    # The GET /metadata endpoint doesn't have a single-record route,
    # so fetch with id filter (submission_id won't work here).
    # We'll retrieve via a direct search by id using raw offset trick.
    # Instead: we pass it through from the results list via query param.
    # For now, search with offset=id-1 isn't reliable.
    # Best approach: fetch recent records and find by id.
    try:
        records = api_client.search_metadata(limit=1, offset=max(0, rid - 1))
        record = next((r for r in records if r["id"] == rid), None)
        if not record:
            # Try a wider search
            records = api_client.search_metadata(limit=500)
            record = next((r for r in records if r["id"] == rid), None)
        if not record:
            flash(f"Record #{rid} not found.", "error")
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
