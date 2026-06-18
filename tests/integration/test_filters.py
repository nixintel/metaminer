"""
Integration tests for auto-tagging filters:
  - load_active_filters scoping (global + project, active only)
  - inline tagging via process_single_file (passed FilterSet and the None-fallback)
  - backfill_scan (additive, scoping, reasons, idempotent, Task counters)

Uses the real test Postgres. The backfill test uses its own committed sessions (the
backfill opens fresh sessions, so seed data must be committed to be visible).
"""
import json
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

pytestmark = pytest.mark.integration

from app.models.project import Project
from app.models.file_submission import FileSubmission
from app.models.metadata_record import MetadataRecord
from app.models.filter_criteria import FilterCriteria
from app.models.task import Task
from app.services.filter_service import load_active_filters, FilterSet, CompiledFilter
from app.services.metadata_service import process_single_file
from app.workers.filter_tasks import backfill_scan


# ── load_active_filters scoping ────────────────────────────────────────────────

class TestLoadActiveFilters:
    async def _seed(self, db):
        p1 = Project(name="P1")
        p2 = Project(name="P2")
        db.add_all([p1, p2])
        await db.flush()
        db.add_all([
            FilterCriteria(name="global-active", filter_type="keyword", value="g", project_id=None, is_active=True),
            FilterCriteria(name="global-inactive", filter_type="keyword", value="gi", project_id=None, is_active=False),
            FilterCriteria(name="p1-active", filter_type="keyword", value="a", project_id=p1.id, is_active=True),
            FilterCriteria(name="p2-active", filter_type="keyword", value="b", project_id=p2.id, is_active=True),
        ])
        await db.flush()
        return p1, p2

    async def test_project_scope_includes_globals_excludes_other_projects(self, db):
        p1, p2 = await self._seed(db)
        fs = await load_active_filters(db, p1.id)
        names = {f.name for f in fs.filters}
        assert names == {"global-active", "p1-active"}  # p2 excluded, inactive excluded

    async def test_global_scope_loads_all_active(self, db):
        await self._seed(db)
        fs = await load_active_filters(db, None)
        names = {f.name for f in fs.filters}
        assert names == {"global-active", "p1-active", "p2-active"}
        assert "global-inactive" not in names


# ── Inline tagging via process_single_file ─────────────────────────────────────

class TestInlineTagging:
    async def _project(self, db):
        p = Project(name="Inline")
        db.add(p)
        await db.flush()
        return p

    async def _record_for(self, db, submission_id):
        return (await db.execute(
            select(MetadataRecord).where(MetadataRecord.submission_id == submission_id)
        )).scalars().first()

    async def test_passed_filterset_tags_record(self, db, tmp_path):
        p = await self._project(db)
        f = tmp_path / "secret_report.txt"
        f.write_text("hello world")
        # FileName ("secret_report.txt") lands in raw_json → keyword "secret" matches.
        fs = FilterSet([CompiledFilter(1, "Secrets", "keyword", "secret", None)])
        result = await process_single_file(
            db=db, project_id=p.id, file_path=str(f),
            submission_mode="manual", active_filters=fs,
        )
        rec = await self._record_for(db, result["submission_id"])
        assert rec.interesting is True
        assert "keyword=secret" in rec.interesting_reason

    async def test_no_match_leaves_record_unflagged(self, db, tmp_path):
        p = await self._project(db)
        f = tmp_path / "ordinary.txt"
        f.write_text("hello")
        fs = FilterSet([CompiledFilter(1, "x", "keyword", "zzzznomatch", None)])
        result = await process_single_file(
            db=db, project_id=p.id, file_path=str(f),
            submission_mode="manual", active_filters=fs,
        )
        rec = await self._record_for(db, result["submission_id"])
        assert rec.interesting is False
        assert rec.interesting_reason is None

    async def test_none_fallback_loads_filters_from_db(self, db, tmp_path):
        p = await self._project(db)
        db.add(FilterCriteria(name="fn", filter_type="keyword", value="invoice",
                              project_id=None, is_active=True))
        await db.flush()
        f = tmp_path / "invoice_2024.txt"
        f.write_text("x")
        # active_filters omitted → process_single_file loads from db (same session).
        result = await process_single_file(
            db=db, project_id=p.id, file_path=str(f), submission_mode="manual",
        )
        rec = await self._record_for(db, result["submission_id"])
        assert rec.interesting is True
        assert "keyword=invoice" in rec.interesting_reason


# ── Backfill ───────────────────────────────────────────────────────────────────

class TestBackfill:
    async def _seed(self, SessionLocal):
        """Two projects, several records (one already interesting), a global keyword filter."""
        async with SessionLocal() as db:
            p1 = Project(name="BF1")
            p2 = Project(name="BF2")
            db.add_all([p1, p2])
            await db.flush()

            sub1 = FileSubmission(project_id=p1.id, original_filename="a", submission_mode="manual",
                                  source_url="https://x.com/invoice/a")
            sub2 = FileSubmission(project_id=p2.id, original_filename="b", submission_mode="manual",
                                  source_url="https://x.com/other/b")
            db.add_all([sub1, sub2])
            await db.flush()

            # p1: matches keyword "invoice" (via source_url), not yet interesting
            rec_match = MetadataRecord(submission_id=sub1.id, raw_json=json.dumps({"Author": "x"}),
                                       file_type="PDF", interesting=False)
            # p1: already interesting (manual) — must be preserved, not rescanned
            rec_manual = MetadataRecord(submission_id=sub1.id, raw_json=json.dumps({"Author": "invoice"}),
                                        file_type="PDF", interesting=True, interesting_reason="manual")
            # p2: matches "invoice" too (source_url has "other", raw has "invoice" word? no) -> craft raw match
            rec_p2 = MetadataRecord(submission_id=sub2.id, raw_json=json.dumps({"Title": "Big invoice 2024"}),
                                    file_type="XLSX", interesting=False)
            db.add_all([rec_match, rec_manual, rec_p2])
            await db.flush()

            gfilter = FilterCriteria(name="Invoices", filter_type="keyword", value="invoice",
                                     project_id=None, is_active=True)
            db.add(gfilter)
            await db.flush()

            ids = dict(p1=p1.id, p2=p2.id, rec_match=rec_match.id, rec_manual=rec_manual.id,
                       rec_p2=rec_p2.id, gfilter=gfilter.id)
            await db.commit()
            return ids

    async def _new_task(self, SessionLocal, project_id):
        async with SessionLocal() as db:
            t = Task(project_id=project_id, task_type="filter_backfill", status="pending")
            db.add(t)
            await db.flush()
            tid = t.id
            await db.commit()
            return tid

    async def _get(self, SessionLocal, model, pk):
        async with SessionLocal() as db:
            return await db.get(model, pk)

    async def test_whole_db_backfill_global_filter(self, test_engine):
        SessionLocal = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
        ids = await self._seed(SessionLocal)
        tid = await self._new_task(SessionLocal, None)

        scanned, flagged = await backfill_scan(SessionLocal, tid, [ids["gfilter"]], None)

        # Two False rows (rec_match in p1, rec_p2 in p2) both match "invoice"; the manual
        # row is skipped by the interesting=false clause.
        assert flagged == 2
        assert scanned == 2

        rec_match = await self._get(SessionLocal, MetadataRecord, ids["rec_match"])
        rec_manual = await self._get(SessionLocal, MetadataRecord, ids["rec_manual"])
        rec_p2 = await self._get(SessionLocal, MetadataRecord, ids["rec_p2"])
        assert rec_match.interesting and "keyword=invoice" in rec_match.interesting_reason
        assert rec_p2.interesting and "keyword=invoice" in rec_p2.interesting_reason
        # Manual mark preserved (reason not overwritten).
        assert rec_manual.interesting and rec_manual.interesting_reason == "manual"

        task = await self._get(SessionLocal, Task, tid)
        assert task.status == "completed"
        assert task.files_processed == 2

    async def test_project_scope_limits_backfill(self, test_engine):
        SessionLocal = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
        ids = await self._seed(SessionLocal)
        tid = await self._new_task(SessionLocal, ids["p1"])

        scanned, flagged = await backfill_scan(SessionLocal, tid, [ids["gfilter"]], ids["p1"])

        # Only p1's single False row is in scope.
        assert flagged == 1
        rec_p2 = await self._get(SessionLocal, MetadataRecord, ids["rec_p2"])
        assert rec_p2.interesting is False  # p2 untouched

    async def test_idempotent_rerun(self, test_engine):
        SessionLocal = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
        ids = await self._seed(SessionLocal)
        tid1 = await self._new_task(SessionLocal, None)
        await backfill_scan(SessionLocal, tid1, [ids["gfilter"]], None)

        tid2 = await self._new_task(SessionLocal, None)
        scanned2, flagged2 = await backfill_scan(SessionLocal, tid2, [ids["gfilter"]], None)
        # Nothing left to flag (already-interesting rows are excluded).
        assert flagged2 == 0
        assert scanned2 == 0
