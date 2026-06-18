"""
Integration tests for filter groups:
  - group CRUD + scope-eligible membership (router functions called directly — no HTTP harness)
  - get_record_matches derivation ("only the group(s)" rule)
  - matched_group_id / matched_filter_id search (EXISTS on the join table)
  - inline ingestion records multiple matches
Uses the real test Postgres via the `db` fixture (single session, flush-visible).
"""
import pytest
from fastapi import HTTPException
from sqlalchemy import select, func

pytestmark = pytest.mark.integration

from app.models.project import Project
from app.models.file_submission import FileSubmission
from app.models.metadata_record import MetadataRecord
from app.models.metadata_filter_match import MetadataFilterMatch
from app.models.filter_criteria import FilterCriteria
from app.models.filter_group import FilterGroup
from app.services.query_service import get_record_matches, query_metadata
from app.services.filter_service import FilterSet, CompiledFilter
from app.services.metadata_service import process_single_file
from app.routers import filter_groups as fg_router
from app.schemas.filter_group import FilterGroupCreate, FilterGroupBackfillRequest


# ── Group CRUD + scope eligibility (router functions, called directly) ──────────

class TestGroupCrud:
    async def test_create_group_with_members(self, db):
        p = Project(name="GC")
        db.add(p)
        await db.flush()
        f1 = FilterCriteria(name="a", filter_type="keyword", value="x", project_id=None, is_active=True)
        f2 = FilterCriteria(name="b", filter_type="keyword", value="y", project_id=p.id, is_active=True)
        db.add_all([f1, f2])
        await db.flush()

        resp = await fg_router.create_filter_group(
            FilterGroupCreate(name="G1", project_id=p.id, filter_ids=[f1.id, f2.id]), db
        )
        assert resp.name == "G1"
        assert {m.id for m in resp.filters} == {f1.id, f2.id}

    async def test_global_group_rejects_project_filter(self, db):
        p = Project(name="GC2")
        db.add(p)
        await db.flush()
        fp = FilterCriteria(name="proj", filter_type="keyword", value="z", project_id=p.id, is_active=True)
        db.add(fp)
        await db.flush()
        with pytest.raises(HTTPException) as ei:
            await fg_router.create_filter_group(
                FilterGroupCreate(name="GG", project_id=None, filter_ids=[fp.id]), db
            )
        assert ei.value.status_code == 422

    async def test_empty_group_backfill_refused(self, db):
        grp = await fg_router.create_filter_group(
            FilterGroupCreate(name="empty", project_id=None, filter_ids=[]), db
        )
        with pytest.raises(HTTPException) as ei:
            await fg_router.backfill_filter_group(
                grp.id, FilterGroupBackfillRequest(project_id=None), db
            )
        assert ei.value.status_code == 400


# ── "only the group(s)" derivation ─────────────────────────────────────────────

class TestRecordMatches:
    async def _record(self, db, reason="x"):
        p = Project(name="RM")
        db.add(p)
        await db.flush()
        sub = FileSubmission(project_id=p.id, original_filename="f", submission_mode="manual")
        db.add(sub)
        await db.flush()
        rec = MetadataRecord(submission_id=sub.id, raw_json="{}", interesting=True, interesting_reason=reason)
        db.add(rec)
        await db.flush()
        return rec

    async def test_grouped_filter_shown_as_group_ungrouped_standalone(self, db):
        rec = await self._record(db)
        f_grouped = FilterCriteria(name="grouped", filter_type="keyword", value="a", is_active=True)
        f_solo = FilterCriteria(name="solo", filter_type="keyword", value="b", is_active=True)
        db.add_all([f_grouped, f_solo])
        await db.flush()
        grp = FilterGroup(name="Pattern G", project_id=None, is_active=True)
        grp.filters = [f_grouped]
        db.add(grp)
        await db.flush()
        db.add_all([
            MetadataFilterMatch(metadata_id=rec.id, filter_id=f_grouped.id),
            MetadataFilterMatch(metadata_id=rec.id, filter_id=f_solo.id),
        ])
        await db.flush()

        res = await get_record_matches(db, rec.id, rec.interesting_reason)
        assert [g["name"] for g in res["groups"]] == ["Pattern G"]
        assert [f["name"] for f in res["filters"]] == ["solo"]  # grouped one absorbed into its group
        assert res["manual"] is False

    async def test_filter_in_two_groups_shows_both(self, db):
        rec = await self._record(db)
        f = FilterCriteria(name="shared", filter_type="keyword", value="a", is_active=True)
        db.add(f)
        await db.flush()
        g1 = FilterGroup(name="Alpha", project_id=None, is_active=True)
        g2 = FilterGroup(name="Beta", project_id=None, is_active=True)
        g1.filters = [f]
        g2.filters = [f]
        db.add_all([g1, g2])
        await db.flush()
        db.add(MetadataFilterMatch(metadata_id=rec.id, filter_id=f.id))
        await db.flush()

        res = await get_record_matches(db, rec.id, rec.interesting_reason)
        assert sorted(g["name"] for g in res["groups"]) == ["Alpha", "Beta"]
        assert res["filters"] == []  # the only matched filter is grouped

    async def test_manual_flag(self, db):
        rec = await self._record(db, reason="manual")
        res = await get_record_matches(db, rec.id, rec.interesting_reason)
        assert res["manual"] is True


# ── Search by group / filter (EXISTS on the join table) ────────────────────────

class TestGroupSearch:
    async def _seed(self, db):
        p = Project(name="GS")
        db.add(p)
        await db.flush()
        sub = FileSubmission(project_id=p.id, original_filename="f", submission_mode="manual")
        db.add(sub)
        await db.flush()
        hit = MetadataRecord(submission_id=sub.id, raw_json="{}", interesting=True)
        miss = MetadataRecord(submission_id=sub.id, raw_json="{}", interesting=False)
        db.add_all([hit, miss])
        await db.flush()
        f = FilterCriteria(name="f", filter_type="keyword", value="a", is_active=True)
        db.add(f)
        await db.flush()
        grp = FilterGroup(name="G", project_id=None, is_active=True)
        grp.filters = [f]
        db.add(grp)
        await db.flush()
        db.add(MetadataFilterMatch(metadata_id=hit.id, filter_id=f.id))
        await db.flush()
        return dict(project=p.id, hit=hit.id, miss=miss.id, filter=f.id, group=grp.id)

    async def test_matched_group_id_returns_only_matching(self, db):
        ids = await self._seed(db)
        rows = await query_metadata(db, {"project_id": ids["project"], "matched_group_id": ids["group"]})
        assert [r["id"] for r in rows] == [ids["hit"]]

    async def test_matched_filter_id_exists(self, db):
        ids = await self._seed(db)
        rows = await query_metadata(db, {"project_id": ids["project"], "matched_filter_id": ids["filter"]})
        assert [r["id"] for r in rows] == [ids["hit"]]


# ── Inline ingestion records multiple matches ──────────────────────────────────

class TestInlineMultiMatch:
    async def test_two_filters_match_one_file(self, db, tmp_path):
        p = Project(name="IM")
        db.add(p)
        await db.flush()
        # Real filter rows (the match rows FK to filters.id).
        f1 = FilterCriteria(name="S", filter_type="keyword", value="secret", is_active=True)
        f2 = FilterCriteria(name="I", filter_type="keyword", value="invoice", is_active=True)
        db.add_all([f1, f2])
        await db.flush()
        fs = FilterSet([
            CompiledFilter(f1.id, f1.name, f1.filter_type, f1.value, None),
            CompiledFilter(f2.id, f2.name, f2.filter_type, f2.value, None),
        ])
        # Filename "secret_invoice.txt" lands in raw_json → both keyword filters match.
        f = tmp_path / "secret_invoice.txt"
        f.write_text("hello")

        result = await process_single_file(
            db=db, project_id=p.id, file_path=str(f), submission_mode="manual", active_filters=fs,
        )
        n = (await db.execute(
            select(func.count()).select_from(MetadataFilterMatch)
            .join(MetadataRecord, MetadataRecord.id == MetadataFilterMatch.metadata_id)
            .where(MetadataRecord.submission_id == result["submission_id"])
        )).scalar()
        assert n == 2
