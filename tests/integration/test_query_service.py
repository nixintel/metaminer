"""
Integration tests for app/services/query_service.py

What these tests do
-------------------
They insert real Project, FileSubmission, and MetadataRecord rows into a
test Postgres database and call query_metadata() with various filter
combinations.  This verifies that:

  - Each filter parameter (file_type, author, submission_mode, etc.)
    correctly narrows the result set.
  - Pagination (limit / offset) works as expected.
  - The result dicts contain the expected keys including project_id and
    project_name, which are joined in from the projects table.
  - Full-text search across author/title/creator_tool/etc. works.
  - An unknown project returns an empty list rather than an error.

Why these can't be unit tests
------------------------------
query_metadata() builds a multi-table SQL JOIN with dynamic filters and
sorts.  Only running it against a real database confirms that the query
compiles, executes, and returns the right rows.

Run with:
    pytest tests/integration/test_query_service.py
"""
import json
import pytest

pytestmark = pytest.mark.integration

from app.models.project import Project
from app.models.file_submission import FileSubmission
from app.models.metadata_record import MetadataRecord
from app.services.query_service import query_metadata


@pytest.fixture
async def populated_db(db):
    """
    Creates one project with two submissions and one metadata record each:
      - report.pdf   | type=PDF  | author=Alice Smith | mode=single
      - budget.xlsx  | type=XLSX | author=Bob Jones   | mode=crawl
    """
    project = Project(name="Query Service Test Project")
    db.add(project)
    await db.flush()

    sub_pdf = FileSubmission(
        project_id=project.id,
        original_filename="report.pdf",
        file_hash_sha256="a" * 64,
        submission_mode="single",
        source_url=None,
    )
    sub_xlsx = FileSubmission(
        project_id=project.id,
        original_filename="budget.xlsx",
        file_hash_sha256="b" * 64,
        submission_mode="crawl",
        source_url="https://example.com/budget.xlsx",
    )
    db.add_all([sub_pdf, sub_xlsx])
    await db.flush()

    rec_pdf = MetadataRecord(
        submission_id=sub_pdf.id,
        raw_json=json.dumps({"File": {"FileName": "report.pdf"}}),
        file_name="report.pdf",
        file_type="PDF",
        mime_type="application/pdf",
        author="Alice Smith",
        title="Annual Report 2024",
        creator_tool="Microsoft Word",
        producer="Acrobat Distiller",
    )
    rec_xlsx = MetadataRecord(
        submission_id=sub_xlsx.id,
        raw_json=json.dumps({"File": {"FileName": "budget.xlsx"}}),
        file_name="budget.xlsx",
        file_type="XLSX",
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        author="Bob Jones",
        title="Budget 2024",
        creator_tool="Microsoft Excel",
    )
    db.add_all([rec_pdf, rec_xlsx])
    await db.flush()

    return {
        "project": project,
        "records": [rec_pdf, rec_xlsx],
    }


class TestQueryMetadataFilters:
    async def test_filter_by_project_id_returns_both_records(self, db, populated_db):
        results = await query_metadata(db, {"project_id": populated_db["project"].id})
        assert len(results) == 2

    async def test_unknown_project_returns_empty_list(self, db):
        results = await query_metadata(db, {"project_id": 999_999})
        assert results == []

    async def test_filter_by_file_type(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "file_type": "PDF"}
        )
        assert len(results) == 1
        assert results[0]["file_type"] == "PDF"

    async def test_filter_by_file_type_case_insensitive(self, db, populated_db):
        # ilike means "pdf" should match "PDF"
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "file_type": "pdf"}
        )
        assert len(results) == 1

    async def test_filter_by_author_partial_match(self, db, populated_db):
        # author filter uses ILIKE %term% so a partial name works
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "author": "alice"}
        )
        assert len(results) == 1
        assert "Alice" in results[0]["author"]

    async def test_filter_by_submission_mode(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "submission_mode": "crawl"}
        )
        assert len(results) == 1
        assert results[0]["submission_mode"] == "crawl"

    async def test_filter_by_mime_type(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "mime_type": "application/pdf"}
        )
        assert len(results) == 1
        assert results[0]["mime_type"] == "application/pdf"

    async def test_filter_by_creator_tool(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "creator_tool": "Word"}
        )
        assert len(results) == 1
        assert "Word" in results[0]["creator_tool"]

    async def test_fulltext_search_q_matches_title(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "q": "Annual Report"}
        )
        assert len(results) == 1
        assert "Annual Report" in results[0]["title"]

    async def test_fulltext_search_q_no_match_returns_empty(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "q": "zzz_no_match_zzz"}
        )
        assert results == []

    async def test_source_url_contains_filter(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "source_url__contains": "example.com"}
        )
        assert len(results) == 1
        assert results[0]["source_url"] == "https://example.com/budget.xlsx"


class TestQueryMetadataPagination:
    async def test_limit_restricts_result_count(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "limit": 1}
        )
        assert len(results) == 1

    async def test_offset_skips_first_result(self, db, populated_db):
        all_results = await query_metadata(
            db, {"project_id": populated_db["project"].id}
        )
        offset_results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "offset": 1}
        )
        assert len(offset_results) == 1
        assert offset_results[0]["id"] != all_results[0]["id"]


class TestQueryMetadataResultShape:
    async def test_result_includes_project_fields(self, db, populated_db):
        project = populated_db["project"]
        results = await query_metadata(db, {"project_id": project.id, "limit": 1})
        row = results[0]
        assert row["project_id"] == project.id
        assert row["project_name"] == "Query Service Test Project"

    async def test_result_includes_submission_mode(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "file_type": "PDF"}
        )
        assert "submission_mode" in results[0]

    async def test_raw_json_is_parsed_to_dict(self, db, populated_db):
        results = await query_metadata(
            db, {"project_id": populated_db["project"].id, "file_type": "PDF"}
        )
        # raw_json should be a dict (parsed), not a string
        assert isinstance(results[0]["raw_json"], dict)
