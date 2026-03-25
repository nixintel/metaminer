"""
Unit tests for app/services/metadata_service.py

Tests the pure helper functions that map exiftool JSON to database columns.
No database or filesystem access required.
"""
import json
import pytest
from app.services.metadata_service import _extract_promoted, _make_record, PROMOTED_FIELDS

pytestmark = pytest.mark.unit


class TestExtractPromoted:
    def test_fields_extracted_from_grouped_dict(self):
        meta = {
            "File": {
                "FileName": "report.pdf",
                "FileType": "PDF",
                "FileTypeExtension": "pdf",
                "MIMEType": "application/pdf",
                "FileSize": "102400",
            }
        }
        result = _extract_promoted(meta)
        assert result["file_name"] == "report.pdf"
        assert result["file_type"] == "PDF"
        assert result["file_type_extension"] == "pdf"
        assert result["mime_type"] == "application/pdf"
        assert result["file_size"] == "102400"

    def test_missing_fields_return_none(self):
        meta = {"File": {"FileName": "test.pdf"}}
        result = _extract_promoted(meta)
        assert result["author"] is None
        assert result["title"] is None
        assert result["creator_tool"] is None
        assert result["producer"] is None
        assert result["pdf_version"] is None

    def test_multiple_sections_merged(self):
        meta = {
            "File": {"FileName": "doc.pdf", "FileType": "PDF"},
            "PDF": {"Author": "Alice", "Title": "My Doc", "PDFVersion": "1.7"},
            "XMP": {"CreatorTool": "Word", "Producer": "Acrobat"},
        }
        result = _extract_promoted(meta)
        assert result["file_name"] == "doc.pdf"
        assert result["author"] == "Alice"
        assert result["title"] == "My Doc"
        assert result["pdf_version"] == "1.7"
        assert result["creator_tool"] == "Word"
        assert result["producer"] == "Acrobat"

    def test_numeric_value_cast_to_string(self):
        # exiftool sometimes returns FileSize as an integer
        meta = {"File": {"FileSize": 1024}}
        result = _extract_promoted(meta)
        assert result["file_size"] == "1024"

    def test_empty_dict_returns_all_none(self):
        result = _extract_promoted({})
        for col_name in PROMOTED_FIELDS.values():
            assert result[col_name] is None

    def test_all_promoted_field_keys_present(self):
        # Result always contains every promoted column, even if None
        result = _extract_promoted({})
        for col_name in PROMOTED_FIELDS.values():
            assert col_name in result

    def test_date_fields_extracted(self):
        meta = {
            "EXIF": {
                "CreateDate": "2024:01:15 10:30:00",
                "ModifyDate": "2024:06:01 09:00:00",
            }
        }
        result = _extract_promoted(meta)
        assert result["create_date"] == "2024:01:15 10:30:00"
        assert result["modify_date"] == "2024:06:01 09:00:00"


class TestMakeRecord:
    def test_creates_record_with_correct_fields(self):
        meta = {"File": {"FileName": "test.pdf", "FileType": "PDF"}}
        record = _make_record(
            submission_id=42, meta=meta, pdf_variant="original", version="12.76"
        )
        assert record.submission_id == 42
        assert record.pdf_variant == "original"
        assert record.exiftool_version == "12.76"
        assert record.file_name == "test.pdf"
        assert record.file_type == "PDF"

    def test_raw_json_is_valid_json_matching_input(self):
        meta = {"File": {"FileName": "test.pdf", "FileSize": "1024"}}
        record = _make_record(submission_id=1, meta=meta, pdf_variant=None, version=None)
        parsed = json.loads(record.raw_json)
        assert parsed == meta

    def test_none_pdf_variant_stored_as_none(self):
        record = _make_record(submission_id=1, meta={}, pdf_variant=None, version=None)
        assert record.pdf_variant is None

    def test_rollback_variant(self):
        record = _make_record(submission_id=5, meta={}, pdf_variant="rollback", version="12.76")
        assert record.pdf_variant == "rollback"

    def test_none_version(self):
        record = _make_record(submission_id=1, meta={}, pdf_variant=None, version=None)
        assert record.exiftool_version is None
