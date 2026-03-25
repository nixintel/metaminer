"""
Unit tests for app/services/pdf_service.py

Tests PDF detection and structural error pattern matching.
No subprocess calls or database access required.
"""
import pytest
from app.services.pdf_service import is_pdf, _is_structural_pdf_error

pytestmark = pytest.mark.unit


class TestIsPdf:
    def test_pdf_extension_returns_true(self, tmp_path):
        # .pdf extension is sufficient — no magic byte check needed
        f = tmp_path / "report.pdf"
        f.write_bytes(b"not a real pdf at all")
        assert is_pdf(f) is True

    def test_uppercase_pdf_extension_returns_true(self, tmp_path):
        f = tmp_path / "REPORT.PDF"
        f.write_bytes(b"content")
        assert is_pdf(f) is True

    def test_non_pdf_extension_with_magic_bytes_returns_true(self, tmp_path):
        # File has no .pdf extension but starts with %PDF magic bytes
        f = tmp_path / "disguised.bin"
        f.write_bytes(b"%PDF-1.4 rest of file content")
        assert is_pdf(f) is True

    def test_non_pdf_extension_without_magic_bytes_returns_false(self, tmp_path):
        f = tmp_path / "document.docx"
        f.write_bytes(b"PK\x03\x04")  # ZIP magic (used by .docx), not PDF
        assert is_pdf(f) is False

    def test_missing_file_returns_false(self, tmp_path):
        # File does not exist — exception is caught internally
        assert is_pdf(tmp_path / "nonexistent.bin") is False

    def test_empty_file_returns_false(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        assert is_pdf(f) is False

    def test_path_as_string_accepted(self, tmp_path):
        f = tmp_path / "report.pdf"
        f.write_bytes(b"data")
        assert is_pdf(str(f)) is True


class TestIsStructuralPdfError:
    @pytest.mark.parametrize("stderr", [
        "Error: Invalid xref table",
        "Warning: invalid PDF structure",
        "Error: bad xref entry at offset 1234",
        "PDF file is corrupted",
        "Attempting to rebuild xref table",
    ])
    def test_known_error_patterns_return_true(self, stderr):
        assert _is_structural_pdf_error(stderr) is True

    @pytest.mark.parametrize("stderr", [
        "Permission denied writing file",
        "File not found",
        "Timeout during write operation",
        "1 image files updated",
        "1 image files unchanged",
        "",
    ])
    def test_unrelated_strings_return_false(self, stderr):
        assert _is_structural_pdf_error(stderr) is False

    def test_matching_is_case_insensitive(self):
        assert _is_structural_pdf_error("INVALID XREF TABLE") is True
        assert _is_structural_pdf_error("CORRUPTED FILE") is True
        assert _is_structural_pdf_error("BAD XREF") is True
