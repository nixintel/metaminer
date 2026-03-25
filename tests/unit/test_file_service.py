"""
Unit tests for app/services/file_service.py

Tests file copy, retain, and delete operations using pytest's tmp_path fixture
so no real filesystem paths from config are touched.
"""
import pytest
from pathlib import Path
from unittest.mock import patch
from app.services.file_service import delete_file_safe, retain_file, make_temp_copy

pytestmark = pytest.mark.unit


class TestDeleteFileSafe:
    def test_deletes_existing_file(self, tmp_path):
        f = tmp_path / "to_delete.txt"
        f.write_bytes(b"content")
        delete_file_safe(f)
        assert not f.exists()

    def test_silently_ignores_missing_file(self, tmp_path):
        # Should not raise any exception
        delete_file_safe(tmp_path / "nonexistent.txt")

    def test_accepts_string_path(self, tmp_path):
        f = tmp_path / "file.txt"
        f.write_bytes(b"x")
        delete_file_safe(str(f))
        assert not f.exists()


class TestRetainFile:
    def test_copies_file_to_project_subdirectory(self, tmp_path):
        retained_base = tmp_path / "retained"
        src = tmp_path / "source.pdf"
        src.write_bytes(b"PDF content")

        with patch("app.services.file_service.settings") as mock_cfg:
            mock_cfg.RETAINED_FILES_DIR = retained_base
            dest = retain_file(src, project_id=7, original_name="source.pdf")

        assert dest.exists()
        assert dest.parent == retained_base / "7"
        assert dest.name == "source.pdf"
        assert dest.read_bytes() == b"PDF content"

    def test_original_file_still_exists_after_retain(self, tmp_path):
        # retain_file is a copy, not a move
        retained_base = tmp_path / "retained"
        src = tmp_path / "source.pdf"
        src.write_bytes(b"content")

        with patch("app.services.file_service.settings") as mock_cfg:
            mock_cfg.RETAINED_FILES_DIR = retained_base
            retain_file(src, project_id=1, original_name="source.pdf")

        assert src.exists()

    def test_name_collision_appends_hash_suffix(self, tmp_path):
        retained_base = tmp_path / "retained"
        src1 = tmp_path / "a.pdf"
        src2 = tmp_path / "b.pdf"
        src1.write_bytes(b"file one content")
        src2.write_bytes(b"file two content")

        with patch("app.services.file_service.settings") as mock_cfg:
            mock_cfg.RETAINED_FILES_DIR = retained_base
            dest1 = retain_file(src1, project_id=1, original_name="report.pdf")
            dest2 = retain_file(src2, project_id=1, original_name="report.pdf")

        assert dest1.exists()
        assert dest2.exists()
        assert dest1 != dest2  # second file gets a hash suffix to avoid collision

    def test_creates_project_subdirectory_if_missing(self, tmp_path):
        retained_base = tmp_path / "retained"
        src = tmp_path / "file.pdf"
        src.write_bytes(b"data")

        with patch("app.services.file_service.settings") as mock_cfg:
            mock_cfg.RETAINED_FILES_DIR = retained_base
            retain_file(src, project_id=99, original_name="file.pdf")

        assert (retained_base / "99").is_dir()


class TestMakeTempCopy:
    def test_creates_copy_in_temp_dir(self, tmp_path):
        temp_dir = tmp_path / "temp"
        src = tmp_path / "original.pdf"
        src.write_bytes(b"PDF bytes")

        with patch("app.services.file_service.settings") as mock_cfg:
            mock_cfg.TEMP_DIR = temp_dir
            dest = make_temp_copy(src)

        assert dest.exists()
        assert dest.parent == temp_dir
        assert dest.read_bytes() == b"PDF bytes"

    def test_suffix_appended_to_filename(self, tmp_path):
        temp_dir = tmp_path / "temp"
        src = tmp_path / "doc.pdf"
        src.write_bytes(b"content")

        with patch("app.services.file_service.settings") as mock_cfg:
            mock_cfg.TEMP_DIR = temp_dir
            dest = make_temp_copy(src, suffix="_rollback_work")

        assert "_rollback_work" in dest.name
        assert dest.suffix == ".pdf"

    def test_filename_includes_hash_fragment(self, tmp_path):
        temp_dir = tmp_path / "temp"
        src = tmp_path / "doc.pdf"
        src.write_bytes(b"content")

        with patch("app.services.file_service.settings") as mock_cfg:
            mock_cfg.TEMP_DIR = temp_dir
            dest = make_temp_copy(src)

        # Filename should be: stem_<8-char-hash>.ext
        assert dest.stem != "doc"  # hash fragment appended
