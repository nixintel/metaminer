"""
Unit tests for app/workers/bulk_tasks.py

Tests the _get_all_files helper that discovers files from a list of paths.
No Celery, database, or network access required.
"""
import pytest
from pathlib import Path
from app.workers.bulk_tasks import _get_all_files

pytestmark = pytest.mark.unit


class TestGetAllFiles:
    def test_single_file_path(self, tmp_path):
        f = tmp_path / "file.pdf"
        f.write_bytes(b"content")
        result = _get_all_files([str(f)])
        assert result == [f]

    def test_directory_returns_all_files(self, tmp_path):
        (tmp_path / "a.pdf").write_bytes(b"a")
        (tmp_path / "b.docx").write_bytes(b"b")
        (tmp_path / "c.xlsx").write_bytes(b"c")
        result = _get_all_files([str(tmp_path)])
        assert len(result) == 3
        assert set(result) == {
            tmp_path / "a.pdf",
            tmp_path / "b.docx",
            tmp_path / "c.xlsx",
        }

    def test_directory_recurses_into_subdirectories(self, tmp_path):
        subdir = tmp_path / "sub"
        subdir.mkdir()
        deep = subdir / "deep"
        deep.mkdir()
        (tmp_path / "root.pdf").write_bytes(b"r")
        (subdir / "nested.docx").write_bytes(b"n")
        (deep / "deep.xlsx").write_bytes(b"d")
        result = _get_all_files([str(tmp_path)])
        assert len(result) == 3

    def test_missing_path_returns_empty_list(self, tmp_path):
        result = _get_all_files([str(tmp_path / "nonexistent")])
        assert result == []

    def test_mixed_file_and_directory_paths(self, tmp_path):
        direct = tmp_path / "direct.pdf"
        direct.write_bytes(b"d")
        subdir = tmp_path / "folder"
        subdir.mkdir()
        (subdir / "inside.docx").write_bytes(b"i")
        result = _get_all_files([str(direct), str(subdir)])
        assert len(result) == 2
        assert direct in result
        assert (subdir / "inside.docx") in result

    def test_empty_directory_returns_empty_list(self, tmp_path):
        result = _get_all_files([str(tmp_path)])
        assert result == []

    def test_empty_input_list(self):
        result = _get_all_files([])
        assert result == []

    def test_directories_not_included_in_results(self, tmp_path):
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "file.pdf").write_bytes(b"f")
        result = _get_all_files([str(tmp_path)])
        # Only files, never directories
        assert all(p.is_file() for p in result)
