"""
Unit tests for app/utils/helpers.py

Tests the SHA-256 hashing utilities. No infrastructure required.
"""
import hashlib
import pytest
from app.utils.helpers import sha256_bytes, sha256_file

pytestmark = pytest.mark.unit


class TestSha256Bytes:
    def test_known_value(self):
        data = b"hello"
        assert sha256_bytes(data) == hashlib.sha256(b"hello").hexdigest()

    def test_empty_bytes(self):
        assert sha256_bytes(b"") == hashlib.sha256(b"").hexdigest()

    def test_returns_hex_string(self):
        result = sha256_bytes(b"test")
        assert isinstance(result, str)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_different_inputs_produce_different_hashes(self):
        assert sha256_bytes(b"a") != sha256_bytes(b"b")


class TestSha256File:
    def test_matches_sha256_bytes(self, tmp_path):
        content = b"file content here"
        f = tmp_path / "test.bin"
        f.write_bytes(content)
        assert sha256_file(f) == sha256_bytes(content)

    def test_accepts_string_path(self, tmp_path):
        content = b"abc"
        f = tmp_path / "test.bin"
        f.write_bytes(content)
        assert sha256_file(str(f)) == hashlib.sha256(content).hexdigest()

    def test_large_file_chunked_correctly(self, tmp_path):
        # Content larger than default chunk_size (65536) to exercise chunked reading
        content = b"x" * 200_000
        f = tmp_path / "large.bin"
        f.write_bytes(content)
        assert sha256_file(f) == hashlib.sha256(content).hexdigest()

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        assert sha256_file(f) == hashlib.sha256(b"").hexdigest()
