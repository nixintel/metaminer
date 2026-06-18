"""
Unit tests for the auto-tagging matcher (app/services/filter_service.py).

Pure logic, no DB. Covers the three filter types, grouped/flat exif, case-insensitivity,
empty values, invalid regex rejection, and short-circuit/reason behaviour.
"""
import json
import pytest

pytestmark = pytest.mark.unit

from app.services.filter_service import (
    CompiledFilter,
    FilterSet,
    validate_filter,
    FilterValidationError,
    DEFAULT_EMAIL_REGEX,
)


def _f(id, filter_type, value, name="f", project_id=None):
    return CompiledFilter(id, name, filter_type, value, project_id)


def _fs(*filters):
    return FilterSet(list(filters))


# ── validate_filter ─────────────────────────────────────────────────────────

class TestValidateFilter:
    def test_keyword_requires_value(self):
        with pytest.raises(FilterValidationError):
            validate_filter("keyword", "   ")

    def test_keyword_trims(self):
        assert validate_filter("keyword", "  invoice  ") == "invoice"

    def test_exif_field_requires_value(self):
        with pytest.raises(FilterValidationError):
            validate_filter("exif_field", "")

    def test_regex_blank_becomes_default_email(self):
        assert validate_filter("regex", "") == DEFAULT_EMAIL_REGEX

    def test_regex_valid_passes(self):
        assert validate_filter("regex", r"\d{3}") == r"\d{3}"

    def test_regex_invalid_rejected(self):
        with pytest.raises(FilterValidationError):
            validate_filter("regex", "(unclosed")

    def test_unknown_type_rejected(self):
        with pytest.raises(FilterValidationError):
            validate_filter("nope", "x")


# ── keyword ──────────────────────────────────────────────────────────────────

class TestKeyword:
    def test_match_in_raw_json(self):
        raw = json.dumps({"PDF": {"Author": "Acme Invoice Dept"}})
        matched, reason = _fs(_f(1, "keyword", "invoice")).evaluate(None, raw, None)
        assert matched and "keyword=invoice" in reason

    def test_match_in_source_url(self):
        matched, _ = _fs(_f(1, "keyword", "secret")).evaluate(
            "https://x.com/secret/doc", "{}", None
        )
        assert matched

    def test_case_insensitive(self):
        raw = json.dumps({"Author": "INVOICE Dept"})
        matched, _ = _fs(_f(1, "keyword", "invoice")).evaluate(None, raw, None)
        assert matched

    def test_no_match(self):
        matched, reason = _fs(_f(1, "keyword", "zzz")).evaluate("http://a.com", "{}", None)
        assert not matched and reason is None


# ── regex ──────────────────────────────────────────────────────────────────────

class TestRegex:
    def test_default_email_matches(self):
        raw = json.dumps({"Author": "jane.doe@example.com"})
        matched, _ = _fs(_f(1, "regex", DEFAULT_EMAIL_REGEX)).evaluate(None, raw, None)
        assert matched

    def test_default_email_no_match(self):
        raw = json.dumps({"Author": "no address here"})
        matched, _ = _fs(_f(1, "regex", DEFAULT_EMAIL_REGEX)).evaluate(None, raw, None)
        assert not matched

    def test_ignorecase(self):
        matched, _ = _fs(_f(1, "regex", "abc")).evaluate("http://X/ABC", "{}", None)
        assert matched


# ── exif_field ───────────────────────────────────────────────────────────────

class TestExifField:
    def test_grouped_leaf_key(self):
        exif = {"PDF": {"Author": "x"}}
        matched, _ = _fs(_f(1, "exif_field", "Author")).evaluate(None, json.dumps(exif), exif)
        assert matched

    def test_grouped_qualified_key(self):
        exif = {"GPS": {"GPSLatitude": "51.5"}}
        matched, _ = _fs(_f(1, "exif_field", "GPS:GPSLatitude")).evaluate(None, json.dumps(exif), exif)
        assert matched

    def test_flat_dict(self):
        exif = {"Author": "x"}
        matched, _ = _fs(_f(1, "exif_field", "author")).evaluate(None, json.dumps(exif), exif)
        assert matched  # case-insensitive

    def test_present_but_empty_no_match(self):
        exif = {"Author": "   "}
        matched, _ = _fs(_f(1, "exif_field", "Author")).evaluate(None, json.dumps(exif), exif)
        assert not matched

    def test_absent_no_match(self):
        exif = {"Title": "x"}
        matched, _ = _fs(_f(1, "exif_field", "Author")).evaluate(None, json.dumps(exif), exif)
        assert not matched


# ── short-circuit / reason ─────────────────────────────────────────────────────

class TestShortCircuit:
    def test_returns_first_match_reason(self):
        raw = json.dumps({"Author": "invoice"})
        fs = _fs(
            _f(1, "keyword", "zzz", name="A"),
            _f(2, "keyword", "invoice", name="B"),
            _f(3, "keyword", "invoice", name="C"),
        )
        matched, reason = fs.evaluate(None, raw, None)
        assert matched
        assert "filter #2" in reason and "B" in reason

    def test_empty_filterset_no_match(self):
        matched, reason = _fs().evaluate("http://a", "{}", None)
        assert not matched and reason is None


# ── evaluate_all (no short-circuit) ────────────────────────────────────────────

class TestEvaluateAll:
    def test_returns_all_matching_ids_in_order(self):
        raw = json.dumps({"Author": "invoice secret"})
        fs = _fs(
            _f(1, "keyword", "invoice", name="A"),
            _f(2, "keyword", "zzz", name="B"),       # no match
            _f(3, "keyword", "secret", name="C"),
            _f(4, "keyword", "invoice", name="D"),
        )
        ids, first_reason = fs.evaluate_all(None, raw, None)
        assert ids == [1, 3, 4]
        assert "filter #1" in first_reason and "A" in first_reason

    def test_no_matches(self):
        ids, first_reason = _fs(_f(1, "keyword", "zzz")).evaluate_all("http://a", "{}", None)
        assert ids == [] and first_reason is None

    def test_empty_filterset(self):
        ids, first_reason = _fs().evaluate_all("http://a", "{}", None)
        assert ids == [] and first_reason is None

    def test_exif_field_lazy_flatten_still_works(self):
        exif = {"GPS": {"GPSLatitude": "51.5"}}
        fs = _fs(
            _f(1, "keyword", "nomatch"),
            _f(2, "exif_field", "GPS:GPSLatitude", name="geo"),
        )
        ids, _ = fs.evaluate_all(None, json.dumps(exif), exif)
        assert ids == [2]
