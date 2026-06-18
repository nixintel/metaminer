"""
Auto-tagging filter matcher.

Pure, DB-free matching logic plus a thin DB loader. A FilterSet is compiled once
(regexes pre-compiled, keywords lowercased) and evaluated against many records,
short-circuiting on the first match. Used both inline during ingestion and by the
backfill task.

Matching scope per type:
  - keyword    : case-insensitive substring of (source_url + "\\n" + raw_json string)
  - regex      : re.search (IGNORECASE) over the same combined text
  - exif_field : the named field exists in the (flattened) exif dict AND is non-empty

Note: raw_json is matched in its serialized form (json.dumps escapes non-ASCII, e.g.
"café" -> "caf\\u00e9"), consistent with the existing raw_json ILIKE search in
query_service. Keyword/regex therefore match the escaped text.
"""
import json
import logging
import re

logger = logging.getLogger("metaminer.filter_service")

FILTER_TYPES = ("keyword", "regex", "exif_field")

# Offered as the prefilled value when creating a regex filter.
DEFAULT_EMAIL_REGEX = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

# Upper bound on the text a regex scans, to bound ReDoS exposure on attacker-influenced
# metadata. Exif JSON is normally tiny; a crafted file could be huge.
_MAX_REGEX_TEXT = 1_000_000


class FilterValidationError(ValueError):
    """Raised when a filter's value is invalid (empty, or an uncompilable regex)."""


def validate_filter(filter_type: str, value: str) -> str:
    """Validate (and normalize) a filter at create/update time.

    Returns the value to store (for regex, a blank value becomes the default email regex).
    Raises FilterValidationError on invalid input.
    """
    if filter_type not in FILTER_TYPES:
        raise FilterValidationError(f"Unknown filter_type: {filter_type!r}")

    value = (value or "").strip()

    if filter_type == "regex":
        if not value:
            value = DEFAULT_EMAIL_REGEX
        try:
            re.compile(value)
        except re.error as e:
            raise FilterValidationError(f"Invalid regular expression: {e}")
        return value

    # keyword / exif_field
    if not value:
        raise FilterValidationError(f"A non-empty value is required for a {filter_type} filter")
    return value


def _flatten_exif(meta: dict) -> dict:
    """Index a (possibly grouped) exiftool dict by both leaf 'Key' and 'Group:Key'.

    exiftool emits either grouped ({"PDF": {"Author": ...}}) or flat ({"Author": ...}).
    """
    flat: dict = {}
    if not isinstance(meta, dict):
        return flat
    for group, section in meta.items():
        if isinstance(section, dict):
            for k, v in section.items():
                flat[k] = v
                flat[f"{group}:{k}"] = v
        else:
            flat[group] = section
    return flat


class CompiledFilter:
    """A single filter pre-compiled for fast repeated evaluation."""
    __slots__ = ("id", "name", "filter_type", "value", "project_id", "_needle", "_regex", "_field")

    def __init__(self, id, name, filter_type, value, project_id):
        self.id = id
        self.name = name
        self.filter_type = filter_type
        self.value = value
        self.project_id = project_id
        self._needle = value.lower() if filter_type == "keyword" else None
        self._regex = re.compile(value, re.IGNORECASE) if filter_type == "regex" else None
        self._field = value.strip().lower() if filter_type == "exif_field" else None

    @property
    def reason(self) -> str:
        # Frozen descriptor — traceable even after the filter is renamed/deleted.
        return f"{self.name} (filter #{self.id}): {self.filter_type}={self.value}"

    def matches(self, combined: str, combined_lower: str, exif_flat: dict) -> bool:
        if self.filter_type == "keyword":
            return self._needle in combined_lower
        if self.filter_type == "regex":
            return self._regex.search(combined[:_MAX_REGEX_TEXT]) is not None
        # exif_field: present AND non-empty (checked against both 'Key' and 'Group:Key')
        if self._field in exif_flat:
            v = exif_flat[self._field]
            return v is not None and str(v).strip() != ""
        return False


class FilterSet:
    """A compiled collection of filters evaluated together against one record."""
    __slots__ = ("filters",)

    def __init__(self, filters: list[CompiledFilter]):
        self.filters = filters

    def __bool__(self) -> bool:
        return bool(self.filters)

    def __len__(self) -> int:
        return len(self.filters)

    def _prepare(self, source_url, raw_json_str: str):
        """Build the combined search text once. exif_flat is built lazily (only when an
        exif_field filter needs it) by the caller via _need_exif()."""
        combined = f"{source_url or ''}\n{raw_json_str or ''}"
        return combined, combined.lower()

    def evaluate(self, source_url, raw_json_str: str, exif: dict | None) -> tuple[bool, str | None]:
        """Return (matched, reason) for the first matching filter, else (False, None)."""
        if not self.filters:
            return (False, None)
        combined, combined_lower = self._prepare(source_url, raw_json_str)
        exif_flat = None
        for f in self.filters:
            if f.filter_type == "exif_field" and exif_flat is None:
                exif_flat = {k.lower(): v for k, v in _flatten_exif(exif or {}).items()}
            if f.matches(combined, combined_lower, exif_flat or {}):
                return (True, f.reason)
        return (False, None)

    def evaluate_all(self, source_url, raw_json_str: str, exif: dict | None) -> tuple[list[int], str | None]:
        """Return (matched_filter_ids, first_reason) — ALL matches, no short-circuit.

        Iterates filters in order; matched_filter_ids preserves that order and
        first_reason is the first matching filter's descriptor (the legacy single-match
        reason, kept for back-compat display).
        """
        if not self.filters:
            return ([], None)
        combined, combined_lower = self._prepare(source_url, raw_json_str)
        exif_flat = None
        matched_ids: list[int] = []
        first_reason: str | None = None
        for f in self.filters:
            if f.filter_type == "exif_field" and exif_flat is None:
                exif_flat = {k.lower(): v for k, v in _flatten_exif(exif or {}).items()}
            if f.matches(combined, combined_lower, exif_flat or {}):
                matched_ids.append(f.id)
                if first_reason is None:
                    first_reason = f.reason
        return (matched_ids, first_reason)


def compile_filters(rows) -> FilterSet:
    """Compile FilterCriteria ORM rows into a FilterSet. Bad regexes are skipped + logged
    (they should have been rejected at create-time; this guards against legacy/corrupt data)."""
    compiled: list[CompiledFilter] = []
    for r in rows:
        try:
            compiled.append(CompiledFilter(r.id, r.name, r.filter_type, r.value, r.project_id))
        except re.error as e:
            logger.warning("Skipping filter #%s with invalid regex: %s", getattr(r, "id", "?"), e)
    return FilterSet(compiled)


async def load_active_filters(db, project_id: int | None) -> FilterSet:
    """Load active filters that apply to the given project (its own filters + globals)."""
    from sqlalchemy import select, or_
    from app.models.filter_criteria import FilterCriteria

    stmt = select(FilterCriteria).where(FilterCriteria.is_active.is_(True))
    if project_id is not None:
        stmt = stmt.where(
            or_(FilterCriteria.project_id == project_id, FilterCriteria.project_id.is_(None))
        )
    rows = (await db.execute(stmt)).scalars().all()
    return compile_filters(rows)
