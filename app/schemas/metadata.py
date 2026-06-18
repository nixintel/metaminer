from datetime import datetime
from typing import Literal, Union
from pydantic import BaseModel, Field
import json


class MetadataRecordResponse(BaseModel):
    id: int
    submission_id: int
    pdf_variant: str | None
    raw_json: dict
    exiftool_version: str | None
    extracted_at: datetime
    file_name: str | None
    file_type: str | None
    file_type_extension: str | None
    mime_type: str | None
    file_size: str | None
    create_date: str | None
    modify_date: str | None
    author: str | None
    title: str | None
    creator_tool: str | None
    producer: str | None
    pdf_version: str | None
    interesting: bool = False
    interesting_reason: str | None = None
    # Submission context
    source_url: str | None = None
    submission_mode: str | None = None
    project_id: int | None = None
    project_name: str | None = None

    model_config = {"from_attributes": True}

    @classmethod
    def from_orm_with_context(cls, record, submission, project):
        data = record.__dict__.copy()
        data["raw_json"] = json.loads(record.raw_json) if isinstance(record.raw_json, str) else record.raw_json
        data["source_url"] = submission.source_url
        data["submission_mode"] = submission.submission_mode
        data["project_id"] = project.id
        data["project_name"] = project.name
        return cls(**data)


class MetadataRecordUpdate(BaseModel):
    # Typed model (not a free-form dict) so a later auto-classification phase can add
    # e.g. interesting_reason without breaking existing callers.
    interesting: bool | None = None


class MetadataQueryParams(BaseModel):
    project_id: int | None = None
    file_type: str | None = None
    file_type__in: str | None = None  # comma-separated list
    author: str | None = None
    title: str | None = None
    creator_tool: str | None = None
    producer: str | None = None
    mime_type: str | None = None
    pdf_variant: str | None = None
    submission_mode: str | None = None
    source_url__contains: str | None = None
    extracted_after: datetime | None = None
    extracted_before: datetime | None = None
    q: str | None = None
    raw_contains: str | None = None
    sort_by: str = "extracted_at"
    order: str = "desc"
    limit: int = 50
    offset: int = 0


class FilterCondition(BaseModel):
    field: str
    op: Literal["contains", "equals", "starts_with", "before", "after", "in"]
    value: str


# NOTE: this is the ad-hoc query-builder tree node (AND/OR of conditions), unrelated to
# the persisted FilterGroup entity (app/models/filter_group.py). Named QueryGroup to avoid
# confusion with the auto-tagging filter groups.
class QueryGroup(BaseModel):
    operator: Literal["AND", "OR"]
    conditions: list[Union["QueryGroup", FilterCondition]]


QueryGroup.model_rebuild()


class QueryRequest(BaseModel):
    operator: Literal["AND", "OR"] = "AND"
    conditions: list[Union[QueryGroup, FilterCondition]] = []
    sort_by: str = "extracted_at"
    order: Literal["asc", "desc"] = "desc"
    limit: int = Field(50, ge=1, le=500)
    offset: int = Field(0, ge=0)
