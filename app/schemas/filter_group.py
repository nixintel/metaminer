from datetime import datetime
from pydantic import BaseModel, field_validator


class FilterMemberRef(BaseModel):
    id: int
    name: str

    model_config = {"from_attributes": True}


class FilterGroupCreate(BaseModel):
    name: str
    project_id: int | None = None  # None = global (all projects)
    is_active: bool = True
    filter_ids: list[int] = []

    @field_validator("name")
    @classmethod
    def name_required(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        return v


class FilterGroupUpdate(BaseModel):
    name: str | None = None
    is_active: bool | None = None
    # filter_ids: None = leave membership unchanged; [] = clear all members.
    filter_ids: list[int] | None = None
    # Note: project_id (scope) is not editable after creation (mirrors single filters).


class FilterGroupResponse(BaseModel):
    id: int
    name: str
    project_id: int | None
    is_active: bool
    created_at: datetime
    updated_at: datetime
    filters: list[FilterMemberRef] = []

    model_config = {"from_attributes": True}


class FilterGroupBackfillRequest(BaseModel):
    # project_id None = scan the whole database (subject to the group's own scope).
    project_id: int | None = None
