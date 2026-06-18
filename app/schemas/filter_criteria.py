from datetime import datetime
from typing import Literal
from pydantic import BaseModel, field_validator, model_validator

from app.services.filter_service import validate_filter, FilterValidationError

FilterType = Literal["keyword", "regex", "exif_field"]


class FilterCriteriaCreate(BaseModel):
    name: str
    filter_type: FilterType
    value: str = ""
    project_id: int | None = None  # None = global (all projects)
    is_active: bool = True

    @field_validator("name")
    @classmethod
    def name_required(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("name is required")
        return v

    @model_validator(mode="after")
    def validate_value(self):
        # Normalizes (blank regex -> default email regex) and rejects empty/invalid values.
        try:
            self.value = validate_filter(self.filter_type, self.value)
        except FilterValidationError as e:
            raise ValueError(str(e))
        return self


class FilterCriteriaUpdate(BaseModel):
    name: str | None = None
    filter_type: FilterType | None = None
    value: str | None = None
    project_id: int | None = None
    is_active: bool | None = None
    # Sentinel so callers can clear project_id (make global) explicitly if needed.

    # Note: cross-field validation (value vs filter_type) is handled in the router,
    # which knows the existing row's filter_type when only `value` is changed.


class FilterCriteriaResponse(BaseModel):
    id: int
    name: str
    filter_type: str
    value: str
    project_id: int | None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class FilterBackfillRequest(BaseModel):
    # project_id None = scan the whole database (subject to each filter's own scope).
    project_id: int | None = None
