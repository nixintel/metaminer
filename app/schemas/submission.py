from pydantic import BaseModel


class ManualSubmit(BaseModel):
    project_id: int
    paths: list[str]  # files and/or directories (processed recursively)
    retain_files: bool = False
    pdf_mode: bool | None = None  # None = use global config default


class SubmissionResponse(BaseModel):
    submission_id: int
    project_id: int
    original_filename: str
    submission_mode: str
    records_created: int
    skipped_duplicate: bool = False
