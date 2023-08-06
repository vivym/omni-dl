from datetime import datetime

from pydantic import Field, HttpUrl

from .base import BaseSchema


class Task(BaseSchema):
    id: str
    url: str
    ext: str
    headers: dict[str, str] | None = None
    remark: str | None = None

    storage_uri: str | None = None
    storage_options: dict[str, str] | None = None
    size: int | None = None
    checksum: str | None = None
    checksum_algo: str | None = None

    status: str
    download_id: str | None = None
    retry_count: int
    created_at: datetime
    updated_at: datetime

    created_by: str
    updated_by: str


class TaskResponse(BaseSchema):
    task: Task | None


class TaskCreation(BaseSchema):
    url: HttpUrl = Field(..., description="URL to download")
    ext: str = Field(..., description="File extension")
    headers: dict[str, str] | None = None
    remark: str | None = None


class TaskGlobalStats(BaseSchema):
    pending: int
    downloading: int
    downloaded: int
    failed: int
    total: int


class TaskDownloadResult(BaseSchema):
    type: str
    error: str | None = None
    task: Task


class TaskLocalStats(BaseSchema):
    items_per_second: float
    bytes_per_second: float

    total_items: int
    total_bytes: int

    failed_items: int
