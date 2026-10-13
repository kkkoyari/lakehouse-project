import re
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4


PART_FILE_RE = re.compile(
    r"^part-\d{8}T\d{12}Z?-\d{8}T\d{12}Z?-[0-9a-f]{32}\.parquet$"
)


def _timestamp_token(value: datetime) -> str:
    if value.tzinfo is not None and value.utcoffset() is not None:
        value = value.astimezone(timezone.utc)
        return value.strftime("%Y%m%dT%H%M%S%fZ")
    return value.strftime("%Y%m%dT%H%M%S%f")


def part_file_name(
    min_timestamp: datetime,
    max_timestamp: datetime,
    file_id: Optional[str] = None,
) -> str:
    """Build a unique Parquet filename with its timestamp range."""
    if min_timestamp is None or max_timestamp is None:
        raise ValueError("Filename timestamp bounds must not be null")

    file_id = file_id or uuid4().hex
    return (
        f"part-{_timestamp_token(min_timestamp)}-"
        f"{_timestamp_token(max_timestamp)}-{file_id}.parquet"
    )
