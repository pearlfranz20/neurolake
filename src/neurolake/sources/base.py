"""Common interface for BIDS dataset sources.

A source exposes one dataset snapshot: its file listing (paths relative to the
dataset root, with sizes) and a way to read the small text metadata files. Imaging
payloads are never downloaded; the pipeline only needs their names and sizes.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

# Metadata files worth fetching. Everything else is listed but not read.
_METADATA_PATTERNS = [
    re.compile(r"^dataset_description\.json$"),
    re.compile(r"^participants\.tsv$"),
    re.compile(r"(^|/)[^/]+_sessions\.tsv$"),
    re.compile(r"(^|/)[^/]+_scans\.tsv$"),
    re.compile(r"(^|/)[^/]+\.json$"),
]

# Derivatives and source data are separate BIDS datasets with their own rules.
_EXCLUDED_PREFIXES = ("derivatives/", "sourcedata/", "code/", ".git/", ".datalad/")


class MetadataReadError(Exception):
    """A metadata file exists in the listing but its content cannot be used (missing
    content, bad encoding, 404). Recorded as a data-quality issue, not a failed run.

    `kind` separates problems with the dataset ("encoding", "missing") from content
    that simply was not fetched ("not_fetched"), which is not the dataset's fault."""

    def __init__(self, message: str, kind: str = "missing"):
        super().__init__(message)
        self.kind = kind


@dataclass(frozen=True)
class FileEntry:
    path: str
    size_bytes: int | None


def is_excluded(path: str) -> bool:
    return path.startswith(_EXCLUDED_PREFIXES) or "/." in f"/{path}"


def is_metadata(path: str) -> bool:
    return not is_excluded(path) and any(p.search(path) for p in _METADATA_PATTERNS)


class DatasetSource(Protocol):
    dataset_id: str
    snapshot: str
    source_name: str

    def list_files(self) -> Iterable[FileEntry]: ...

    def read_text(self, path: str) -> str: ...
