"""Read a BIDS dataset from a local directory (for example a `datalad clone`)."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

from .base import FileEntry, MetadataReadError, is_excluded


class LocalBidsSource:
    source_name = "local"

    def __init__(self, root: str | Path, dataset_id: str | None = None, snapshot: str = "local"):
        self.root = Path(root).resolve()
        if not (self.root / "dataset_description.json").exists():
            raise FileNotFoundError(f"{self.root} has no dataset_description.json; not a BIDS root")
        self.dataset_id = dataset_id or self.root.name
        self.snapshot = snapshot

    def list_files(self) -> Iterator[FileEntry]:
        for dirpath, dirnames, filenames in os.walk(self.root):
            dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
            for name in sorted(filenames):
                full = Path(dirpath) / name
                rel = full.relative_to(self.root).as_posix()
                if is_excluded(rel):
                    continue
                # DataLad/git-annex leaves broken symlinks for content that is not
                # fetched. Keep the entry, but the size is unknown.
                try:
                    size = full.stat().st_size
                except FileNotFoundError:
                    size = None
                yield FileEntry(rel, size)

    def read_text(self, path: str) -> str:
        try:
            return (self.root / path).read_text(encoding="utf-8-sig")
        except (FileNotFoundError, UnicodeDecodeError) as exc:
            raise MetadataReadError(f"{type(exc).__name__}: {exc}") from exc
