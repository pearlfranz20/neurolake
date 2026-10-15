"""Read a BIDS dataset from a local directory, including DataLad / git-annex clones
where imaging content has not been fetched."""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from pathlib import Path

from .base import FileEntry, MetadataReadError, is_excluded

# git-annex keys embed the content size: MD5E-s5663237--4608ff...nii.gz
_ANNEX_KEY_SIZE = re.compile(r"(?:^|/)[A-Z0-9]+-s(\d+)(?:-[^/]*)?--[^/]+$")
_ANNEX_POINTER_MAX_BYTES = 1024


def annex_size(link_target: str) -> int | None:
    """Content size from a git-annex symlink target or pointer, if it holds a key."""
    m = _ANNEX_KEY_SIZE.search(link_target.strip())
    return int(m.group(1)) if m else None


class LocalBidsSource:
    source_name = "local"

    def __init__(self, root: str | Path, dataset_id: str | None = None, snapshot: str = "local"):
        self.root = Path(root).resolve()
        if (
            not (self.root / "dataset_description.json").exists()
            and not (self.root / "dataset_description.json").is_symlink()
        ):
            raise FileNotFoundError(f"{self.root} has no dataset_description.json; not a BIDS root")
        self.dataset_id = dataset_id or self.root.name
        self.snapshot = snapshot

    def _size(self, full: Path) -> int | None:
        if full.is_symlink():
            target = os.readlink(full)
            if "annex/objects" in target:
                return annex_size(target)
        try:
            size = full.stat().st_size
        except FileNotFoundError:
            return None
        # Unlocked annex files that were never fetched are tiny pointer files.
        if size <= _ANNEX_POINTER_MAX_BYTES:
            try:
                head = full.read_bytes()[:_ANNEX_POINTER_MAX_BYTES]
            except OSError:
                return size
            if head.startswith(b"/annex/objects/"):
                return annex_size(head.decode("ascii", "replace"))
        return size

    def list_files(self) -> Iterator[FileEntry]:
        for dirpath, dirnames, filenames in os.walk(self.root):
            dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
            for name in sorted(filenames):
                full = Path(dirpath) / name
                rel = full.relative_to(self.root).as_posix()
                if is_excluded(rel):
                    continue
                yield FileEntry(rel, self._size(full))

    def read_text(self, path: str) -> str:
        full = self.root / path
        try:
            raw = full.read_bytes()
        except FileNotFoundError as exc:
            if full.is_symlink():
                raise MetadataReadError("content not fetched (git-annex)", kind="not_fetched") from exc
            raise MetadataReadError(f"FileNotFoundError: {exc}") from exc
        if raw.startswith(b"/annex/objects/") and len(raw) <= _ANNEX_POINTER_MAX_BYTES:
            raise MetadataReadError("content not fetched (git-annex)", kind="not_fetched")
        try:
            return raw.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise MetadataReadError(f"UnicodeDecodeError: {exc}", kind="encoding") from exc
