"""OpenNeuro datasets via their DataLad mirrors on GitHub (github.com/OpenNeuroDatasets).

Each snapshot is a git tag. A shallow clone of the tag brings every metadata file
(small text files live in git) and leaves imaging data as git-annex symlinks, whose
keys still carry the file size. So a full metadata ingest needs only a few hundred
KB to a few MB per dataset and no OpenNeuro API access.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from .base import FileEntry
from .local import LocalBidsSource

log = logging.getLogger(__name__)

MIRROR_URL = "https://github.com/OpenNeuroDatasets/{dataset}.git"
_SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
_NUMERIC = re.compile(r"^\d+$")
# GitHub answers a request for a repository that does not exist with an auth prompt.
_ABSENT_MARKERS = ("could not read Username", "Repository not found", "not found")


class MirrorError(RuntimeError):
    pass


def _git(*args: str, timeout: int = 600) -> str:
    proc = subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ, "GIT_TERMINAL_PROMPT": "0"},
    )
    if proc.returncode != 0:
        raise MirrorError(f"git {' '.join(args[:2])} failed: {proc.stderr.strip()[-500:]}")
    return proc.stdout


def _snapshot_tags(tags: list[str]) -> list[str]:
    """Snapshot tags oldest first. x.y.z tags when there are any; datasets that only
    have OpenNeuro's early numeric tags (00001, 00002...) use those. The two families
    are never mixed, so "first" and "latest" are chosen from the same series."""
    versions = sorted((tuple(map(int, m.groups())), t) for t in tags if (m := _SEMVER.match(t)))
    if versions:
        return [t for _, t in versions]
    return sorted((t for t in tags if _NUMERIC.match(t)), key=int)


def latest_snapshot_tag(tags: list[str]) -> str | None:
    ordered = _snapshot_tags(tags)
    return ordered[-1] if ordered else None


def earliest_snapshot_tag(tags: list[str]) -> str | None:
    ordered = _snapshot_tags(tags)
    return ordered[0] if ordered else None


def resolve_snapshot(dataset_id: str, snapshot: str | None) -> str:
    """Turn None/"latest"/"first" into a concrete tag; any other value is used as given."""
    if snapshot not in (None, "latest", "first"):
        return snapshot
    pick = earliest_snapshot_tag if snapshot == "first" else latest_snapshot_tag
    tag = pick(list_tags(dataset_id))
    if tag is None:
        raise MirrorError(f"{dataset_id} has no snapshot tags")
    return tag


def list_tags(dataset_id: str) -> list[str]:
    out = _git("ls-remote", "--tags", "--refs", MIRROR_URL.format(dataset=dataset_id), timeout=120)
    return [line.split("refs/tags/", 1)[1] for line in out.splitlines() if "refs/tags/" in line]


def probe(dataset_id: str, attempts: int = 4) -> tuple[str, str | None]:
    """Classify a mirror as ("found", tag), ("no_snapshot", None) or ("absent", None).

    Transient failures (timeouts, rate limits, network errors) are retried and, if
    they persist, raised: treating them as "absent" would silently bias a sample."""
    delay = 2.0
    for attempt in range(attempts):
        try:
            tag = latest_snapshot_tag(list_tags(dataset_id))
            return ("found", tag) if tag else ("no_snapshot", None)
        except MirrorError as exc:
            if any(m in str(exc) for m in _ABSENT_MARKERS):
                return ("absent", None)
            if attempt == attempts - 1:
                raise
        except subprocess.TimeoutExpired:
            if attempt == attempts - 1:
                raise MirrorError(f"{dataset_id}: ls-remote timed out") from None
        time.sleep(delay)
        delay *= 2
    raise AssertionError("unreachable")


class GitHubMirrorSource:
    """Use as a context manager so the temporary clone is removed afterwards."""

    source_name = "openneuro-github"

    def __init__(self, dataset_id: str, snapshot: str | None = None, workdir: str | Path | None = None):
        self.dataset_id = dataset_id
        self.snapshot = snapshot = resolve_snapshot(dataset_id, snapshot)
        self._tmp = Path(tempfile.mkdtemp(prefix=f"{dataset_id}-", dir=workdir))
        dest = self._tmp / dataset_id
        log.info("cloning %s@%s", dataset_id, snapshot)
        try:
            _git(
                "-c",
                "advice.detachedHead=false",
                "clone",
                "--quiet",
                "--depth",
                "1",
                "--branch",
                snapshot,
                "--single-branch",
                MIRROR_URL.format(dataset=dataset_id),
                str(dest),
            )
            self._local = LocalBidsSource(dest, dataset_id=dataset_id, snapshot=snapshot)
            committed = _git("-C", str(dest), "log", "-1", "--format=%cI").strip()
            self.snapshot_created_at = datetime.fromisoformat(committed).astimezone(timezone.utc)
        except Exception:
            self.close()
            raise

    def list_files(self) -> Iterator[FileEntry]:
        return self._local.list_files()

    def read_text(self, path: str) -> str:
        return self._local.read_text(path)

    def close(self) -> None:
        shutil.rmtree(self._tmp, ignore_errors=True)

    def __enter__(self) -> GitHubMirrorSource:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
