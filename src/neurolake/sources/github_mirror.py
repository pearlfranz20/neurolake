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
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

from .base import FileEntry
from .local import LocalBidsSource

log = logging.getLogger(__name__)

MIRROR_URL = "https://github.com/OpenNeuroDatasets/{dataset}.git"
_SEMVER = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


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


def latest_snapshot_tag(tags: list[str]) -> str | None:
    """Highest x.y.z tag. OpenNeuro's very old numeric tags (00001...) are ignored."""
    versions = [(tuple(map(int, m.groups())), t) for t in tags if (m := _SEMVER.match(t))]
    return max(versions)[1] if versions else None


def list_tags(dataset_id: str) -> list[str]:
    out = _git("ls-remote", "--tags", "--refs", MIRROR_URL.format(dataset=dataset_id), timeout=120)
    return [line.split("refs/tags/", 1)[1] for line in out.splitlines() if "refs/tags/" in line]


def probe(dataset_id: str) -> str | None:
    """Latest snapshot tag of a mirror, or None if the mirror does not exist."""
    try:
        return latest_snapshot_tag(list_tags(dataset_id))
    except MirrorError:
        return None


class GitHubMirrorSource:
    """Use as a context manager so the temporary clone is removed afterwards."""

    source_name = "openneuro-github"

    def __init__(self, dataset_id: str, snapshot: str | None = None, workdir: str | Path | None = None):
        self.dataset_id = dataset_id
        if snapshot is None:
            snapshot = latest_snapshot_tag(list_tags(dataset_id))
            if snapshot is None:
                raise MirrorError(f"{dataset_id} has no x.y.z snapshot tags")
        self.snapshot = snapshot
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
