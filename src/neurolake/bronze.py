"""Bronze layer: land a dataset snapshot as raw Parquet, one partition per
(dataset_id, snapshot). Re-ingesting a snapshot replaces its partition, so runs are
idempotent and safe to retry.

Layout:
    <lake>/bronze/<table>/dataset_id=<id>/snapshot=<tag>/data.parquet

Tables (every row carries the run_id that wrote it):
    raw_files        every file in the snapshot (path, size)
    raw_json_docs    raw text of every JSON metadata file (validity checked downstream)
    raw_tsv_rows     participants/sessions/scans TSV rows as JSON objects
    raw_read_errors  metadata files whose content could not be read
    raw_ingest_runs  one row per ingest run; written last, it is the commit marker

Consistency: all tables are written to a staging folder outside the bronze globs,
then moved into place with raw_ingest_runs last. Staging models only read rows whose
run_id matches the dataset's current run, so a crash mid-publish can leave a dataset
empty until the next ingest, but never mixes two runs. A per-dataset lock file stops
two ingests of the same dataset from interleaving.
"""

from __future__ import annotations

import csv
import fcntl
import io
import json
import logging
import re
import shutil
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pathspec
import pyarrow as pa
import pyarrow.parquet as pq

from .sources.base import DatasetSource, MetadataReadError, is_metadata

log = logging.getLogger(__name__)

_RUN = ("run_id", pa.string())
_SCHEMAS = {
    "raw_files": pa.schema(
        [_RUN, ("path", pa.string()), ("size_bytes", pa.int64()), ("is_bidsignored", pa.bool_())]
    ),
    "raw_json_docs": pa.schema([_RUN, ("path", pa.string()), ("content", pa.string())]),
    "raw_tsv_rows": pa.schema(
        [
            _RUN,
            ("path", pa.string()),
            ("row_number", pa.int32()),
            ("row_json", pa.string()),
            ("is_malformed", pa.bool_()),
        ]
    ),
    "raw_read_errors": pa.schema(
        [_RUN, ("path", pa.string()), ("error", pa.string()), ("error_kind", pa.string())]
    ),
    "raw_ingest_runs": pa.schema(
        [
            _RUN,
            ("source", pa.string()),
            ("snapshot_created_at", pa.timestamp("us", tz="UTC")),
            ("started_at", pa.timestamp("us", tz="UTC")),
            ("finished_at", pa.timestamp("us", tz="UTC")),
            ("n_files", pa.int64()),
            ("n_ignored_files", pa.int64()),
            ("bidsignored_core_files", pa.list_(pa.string())),
            ("n_metadata_files", pa.int64()),
            ("n_read_errors", pa.int64()),
        ]
    ),
}
# raw_ingest_runs must stay last: it is the commit marker.
TABLES = tuple(_SCHEMAS)


@dataclass
class IngestResult:
    dataset_id: str
    snapshot: str
    run_id: str
    n_files: int
    n_metadata_files: int
    n_read_errors: int


def parse_tsv(text: str) -> list[tuple[dict, bool]]:
    """Parse a BIDS TSV into (row, is_malformed) pairs. Values stay strings; typing
    and the BIDS `n/a` convention are handled in dbt so the raw layer stays raw."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    reader = csv.reader(io.StringIO("\n".join(lines)), delimiter="\t", quoting=csv.QUOTE_NONE)
    header = [h.strip() for h in next(reader)]
    rows = []
    for values in reader:
        values = [v.strip() for v in values]
        malformed = len(values) != len(header)
        row = dict(zip(header, values, strict=False))
        if len(values) > len(header):
            row["_extra"] = values[len(header) :]
        rows.append((row, malformed))
    return rows


@contextmanager
def _dataset_lock(lake: Path, dataset_id: str) -> Iterator[None]:
    lock_dir = lake / "_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    with open(lock_dir / f"{dataset_id}.lock", "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def _publish(staged: Path, part: Path, trash: Path) -> None:
    """Swap a staged partition into place, keeping the gap as small as possible. The
    old partition is moved outside the bronze globs before it is deleted."""
    part.parent.mkdir(parents=True, exist_ok=True)
    if part.exists():
        part.rename(trash)
    staged.rename(part)


_CORE_FILES = re.compile(
    r"^(dataset_description\.json|participants\.(tsv|json)|README(\.\w+)?|CHANGES|LICENSE|CITATION\.cff)$"
)


def _is_core_file(path: str) -> bool:
    return bool(_CORE_FILES.match(path))


def _bidsignore(source: DatasetSource) -> pathspec.GitIgnoreSpec | None:
    """The dataset's .bidsignore (gitignore syntax), as the BIDS validator applies it."""
    try:
        text = source.read_text(".bidsignore")
    except (MetadataReadError, FileNotFoundError):
        return None
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    return pathspec.GitIgnoreSpec.from_lines(lines) if lines else None


def ingest(source: DatasetSource, lake: str | Path, max_workers: int = 8) -> IngestResult:
    """Land one snapshot. Content-level read problems (bad encoding, missing file) are
    recorded and surfaced as data-quality issues; anything else (network failures
    after retries, API errors) propagates so the run fails and can be retried."""
    lake = Path(lake)
    run_id = uuid.uuid4().hex
    started = datetime.now(timezone.utc)
    log.info("ingest %s:%s from %s", source.dataset_id, source.snapshot, source.source_name)

    files = list(source.list_files())
    # Files matched by .bidsignore are kept in the listing (they exist, which matters
    # for references from scans.tsv) but are never read or checked.
    ignore = _bidsignore(source)
    matched = {f.path for f in files if ignore and ignore.match_file(f.path)}
    # Core files are never ignored, even when a broad pattern (say **/*dat*) matches them.
    core_hits = sorted(p for p in matched if _is_core_file(p))
    ignored = matched - set(core_hits)
    if ignored:
        log.info("%s: %d files covered by .bidsignore", source.dataset_id, len(ignored))
    meta_paths = [f.path for f in files if is_metadata(f.path) and f.path not in ignored]

    def fetch(path: str) -> tuple[str, str | None, MetadataReadError | None]:
        try:
            return path, source.read_text(path), None
        except MetadataReadError as exc:
            return path, None, exc

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        fetched = list(pool.map(fetch, meta_paths))

    json_rows, tsv_rows, error_rows = [], [], []
    for path, text, err in fetched:
        if err is not None:
            log.warning("could not read %s: %s", path, err)
            error_rows.append({"run_id": run_id, "path": path, "error": str(err), "error_kind": err.kind})
        elif path.endswith(".json"):
            json_rows.append({"run_id": run_id, "path": path, "content": text})
        else:
            for i, (row, bad) in enumerate(parse_tsv(text), start=1):
                tsv_rows.append(
                    {
                        "run_id": run_id,
                        "path": path,
                        "row_number": i,
                        "row_json": json.dumps(row),
                        "is_malformed": bad,
                    }
                )

    out = {
        "raw_files": [
            {
                "run_id": run_id,
                "path": f.path,
                "size_bytes": f.size_bytes,
                "is_bidsignored": f.path in ignored,
            }
            for f in files
        ],
        "raw_json_docs": json_rows,
        "raw_tsv_rows": tsv_rows,
        "raw_read_errors": error_rows,
        "raw_ingest_runs": [
            {
                "run_id": run_id,
                "source": source.source_name,
                "snapshot_created_at": getattr(source, "snapshot_created_at", None),
                "started_at": started,
                "finished_at": datetime.now(timezone.utc),
                "n_files": len(files),
                "n_ignored_files": len(ignored),
                "bidsignored_core_files": core_hits,
                "n_metadata_files": len(meta_paths),
                "n_read_errors": len(error_rows),
            }
        ],
    }

    staging = lake / "_staging" / run_id
    try:
        for table in TABLES:
            dest = staging / table
            dest.mkdir(parents=True)
            pq.write_table(pa.Table.from_pylist(out[table], schema=_SCHEMAS[table]), dest / "data.parquet")
        with _dataset_lock(lake, source.dataset_id):
            for table in TABLES:  # raw_ingest_runs last
                part = (
                    lake
                    / "bronze"
                    / table
                    / f"dataset_id={source.dataset_id}"
                    / f"snapshot={source.snapshot}"
                )
                _publish(staging / table, part, staging / f"old-{table}")
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    return IngestResult(
        source.dataset_id, source.snapshot, run_id, len(files), len(meta_paths), len(error_rows)
    )
