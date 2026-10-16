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

Incremental runs: `ingest_unless_current` skips a snapshot that is already the
dataset's current run and fully published (see `is_current_and_complete`).
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
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager, contextmanager, nullcontext
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
    skipped: bool = False


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
    return _ingest(source, lake, max_workers, publish_lock=_dataset_lock(lake, source.dataset_id))


def _ingest(
    source: DatasetSource, lake: Path, max_workers: int, publish_lock: AbstractContextManager
) -> IngestResult:
    """ingest() with the lock to hold while publishing passed in. ingest_unless_current
    already holds the dataset lock and passes nullcontext(): flock is not re-entrant,
    so taking it twice in one process would deadlock."""
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
        with publish_lock:
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


def _partition(lake: Path, table: str, dataset_id: str, snapshot: str) -> Path:
    return lake / "bronze" / table / f"dataset_id={dataset_id}" / f"snapshot={snapshot}" / "data.parquet"


def current_run(lake: str | Path, dataset_id: str) -> dict | None:
    """The dataset's current run, defined as in stg_ingest_runs.is_current: the
    raw_ingest_runs row with the latest finished_at across all of its snapshots, ties
    broken by the larger run_id. Rows without finished_at are ignored. Reads the
    Parquet directly, so it needs neither dbt nor the DuckDB file. Raises on an
    unreadable partition."""
    best = None
    root = Path(lake) / "bronze" / "raw_ingest_runs" / f"dataset_id={dataset_id}"
    for f in sorted(root.glob("snapshot=*/data.parquet")):
        for row in pq.read_table(f).to_pylist():
            if row.get("finished_at") is None or row.get("run_id") is None:
                continue
            if best is None or (row["finished_at"], row["run_id"]) > (best["finished_at"], best["run_id"]):
                best = {**row, "snapshot": f.parent.name.removeprefix("snapshot=")}
    return best


_RUN_FIELDS = ("n_files", "n_metadata_files", "n_read_errors")


def _complete_run(lake: Path, dataset_id: str, snapshot: str) -> dict | None:
    """The current run if it is for `snapshot` and fully published, else None. Any
    problem reading the lake (a corrupt partition, a missing column, odd values) means
    "not complete", so the dataset is re-ingested instead of failing on every run."""
    try:
        run = current_run(lake, dataset_id)
        if run is None or run["snapshot"] != snapshot:
            return None
        if any(run.get(k) is None for k in _RUN_FIELDS):
            return None
        expected_rows = {"raw_files": run["n_files"], "raw_read_errors": run["n_read_errors"]}
        for table in TABLES:
            f = _partition(lake, table, dataset_id, snapshot)
            if not f.exists():
                return None
            if "run_id" not in pq.read_schema(f).names:
                return None
            ids = pq.read_table(f, columns=["run_id"]).column("run_id")
            if set(ids.to_pylist()) - {run["run_id"]}:
                return None
            if table in expected_rows and len(ids) != expected_rows[table]:
                return None
        return run
    except (pa.ArrowException, OSError, KeyError, TypeError, ValueError) as exc:
        log.warning(
            "%s:%s: cannot verify bronze (%s: %s); re-ingesting",
            dataset_id,
            snapshot,
            type(exc).__name__,
            exc,
        )
        return None


def is_current_and_complete(lake: str | Path, dataset_id: str, snapshot: str) -> bool:
    """True when `snapshot` is the dataset's current run and that run is fully published.

    Fully published means every bronze table has a partition for (dataset, snapshot)
    and no row in any of them carries another run_id. Tables can legitimately be empty
    (most clean datasets have no read errors), and an empty partition has no run_id to
    compare. That is safe because _publish swaps raw_files first and raw_ingest_runs
    last: if a later run crashed mid-publish, raw_ingest_runs still names the older run
    while raw_files already holds the newer one, so raw_files exposes it. raw_files and
    raw_read_errors must also hold exactly the row counts the run recorded, which
    catches the corner where the newer run left one of them empty. A lake that cannot
    be read counts as not complete."""
    return _complete_run(Path(lake), dataset_id, snapshot) is not None


def ingest_unless_current(
    lake: str | Path,
    dataset_id: str,
    snapshot: str,
    open_source: Callable[[], AbstractContextManager[DatasetSource]],
    force: bool = False,
) -> IngestResult:
    """Ingest a resolved snapshot unless it is already current and complete.

    `open_source` is only called when an ingest is needed, so a skipped dataset is
    never cloned. The dataset lock is held across the check, the fetch and the
    publish: a second worker on the same dataset waits, then sees the finished run
    and skips instead of fetching it again. A skip trusts the snapshot tag (not the
    commit it points to) and keeps bronze written by older ingest code; use force
    after either changes."""
    lake = Path(lake)
    with _dataset_lock(lake, dataset_id):
        run = None if force else _complete_run(lake, dataset_id, snapshot)
        if run is not None:
            return IngestResult(
                dataset_id,
                snapshot,
                run["run_id"],
                run["n_files"],
                run["n_metadata_files"],
                run["n_read_errors"],
                skipped=True,
            )
        with open_source() as source:
            if (source.dataset_id, source.snapshot) != (dataset_id, snapshot):
                raise ValueError(
                    f"source is {source.dataset_id}:{source.snapshot}, expected {dataset_id}:{snapshot}"
                )
            return _ingest(source, lake, 8, publish_lock=nullcontext())
