"""neurolake command line.

neurolake ingest-openneuro ds000117 ds003097 --lake data/lake
neurolake ingest-mirror ds000117 ds002718 [--force] --lake data/lake
neurolake ingest-local ~/datasets/ds000117 --lake data/lake
neurolake transform --lake data/lake
neurolake report --lake data/lake --out data/report.html
neurolake demo                 # fixtures -> ingest -> transform -> report, offline
"""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from . import bronze, fixtures
from . import transform as transform_mod
from .report import write_report
from .sources import LocalBidsSource, OpenNeuroSource, github_mirror

app = typer.Typer(add_completion=False, no_args_is_help=True, help="BIDS metadata lakehouse.")

DEFAULT_LAKE = Path("data/lake")
LakeOpt = typer.Option(DEFAULT_LAKE, "--lake", help="Lake root directory.")


@app.callback()
def _main(verbose: bool = typer.Option(False, "--verbose", "-v")):
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )


@app.command("ingest-openneuro")
def ingest_openneuro(
    dataset_ids: list[str] = typer.Argument(..., help="OpenNeuro accession numbers, e.g. ds000117"),
    snapshot: str | None = typer.Option(None, help="Snapshot tag; defaults to the latest."),
    lake: Path = LakeOpt,
):
    """Land OpenNeuro snapshots (metadata only) into the bronze layer."""
    if snapshot and len(dataset_ids) > 1:
        raise typer.BadParameter("--snapshot only makes sense with a single dataset")
    for ds in dataset_ids:
        r = bronze.ingest(OpenNeuroSource(ds, snapshot), lake)
        typer.echo(
            f"{r.dataset_id}:{r.snapshot}  {r.n_files} files, "
            f"{r.n_metadata_files} metadata, {r.n_read_errors} read errors"
        )


@app.command("ingest-mirror")
def ingest_mirror(
    dataset_ids: list[str] = typer.Argument(None, help="OpenNeuro accession numbers, e.g. ds000117"),
    from_file: Path | None = typer.Option(None, "--from-file", exists=True, help="One accession per line."),
    snapshot: str | None = typer.Option(
        None, help='Snapshot tag, or "first" / "latest" (the default) for each dataset.'
    ),
    force: bool = typer.Option(False, "--force", help="Re-ingest even if the snapshot is already current."),
    workers: int = typer.Option(4, min=1, max=16, help="Datasets ingested in parallel."),
    lake: Path = LakeOpt,
):
    """Land OpenNeuro snapshots from their GitHub (DataLad) mirrors. No OpenNeuro API needed.

    A dataset whose resolved snapshot is already its current, fully published run is
    skipped without cloning; --force re-ingests it. With several datasets a failure is
    reported and the rest carry on; the command exits non-zero if any dataset failed."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    ids = list(dataset_ids or [])
    if from_file:
        ids += [
            ln.strip() for ln in from_file.read_text().splitlines() if ln.strip() and not ln.startswith("#")
        ]
    ids = list(dict.fromkeys(ids))
    if not ids:
        raise typer.BadParameter("give dataset ids or --from-file")
    if snapshot not in (None, "first", "latest") and len(ids) > 1:
        raise typer.BadParameter("a specific --snapshot tag only makes sense with a single dataset")

    n_ingested = n_skipped = 0
    failed = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(github_mirror.ingest_mirror, ds, snapshot, lake, force): ds for ds in ids}
        for fut in as_completed(futures):
            ds = futures[fut]
            try:
                r = fut.result()
            except Exception as exc:  # keep going; report at the end
                failed.append(ds)
                typer.echo(f"{ds}  FAILED: {exc}", err=True)
                continue
            if r.skipped:
                n_skipped += 1
                typer.echo(f"{r.dataset_id}:{r.snapshot}  skipped (unchanged at {r.snapshot})")
            else:
                n_ingested += 1
                typer.echo(
                    f"{r.dataset_id}:{r.snapshot}  ingested: {r.n_files} files, "
                    f"{r.n_metadata_files} metadata, {r.n_read_errors} read errors"
                )
    typer.echo(f"{n_ingested} ingested, {n_skipped} skipped, {len(failed)} failed")
    if failed:
        raise typer.Exit(code=1)


@app.command("discover-mirrors")
def discover_mirrors(
    sample: int = typer.Option(100, help="How many datasets to return."),
    max_id: int = typer.Option(7500, help="Highest accession number to consider."),
    seed: int = typer.Option(42, help="Random seed, so a sample can be reproduced."),
    workers: int = typer.Option(8, min=1, max=32),
    log_file: Path | None = typer.Option(None, help="CSV of every probe: id, status, tag."),
):
    """Print a reproducible random sample of OpenNeuro accessions that have a GitHub mirror.

    Accessions ds000001..max_id are shuffled with `seed` and probed in that order; the
    sample is exactly the first `sample` of them with a snapshot tag. Every accession
    before the cut-off is resolved for certain (transient errors are retried, and a
    persistent one aborts), so the same seed always yields the same sample."""
    import csv
    import random
    import sys
    from concurrent.futures import ThreadPoolExecutor

    from .sources.github_mirror import probe

    candidates = [f"ds{n:06d}" for n in range(1, max_id + 1)]
    random.Random(seed).shuffle(candidates)
    found: list[str] = []
    log_rows: list[tuple[int, str, str, str]] = []
    batch = workers * 4
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for start in range(0, len(candidates), batch):
            ids = candidates[start : start + batch]
            for pos, (ds, (status, tag)) in enumerate(zip(ids, pool.map(probe, ids), strict=True), start):
                if len(found) < sample:
                    log_rows.append((pos, ds, status, tag or ""))
                    if status == "found":
                        found.append(ds)
            if len(found) >= sample:
                break
    if log_file:
        with open(log_file, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["shuffle_position", "dataset_id", "status", "tag"])
            w.writerows(log_rows)
    typer.echo(f"# seed={seed} max_id={max_id} probed={len(log_rows)} found={len(found)}", err=True)
    print(f"# discover-mirrors --sample {sample} --max-id {max_id} --seed {seed}", file=sys.stdout)
    for ds in sorted(found):
        typer.echo(ds)


@app.command("ingest-local")
def ingest_local(
    paths: list[Path] = typer.Argument(..., exists=True, file_okay=False),
    snapshot: str = typer.Option("local", help="Label for this snapshot."),
    lake: Path = LakeOpt,
):
    """Land local BIDS directories into the bronze layer."""
    for p in paths:
        r = bronze.ingest(LocalBidsSource(p, snapshot=snapshot), lake)
        typer.echo(f"{r.dataset_id}:{r.snapshot}  {r.n_files} files, {r.n_metadata_files} metadata")


def run_dbt(lake: Path, args: list[str]) -> None:
    try:
        transform_mod.run_dbt(lake, args)
    except transform_mod.DbtError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def transform(
    lake: Path = LakeOpt,
    select: str | None = typer.Option(None, help="dbt node selection."),
):
    """Build and test all dbt models (dbt build)."""
    run_dbt(lake, ["build", *(["--select", select] if select else [])])


@app.command()
def report(lake: Path = LakeOpt, out: Path = typer.Option(Path("data/report.html"))):
    """Render the quality marts to a standalone HTML report."""
    path = write_report(lake / "neurolake.duckdb", out)
    typer.echo(f"wrote {path}")


@app.command("make-fixtures")
def make_fixtures(out: Path = typer.Argument(Path("data/fixtures"))):
    """Write synthetic BIDS datasets with known defects."""
    for root in fixtures.generate(out):
        typer.echo(root)


@app.command()
def demo(lake: Path = LakeOpt, out: Path = typer.Option(Path("data/report.html"))):
    """Run the whole pipeline offline on the synthetic fixtures."""
    roots = fixtures.generate(lake.parent / "fixtures")
    for root in roots:
        bronze.ingest(LocalBidsSource(root), lake)
    run_dbt(lake, ["build"])
    report(lake, out)


if __name__ == "__main__":
    app()
