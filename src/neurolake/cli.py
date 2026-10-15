"""neurolake command line.

neurolake ingest-openneuro ds000117 ds003097 --lake data/lake
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
from .sources import GitHubMirrorSource, LocalBidsSource, OpenNeuroSource

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
    snapshot: str | None = typer.Option(None, help="Snapshot tag; defaults to the latest."),
    workers: int = typer.Option(4, min=1, max=16, help="Datasets ingested in parallel."),
    lake: Path = LakeOpt,
):
    """Land OpenNeuro snapshots from their GitHub (DataLad) mirrors. No OpenNeuro API needed.

    With several datasets a failure is reported and the rest carry on; the command
    exits non-zero if any dataset failed."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    ids = list(dataset_ids or [])
    if from_file:
        ids += [
            ln.strip() for ln in from_file.read_text().splitlines() if ln.strip() and not ln.startswith("#")
        ]
    ids = list(dict.fromkeys(ids))
    if not ids:
        raise typer.BadParameter("give dataset ids or --from-file")
    if snapshot and len(ids) > 1:
        raise typer.BadParameter("--snapshot only makes sense with a single dataset")

    def one(ds: str):
        with GitHubMirrorSource(ds, snapshot) as src:
            return bronze.ingest(src, lake)

    failed = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(one, ds): ds for ds in ids}
        for fut in as_completed(futures):
            ds = futures[fut]
            try:
                r = fut.result()
            except Exception as exc:  # keep going; report at the end
                failed.append(ds)
                typer.echo(f"{ds}  FAILED: {exc}", err=True)
                continue
            typer.echo(
                f"{r.dataset_id}:{r.snapshot}  {r.n_files} files, "
                f"{r.n_metadata_files} metadata, {r.n_read_errors} read errors"
            )
    typer.echo(f"{len(ids) - len(failed)}/{len(ids)} datasets ingested")
    if failed:
        raise typer.Exit(code=1)


@app.command("discover-mirrors")
def discover_mirrors(
    sample: int = typer.Option(100, help="How many existing datasets to return."),
    max_id: int = typer.Option(6500, help="Highest accession number to consider."),
    seed: int = typer.Option(42, help="Random seed, so a sample can be reproduced."),
    workers: int = typer.Option(8, min=1, max=32),
):
    """Print a reproducible random sample of OpenNeuro accessions that have a GitHub mirror."""
    import random
    from concurrent.futures import ThreadPoolExecutor

    from .sources.github_mirror import probe

    candidates = [f"ds{n:06d}" for n in range(1, max_id + 1)]
    random.Random(seed).shuffle(candidates)
    found: list[str] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for start in range(0, len(candidates), workers * 4):
            batch = candidates[start : start + workers * 4]
            for ds, tag in zip(batch, pool.map(probe, batch), strict=True):
                if tag and len(found) < sample:
                    found.append(ds)
            if len(found) >= sample:
                break
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
