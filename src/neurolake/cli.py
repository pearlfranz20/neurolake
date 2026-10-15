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
from .sources import LocalBidsSource, OpenNeuroSource

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
