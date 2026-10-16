"""Weekly OpenNeuro metadata refresh.

ingest (one mapped task per dataset)  ->  dbt build  ->  HTML report

Datasets come from the `dataset_ids` param, so a manual run can target any list.
Ingest tasks are idempotent: each overwrites its own (dataset, snapshot) partition,
so retries and re-runs are safe. A dataset whose latest snapshot is already its
current, fully published run is not cloned again: the task succeeds and returns
status "skipped", so transform still runs. Set the `force` param to re-ingest
everything (for example after a change to the ingest code). Works on Airflow 3
(airflow.sdk) and 2.x.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

try:  # Airflow 3
    from airflow.sdk import Param, dag, get_current_context, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context

LAKE = os.environ.get("NEUROLAKE_LAKE", "/opt/neurolake/data/lake")
REPORT = os.environ.get("NEUROLAKE_REPORT", "/opt/neurolake/data/report.html")

DEFAULT_DATASETS = [
    "ds000001",  # balloon analog risk task, fMRI
    "ds000117",  # multimodal face recognition: MEG, EEG, fMRI
    "ds002718",  # face processing, EEG
]


@dag(
    dag_id="neurolake_openneuro",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "dataset_ids": Param(DEFAULT_DATASETS, type="array", description="OpenNeuro accessions"),
        "force": Param(False, type="boolean", description="Re-ingest datasets even if unchanged"),
    },
    tags=["neuroimaging", "bids", "dbt"],
    doc_md=__doc__,
)
def neurolake_openneuro():
    @task
    def dataset_ids() -> list[str]:
        # de-duplicated: two mapped tasks must never ingest the same dataset
        return list(dict.fromkeys(get_current_context()["params"]["dataset_ids"]))

    @task(max_active_tis_per_dag=4)
    def ingest(dataset_id: str) -> dict:
        # The GitHub (DataLad) mirrors are what the audit ran on; the OpenNeuro API
        # source (OpenNeuroSource) is a drop-in alternative.
        # Not AirflowSkipException for unchanged datasets: a skipped upstream would
        # skip transform too.
        from neurolake.sources.github_mirror import ingest_mirror

        force = bool(get_current_context()["params"].get("force", False))
        r = ingest_mirror(dataset_id, None, LAKE, force=force)
        return {
            "status": "skipped" if r.skipped else "ingested",
            "dataset_id": r.dataset_id,
            "snapshot": r.snapshot,
            "n_files": r.n_files,
            "n_read_errors": r.n_read_errors,
        }

    @task
    def transform(results: list[dict]) -> None:
        # dbt runs in a subprocess and fails the task if any model or test fails.
        from pathlib import Path

        from neurolake.transform import run_dbt

        run_dbt(Path(LAKE), ["build"])

    @task
    def report() -> str:
        from pathlib import Path

        from neurolake.report import write_report

        return str(write_report(Path(LAKE) / "neurolake.duckdb", REPORT))

    landed = ingest.expand(dataset_id=dataset_ids())
    transform(landed) >> report()


neurolake_openneuro()
