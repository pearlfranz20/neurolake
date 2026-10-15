from pathlib import Path

import pytest

from neurolake import bronze, fixtures
from neurolake.sources import LocalBidsSource
from neurolake.transform import run_dbt


@pytest.fixture(scope="session")
def built_lake(tmp_path_factory) -> Path:
    """Fixtures -> bronze -> dbt build, once per test session."""
    root = tmp_path_factory.mktemp("neurolake")
    lake = root / "lake"
    for ds_root in fixtures.generate(root / "bids"):
        bronze.ingest(LocalBidsSource(ds_root), lake)
    run_dbt(lake, ["build", "--quiet"])
    return lake
