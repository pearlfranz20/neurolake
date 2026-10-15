import duckdb
import pytest

from neurolake import bronze, fixtures
from neurolake.sources import LocalBidsSource, MetadataReadError, is_metadata


def test_parse_tsv_flags_short_and_long_rows():
    rows = bronze.parse_tsv("a\tb\n1\t2\n3\n4\t5\t6\n\n")
    assert rows[0] == ({"a": "1", "b": "2"}, False)
    assert rows[1] == ({"a": "3"}, True)
    assert rows[2] == ({"a": "4", "b": "5", "_extra": ["6"]}, True)


def test_parse_tsv_strips_bom_free_header_whitespace():
    assert bronze.parse_tsv("participant_id \tage\nsub-01\t20\n")[0][0] == {
        "participant_id": "sub-01",
        "age": "20",
    }


def test_metadata_selection():
    assert is_metadata("participants.tsv")
    assert is_metadata("task-rest_bold.json")
    assert is_metadata("sub-01/sub-01_scans.tsv")
    assert not is_metadata("sub-01/func/sub-01_task-rest_bold.nii.gz")
    assert not is_metadata("sub-01/func/sub-01_task-rest_events.tsv")
    assert not is_metadata("derivatives/fmriprep/dataset_description.json")


def test_local_source_skips_derivatives_and_hidden(tmp_path):
    (root,) = [r for r in fixtures.generate(tmp_path) if r.name == "ds900002"]
    (root / "derivatives" / "x").mkdir(parents=True)
    (root / "derivatives" / "x" / "dataset_description.json").write_text("{}")
    (root / ".bidsignore").write_text("*.tmp")
    paths = {f.path for f in LocalBidsSource(root).list_files()}
    assert "participants.tsv" in paths
    assert not any(p.startswith(("derivatives/", ".")) for p in paths)


def test_reingest_replaces_partition(tmp_path):
    (root,) = [r for r in fixtures.generate(tmp_path / "bids") if r.name == "ds900003"]
    lake = tmp_path / "lake"
    first = bronze.ingest(LocalBidsSource(root), lake)
    second = bronze.ingest(LocalBidsSource(root), lake)
    assert first.run_id != second.run_id
    glob = f"{lake}/bronze/raw_files/*/*/data.parquet"
    n = duckdb.sql(f"select count(*) from read_parquet('{glob}')").fetchone()[0]
    assert n == second.n_files  # not doubled
    runs = duckdb.sql(f"select run_id from read_parquet('{lake}/bronze/raw_ingest_runs/*/*/data.parquet')")
    assert runs.fetchall() == [(second.run_id,)]
    assert not any((lake / "_staging").iterdir())


class FlakySource:
    """A source whose metadata reads fail in a configurable way."""

    source_name = "flaky"

    def __init__(self, root, exc):
        self._inner = LocalBidsSource(root)
        self.dataset_id, self.snapshot = self._inner.dataset_id, self._inner.snapshot
        self._exc = exc

    def list_files(self):
        return self._inner.list_files()

    def read_text(self, path):
        if path == "participants.tsv":
            raise self._exc
        return self._inner.read_text(path)


def _count(lake, table):
    glob = f"{lake}/bronze/{table}/*/*/data.parquet"
    return duckdb.sql(f"select count(*) from read_parquet('{glob}')").fetchone()[0]


def test_content_errors_are_recorded_not_fatal(tmp_path):
    (root,) = [r for r in fixtures.generate(tmp_path / "bids") if r.name == "ds900002"]
    lake = tmp_path / "lake"
    r = bronze.ingest(FlakySource(root, MetadataReadError("UnicodeDecodeError: bad byte")), lake)
    assert r.n_read_errors == 1
    rows = duckdb.sql(
        f"select path, error from read_parquet('{lake}/bronze/raw_read_errors/*/*/data.parquet')"
    ).fetchall()
    assert rows == [("participants.tsv", "UnicodeDecodeError: bad byte")]


def test_transport_errors_fail_the_run_and_keep_previous_data(tmp_path):
    (root,) = [r for r in fixtures.generate(tmp_path / "bids") if r.name == "ds900002"]
    lake = tmp_path / "lake"
    good = bronze.ingest(LocalBidsSource(root), lake)
    with pytest.raises(ConnectionError):
        bronze.ingest(FlakySource(root, ConnectionError("network down")), lake)
    runs = duckdb.sql(f"select run_id from read_parquet('{lake}/bronze/raw_ingest_runs/*/*/data.parquet')")
    assert runs.fetchall() == [(good.run_id,)]
    assert _count(lake, "raw_files") == good.n_files
    assert not any((lake / "_staging").iterdir())


def test_every_table_row_carries_the_run_id(tmp_path):
    (root,) = [r for r in fixtures.generate(tmp_path / "bids") if r.name == "ds900001"]
    lake = tmp_path / "lake"
    r = bronze.ingest(LocalBidsSource(root), lake)
    for table in bronze.TABLES:
        ids = duckdb.sql(
            f"select distinct run_id from read_parquet('{lake}/bronze/{table}/*/*/data.parquet')"
        ).fetchall()
        assert ids in ([(r.run_id,)], []), table
