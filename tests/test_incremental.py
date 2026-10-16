"""Incremental ingest: skip a dataset whose resolved snapshot is already its current,
fully published run. Offline: the mirror is faked with a local fixture."""

import shutil

import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from neurolake import bronze, cli, fixtures
from neurolake.sources import LocalBidsSource
from neurolake.sources import github_mirror as gm

DS = "ds900001"  # a fixture with no read errors


@pytest.fixture
def mirror(tmp_path, monkeypatch):
    """A fake mirror for DS whose tags can change between runs. Records every clone."""
    (root,) = [r for r in fixtures.generate(tmp_path / "bids") if r.name == DS]
    state = {"tags": ["1.0.0"], "clones": []}

    def resolve(dataset_id, snapshot):
        if snapshot in (None, "latest"):
            return state["tags"][-1]
        if snapshot == "first":
            return state["tags"][0]
        return snapshot

    class FakeMirrorSource(LocalBidsSource):
        source_name = "openneuro-github"

        def __init__(self, dataset_id, snapshot):
            state["clones"].append((dataset_id, snapshot))
            super().__init__(root, dataset_id=dataset_id, snapshot=snapshot)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            pass

    monkeypatch.setattr(gm, "resolve_snapshot", resolve)
    monkeypatch.setattr(gm, "GitHubMirrorSource", FakeMirrorSource)
    return state


def run(lake, *args):
    result = CliRunner().invoke(cli.app, ["ingest-mirror", DS, "--lake", str(lake), *args])
    return result


def bronze_files(lake):
    return {
        p.relative_to(lake): (p.stat().st_ino, p.stat().st_mtime_ns)
        for p in (lake / "bronze").rglob("*")
        if p.is_file()
    }


def current(lake):
    run_ = bronze.current_run(lake, DS)
    return run_["snapshot"], run_["run_id"]


def test_unchanged_second_ingest_is_skipped_and_writes_nothing(tmp_path, mirror):
    lake = tmp_path / "lake"
    first = run(lake)
    assert first.exit_code == 0, first.output
    assert "ingested" in first.output and "1 ingested, 0 skipped, 0 failed" in first.output
    before = bronze_files(lake)

    second = run(lake)
    assert second.exit_code == 0, second.output
    assert f"{DS}:1.0.0  skipped (unchanged at 1.0.0)" in second.output
    assert "0 ingested, 1 skipped, 0 failed" in second.output
    assert mirror["clones"] == [(DS, "1.0.0")]  # never cloned the second time
    assert bronze_files(lake) == before


def test_force_reingests(tmp_path, mirror):
    lake = tmp_path / "lake"
    run(lake)
    _, old_run = current(lake)
    forced = run(lake, "--force")
    assert forced.exit_code == 0 and "1 ingested, 0 skipped" in forced.output
    assert len(mirror["clones"]) == 2
    assert current(lake)[1] != old_run


def test_new_tag_is_ingested(tmp_path, mirror):
    lake = tmp_path / "lake"
    run(lake)
    mirror["tags"].append("1.0.1")
    out = run(lake)
    assert "1 ingested, 0 skipped" in out.output
    assert current(lake)[0] == "1.0.1"


def test_current_run_for_another_tag_is_not_skipped(tmp_path, mirror):
    """Ingest 1.0.1, then the first snapshot (1.0.0), which becomes current. A default
    run must bring 1.0.1 back, even though its partitions are already complete."""
    lake = tmp_path / "lake"
    mirror["tags"] = ["1.0.0", "1.0.1"]
    run(lake)
    assert "1 ingested" in run(lake, "--snapshot", "first").output
    assert current(lake)[0] == "1.0.0"
    assert not bronze.is_current_and_complete(lake, DS, "1.0.1")

    back = run(lake)
    assert "1 ingested, 0 skipped" in back.output
    assert current(lake)[0] == "1.0.1"
    assert "0 ingested, 1 skipped" in run(lake).output
    assert "0 ingested, 1 skipped" in run(lake, "--snapshot", "1.0.1").output


def test_half_published_run_is_reingested(tmp_path, mirror):
    """A crash after raw_files but before raw_ingest_runs: the old commit marker stays,
    the data tables carry the newer run_id. That must not count as complete."""
    lake = tmp_path / "lake"
    run(lake)
    marker = bronze._partition(lake, "raw_ingest_runs", DS, "1.0.0")
    old_marker = marker.read_bytes()
    run(lake, "--force")
    marker.write_bytes(old_marker)  # as if the second publish stopped before the marker

    assert not bronze.is_current_and_complete(lake, DS, "1.0.0")
    out = run(lake)
    assert "1 ingested, 0 skipped" in out.output
    assert bronze.is_current_and_complete(lake, DS, "1.0.0")


def test_missing_partition_is_incomplete(tmp_path, mirror):
    lake = tmp_path / "lake"
    run(lake)
    shutil.rmtree(bronze._partition(lake, "raw_tsv_rows", DS, "1.0.0").parent)
    assert not bronze.is_current_and_complete(lake, DS, "1.0.0")
    assert "1 ingested" in run(lake).output


def test_dataset_without_read_errors_is_complete(tmp_path, mirror):
    lake = tmp_path / "lake"
    run(lake)
    errors = pq.read_table(bronze._partition(lake, "raw_read_errors", DS, "1.0.0"))
    assert errors.num_rows == 0  # an empty partition, no run_id to compare
    assert bronze.is_current_and_complete(lake, DS, "1.0.0")


def test_empty_table_with_rows_expected_is_incomplete(tmp_path):
    """A run that recorded read errors but whose raw_read_errors partition is empty
    (left by a newer run that crashed mid-publish) is not complete."""
    (root,) = [r for r in fixtures.generate(tmp_path / "bids") if r.name == "ds900002"]
    lake = tmp_path / "lake"
    r = bronze.ingest(LocalBidsSource(root, snapshot="1.0.0"), lake)
    assert r.n_read_errors > 0
    assert bronze.is_current_and_complete(lake, "ds900002", "1.0.0")
    part = bronze._partition(lake, "raw_read_errors", "ds900002", "1.0.0")
    pq.write_table(pq.read_table(part).slice(0, 0), part)
    assert not bronze.is_current_and_complete(lake, "ds900002", "1.0.0")


def test_fresh_lake_is_not_complete(tmp_path):
    assert bronze.current_run(tmp_path / "nothing", DS) is None
    assert not bronze.is_current_and_complete(tmp_path / "nothing", DS, "1.0.0")


def test_failure_is_reported_and_others_continue(tmp_path, mirror, monkeypatch):
    lake = tmp_path / "lake"
    run(lake)
    real = gm.resolve_snapshot

    def resolve(dataset_id, snapshot):
        if dataset_id == "ds999999":
            raise gm.MirrorError("ds999999 has no snapshot tags")
        return real(dataset_id, snapshot)

    monkeypatch.setattr(gm, "resolve_snapshot", resolve)
    out = CliRunner().invoke(cli.app, ["ingest-mirror", DS, "ds999999", "--lake", str(lake)])
    assert out.exit_code == 1
    assert "ds999999  FAILED" in out.output
    assert "0 ingested, 1 skipped, 1 failed" in out.output
