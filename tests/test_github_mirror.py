import pytest

from neurolake.sources import github_mirror as gm

TAGS = ["00001", "00002", "1.0.10", "1.0.2", "1.0.9", "2.0.0", "not-a-snapshot"]


def test_semver_ordering_is_numeric():
    assert gm.latest_snapshot_tag(TAGS) == "2.0.0"
    assert gm.earliest_snapshot_tag(TAGS) == "1.0.2"


def test_numeric_tags_used_only_without_semver():
    tags = ["00010", "00002", "draft"]
    assert gm.earliest_snapshot_tag(tags) == "00002"
    assert gm.latest_snapshot_tag(tags) == "00010"


def test_no_snapshot_tags():
    assert gm.latest_snapshot_tag(["draft"]) is None
    assert gm.earliest_snapshot_tag([]) is None


def test_resolve_snapshot(monkeypatch):
    monkeypatch.setattr(gm, "list_tags", lambda ds: TAGS)
    assert gm.resolve_snapshot("ds1", None) == "2.0.0"
    assert gm.resolve_snapshot("ds1", "latest") == "2.0.0"
    assert gm.resolve_snapshot("ds1", "first") == "1.0.2"
    assert gm.resolve_snapshot("ds1", "1.0.9") == "1.0.9"
    monkeypatch.setattr(gm, "list_tags", lambda ds: [])
    with pytest.raises(gm.MirrorError):
        gm.resolve_snapshot("ds1", "first")
