"""OpenNeuro client against a mocked API (no network)."""

import json

import httpx
import pytest

from neurolake.sources import MetadataReadError
from neurolake.sources.openneuro import OpenNeuroError, OpenNeuroSource

TREE = {
    None: [
        {"id": "f1", "filename": "dataset_description.json", "size": 120, "directory": False},
        {"id": "d1", "filename": "sub-01", "size": 0, "directory": True},
        {"id": "d9", "filename": "derivatives", "size": 0, "directory": True},
    ],
    "d1": [{"id": "d2", "filename": "func", "size": 0, "directory": True}],
    "d2": [
        {"id": "f2", "filename": "sub-01_task-rest_bold.nii.gz", "size": 9000, "directory": False},
        {"id": "f3", "filename": "sub-01_task-rest_bold.json", "size": 40, "directory": False},
    ],
}


def make_client(fail_first: int = 0):
    calls = {"graphql": 0, "trees": [], "files": []}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/crn/graphql":
            calls["graphql"] += 1
            if calls["graphql"] <= fail_first:
                return httpx.Response(503)
            body = json.loads(request.content)
            v = body["variables"]
            if "latestSnapshot" in body["query"]:
                return httpx.Response(200, json={"data": {"dataset": {"latestSnapshot": {"tag": "1.0.2"}}}})
            calls["trees"].append(v["tree"])
            assert v["tag"] == "1.0.2"
            return httpx.Response(200, json={"data": {"snapshot": {"files": TREE[v["tree"]]}}})
        calls["files"].append(request.url.path)
        return httpx.Response(200, content=b'\xef\xbb\xbf{"TaskName": "rest"}')

    return httpx.Client(transport=httpx.MockTransport(handler)), calls


def test_walks_tree_and_builds_full_paths(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    client, calls = make_client()
    src = OpenNeuroSource("ds000001", client=client)
    assert src.snapshot == "1.0.2"
    files = {f.path: f.size_bytes for f in src.list_files()}
    assert files == {
        "dataset_description.json": 120,
        "sub-01/func/sub-01_task-rest_bold.nii.gz": 9000,
        "sub-01/func/sub-01_task-rest_bold.json": 40,
    }
    assert "d9" not in calls["trees"]  # derivatives never listed


def test_read_text_uses_colon_paths_and_strips_bom():
    client, calls = make_client()
    src = OpenNeuroSource("ds000001", snapshot="1.0.2", client=client)
    assert src.read_text("sub-01/func/sub-01_task-rest_bold.json") == '{"TaskName": "rest"}'
    assert calls["files"] == [
        "/crn/datasets/ds000001/snapshots/1.0.2/files/sub-01:func:sub-01_task-rest_bold.json"
    ]


def test_retries_transient_errors(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    client, calls = make_client(fail_first=2)
    assert OpenNeuroSource("ds000001", client=client).snapshot == "1.0.2"
    assert calls["graphql"] == 3


def test_graphql_errors_raise():
    def handler(request):
        return httpx.Response(200, json={"errors": [{"message": "nope"}], "data": None})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(OpenNeuroError):
        OpenNeuroSource("ds999999", client=client)


def test_retries_500_and_honours_retry_after(monkeypatch):
    sleeps = []
    monkeypatch.setattr("time.sleep", sleeps.append)
    statuses = iter([500, 429])

    def handler(request):
        status = next(statuses, 200)
        if status != 200:
            return httpx.Response(status, headers={"Retry-After": "7"} if status == 429 else {})
        return httpx.Response(200, json={"data": {"dataset": {"latestSnapshot": {"tag": "2.0.0"}}}})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    assert OpenNeuroSource("ds000001", client=client).snapshot == "2.0.0"
    assert sleeps == [1.0, 7.0]


def test_missing_file_is_a_content_error_but_outage_is_not(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)

    def handler(request):
        return httpx.Response(404 if "missing" in request.url.path else 503)

    src = OpenNeuroSource(
        "ds000001", snapshot="1.0.0", client=httpx.Client(transport=httpx.MockTransport(handler))
    )
    with pytest.raises(MetadataReadError):
        src.read_text("missing.json")
    with pytest.raises(httpx.HTTPStatusError):
        src.read_text("sub-01/sub-01_scans.tsv")
