"""OpenNeuro source: lists a snapshot via the public GraphQL API and fetches
metadata files over HTTPS. No authentication is needed for public datasets."""

from __future__ import annotations

import time
from collections.abc import Iterator

import httpx

from .base import FileEntry, MetadataReadError, is_excluded

GRAPHQL_URL = "https://openneuro.org/crn/graphql"
FILE_URL = "https://openneuro.org/crn/datasets/{dataset}/snapshots/{tag}/files/{path}"

_LATEST_SNAPSHOT = """
query latest($datasetId: ID!) {
  dataset(id: $datasetId) { latestSnapshot { tag } }
}
"""

# Directories are walked one level at a time with the documented `tree` argument,
# so we always know each file's full path.
_FILES = """
query files($datasetId: ID!, $tag: String!, $tree: String) {
  snapshot(datasetId: $datasetId, tag: $tag) {
    files(tree: $tree) { id filename size directory }
  }
}
"""


RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class OpenNeuroError(RuntimeError):
    pass


def _retry_after(resp: httpx.Response, default: float) -> float:
    try:
        return min(float(resp.headers["Retry-After"]), 120.0)
    except (KeyError, ValueError):
        return default


class OpenNeuroSource:
    source_name = "openneuro"

    def __init__(
        self,
        dataset_id: str,
        snapshot: str | None = None,
        client: httpx.Client | None = None,
        max_retries: int = 4,
    ):
        self.dataset_id = dataset_id
        self._client = client or httpx.Client(
            timeout=httpx.Timeout(30.0), headers={"User-Agent": "neurolake/0.1"}
        )
        self._max_retries = max_retries
        self.snapshot = snapshot or self._latest_snapshot()

    # -- HTTP helpers -------------------------------------------------------

    def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Send a request, retrying network errors and 429/5xx with exponential
        backoff (honouring Retry-After). Raises once retries are exhausted."""
        delay = 1.0
        for attempt in range(self._max_retries + 1):
            last = attempt == self._max_retries
            try:
                resp = self._client.request(method, url, **kwargs)
            except httpx.TransportError:
                if last:
                    raise
                time.sleep(delay)
            else:
                if resp.status_code not in RETRYABLE_STATUS or last:
                    resp.raise_for_status()
                    return resp
                time.sleep(_retry_after(resp, delay))
            delay *= 2
        raise AssertionError("unreachable")

    def _graphql(self, query: str, variables: dict) -> dict:
        resp = self._request("POST", GRAPHQL_URL, json={"query": query, "variables": variables})
        body = resp.json()
        if body.get("errors"):
            raise OpenNeuroError(f"GraphQL error for {self.dataset_id}: {body['errors']}")
        return body["data"]

    def _latest_snapshot(self) -> str:
        data = self._graphql(_LATEST_SNAPSHOT, {"datasetId": self.dataset_id})
        snap = (data.get("dataset") or {}).get("latestSnapshot")
        if not snap:
            raise OpenNeuroError(f"{self.dataset_id} has no published snapshot")
        return snap["tag"]

    # -- DatasetSource ------------------------------------------------------

    def list_files(self) -> Iterator[FileEntry]:
        stack: list[tuple[str | None, str]] = [(None, "")]
        while stack:
            tree, prefix = stack.pop()
            data = self._graphql(_FILES, {"datasetId": self.dataset_id, "tag": self.snapshot, "tree": tree})
            snapshot = data.get("snapshot")
            if snapshot is None:
                raise OpenNeuroError(f"{self.dataset_id}:{self.snapshot} not found")
            for f in snapshot["files"]:
                path = f"{prefix}{f['filename']}"
                if f["directory"]:
                    if not is_excluded(path + "/"):
                        stack.append((f["id"], path + "/"))
                elif not is_excluded(path):
                    yield FileEntry(path, f.get("size"))

    def read_text(self, path: str) -> str:
        # The file API takes the dataset-relative path with ":" as the separator.
        url = FILE_URL.format(dataset=self.dataset_id, tag=self.snapshot, path=path.replace("/", ":"))
        try:
            resp = self._request("GET", url, follow_redirects=True)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise MetadataReadError(f"HTTP 404 for {path}") from exc
            raise
        try:
            return resp.content.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise MetadataReadError(f"UnicodeDecodeError: {exc}") from exc
