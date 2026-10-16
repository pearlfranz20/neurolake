"""dbt-duckdb plugin: JSON helpers DuckDB does not have.

A JSON object may repeat a key. The standard leaves the result undefined, and
readers disagree: DuckDB's json_extract returns the first value, while Python,
JavaScript and the BIDS validator keep the last. Real sidecars do this (one
OpenNeuro snapshot repeats IntendedFor in 24 fieldmap sidecars), so neurolake
resolves every document the way most readers do, last value wins, and reports the
repetition itself as a data-quality issue.

Registered on every dbt connection through `plugins` in dbt/profiles.yml:

    bids_json_last_wins(text)       -> text      the document with repeated keys
                                                 resolved, last value wins
    bids_json_duplicate_keys(text)  -> text[]    JSON paths of keys that repeat,
                                                 e.g. ['$.IntendedFor']

Both return NULL for text that is not valid JSON; validity is still judged by
DuckDB's json_valid, so the two never disagree on which files are broken.
"""

from __future__ import annotations

import json

from dbt.adapters.duckdb.plugins import BasePlugin
from duckdb import DuckDBPyConnection


class _Pairs(list):
    """A JSON object as the ordered list of its (key, value) pairs."""


def last_wins(text: str) -> str | None:
    try:
        return json.dumps(json.loads(text), ensure_ascii=False, separators=(",", ":"))
    except (ValueError, RecursionError):
        return None


def duplicate_keys(text: str) -> list[str] | None:
    """Paths of repeated keys. Nested objects are reported under their parent key
    ($.Outer.inner); positions inside arrays are not tracked ($.List[*].key)."""
    try:
        doc = json.loads(text, object_pairs_hook=_Pairs)
    except (ValueError, RecursionError):
        return None
    found: list[str] = []

    def walk(node, path: str) -> None:
        if isinstance(node, _Pairs):
            seen: set[str] = set()
            for key, value in node:
                if key in seen and f"{path}.{key}" not in found:
                    found.append(f"{path}.{key}")
                seen.add(key)
                walk(value, f"{path}.{key}")
        elif isinstance(node, list):
            for item in node:
                walk(item, f"{path}[*]")

    walk(doc, "$")
    return found


class Plugin(BasePlugin):
    def configure_connection(self, conn: DuckDBPyConnection) -> None:
        conn.create_function("bids_json_last_wins", last_wins, ["VARCHAR"], "VARCHAR", side_effects=False)
        conn.create_function(
            "bids_json_duplicate_keys", duplicate_keys, ["VARCHAR"], "VARCHAR[]", side_effects=False
        )
