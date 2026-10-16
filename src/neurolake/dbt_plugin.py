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

Both return NULL for text Python cannot parse. Validity is still judged by DuckDB's
json_valid, and stg_json_docs only uses the resolved document for files that repeat
a key, so every other file is read exactly as DuckDB reads it.
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
    """Paths of repeated keys, e.g. ['$.IntendedFor']. Nested objects are reported under
    their parent key ($.Outer.inner), positions inside arrays are not tracked
    ($.List[*].key), and only the value that is kept (the last) is searched further.
    Iterative, so a deeply nested document cannot exhaust the stack."""
    try:
        doc = json.loads(text, object_pairs_hook=_Pairs)
    except (ValueError, RecursionError):
        return None
    found: list[str] = []
    stack = [(doc, "$")]
    while stack:
        node, path = stack.pop()
        if isinstance(node, _Pairs):
            kept: dict[str, object] = {}
            for key, value in node:
                if key in kept and f"{path}.{key}" not in found:
                    found.append(f"{path}.{key}")
                kept[key] = value
            stack.extend((value, f"{path}.{key}") for key, value in reversed(kept.items()))
        elif isinstance(node, list):
            stack.extend((item, f"{path}[*]") for item in reversed(node))
    return found


class Plugin(BasePlugin):
    def configure_connection(self, conn: DuckDBPyConnection) -> None:
        conn.create_function("bids_json_last_wins", last_wins, ["VARCHAR"], "VARCHAR", side_effects=False)
        conn.create_function(
            "bids_json_duplicate_keys", duplicate_keys, ["VARCHAR"], "VARCHAR[]", side_effects=False
        )
