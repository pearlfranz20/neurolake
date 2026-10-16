# Changelog

Dates are those of the commits on `main`.

## 0.2.0 (2026-10-16)

### Added
- **First vs latest snapshot comparison** (#1). `ingest-mirror --snapshot first|latest` and `audit/history.py` run the rules on each audited dataset's first published snapshot as well as its latest, and the audit page reports whether updates fix metadata (they rarely do).
- **Incremental ingest** (#2). `ingest-mirror` and the Airflow DAG skip a dataset whose resolved snapshot is already its current, fully published run. `--force` (and a DAG `force` param) re-ingests. The check runs under the per-dataset lock, and bronze that cannot be read is re-ingested rather than failing.
- **Per-dataset quality pages** (#3). A sortable, filterable table of the 150 audited datasets and one page per dataset, rendered from the committed `audit/datasets.json`.
- **Cross-check against the official bids-validator** (#4). The legacy 1.15.0 validator runs on the same snapshots. Every disagreement is classified, and the audit page reports agreement only over the datasets the validator could assess.
- **`duplicate_json_key` rule** (#5). A key repeated within one JSON file is reported.

### Changed
- Repeated JSON keys resolve to their last value, as Python, JavaScript and the BIDS validator read them (#5). DuckDB's own functions returned the first value, which produced 32 false IntendedFor errors in one audited dataset. A dbt-duckdb plugin (`neurolake.dbt_plugin`) provides the functions, and `stg_json_docs` is now a table.
- New dependency: `numpy`, which DuckDB needs to run the plugin's Python functions (#5).
- `stg_ingest_runs` breaks ties on `finished_at` by `run_id`, matching the ingest skip check (#2).
- Audit outputs are ordered deterministically.

## 0.1.0 (2026-10-15)

First release.

- BIDS metadata ingest from OpenNeuro's GitHub mirrors, the OpenNeuro API (tested against a mock) and local folders into hive-partitioned Parquet.
- dbt models on DuckDB, with the BIDS inheritance principle resolved in SQL.
- Data-quality rules in `fct_dq_issue`, and an HTML QC report.
- An Airflow 3 DAG.
- The published audit of 150 randomly sampled OpenNeuro datasets.
