"""Do OpenNeuro datasets get cleaner after they are first published?

    python audit/history.py pairs   [probe_log.csv] [pairs.csv]
    python audit/history.py ingest  [pairs.csv] [lake_root]
    python audit/history.py analyze [lake_root] [out.json]

For every dataset in the audit sample, `pairs` records its first published snapshot
next to the latest one the audit used (the tag in the probe log), so the comparison
targets exactly the snapshots on the published page. `ingest` lands the two sets
into separate lakes, <lake_root>/first and <lake_root>/latest, and builds both with
dbt. The same models and rules run on both, so any difference comes from the data.

`analyze` compares the two quality pictures, restricted to raw datasets with more
than one snapshot. A finding is matched across snapshots by its rule, object and
detail, except for rules whose detail only summarises one object (counts, value
lists): those match on rule and object, so "across 139 recordings" becoming "across
142 recordings" is the same finding, not a fix plus a new one. Renaming a file still
counts as one fix and one new finding; dataset-level answers use finding counts,
which are immune to that.
"""

from __future__ import annotations

import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb

from neurolake import bronze
from neurolake import transform as transform_mod
from neurolake.sources.github_mirror import GitHubMirrorSource, earliest_snapshot_tag, list_tags

SIDES = ("first", "latest")


def pairs(probe_log: str = "audit/probe_log.csv", out: str = "audit/history_pairs.csv") -> None:
    with open(probe_log, newline="") as fh:
        latest = {r["dataset_id"]: r["tag"] for r in csv.DictReader(fh) if r["status"] == "found"}

    def first(ds: str) -> str:
        tag = earliest_snapshot_tag(list_tags(ds))
        if tag is None:
            raise RuntimeError(f"{ds}: no snapshot tags")
        return tag

    with ThreadPoolExecutor(max_workers=8) as pool:
        firsts = dict(zip(latest, pool.map(first, latest), strict=True))
    with open(out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["dataset_id", "first", "latest"])
        for ds in sorted(latest):
            w.writerow([ds, firsts[ds], latest[ds]])
    n_multi = sum(firsts[d] != latest[d] for d in latest)
    print(f"wrote {out}: {len(latest)} datasets, {n_multi} with more than one snapshot")


def ingest(pairs_csv: str = "audit/history_pairs.csv", root: str = "data/history", workers: int = 6) -> None:
    with open(pairs_csv, newline="") as fh:
        rows = list(csv.DictReader(fh))
    jobs = [(side, r["dataset_id"], r[side]) for r in rows for side in SIDES]

    def one(side: str, ds: str, tag: str):
        with GitHubMirrorSource(ds, tag) as src:
            return bronze.ingest(src, Path(root) / side)

    failed = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(one, *job): job for job in jobs}
        for fut in as_completed(futures):
            side, ds, tag = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                failed.append((side, ds, tag))
                print(f"{side} {ds}@{tag} FAILED: {exc}", file=sys.stderr)
    if failed:
        raise SystemExit(f"{len(failed)} ingests failed; rerun to retry (ingest is idempotent)")
    for side in SIDES:
        transform_mod.run_dbt(Path(root) / side, ["build"])
    print(f"ingested {len(jobs)} snapshots into {root}/{{first,latest}}")


# Per dataset and side: snapshot, type, and error/warning counts.
_PER_DATASET = """
    select d.dataset_id, d.snapshot, d.snapshot_created_at, d.dataset_type, d.n_scans,
           count(i.issue_id) filter (where i.severity = 'error') as errors,
           count(i.issue_id) filter (where i.severity = 'warning') as warnings
    from {side}.marts.dim_dataset d
    left join {side}.marts.fct_dq_issue i using (dataset_id)
    group by all
"""

# Rules that emit at most one finding per object, with a detail that summarises it
# (counts, value lists). The detail changes whenever data is added, so it is left
# out of the key that matches findings across snapshots.
SUMMARY_RULES = (
    "inconsistent_repetition_time",
    "inconsistent_sampling_frequency",
    "subject_missing_from_participants",
    "participant_id_without_prefix",
    "duplicate_participant_id",
    "bidsignore_hides_all_data",
)

_ISSUES = """
    select dataset_id, rule_id, severity, object_id,
           concat_ws('|', rule_id, object_id,
                     case when rule_id in {summary_rules} then '' else detail end) as match_key
    from {side}.marts.fct_dq_issue
    where severity in ('error', 'warning')
"""


def analyze(root: str = "data/history", out: str = "audit/history.json") -> dict:
    con = duckdb.connect()
    for side in SIDES:
        con.execute(f"attach '{Path(root) / side / 'neurolake.duckdb'}' as {side} (read_only)")
        con.execute(f"create temp table ds_{side} as {_PER_DATASET.format(side=side)}")
        con.execute(
            f"create temp table iss_{side} as {_ISSUES.format(side=side, summary_rules=SUMMARY_RULES)}"
        )
        dupes = con.execute(
            f"select count(*) - count(distinct (dataset_id, match_key)) from iss_{side}"
        ).fetchone()[0]
        if dupes:
            raise SystemExit(f"{side}: {dupes} findings share a match key; extend SUMMARY_RULES logic")

    # Raw in both snapshots, and actually different snapshots.
    con.execute("""
        create temp table cohort as
        select f.dataset_id,
               f.snapshot as first_snapshot, l.snapshot as latest_snapshot,
               f.snapshot_created_at as first_at, l.snapshot_created_at as latest_at,
               f.errors as first_errors, l.errors as latest_errors,
               f.warnings as first_warnings, l.warnings as latest_warnings,
               f.n_scans as first_scans, l.n_scans as latest_scans
        from ds_first f join ds_latest l using (dataset_id)
        where f.snapshot <> l.snapshot
          and f.dataset_type <> 'derivative' and l.dataset_type <> 'derivative'
    """)

    def q(sql: str) -> list[dict]:
        cur = con.execute(sql)
        cols = [c[0] for c in cur.description]
        return [
            {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in zip(cols, r, strict=True)}
            for r in cur.fetchall()
        ]

    result: dict = {}
    result["sample"] = q("""
        select (select count(*) from ds_latest) as datasets,
               (select count(*) from ds_first f join ds_latest l using (dataset_id)
                 where f.snapshot <> l.snapshot) as multi_snapshot,
               (select count(*) from cohort) as cohort,
               round(median(date_diff('day', first_at, latest_at)) / 365.25, 1) as median_years_between,
               min(first_at)::date as earliest_first, max(latest_at)::date as latest_latest
        from cohort
    """)[0]

    # Status of each dataset at each end: clean, warnings only, or errors.
    result["status"] = q("""
        select side, status, count(*) as datasets from (
            select 'first' as side, case when first_errors > 0 then 'errors'
                when first_warnings > 0 then 'warnings only' else 'clean' end as status from cohort
            union all
            select 'latest', case when latest_errors > 0 then 'errors'
                when latest_warnings > 0 then 'warnings only' else 'clean' end from cohort
        ) group by all order by side, status
    """)

    result["transitions"] = q("""
        select
            count(*) filter (where first_errors > 0 and latest_errors = 0) as errors_fixed,
            count(*) filter (where first_errors = 0 and latest_errors > 0) as errors_introduced,
            count(*) filter (where first_errors > 0 and latest_errors > 0) as errors_persist,
            count(*) filter (where first_errors + first_warnings > latest_errors + latest_warnings)
                as fewer_issues,
            count(*) filter (where first_errors + first_warnings < latest_errors + latest_warnings)
                as more_issues,
            count(*) filter (where first_errors + first_warnings = latest_errors + latest_warnings)
                as same_issues
        from cohort
    """)[0]

    # Matched issues: resolved (only in first), introduced (only in latest), persisting.
    con.execute("""
        create temp table matched as
        select coalesce(f.dataset_id, l.dataset_id) as dataset_id,
               coalesce(f.rule_id, l.rule_id) as rule_id,
               coalesce(f.object_id, l.object_id) as object_id,
               case when l.match_key is null then 'resolved'
                    when f.match_key is null then 'introduced' else 'persisting' end as fate
        from (select * from iss_first where dataset_id in (select dataset_id from cohort)) f
        full outer join (select * from iss_latest where dataset_id in (select dataset_id from cohort)) l
          on f.dataset_id = l.dataset_id and f.match_key = l.match_key
    """)
    # Datasets whose findings changed at all, and why: an update that adds most of the
    # data (an unfinished first upload) clears "participant has no data" without any
    # metadata being fixed, so data growth is reported next to every change. A dataset
    # with no data files at first "grew" if it has any later.
    con.execute("""
        create temp table changed as
        select c.dataset_id, c.first_snapshot, c.latest_snapshot, c.first_scans, c.latest_scans,
               case when coalesce(c.first_scans, 0) = 0 then coalesce(c.latest_scans, 0) > 0
                    else coalesce(c.latest_scans >= 1.5 * c.first_scans, false) end as data_grew,
               coalesce(c.latest_scans < c.first_scans, false) as data_shrank,
               count(*) filter (where m.fate = 'resolved') as resolved,
               count(*) filter (where m.fate = 'introduced') as introduced,
               count(distinct m.object_id) filter (where m.fate = 'resolved') as resolved_objects,
               count(*) filter (where m.fate = 'introduced' and m.rule_id = 'participant_without_data')
                   as participants_lost_data,
               list(distinct m.rule_id order by m.rule_id) filter (where m.fate = 'resolved')
                   as resolved_rules,
               list(distinct m.rule_id order by m.rule_id) filter (where m.fate = 'introduced')
                   as introduced_rules
        from cohort c join matched m using (dataset_id)
        group by all
        having count(*) filter (where m.fate <> 'persisting') > 0
    """)
    result["changed"] = q("select * from changed order by resolved desc, dataset_id")
    result["changed_summary"] = q("""
        select count(*) as datasets,
               count(*) filter (where data_grew) as data_grew,
               count(*) filter (where resolved > 0 and introduced = 0) as only_resolved,
               count(*) filter (where introduced > 0 and resolved = 0) as only_introduced,
               count(*) filter (where resolved > 0 and introduced > 0) as both,
               count(*) filter (where data_grew and list_contains(resolved_rules, 'participant_without_data'))
                   as grew_and_cleared_participant_without_data,
               sum(resolved) as resolved, sum(introduced) as introduced
        from changed
    """)[0]
    result["changed_summary"]["same_count_but_changed"] = q("""
        select count(*) as n from changed join cohort using (dataset_id)
        where first_errors + first_warnings = latest_errors + latest_warnings
    """)[0]["n"]
    # The dataset that accounts for most resolved findings, and how many findings each of
    # its resolved objects carried (min and max, so the page never assumes they are equal).
    top = q("select dataset_id from changed order by resolved desc, dataset_id limit 1")
    if top:
        result["top_resolver"] = q(f"""
            with per_object as (
                select object_id, count(*) as n from matched
                where dataset_id = '{top[0]["dataset_id"]}' and fate = 'resolved' group by object_id
            )
            select '{top[0]["dataset_id"]}' as dataset_id, count(*) as objects,
                   min(n) as per_object_min, max(n) as per_object_max, sum(n) as resolved,
                   (select list(distinct rule_id order by rule_id) from matched
                    where dataset_id = '{top[0]["dataset_id"]}' and fate = 'resolved') as rules
            from per_object
        """)[0]

    result["issue_fates"] = q("""
        select fate, count(*) as issues, count(distinct dataset_id) as datasets
        from matched group by fate order by fate
    """)

    # Per rule: in how many datasets the rule fired at each end, and was cleared/new.
    result["rules"] = q("""
        with per as (
            select c.dataset_id, r.rule_id,
                   exists (select 1 from iss_first i where i.dataset_id = c.dataset_id
                           and i.rule_id = r.rule_id) as in_first,
                   exists (select 1 from iss_latest i where i.dataset_id = c.dataset_id
                           and i.rule_id = r.rule_id) as in_latest
            from cohort c
            cross join (select distinct rule_id from iss_first
                        union select distinct rule_id from iss_latest) r
        )
        select rule_id,
               count(*) filter (where in_first) as datasets_first,
               count(*) filter (where in_latest) as datasets_latest,
               count(*) filter (where in_first and not in_latest) as cleared,
               count(*) filter (where in_latest and not in_first) as appeared
        from per group by rule_id
        having count(*) filter (where in_first or in_latest) > 0
        order by datasets_first desc, rule_id
    """)

    # Sign test on datasets whose issue count changed: is fewer more common than more?
    t = result["transitions"]
    result["sign_test"] = {
        "fewer": t["fewer_issues"],
        "more": t["more_issues"],
        "p_two_sided": round(_sign_test(t["fewer_issues"], t["more_issues"]), 4),
    }

    result["datasets"] = q("""
        select dataset_id, first_snapshot, latest_snapshot,
               first_errors, latest_errors, first_warnings, latest_warnings
        from cohort order by dataset_id
    """)

    Path(out).write_text(json.dumps(result, indent=2) + "\n")
    print(f"wrote {out}")
    return result


def _sign_test(k: int, m: int) -> float:
    """Exact two-sided binomial test of k successes in k+m trials at p = 0.5."""
    from math import comb

    n = k + m
    if n == 0:
        return 1.0
    lo = min(k, m)
    tail = sum(comb(n, i) for i in range(lo + 1)) / 2**n
    return min(1.0, 2 * tail)


if __name__ == "__main__":
    cmd, *args = sys.argv[1:] or ["analyze"]
    {"pairs": pairs, "ingest": ingest, "analyze": analyze}[cmd](*args)
