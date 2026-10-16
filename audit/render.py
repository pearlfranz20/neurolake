"""Render findings.json into the audit write-up.

    python audit/render.py [findings.json] [out_dir]

Writes two files with the same content:
    <out_dir>/openneuro-audit.fragment.html  page body, for hosts that add the skeleton
    <out_dir>/index.html                      full standalone document (GitHub Pages)

Every number in the prose is read from findings.json, never typed by hand.
"""

from __future__ import annotations

import html
import json
import sys
from pathlib import Path

LABELS = {
    "missing_participants_tsv": "No participants.tsv",
    "missing_recommended_sidecar_field": "Recommended sidecar field missing",
    "participant_without_data": "Listed participant has no data",
    "subject_missing_from_participants": "Subject missing from participants.tsv",
    "inconsistent_sampling_frequency": "Sampling rates differ within a task",
    "inconsistent_repetition_time": "TRs differ within a task",
    "bidsignore_matches_core_files": ".bidsignore matches core files",
    "missing_required_sidecar_field": "Required sidecar field missing",
    "ambiguous_sidecar_inheritance": "Two sidecars apply at one level",
    "fieldmap_intended_for_missing_target": "Fieldmap points at a missing run",
    "scans_tsv_missing_file": "scans.tsv lists a missing file",
    "malformed_tsv_row": "Malformed TSV row",
    "invalid_json": "Invalid JSON",
    "metadata_read_error": "Unreadable metadata file",
    "metadata_not_fetched": "Metadata not fetched (annexed)",
    "implausible_repetition_time": "Implausible BOLD TR",
    "subject_folders_not_at_root": "Subject folders not at root",
    "dataset_description_missing": "No dataset_description.json",
    "dataset_description_missing_field": "Description lacks Name or BIDSVersion",
    "participant_id_without_prefix": "participant_id lacks sub- prefix",
    "duplicate_participant_id": "Duplicate participant_id",
    "bidsignore_hides_all_data": ".bidsignore hides all data",
}
SEV_ICON = {"error": "▲", "warning": "◆", "info": "○"}

CSS = """
:root {
  /* Layout: one 68ch reading column; charts and tables may widen to 760px. */
  --bg: #f7f8fa; --surface: #ffffff; --fg: #141a22; --muted: #586274; --faint: #8a93a3;
  --line: #e1e5eb; --grid: #edf0f4; --accent: #2a5ea8; --accent-soft: #e6eef9;
  --series: #2a78d6;
  --critical: #d03b3b; --warning: #fab219; --info: #9aa3b2;
  --code-bg: #eef1f5;
  --font-display: "IBM Plex Sans Condensed", "Arial Narrow", system-ui, sans-serif;
  --font-body: "IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
  --font-mono: "IBM Plex Mono", ui-monospace, "SFMono-Regular", Menlo, monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #0f1318; --surface: #161b22; --fg: #e4e8ee; --muted: #a2acbb; --faint: #7c8696;
  --line: #2a323d; --grid: #1f262f; --accent: #86afe6; --accent-soft: #1b2738;
  --series: #3987e5; --info: #6f7888; --code-bg: #1d242d; color-scheme: dark; } }
:root[data-theme="dark"] {
  --bg: #0f1318; --surface: #161b22; --fg: #e4e8ee; --muted: #a2acbb; --faint: #7c8696;
  --line: #2a323d; --grid: #1f262f; --accent: #86afe6; --accent-soft: #1b2738;
  --series: #3987e5; --info: #6f7888; --code-bg: #1d242d; color-scheme: dark; }
body { background: var(--bg); color: var(--fg); font-family: var(--font-body); font-size: 16px; line-height: 1.6; margin: 0; }
main { max-width: 760px; margin: 0 auto; padding-block: 40px 72px;
  padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px));
  display: grid; gap: 44px; }
section, header { display: grid; gap: 14px; min-width: 0; }
h1, h2 { font-family: var(--font-display); font-weight: 600; text-wrap: balance; margin: 0; letter-spacing: -0.01em; }
h1 { font-size: clamp(1.9rem, 5vw, 2.6rem); line-height: 1.12; }
h2 { font-size: 1.4rem; line-height: 1.25; }
p, ul, ol { margin: 0; max-width: 68ch; }
ul, ol { padding-left: 1.2em; display: grid; gap: 6px; }
.eyebrow { font-family: var(--font-mono); font-size: 0.75rem; letter-spacing: 0.08em; text-transform: uppercase; color: var(--accent); }
.lede { font-size: 1.12rem; color: var(--muted); }
code, .mono { font-family: var(--font-mono); font-size: 0.86em; }
code { background: var(--code-bg); padding: 1px 5px; border-radius: 4px; overflow-wrap: anywhere; }
a { color: var(--accent); }
a:focus-visible, [tabindex]:focus-visible, summary:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; }
.stat { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px; display: grid; gap: 2px; }
.stat b { font-family: var(--font-display); font-size: 1.9rem; font-weight: 600; font-variant-numeric: tabular-nums; line-height: 1.1; }
.stat span { font-size: 0.85rem; color: var(--muted); }
figure { margin: 0; display: grid; gap: 10px; min-width: 0; }
figcaption { font-size: 0.85rem; color: var(--muted); max-width: 68ch; }
.chart { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 18px 16px 14px; overflow-x: auto; position: relative; }
.legend { display: flex; flex-wrap: wrap; gap: 6px 16px; font-size: 0.82rem; color: var(--muted); margin-bottom: 12px; }
.legend i { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 6px; vertical-align: -1px; }
.hbars { display: grid; grid-template-columns: minmax(0, 15.5rem) minmax(160px, 1fr) 3.4rem; gap: 6px 12px; align-items: center; min-width: 440px; }
.hbars .lab { font-size: 0.86rem; text-align: right; color: var(--fg); }
.hbars .lab small { color: var(--faint); font-family: var(--font-mono); font-size: 0.72rem; margin-right: 4px; }
.hbars .val { font-size: 0.82rem; color: var(--muted); font-variant-numeric: tabular-nums; }
.track { position: relative; height: 16px; }
.track .gl { position: absolute; top: -4px; bottom: -4px; width: 1px; background: var(--grid); }
.bar { position: absolute; left: 0; top: 2px; height: 12px; border-radius: 0 4px 4px 0; cursor: default; }
.bar.error { background: var(--critical); } .bar.warning { background: var(--warning); } .bar.info { background: var(--info); }
.axis { display: grid; grid-template-columns: minmax(0, 15.5rem) minmax(160px, 1fr) 3.4rem; gap: 0 12px; min-width: 440px; margin-top: 6px; }
.axis .ticks { position: relative; height: 1.2em; font-size: 0.72rem; color: var(--faint); font-variant-numeric: tabular-nums; }
.axis .ticks span { position: absolute; transform: translateX(-50%); }
.axis .ticks span:first-child { transform: none; }
.cols { display: grid; grid-template-columns: 2.4rem 1fr; gap: 0 8px; min-width: 420px; }
.yaxis { position: relative; height: 200px; font-size: 0.72rem; color: var(--faint); font-variant-numeric: tabular-nums; }
.yaxis span { position: absolute; right: 0; transform: translateY(50%); }
.plot { position: relative; height: 200px; display: grid; grid-template-columns: repeat(var(--n), 1fr); gap: 10px; align-items: end; border-bottom: 1px solid var(--line); }
.plot .gl { position: absolute; left: 0; right: 0; height: 1px; background: var(--grid); }
.col { position: relative; height: 100%; display: flex; align-items: flex-end; justify-content: center; }
.col .bar { position: relative; width: min(42px, 100%); border-radius: 4px 4px 0 0; top: auto; background: var(--series); }
.col .pct { position: absolute; font-size: 0.75rem; color: var(--muted); font-variant-numeric: tabular-nums; transform: translateY(-1.4em); }
.xlabels { grid-column: 2; display: grid; grid-template-columns: repeat(var(--n), 1fr); gap: 10px; font-size: 0.75rem; color: var(--muted); text-align: center; padding-top: 6px; line-height: 1.3; }
.xlabels small { display: block; color: var(--faint); font-variant-numeric: tabular-nums; }
.tip { position: absolute; pointer-events: none; background: var(--fg); color: var(--bg); font-size: 0.78rem; line-height: 1.4; padding: 6px 9px; border-radius: 6px; max-width: 260px; z-index: 2; }
.callout { border-left: 3px solid var(--accent); background: var(--accent-soft); padding: 12px 16px; border-radius: 0 8px 8px 0; display: grid; gap: 6px; }
.pairs { display: grid; grid-template-columns: max-content max-content max-content; gap: 4px 14px; font-family: var(--font-mono); font-size: 0.84rem; overflow-x: auto; }
.pairs .h { font-family: var(--font-body); color: var(--muted); font-size: 0.78rem; }
details { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; }
summary { cursor: pointer; font-weight: 600; }
.table-wrap { overflow-x: auto; margin-top: 12px; }
table { border-collapse: collapse; width: 100%; font-size: 0.86rem; font-variant-numeric: tabular-nums; }
th, td { text-align: left; padding: 7px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
th { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); white-space: nowrap; }
td.num, th.num { text-align: right; white-space: nowrap; }
.sev { font-family: var(--font-mono); font-size: 0.75rem; white-space: nowrap; }
.sev.error { color: var(--critical); } .sev.warning { color: var(--fg); } .sev.info { color: var(--muted); }
.sev.warning::before { content: ""; display: inline-block; width: 8px; height: 8px; background: var(--warning); border-radius: 2px; margin-right: 5px; }
pre { background: var(--code-bg); border-radius: 8px; padding: 12px 14px; overflow-x: auto; font-size: 0.82rem; margin: 0; }
footer { color: var(--muted); font-size: 0.85rem; border-top: 1px solid var(--line); padding-top: 16px; }
@media (max-width: 560px) { .hbars, .axis { grid-template-columns: minmax(0, 9.5rem) minmax(150px, 1fr) 3rem; } }
@media (prefers-reduced-motion: no-preference) { .bar { transition: filter 0.15s; } .bar:hover { filter: brightness(1.08); } }
"""

JS = """
(() => {
  const tip = document.createElement('div');
  tip.className = 'tip'; tip.hidden = true; tip.setAttribute('role', 'status');
  let host = null;
  function show(el) {
    const chart = el.closest('.chart'); if (!chart) return;
    if (host !== chart) { chart.appendChild(tip); host = chart; }
    tip.textContent = el.dataset.tip; tip.hidden = false;
    const c = chart.getBoundingClientRect(), r = el.getBoundingClientRect();
    let x = r.left - c.left + chart.scrollLeft + r.width / 2 - tip.offsetWidth / 2;
    x = Math.max(4, Math.min(x, chart.scrollWidth - tip.offsetWidth - 4));
    tip.style.left = x + 'px';
    tip.style.top = (r.top - c.top - tip.offsetHeight - 8) + 'px';
  }
  const hide = () => { tip.hidden = true; };
  document.querySelectorAll('[data-tip]').forEach(el => {
    el.addEventListener('mouseenter', () => show(el));
    el.addEventListener('mouseleave', hide);
    el.addEventListener('focus', () => show(el));
    el.addEventListener('blur', hide);
  });
})();
"""


def e(v) -> str:
    return html.escape(str(v))


def fmt(n) -> str:
    return f"{n:,}"


def pct(a, b) -> str:
    return f"{100 * a / b:.0f}%"


def rules_chart(rules, n_raw) -> str:
    shown = sorted((r for r in rules if r["severity"] != "info"), key=lambda r: -r["pct_datasets"])
    top = max(r["pct_datasets"] for r in shown)
    scale = 5 * (int(top // 5) + 1)  # next multiple of 5 above the largest bar
    ticks = list(range(0, scale + 1, 5))
    rows = []
    for r in shown:
        sev = r["severity"]
        w = 100 * r["pct_datasets"] / scale
        tip = (
            f"{LABELS.get(r['rule_id'], r['rule_id'])}: {r['datasets']} of {n_raw} datasets "
            f"({r['pct_datasets']}%), {fmt(r['issues'])} findings"
        )
        grid = "".join(f'<span class="gl" style="left:{100 * t / scale:.2f}%"></span>' for t in ticks[1:])
        rows.append(
            f'<div class="lab"><small>{SEV_ICON[sev]}</small>{e(LABELS.get(r["rule_id"], r["rule_id"]))}</div>'
            f'<div class="track">{grid}<span class="bar {sev}" style="width:{w:.2f}%" tabindex="0" '
            f'data-tip="{e(tip)}" aria-label="{e(tip)}"></span></div>'
            f'<div class="val">{r["pct_datasets"]:.1f}%</div>'
        )
    tick_html = "".join(f'<span style="left:{100 * t / scale:.2f}%">{t}%</span>' for t in ticks)
    return (
        '<div class="chart"><div class="legend">'
        '<span><i style="background:var(--critical)"></i>▲ error: breaks BIDS</span>'
        '<span><i style="background:var(--warning)"></i>◆ warning: likely to cause trouble</span>'
        "</div>"
        f'<div class="hbars">{"".join(rows)}</div>'
        f'<div class="axis"><div></div><div class="ticks">{tick_html}</div><div></div></div>'
        "</div>"
    )


def year_chart(by_year) -> str:
    n = len(by_year)
    cols, labels = [], []
    for row in by_year:
        p = row["pct_clean"]
        tip = f"{row['period']}: {row['clean']} of {row['datasets']} datasets clean ({p:.0f}%)"
        cols.append(
            f'<div class="col"><span class="pct" style="bottom:{p:.1f}%">{p:.0f}%</span>'
            f'<span class="bar" style="height:{p:.1f}%" tabindex="0" data-tip="{e(tip)}" aria-label="{e(tip)}"></span></div>'
        )
        label = "2019 and earlier" if row["period"].startswith("2019") else row["period"]
        labels.append(f"<div>{e(label)}<small>n={row['datasets']}</small></div>")
    grid = "".join(f'<span class="gl" style="bottom:{t}%"></span>' for t in (25, 50, 75, 100))
    yticks = "".join(f'<span style="bottom:{t}%">{t}%</span>' for t in (0, 25, 50, 75, 100))
    return (
        f'<div class="chart"><div class="cols" style="--n:{n}">'
        f'<div class="yaxis">{yticks}</div>'
        f'<div class="plot" style="--n:{n}">{grid}{"".join(cols)}</div>'
        f'<div class="xlabels" style="--n:{n}">{"".join(labels)}</div>'
        "</div></div>"
    )


def rules_table(rules) -> str:
    rows = "".join(
        f'<tr><td><span class="sev {r["severity"]}">{r["severity"]}</span></td>'
        f'<td>{e(LABELS.get(r["rule_id"], r["rule_id"]))}<br><span class="mono" style="color:var(--faint)">'
        f"{e(r['rule_id'])}</span></td>"
        f'<td class="num">{r["datasets"]}</td><td class="num">{r["pct_datasets"]:.1f}%</td>'
        f'<td class="num">{fmt(r["issues"])}</td><td>{e(r["description"])}</td></tr>'
        for r in rules
    )
    return (
        '<details><summary>All rules as a table</summary><div class="table-wrap"><table><thead><tr>'
        '<th>Severity</th><th>Rule</th><th class="num">Datasets</th><th class="num">Share</th>'
        '<th class="num">Findings</th><th>What it checks</th></tr></thead>'
        f"<tbody>{rows}</tbody></table></div></details>"
    )


def _status(errors: int, warnings: int) -> str:
    return "errors" if errors else ("warnings only" if warnings else "clean")


def history_section(hx: dict) -> list[str]:
    """Do datasets get cleaner after first publication? From audit/history.json.

    Only first and latest snapshots are compared, so changes are dated "by" the latest
    snapshot, never "in" it. Every judgement word is guarded by the number behind it."""
    s = hx["sample"]
    n = s["cohort"]
    t = hx["transitions"]
    cs = hx["changed_summary"]
    st = {(r["side"], r["status"]): r["datasets"] for r in hx["status"]}
    first_clean, latest_clean = st.get(("first", "clean"), 0), st.get(("latest", "clean"), 0)
    first_err, latest_err = st.get(("first", "errors"), 0), st.get(("latest", "errors"), 0)
    unchanged = n - cs["datasets"]
    p_sign = hx["sign_test"]["p_two_sided"]
    top = hx.get("top_resolver")
    states = ("clean", "warnings only", "errors")
    grid = {(a, b): 0 for a in states for b in states}
    for d in hx["datasets"]:
        grid[
            (
                _status(d["first_errors"], d["first_warnings"]),
                _status(d["latest_errors"], d["latest_warnings"]),
            )
        ] += 1
    head = "".join(f'<th class="num">{e(b)}</th>' for b in states)
    body = "".join(
        f"<tr><td>{e(a)}</td>" + "".join(f'<td class="num">{grid[(a, b)]}</td>' for b in states) + "</tr>"
        for a in states
    )

    excluded = s["multi_snapshot"] - n
    cohort_note = (
        f"all {n} are raw datasets"
        if not excluded
        else f"{n} of them are raw datasets (derivatives are left out)"
    )
    errors_text = (
        f"the number with errors is {first_err} at both ends"
        if first_err == latest_err
        else f"the number with errors goes from {first_err} to {latest_err}"
    )
    chance = (
        f"which is well within chance (sign test p = {p_sign:.2f})"
        if p_sign >= 0.2
        else f"(sign test p = {p_sign:.2f})"
    )
    lead = "Updates rarely touch the metadata. " if unchanged >= 0.75 * n else ""

    out = [
        "<section>",
        "<h2>Do datasets get cleaner after release?</h2>",
        f"<p>The year comparison above uses each dataset's latest snapshot, which mixes when a dataset was "
        f"published with how much it was revised. To separate the two I ran the same rules on the first "
        f"published snapshot of every dataset in the sample. {s['multi_snapshot']} of the {s['datasets']} "
        f"have been updated at least once, a median of {s['median_years_between']:g} years apart, and "
        f"{cohort_note}.</p>",
        f"<p>{lead}In {unchanged} of the {n} updated datasets every finding is the same in the latest "
        f"snapshot as in the first. The share that is fully clean moves from {pct(first_clean, n)} to "
        f"{pct(latest_clean, n)} ({first_clean} to {latest_clean}), and {errors_text}: "
        f"{t['errors_fixed']} fixed, {t['errors_introduced']} introduced. Datasets that ended with fewer "
        f"findings outnumber those that ended with more, {t['fewer_issues']} to {t['more_issues']}, "
        f"{chance}.</p>"
        if t["fewer_issues"] > t["more_issues"]
        else f"<p>{lead}In {unchanged} of the {n} updated datasets every finding is the same in the latest "
        f"snapshot as in the first. The share that is fully clean moves from {pct(first_clean, n)} to "
        f"{pct(latest_clean, n)}, and {errors_text}. {t['fewer_issues']} datasets ended with fewer findings "
        f"and {t['more_issues']} with more {chance}.</p>",
        "<figure>",
        '<div class="chart"><div class="table-wrap" style="margin-top:0"><table>'
        f"<thead><tr><th>First snapshot</th>{head}</tr></thead><tbody>{body}</tbody></table></div></div>",
        f"<figcaption>Status of the {n} updated datasets at their first snapshot (rows) and latest snapshot "
        "(columns). The diagonal is datasets whose status did not change.</figcaption>",
        "</figure>",
    ]

    counts = (
        f"<p>Counting individual findings tells a different story: {fmt(cs['resolved'])} resolved against "
        f"{fmt(cs['introduced'])} introduced."
    )
    if top and top["resolved"] > cs["resolved"] / 2:
        tc = next(c for c in hx["changed"] if c["dataset_id"] == top["dataset_id"])
        k = (
            str(top["per_object_min"])
            if top["per_object_min"] == top["per_object_max"]
            else f"{top['per_object_min']} to {top['per_object_max']}"
        )
        if top["rules"] == ["missing_required_sidecar_field"]:
            detail = (
                f"{top['objects']} recordings that each lacked {k} required sidecar fields in its first "
                f"snapshot ({e(tc['first_snapshot'])}) and have them all by {e(tc['latest_snapshot'])}"
            )
        else:
            detail = (
                f"{top['objects']} files with {k} findings each that are gone by {e(tc['latest_snapshot'])}"
            )
        counts += (
            f" But {fmt(top['resolved'])} of those come from one dataset, <code>{e(top['dataset_id'])}</code>: "
            f"{detail}."
        )
    if cs["data_grew"]:
        counts += (
            f" Some improvements are not metadata fixes. In {cs['data_grew']} of the {cs['datasets']} datasets "
            "whose findings changed, the latest snapshot has at least half again as many data files as the "
            "first, which points to an unfinished first upload"
        )
        k = cs["grew_and_cleared_participant_without_data"]
        counts += (
            f", and in {k} of them the gain is listed participants that simply received their data.</p>"
            if k
            else ".</p>"
        )
    else:
        counts += "</p>"
    out.append(counts)

    regress = []
    for c in hx["changed"]:
        intro = c["introduced_rules"] or []
        if "bidsignore_hides_all_data" in intro:
            regress.append(
                f"By {e(c['latest_snapshot'])}, <code>{e(c['dataset_id'])}</code> has a "
                "<code>.bidsignore</code> that covers every data file, so none of its recordings are "
                f"checked any more. Its first snapshot ({e(c['first_snapshot'])}) had no such pattern."
            )
        if c["participants_lost_data"] and c["data_shrank"]:
            regress.append(
                f"<code>{e(c['dataset_id'])}</code> has fewer data files by {e(c['latest_snapshot'])}, and "
                f"{c['participants_lost_data']} people still listed in its <code>participants.tsv</code> "
                "no longer have any data."
            )
    if regress:
        out.append(f"<p>Updates also break things. {' '.join(regress)}</p>")
    if unchanged >= 0.75 * n:
        out.append(
            "<p>The practical reading: problems present at first upload tend to stay, so the cheapest "
            "place to catch them is before the first snapshot is published.</p>"
        )
    out.append("</section>")
    return out


def build(f: dict, hx: dict | None = None) -> str:
    from neurolake.fixtures import MUST_NOT_FLAG

    s = f["sample"][0]
    h = f["headline"][0]
    n_raw = h["raw_datasets"]
    rules = f["rules"]
    by_rule = {r["rule_id"]: r for r in rules if r["severity"] != "info"}
    t = f["recent_vs_older_test"]
    q = f["subjects_quartiles"][0]["q"]
    errors = f["error_datasets"]
    no_sidecar = {r["dataset_id"]: r for r in f["recordings_without_sidecar"]}
    ids = {r["dataset_id"]: r for r in f["id_format_examples"]}
    hetero = f["acquisition_heterogeneity"]
    hetero_ds = sorted({r["dataset_id"] for r in hetero})
    core = f["bidsignore_core"]
    mirrors = f["mirror_estimate"]

    def r(rule, key="datasets"):
        return by_rule.get(rule, {}).get(key, 0)

    def spread(ds, obj=None):
        """Number of distinct values, and min/max, from a heterogeneity finding."""
        row = next(
            (x for x in hetero if x["dataset_id"] == ds and (obj is None or x["object_id"] == obj)), None
        )
        if not row:
            return None
        vals = [float(v) for v in row["detail"].split(":", 1)[1].split(" across")[0].split(",")]
        return len(vals), min(vals), max(vals)

    rest = spread("ds005003", "task-rest")
    ieeg = spread("ds005522")
    n_prefix = r("participant_id_without_prefix")
    n_prefix_only = sum(set(x["rules"]) == {"participant_id_without_prefix"} for x in errors)

    pairs_html = "".join(
        f"<div>{e(ds)}</div><div>{e(', '.join(ids[ds]['in_participants_tsv']))}</div>"
        f"<div>{e(', '.join(ids[ds]['in_data']))}</div>"
        for ds in sorted(ids)
    )

    # what is wrong in each error dataset, from the findings plus checks of the raw files
    known_causes = {
        "ds005697": "its one EEG sidecar is misnamed (<code>sub-16task-...</code>, missing an underscore), "
        "so no sidecar applies to the recording",
    }

    out = [
        "<title>OpenNeuro Metadata Audit</title>",
        '<meta name="claude:viewport-fit-sides" content="cover">',
        '<meta name="description" content="A reproducible audit of BIDS metadata quality across a random sample of OpenNeuro datasets.">',
        '<link rel="preconnect" href="https://fonts.googleapis.com">',
        '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500'
        '&family=IBM+Plex+Sans+Condensed:wght@600&family=IBM+Plex+Sans:wght@400;600&display=swap">',
        f"<style>{CSS}</style>",
        "<main>",
        "<header>",
        '<span class="eyebrow">neurolake audit &middot; October 2026</span>',
        "<h1>How clean is OpenNeuro's metadata?</h1>",
        f'<p class="lede">I checked the BIDS metadata of {s["datasets"]} randomly sampled OpenNeuro datasets '
        f"against {f['rule_count']} rules, most taken from the BIDS specification and schema. "
        "About half pass without a single warning. Hard errors are uncommon. The weak spot is participant "
        "bookkeeping, and a handful of acquisition inconsistencies would quietly trip up a pooled analysis.</p>",
        "</header>",
        '<div class="stats">',
        f'<div class="stat"><b>{s["datasets"]}</b><span>datasets sampled</span></div>',
        f'<div class="stat"><b>{fmt(s["subjects"])}</b><span>subjects</span></div>',
        f'<div class="stat"><b>{fmt(s["data_files"])}</b><span>data files, {s["terabytes"]} TB described</span></div>',
        f'<div class="stat"><b>{pct(h["clean"], n_raw)}</b><span>of raw datasets fully clean</span></div>',
        "</div>",
        "<section>",
        "<h2>Where the problems are</h2>",
        f"<p>Of the {n_raw} raw datasets, {h['clean']} ({pct(h['clean'], n_raw)}) have no warnings or errors, "
        f"{h['with_warnings']} have at least one warning, and {h['with_errors']} "
        f"({pct(h['with_errors'], n_raw)}) have an error that breaks the standard. Most findings are about "
        "consistency across files (participants against data folders, sidecars against each other), which "
        "a check of one file at a time can miss.</p>",
        "<figure>",
        rules_chart(rules, n_raw),
        f"<figcaption>Share of the {n_raw} raw datasets with at least one finding per rule. A dataset can appear "
        "under several rules. Informational findings are left out (they are in the table). "
        "Hover or focus a bar for counts.</figcaption>",
        "</figure>",
        rules_table(rules),
        "</section>",
        "<section>",
        "<h2>Participants are the weak spot</h2>",
        f"<p>{r('missing_participants_tsv')} datasets ({r('missing_participants_tsv', 'pct_datasets'):.0f}%) "
        "have no <code>participants.tsv</code> at all. The file is only RECOMMENDED by BIDS, but without it "
        "age, sex and group are simply not available to anyone reusing the data. Where the file does exist, "
        f"{r('participant_without_data')} datasets list people with no data in the release, "
        f"{r('subject_missing_from_participants')} have data folders for subjects the table never mentions, "
        f"and {n_prefix} write <code>participant_id</code> without the required <code>sub-</code> prefix.</p>",
        "<p>Small formatting slips are enough to break a join. In <code>ds004226</code> the two sides differ "
        "only in zero padding. In <code>ds001838</code> the table also drops the prefix, so its unpadded IDs "
        "match nothing:</p>",
        f'<div class="pairs"><div class="h">dataset</div><div class="h">participants.tsv says</div>'
        f'<div class="h">data folders are</div>{pairs_html}</div>',
        "</section>",
        "<section>",
        "<h2>Hard errors are uncommon and concentrated</h2>",
        f"<p>All errors come from {len(errors)} datasets. In {n_prefix_only} of them the only error is the "
        "missing <code>sub-</code> prefix above. The rest:</p>"
        if n_prefix_only
        else f"<p>All errors come from {len(errors)} datasets:</p>",
        "<ul>",
    ]
    for err in errors:
        ds = err["dataset_id"]
        rule_set = set(err["rules"])
        if rule_set == {"participant_id_without_prefix"} and n_prefix_only:
            continue
        if rule_set == {"participant_id_without_prefix", "duplicate_participant_id"}:
            what = "participant IDs without the prefix, and the same participant listed on several rows"
        elif ds in known_causes:
            what = known_causes[ds]
        elif "missing_required_sidecar_field" in rule_set and ds in no_sidecar:
            ns = no_sidecar[ds]
            what = (
                f"{ns['recordings']} {ns['datatype'].upper()} recording"
                f"{'s' if ns['recordings'] != 1 else ''} with no sidecar at all, so fields BIDS requires "
                "(sampling rate, power-line frequency, task name) are undefined"
            )
        elif "fieldmap_intended_for_missing_target" in rule_set:
            what = (
                f"{err['issues']} fieldmap <code>IntendedFor</code> entries point at runs that are not "
                "in the dataset"
            )
        elif "malformed_tsv_row" in rule_set:
            what = "a <code>participants.tsv</code> row with a trailing tab, one column more than the header"
        else:
            what = f"{err['issues']} findings"
        out.append(f"<li><code>{e(ds)}</code>: {what}.</li>")
    out += [
        "</ul>",
        "<p>Each of these is cheap to fix and expensive to discover late, which is the case for checking "
        "metadata in a pipeline rather than at analysis time.</p>",
        "</section>",
        "<section>",
        "<h2>Same task, different acquisition</h2>",
        f"<p>{len(hetero_ds)} datasets mix acquisition parameters within a single task and acquisition label: "
        "repetition times for fMRI, or sampling rates for EEG, MEG and iEEG (differences under 0.5% are "
        "ignored as rounding). None of this breaks BIDS, and much of it is deliberate. Multi-site and clinical "
        "recordings vary by scanner or hospital. It still matters, because a pipeline that reads one TR or one "
        "sampling rate per task will be wrong for some runs.</p>",
    ]
    if rest and ieeg:
        out.append(
            f"<p>The largest spread is in <code>ds005003</code>, where resting-state runs use {rest[0]} different "
            f"TRs ({rest[1]:g} to {rest[2]:g} s). In the iEEG dataset <code>ds005522</code>, recordings of a "
            f"single task range from {ieeg[1]:g} to {ieeg[2]:g} Hz.</p>"
        )
    core_ids = {c["dataset_id"] for c in core}
    out += [
        "</section>",
        "<section>",
        "<h2>.bidsignore files that ignore too much</h2>",
        f"<p>A <code>.bidsignore</code> tells the validator which files to skip. In {len(core)} datasets "
        "a pattern also matches core files such as the README or the dataset description. neurolake never "
        "ignores core files and reports the pattern instead.</p>",
        "<ul>",
    ]
    if "ds004103" in core_ids:
        out.append(
            "<li><code>ds004103</code> ends its ignore file with <code>*</code>, which matches every file. "
            "All of its data sits outside the checks, so it is reported once rather than counted as "
            "participants with no data.</li>"
        )
    if "ds006833" in core_ids:
        out.append(
            "<li><code>ds006833</code> uses <code>**/*dat*</code>, which also matches "
            "<code>dataset_description.json</code>.</li>"
        )
    out += [
        "</ul>",
        "</section>",
        "<section>",
        "<h2>Are newer datasets cleaner?</h2>",
        f"<p>Datasets whose latest snapshot is from 2024 or later are clean {pct(t['recent_clean'], t['recent_n'])} "
        f"of the time ({t['recent_clean']} of {t['recent_n']}), against {pct(t['older_clean'], t['older_n'])} "
        f"for older ones ({t['older_clean']} of {t['older_n']}). Fisher's exact test gives p = {t['fisher_p']:.2f}, "
        "but treat that as suggestive. An earlier version of the rules gave p = 0.07 on the same sample, "
        "so the result depends on which rules count, and a snapshot's year is when the dataset was last "
        f"updated, not first published. The share without <code>participants.tsv</code> falls too "
        f"(p = {t['no_participants_p']:.2f}).</p>",
        "<figure>",
        year_chart(f["by_year"]),
        "<figcaption>Share of raw datasets with no warnings or errors, by the year of their latest snapshot. "
        "Each bar is labelled with the number of datasets in that year.</figcaption>",
        "</figure>",
        "</section>",
        *(history_section(hx) if hx else []),
        "<section>",
        "<h2>How this was done</h2>",
        "<ul>",
        f"<li><b>Sample.</b> Accessions ds000001 to ds00{mirrors['max_id']} were shuffled with a fixed seed "
        "(2026) and probed in that order against OpenNeuro's DataLad mirrors on GitHub. The sample is the first "
        f"{s['datasets']} with a snapshot tag ({mirrors['probed']} probes). Every probe is logged, transient "
        "errors are retried rather than counted as missing, and the same seed reproduces the same list. The "
        f"median dataset has {q[1]:.0f} subjects (middle half {q[0]:.0f} to {q[2]:.0f}, largest "
        f"{f['subjects_quartiles'][0]['max']}).</li>",
        "<li><b>Data.</b> Each dataset's latest snapshot was shallow-cloned. Metadata files live in git, while "
        "imaging data stays as git-annex pointers whose keys carry the file size, so no imaging bytes "
        "were downloaded.</li>",
        "<li><b>Rules.</b> Sidecar field levels come from the BIDS schema, and the inheritance principle "
        "follows the specification text, including the rule that only one sidecar may apply per folder level. "
        "That rule is reported as informational for datasets declaring a version before 1.7.0, when the "
        "inheritance principle was rewritten. The TR, sampling-rate and <code>.bidsignore</code> rules are "
        "heuristics of my own. <code>.bidsignore</code> is honoured, and derivative datasets only get "
        "file-integrity checks.</li>",
        f"<li><b>Checks on the checker.</b> Running on real data exposed false positives in the first version. "
        f"{len(MUST_NOT_FLAG)} such patterns are now test fixtures that must never be flagged. An independent "
        "pass then reproduced the sample and every headline number from scratch, and checked 14 findings "
        "against the raw files. All 14 were real. It corrected one description and found one "
        "<code>.bidsignore</code> artifact, both fixed here, and found no missed violations in five "
        "clean datasets.</li>",
        "</ul>",
        '<div class="callout"><b>Limits</b>',
        f"<p>This covers {s['datasets']} of roughly {fmt(int(mirrors['ci95'][0]))} to "
        f"{fmt(int(mirrors['ci95'][1]))} mirrored datasets"
        + (
            ", at their first and latest snapshots only, not every version in between. "
            if hx
            else ", and only each one's latest snapshot. "
        )
        + "The rules "
        "cover the metadata problems that most often break reuse, not the whole specification, so this "
        "complements the official validator rather than replacing it. Metadata stored in git-annex was not "
        "fetched, and the affected files are marked unknown rather than missing.</p></div>",
        "</section>",
        "<section>",
        "<h2>Reproduce it</h2>",
        "<pre><code>pip install -e .\n"
        f"neurolake discover-mirrors --sample {s['datasets']} --max-id {mirrors['max_id']} --seed 2026 \\\n"
        "    --log-file audit/probe_log.csv &gt; audit/sample_ids.txt\n"
        "neurolake ingest-mirror --from-file audit/sample_ids.txt --lake data/audit\n"
        "neurolake transform --lake data/audit\n"
        "python audit/analyze.py data/audit\n"
        "python audit/render.py"
        + ("\nmake history   # first vs latest snapshots" if hx else "")
        + "</code></pre>",
        "<p>Code, rules and tests: "
        '<a href="https://github.com/pearlfranz20/neurolake">github.com/pearlfranz20/neurolake</a>.</p>',
        "</section>",
        "<footer>By Pearl Franz. Data from OpenNeuro (openneuro.org) via the OpenNeuroDatasets mirrors. "
        'Every chart and count on this page is generated from <span class="mono">audit/findings.json</span>'
        + (' and <span class="mono">audit/history.json</span>' if hx else "")
        + ".</footer>",
        "</main>",
        f"<script>{JS}</script>",
    ]
    return "\n".join(out)


def main(
    findings: str = "audit/findings.json", out_dir: str = "docs", history: str = "audit/history.json"
) -> None:
    f = json.loads(Path(findings).read_text())
    hx = json.loads(Path(history).read_text()) if Path(history).exists() else None
    body = build(f, hx)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "openneuro-audit.fragment.html").write_text(body, encoding="utf-8")
    head, rest = body.split("<main>", 1)
    doc = (
        '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
        f"{head}</head>\n<body>\n<main>{rest}\n</body>\n</html>\n"
    )
    (out / "index.html").write_text(doc, encoding="utf-8")
    print(f"wrote {out}/index.html and {out}/openneuro-audit.fragment.html")


if __name__ == "__main__":
    main(*sys.argv[1:])
