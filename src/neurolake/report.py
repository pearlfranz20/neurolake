"""Render the data-quality marts as one self-contained HTML page."""

from __future__ import annotations

import html
from datetime import datetime, timezone
from pathlib import Path

import duckdb

_CSS = """
:root {
  /* Layout: one reading column; summary table first, then issues per dataset, then acquisition detail. */
  --bg: #f6f7f9; --surface: #ffffff; --fg: #18202b; --muted: #5b6676; --line: #dde2e9;
  --accent: #2c5f9e; --accent-soft: #e4ecf7;
  --err: #b42318; --err-soft: #fde8e6; --warn: #9a5b00; --warn-soft: #fdf1dc;
  --info: #4a5565; --info-soft: #eceff3; --ok: #1d7a4c;
  --font-display: "IBM Plex Sans Condensed", "Arial Narrow", system-ui, sans-serif;
  --font-body: "IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
  --font-mono: "IBM Plex Mono", ui-monospace, "SFMono-Regular", Menlo, monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #0f141b; --surface: #161d27; --fg: #e3e8ef; --muted: #98a3b3; --line: #2a3442;
  --accent: #7fa9e0; --accent-soft: #1d2b3e;
  --err: #ff8a7a; --err-soft: #3a1d1b; --warn: #f0b75c; --warn-soft: #3a2c14;
  --info: #b3bdca; --info-soft: #232c38; --ok: #5fcf95; color-scheme: dark; } }
:root[data-theme="dark"] {
  --bg: #0f141b; --surface: #161d27; --fg: #e3e8ef; --muted: #98a3b3; --line: #2a3442;
  --accent: #7fa9e0; --accent-soft: #1d2b3e;
  --err: #ff8a7a; --err-soft: #3a1d1b; --warn: #f0b75c; --warn-soft: #3a2c14;
  --info: #b3bdca; --info-soft: #232c38; --ok: #5fcf95; color-scheme: dark; }
body { background: var(--bg); color: var(--fg); font-family: var(--font-body); font-size: 15px; line-height: 1.5; }
main { max-width: 1040px; margin: 0 auto; padding-block: 32px 64px;
  padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px));
  display: grid; gap: 40px; }
h1, h2, h3 { font-family: var(--font-display); text-wrap: balance; margin: 0; letter-spacing: -0.01em; }
h1 { font-size: 2rem; font-weight: 600; }
h2 { font-size: 1.35rem; font-weight: 600; }
h3 { font-size: 1.05rem; font-weight: 600; }
p { margin: 0; max-width: 68ch; }
.eyebrow { font-family: var(--font-mono); font-size: 0.75rem; letter-spacing: 0.08em; text-transform: uppercase; color: var(--accent); }
.lede { color: var(--muted); }
header { display: grid; gap: 8px; }
section { display: grid; gap: 14px; min-width: 0; }
.mono, code { font-family: var(--font-mono); font-size: 0.85em; }
.table-wrap { overflow-x: auto; background: var(--surface); border: 1px solid var(--line); border-radius: 8px; }
table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--line); vertical-align: top; }
th { font-size: 0.75rem; font-weight: 600; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; white-space: nowrap; }
tr:last-child td { border-bottom: 0; }
td.num, th.num { text-align: right; white-space: nowrap; }
.score { display: flex; align-items: center; gap: 8px; justify-content: flex-end; }
.bar { width: 72px; height: 6px; background: var(--line); border-radius: 3px; overflow: hidden; flex: none; }
.bar > span { display: block; height: 100%; background: var(--ok); }
.bar.mid > span { background: var(--warn); } .bar.low > span { background: var(--err); }
.chip { display: inline-block; font-family: var(--font-mono); font-size: 0.72rem; padding: 1px 7px; border-radius: 999px; white-space: nowrap; }
.chip.error { color: var(--err); background: var(--err-soft); }
.chip.warning { color: var(--warn); background: var(--warn-soft); }
.chip.info { color: var(--info); background: var(--info-soft); }
.zero { color: var(--muted); }
.ds { display: grid; gap: 10px; }
.ds-head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 12px; }
.ds-head .mono { color: var(--accent); }
.clean { color: var(--ok); }
details { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; }
summary { cursor: pointer; font-weight: 600; }
summary:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
dl { display: grid; grid-template-columns: minmax(0, 18rem) minmax(0, 1fr); gap: 6px 16px; margin: 12px 0 0; }
dt { font-family: var(--font-mono); font-size: 0.82rem; overflow-wrap: anywhere; }
dd { margin: 0; color: var(--muted); }
footer { color: var(--muted); font-size: 0.85rem; }
@media (max-width: 560px) { dl { grid-template-columns: minmax(0, 1fr); } dd { margin-bottom: 8px; } h1 { font-size: 1.6rem; } }
"""


def _e(v) -> str:
    return "" if v is None else html.escape(str(v))


def _list(v) -> str:
    if not v:
        return '<span class="zero">none</span>'
    return ", ".join(_e(x) for x in v)


def _fmt_bytes(n) -> str:
    if n is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return str(n)


def _count(n: int, sev: str) -> str:
    return f'<span class="chip {sev}">{n}</span>' if n else '<span class="zero">0</span>'


def build_report(db_path: str | Path) -> str:
    con = duckdb.connect(str(db_path), read_only=True)
    q = lambda sql: con.execute(sql).fetchall()  # noqa: E731

    datasets = q(
        """select q.dataset_id, q.snapshot, q.dataset_name, q.bids_version, q.n_subjects, q.n_scans,
                  q.datatypes, d.total_bytes, q.n_errors, q.n_warnings, q.n_info, q.clean_scan_pct
           from marts.mart_dataset_quality q join marts.dim_dataset d using (dataset_id)
           order by q.dataset_id"""
    )
    issues = q(
        """select dataset_id, severity, rule_id, object_id, detail from marts.fct_dq_issue
           order by dataset_id, case severity when 'error' then 0 when 'warning' then 1 else 2 end,
                    rule_id, object_id"""
    )
    acq = q(
        """select dataset_id, datatype, suffix, task_label, n_files, n_subjects,
                  repetition_times_s, sampling_frequencies_hz, manufacturers, n_files_without_sidecar
           from marts.mart_acquisition_summary order by 1, 2, 3, 4"""
    )
    rules = q("select rule_id, default_severity, description from reference.dq_rules order by rule_id")
    con.close()

    n_err = sum(r[8] for r in datasets)
    n_warn = sum(r[9] for r in datasets)
    n_scans = sum(r[5] for r in datasets)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    out = [
        "<title>Neurolake QC</title>",
        '<meta name="claude:viewport-fit-sides" content="cover">',
        '<link rel="preconnect" href="https://fonts.googleapis.com">',
        '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500'
        '&family=IBM+Plex+Sans+Condensed:wght@600&family=IBM+Plex+Sans:wght@400;600&display=swap">',
        f"<style>{_CSS}</style>",
        "<main>",
        "<header>",
        '<span class="eyebrow">BIDS metadata quality</span>',
        "<h1>Neurolake QC report</h1>",
        f'<p class="lede">{len(datasets)} datasets, {n_scans} data files checked. '
        f"{n_err} errors and {n_warn} warnings across {len(rules)} rules.</p>",
        "</header>",
        "<section>",
        "<h2>Datasets</h2>",
        '<p class="lede">Clean files are data files with no error or warning attached to them, '
        "their sidecars, or their subject.</p>",
        '<div class="table-wrap"><table><thead><tr><th>Dataset</th><th>Name</th><th>BIDS</th>'
        '<th class="num">Subjects</th><th class="num">Files</th><th>Datatypes</th>'
        '<th class="num">Size</th><th class="num">Errors</th><th class="num">Warnings</th>'
        '<th class="num">Clean files</th></tr></thead><tbody>',
    ]
    for ds, snap, name, bids, nsub, nscan, dtypes, size, ne, nw, _ni, pct in datasets:
        pct = pct if pct is not None else 0
        tier = "" if pct >= 90 else ("mid" if pct >= 60 else "low")
        out.append(
            f'<tr><td><span class="mono">{_e(ds)}</span><br><span class="zero mono">{_e(snap)}</span></td>'
            f"<td>{_e(name) or '<span class=zero>untitled</span>'}</td>"
            f'<td class="mono">{_e(bids) or "<span class=zero>missing</span>"}</td>'
            f'<td class="num">{nsub}</td><td class="num">{nscan}</td><td>{_list(dtypes)}</td>'
            f'<td class="num">{_fmt_bytes(size)}</td><td class="num">{_count(ne, "error")}</td>'
            f'<td class="num">{_count(nw, "warning")}</td>'
            f'<td class="num"><div class="score"><span>{pct:.0f}%</span>'
            f'<span class="bar {tier}"><span style="width:{pct:.0f}%"></span></span></div></td></tr>'
        )
    out.append("</tbody></table></div></section>")

    out.append("<section><h2>Issues</h2>")
    for ds, *_rest in datasets:
        rows = [i for i in issues if i[0] == ds]
        out.append(f'<div class="ds"><div class="ds-head"><h3 class="mono">{_e(ds)}</h3>')
        if not rows:
            out.append('<span class="clean">no issues</span></div></div>')
            continue
        out.append(f'<span class="zero">{len(rows)} findings</span></div>')
        out.append(
            '<div class="table-wrap"><table><thead><tr><th>Severity</th><th>Rule</th>'
            "<th>Where</th><th>Detail</th></tr></thead><tbody>"
        )
        for _, sev, rule, obj, detail in rows:
            out.append(
                f'<tr><td><span class="chip {_e(sev)}">{_e(sev)}</span></td>'
                f'<td class="mono">{_e(rule)}</td><td class="mono">{_e(obj)}</td><td>{_e(detail)}</td></tr>'
            )
        out.append("</tbody></table></div></div>")
    out.append("</section>")

    out.append(
        "<section><h2>Acquisition parameters</h2>"
        '<p class="lede">Resolved through the BIDS inheritance principle. More than one value '
        "in a row means runs that may not pool cleanly.</p>"
        '<div class="table-wrap"><table><thead><tr><th>Dataset</th><th>Datatype</th><th>Suffix</th>'
        '<th>Task</th><th class="num">Files</th><th class="num">Subjects</th><th>TR (s)</th>'
        '<th>Sampling (Hz)</th><th>Scanner</th><th class="num">No sidecar</th></tr></thead><tbody>'
    )
    for ds, dt, suf, task, nf, ns, trs, sfs, mfr, nos in acq:
        flag = lambda v: ' style="color:var(--warn);font-weight:600"' if v and len(v) > 1 else ""  # noqa: E731
        out.append(
            f'<tr><td class="mono">{_e(ds)}</td><td>{_e(dt)}</td><td class="mono">{_e(suf)}</td>'
            f'<td class="mono">{_e(task)}</td><td class="num">{nf}</td><td class="num">{ns}</td>'
            f'<td class="mono"{flag(trs)}>{_list(trs)}</td><td class="mono"{flag(sfs)}>{_list(sfs)}</td>'
            f'<td>{_list(mfr)}</td><td class="num">{nos or "<span class=zero>0</span>"}</td></tr>'
        )
    out.append("</tbody></table></div></section>")

    out.append("<section><details><summary>Rule catalogue</summary><dl>")
    for rule, sev, desc in rules:
        out.append(f'<dt>{_e(rule)}</dt><dd>{_e(desc)} <span class="chip {_e(sev)}">{_e(sev)}</span></dd>')
    out.append("</dl></details></section>")
    out.append(
        f'<footer>Generated {generated} by neurolake from <span class="mono">{_e(Path(db_path).name)}</span>.</footer>'
    )
    out.append("</main>")
    return "\n".join(out)


def write_report(db_path: str | Path, out_path: str | Path) -> Path:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build_report(db_path), encoding="utf-8")
    return out
