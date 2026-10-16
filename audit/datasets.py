"""Browsable per-dataset quality pages for the OpenNeuro audit.

    python audit/datasets.py export <lake.duckdb> [audit/datasets.json]
    python audit/datasets.py render [audit/datasets.json] [docs]

`export` reads a built lake (the 150 latest snapshots on the published audit) and
writes a compact JSON with what the pages need: dataset facts, error/warning/info
counts, and findings grouped by rule with a capped list of examples next to the true
total. When audit/history.json covers a dataset, its first-snapshot counts are added.

`render` builds docs/datasets/index.html (a sortable, filterable table) and one page
per dataset from that JSON alone, so the pages can be rebuilt without the lake.
Rule descriptions come from dbt/seeds/dq_rules.csv.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("audit_render", Path(__file__).with_name("render.py"))
render = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = render
_spec.loader.exec_module(render)
e, fmt, _status = render.e, render.fmt, render._status

MAX_EXAMPLES = 25  # affected objects shown per rule, each with all of its findings
ID_RE = re.compile(r"ds\d{6}")
SNAPSHOT_RE = re.compile(r"[\w.]+")
SEV_ORDER = {"error": 0, "warning": 1, "info": 2}

_DATASETS = """
    select dataset_id, dataset_name, snapshot, cast(snapshot_created_at as date) as snapshot_date,
           dataset_type, bids_version, n_subjects, n_scans, total_bytes, datatypes
    from marts.dim_dataset order by dataset_id
"""

# The cap counts objects, not findings, so a file that is shown is shown with all of
# its findings. n_details is over every finding, not just the examples.
_RULES = """
    with ranked as (
        select *, dense_rank() over (partition by dataset_id, rule_id, severity order by object_id) as obj_rank
        from marts.fct_dq_issue
    )
    select dataset_id, rule_id, severity, count(*) as n,
           count(distinct object_id) as n_objects,
           count(distinct detail) as n_details,
           list([object_id, detail] order by object_id, detail) filter (where obj_rank <= {cap}) as examples
    from ranked
    group by all
"""


# ---------------------------------------------------------------------------- export


def export(
    lake: str,
    out: str = str(ROOT / "audit" / "datasets.json"),
    history: str = str(ROOT / "audit" / "history.json"),
    pairs: str = str(ROOT / "audit" / "history_pairs.csv"),
    max_examples: int = MAX_EXAMPLES,
) -> dict:
    import duckdb

    con = duckdb.connect(lake, read_only=True)
    con.execute("set TimeZone = 'UTC'")
    cur = con.execute(_DATASETS)
    cols = [c[0] for c in cur.description]
    datasets = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    findings: dict[str, list[dict]] = {}
    sql = _RULES.format(cap=int(max_examples))
    for ds_id, rule, sev, n, n_objects, n_details, examples in con.execute(sql).fetchall():
        findings.setdefault(ds_id, []).append(
            {
                "rule_id": rule,
                "severity": sev,
                "count": n,
                "objects": n_objects,
                "details": n_details,
                "examples": [list(x) for x in examples],
            }
        )

    hx = {}
    if Path(history).exists():
        hx = {d["dataset_id"]: d for d in json.loads(Path(history).read_text())["datasets"]}
    else:
        print(f"warning: {history} not found, no first-vs-latest comparison", file=sys.stderr)
    # datasets whose first published snapshot is the one audited: nothing to compare
    single: dict[str, str] = {}
    if Path(pairs).exists():
        with open(pairs, newline="") as fh:
            single = {r["dataset_id"]: r["latest"] for r in csv.DictReader(fh) if r["first"] == r["latest"]}
    else:
        print(f"warning: {pairs} not found, single-snapshot datasets are not marked", file=sys.stderr)

    out_rows = []
    for d in datasets:
        ds_id = d["dataset_id"]
        rules = sorted(
            findings.get(ds_id, []), key=lambda r: (SEV_ORDER[r["severity"]], -r["count"], r["rule_id"])
        )
        counts = {s: sum(r["count"] for r in rules if r["severity"] == s) for s in SEV_ORDER}
        row = {
            "id": ds_id,
            "name": d["dataset_name"],
            "snapshot": d["snapshot"],
            "date": d["snapshot_date"].isoformat() if d["snapshot_date"] else None,
            "type": d["dataset_type"],
            "bids": d["bids_version"],
            "datatypes": list(d["datatypes"] or []),
            "subjects": d["n_subjects"],
            "files": d["n_scans"],
            "bytes": int(d["total_bytes"] or 0),
            "errors": counts["error"],
            "warnings": counts["warning"],
            "info": counts["info"],
            "status": _status(counts["error"], counts["warning"]),
            "findings": rules,
        }
        h = hx.get(ds_id)
        # only when the comparison was made against this very snapshot
        if h and h["latest_snapshot"] == d["snapshot"]:
            row["first"] = {
                "snapshot": h["first_snapshot"],
                "errors": h["first_errors"],
                "warnings": h["first_warnings"],
                "status": _status(h["first_errors"], h["first_warnings"]),
            }
        elif single.get(ds_id) == d["snapshot"]:
            row["only_snapshot"] = True
        out_rows.append(row)

    result = {"max_examples": int(max_examples), "datasets": out_rows}
    text = json.dumps(result, separators=(",", ":"), ensure_ascii=False)
    # one dataset per line: compact, but diffs stay readable
    text = text.replace(',{"id":', ',\n{"id":')
    Path(out).write_text(text + "\n", encoding="utf-8")
    print(f"wrote {out}: {len(out_rows)} datasets, {Path(out).stat().st_size / 1024:.0f} KB")
    return result


# ---------------------------------------------------------------------------- render

EXTRA_CSS = """
.crumbs { font-size: 0.85rem; color: var(--muted); display: flex; flex-wrap: wrap; gap: 4px 14px; }
.facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px 16px; margin: 0; }
.facts div { display: grid; gap: 0; min-width: 0; }
.facts dt { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); }
.facts dd { margin: 0; font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
.status { display: inline-block; font-size: 0.78rem; white-space: nowrap; padding: 1px 8px; border-radius: 999px; border: 1px solid var(--line); background: var(--surface); }
.status.errors { color: var(--critical); border-color: var(--critical); }
.status.warnings::before { content: ""; display: inline-block; width: 8px; height: 8px; background: var(--warning); border-radius: 2px; margin-right: 5px; }
.status.clean { color: var(--muted); }
.controls { display: flex; flex-wrap: wrap; gap: 8px 16px; align-items: center; }
.controls input { font: inherit; font-size: 0.95rem; padding: 7px 10px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface); color: var(--fg); width: min(100%, 22rem); box-sizing: border-box; }
.controls input:focus-visible, th button:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.controls output { font-size: 0.85rem; color: var(--muted); }
main.wide { max-width: 1120px; }
.ds-wrap { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; margin-top: 0; }
.ds-table { border-collapse: separate; border-spacing: 0; }
.ds-table td, .ds-table th { white-space: nowrap; }
.ds-table td.name { white-space: normal; min-width: 14rem; max-width: 26rem; }
.ds-table tbody tr:last-child td { border-bottom: 0; }
.ds-table th:first-child, .ds-table td:first-child { position: sticky; left: 0; background: var(--surface); z-index: 1; box-shadow: inset -1px 0 0 var(--line); }
th button { all: unset; cursor: pointer; display: inline-flex; gap: 4px; align-items: center; }
th button::after { content: "\\2195"; color: var(--faint); font-size: 0.9em; }
th[aria-sort="ascending"] button::after { content: "\\2191"; color: var(--accent); }
th[aria-sort="descending"] button::after { content: "\\2193"; color: var(--accent); }
th[aria-sort] { color: var(--fg); }
.change { color: var(--muted); }
.change.better { color: var(--fg); }
.change.worse { color: var(--critical); }
.rule { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px; display: grid; gap: 8px; min-width: 0; }
.rule h3 { margin: 0; font-size: 1.02rem; line-height: 1.35; display: flex; flex-wrap: wrap; gap: 4px 10px; align-items: baseline; }
.rule h3 .n { font-weight: 400; color: var(--muted); font-size: 0.88rem; font-variant-numeric: tabular-nums; }
.rule .desc { color: var(--muted); font-size: 0.9rem; }
.rule .id { color: var(--faint); font-family: var(--font-mono); font-size: 0.75rem; }
.ex { list-style: none; padding: 0; gap: 4px; font-size: 0.86rem; max-width: none; }
.ex li { overflow-wrap: anywhere; min-width: 0; }
.ex li:has(.d) { display: grid; gap: 1px; padding: 4px 0; border-bottom: 1px solid var(--grid); }
.ex .d { color: var(--muted); padding-left: 6px; }
.ex code { justify-self: start; }
.ex .more { color: var(--muted); }
.findings { display: grid; gap: 12px; }
"""

INDEX_JS = """
(() => {
  const table = document.getElementById('ds');
  const body = table.tBodies[0];
  const rows = Array.from(body.rows);
  const input = document.getElementById('q');
  const out = document.getElementById('shown');
  const heads = Array.from(table.tHead.rows[0].cells);
  function filter() {
    const q = input.value.trim().toLowerCase();
    let n = 0;
    for (const r of rows) {
      const hit = !q || r.dataset.text.includes(q);
      r.hidden = !hit; if (hit) n++;
    }
    out.textContent = n === rows.length ? `${n} datasets` : `${n} of ${rows.length} datasets`;
  }
  function sort(i, dir) {
    const num = heads[i].dataset.type === 'num';
    const key = r => r.cells[i].dataset.v ?? r.cells[i].textContent;
    rows.sort((a, b) => {
      const x = key(a), y = key(b);
      // empty values (not compared) sort last in either direction
      const c = !num ? x.localeCompare(y, undefined, {numeric: true}) * dir
              : (x === '' || y === '') ? (x === y ? 0 : (x === '' ? 1 : -1))
              : (+x - +y) * dir;
      return c || a.cells[0].textContent.localeCompare(b.cells[0].textContent);
    });
    rows.forEach(r => body.appendChild(r));
    heads.forEach((h, j) => { if (j === i) h.setAttribute('aria-sort', dir > 0 ? 'ascending' : 'descending');
                              else h.removeAttribute('aria-sort'); });
  }
  heads.forEach((h, i) => {
    const b = h.querySelector('button'); if (!b) return;
    b.addEventListener('click', () => {
      const cur = h.getAttribute('aria-sort');
      sort(i, cur ? (cur === 'ascending' ? -1 : 1) : (h.dataset.type === 'num' ? -1 : 1));
    });
  });
  input.addEventListener('input', filter);
  filter();
})();
"""

FONTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">\n'
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500'
    '&family=IBM+Plex+Sans+Condensed:wght@600&family=IBM+Plex+Sans:wght@400;600&display=swap">'
)

STATUS_CLASS = {"errors": "errors", "warnings only": "warnings", "clean": "clean"}
STATUS_RANK = {"clean": 0, "warnings only": 1, "errors": 2}


def _doc(title: str, description: str, body: str, script: str = "", wide: bool = False) -> str:
    main_tag = '<main class="wide">' if wide else "<main>"
    return (
        '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n'
        f"<title>{e(title)}</title>\n"
        f'<meta name="description" content="{e(description)}">\n'
        f'{FONTS}\n<link rel="stylesheet" href="style.css">\n</head>\n<body>\n{main_tag}\n{body}\n</main>\n'
        + (f"<script>{script}</script>\n" if script else "")
        + "</body>\n</html>\n"
    )


def size(n: int) -> str:
    for unit, k in (("TB", 1e12), ("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if n >= k:
            return f"{n / k:.1f} {unit}"
    return f"{n} B"


def status_badge(status: str) -> str:
    return f'<span class="status {STATUS_CLASS[status]}">{e(status)}</span>'


def change(d: dict) -> tuple[str, str, int | None]:
    """(text, css class, sort value) for the change since the first snapshot."""
    f = d.get("first")
    if not f:
        return ("only snapshot" if d.get("only_snapshot") else "not compared"), "", None
    before, after = f["errors"] + f["warnings"], d["errors"] + d["warnings"]
    delta = after - before
    if f["status"] != d["status"]:
        text = f"{f['status']} to {d['status']}"
    elif delta:
        text = f"{fmt(before)} to {fmt(after)} findings"
    else:
        text = "no change"
    cls = "better" if delta < 0 else ("worse" if delta > 0 else "")
    return text, cls, delta


def index_page(data: dict) -> str:
    all_ds = data["datasets"]
    compared = sum("first" in d for d in all_ds)
    rows = []
    for d in all_ds:
        name = d["name"] or ""
        ch, ch_cls, ch_v = change(d)
        text = " ".join([d["id"], name, d["status"], d["type"], *d["datatypes"]]).lower()
        rows.append(
            f'<tr data-text="{e(text)}">'
            f'<td><a href="{e(d["id"])}.html"><code>{e(d["id"])}</code></a></td>'
            f'<td class="name">{e(name) if name else "<i>no name</i>"}'
            + (' <span class="id">(derivative)</span>' if d["type"] == "derivative" else "")
            + "</td>"
            f'<td class="num" data-v="{d["subjects"]}">{fmt(d["subjects"])}</td>'
            f'<td class="num" data-v="{d["files"]}">{fmt(d["files"])}</td>'
            f'<td class="num" data-v="{d["errors"]}">{fmt(d["errors"])}</td>'
            f'<td class="num" data-v="{d["warnings"]}">{fmt(d["warnings"])}</td>'
            f'<td data-v="{STATUS_RANK[d["status"]]}">'
            f"{status_badge(d['status'])}</td>"
            f'<td data-v="{"" if ch_v is None else ch_v}"><span class="change {ch_cls}">{e(ch)}</span></td>'
            "</tr>"
        )
    cols = [
        ("Dataset", "text", ""),
        ("Name", "text", ""),
        ("Subjects", "num", "num"),
        ("Data files", "num", "num"),
        ("Errors", "num", "num"),
        ("Warnings", "num", "num"),
        ("Status", "num", ""),
        ("Since first snapshot", "num", ""),
    ]
    head = "".join(
        f'<th scope="col" data-type="{t}"'
        + (f' class="{c}"' if c else "")
        + (' aria-sort="ascending"' if i == 0 else "")
        + f'><button type="button">{e(label)}</button></th>'
        for i, (label, t, c) in enumerate(cols)
    )
    n_status = {s: sum(d["status"] == s for d in all_ds) for s in ("clean", "warnings only", "errors")}
    raw = [d for d in all_ds if d["type"] != "derivative"]
    raw_clean = sum(d["status"] == "clean" for d in raw)
    clean_text = f"{n_status['clean']} of {len(all_ds)} are clean"
    if len(raw) != len(all_ds):
        clean_text += f" ({raw_clean} of the {len(raw)} raw datasets)"
    body = "\n".join(
        [
            '<nav class="crumbs" aria-label="Breadcrumb"><a href="../index.html">OpenNeuro metadata audit</a></nav>',
            "<header>",
            '<span class="eyebrow">neurolake audit &middot; datasets</span>',
            "<h1>Every dataset in the audit</h1>",
            f'<p class="lede">The {len(all_ds)} OpenNeuro datasets behind the audit, each at the snapshot the '
            f"audit checked. {clean_text}, {n_status['warnings only']} have warnings only "
            f"and {n_status['errors']} have at least one error. Open a dataset to see its findings rule by "
            "rule.</p>",
            "</header>",
            "<section>",
            '<div class="controls"><label for="q" class="mono" style="color:var(--muted)">Filter</label>'
            '<input id="q" type="search" placeholder="ID, name, status or datatype" autocomplete="off">'
            f'<output id="shown" aria-live="polite">{len(all_ds)} datasets</output></div>',
            '<div class="table-wrap ds-wrap"><table id="ds" class="ds-table">'
            '<caption class="mono" style="text-align:left;padding:10px;color:var(--muted);font-size:0.78rem">'
            "Select a column heading to sort.</caption>"
            f"<thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>",
            f'<p style="font-size:0.85rem;color:var(--muted)">Errors and warnings count individual findings. '
            f"Status follows the audit: errors if any error, warnings only if any warning, otherwise clean "
            f"(informational findings do not count). The last column compares with the first published "
            f"snapshot for the {compared} raw datasets that have more than one.</p>",
            "</section>",
            "<footer>By Pearl Franz. Data from OpenNeuro (openneuro.org) via the OpenNeuroDatasets mirrors. "
            'Generated from <span class="mono">audit/datasets.json</span> by '
            '<span class="mono">audit/datasets.py</span>.</footer>',
        ]
    )
    return _doc(
        "OpenNeuro Audit Datasets",
        f"BIDS metadata findings for each of the {len(all_ds)} OpenNeuro datasets in the neurolake audit.",
        body,
        INDEX_JS,
        wide=True,
    )


def join_details(details: list[str]) -> list[str]:
    """Lines for one object's findings, with a shared beginning or ending said once:
    "EEGReference (REQUIRED) not set by any applicable sidecar" and "TaskName (REQUIRED) not
    set by ..." become "EEGReference, TaskName (REQUIRED) not set by ...", and
    "IntendedFor target not found: a" and "... found: b" become "IntendedFor target not
    found: a, b". Details with nothing in common stay one per line."""
    details = list(dict.fromkeys(details))
    if len(details) == 1:
        return details
    words = [d.split(" ") for d in details]
    shortest = min(map(len, words))

    def shared(n: int, at_end: bool) -> bool:
        return len({tuple(w[len(w) - n :] if at_end else w[:n]) for w in words}) == 1

    pre = 0
    while pre < shortest - 1 and shared(pre + 1, False):
        pre += 1
    suf = 0
    while pre + suf < shortest - 1 and shared(suf + 1, True):
        suf += 1
    if max(pre, suf) < 2:
        return details
    middles = ", ".join(" ".join(w[pre : len(w) - suf]) for w in words)
    return [" ".join([*words[0][:pre], middles, *words[0][len(words[0]) - suf :]])]


def finding_block(r: dict, descriptions: dict[str, str]) -> str:
    sev = r["severity"]
    label = render.LABELS.get(r["rule_id"], r["rule_id"])
    ex = r["examples"]
    by_obj: dict[str, list[str]] = {}
    for obj, detail in ex:
        by_obj.setdefault(obj, []).append(detail)
    if r["details"] == 1:
        # every finding of this rule, shown or not, has the same detail: say it once
        common = f'<p class="desc">{e(ex[0][1])}</p>'
        items = [f"<li><code>{e(obj)}</code></li>" for obj in by_obj]
    else:
        # one entry per object, with all of its findings (the cap counts objects)
        common = ""
        items = [
            f'<li><code>{e(obj)}</code><span class="d">{"<br>".join(e(x) for x in join_details(dets))}</span></li>'
            for obj, dets in by_obj.items()
        ]
    more_objects, more_findings = r["objects"] - len(by_obj), r["count"] - len(ex)
    if more_objects > 0:
        more = f"and {fmt(more_objects)} more"
        if more_findings != more_objects:
            more += f", with {fmt(more_findings)} findings"
        items.append(f'<li class="more">{more}</li>')
    desc = descriptions.get(r["rule_id"])
    return (
        '<article class="rule">'
        f'<h3><span class="sev {sev}">{e(sev)}</span>{e(label)}'
        f'<span class="n">{fmt(r["count"])} finding{"s" if r["count"] != 1 else ""}</span></h3>'
        + (f'<p class="desc">{e(desc)} <span class="id">{e(r["rule_id"])}</span></p>' if desc else "")
        + common
        + f'<ul class="ex">{"".join(items)}</ul></article>'
    )


def dataset_page(d: dict, descriptions: dict[str, str], cap: int = MAX_EXAMPLES) -> str:
    name = d["name"]
    url = f"https://openneuro.org/datasets/{d['id']}/versions/{d['snapshot']}"
    ch, ch_cls, _ = change(d)
    first = d.get("first")
    facts = [
        ("Snapshot", f'<a href="{e(url)}">{e(d["snapshot"])}</a>'),
        ("Snapshot date", e(d["date"] or "unknown")),
        ("Type", e(d["type"])),
        ("BIDS version", e(d["bids"] or "not available")),
        ("Subjects", fmt(d["subjects"])),
        ("Data files", fmt(d["files"])),
        ("Size described", size(d["bytes"])),
        ("Datatypes", e(", ".join(d["datatypes"]) or "none")),
        ("Errors", fmt(d["errors"])),
        ("Warnings", fmt(d["warnings"])),
        ("Info", fmt(d["info"])),
        ("Status", status_badge(d["status"])),
    ]
    if first:
        facts.append(
            (
                f"Since first snapshot ({e(first['snapshot'])})",
                f'<span class="change {ch_cls}">{e(ch)}</span>',
            )
        )
    facts_html = "".join(f"<div><dt>{k}</dt><dd>{v}</dd></div>" for k, v in facts)
    blocks = [finding_block(r, descriptions) for r in d["findings"]]
    if blocks:
        findings = (
            (
                f"<p>{len(blocks)} rules fired, most severe first. "
                if len(blocks) > 1
                else "<p>One rule fired. "
            )
            + f'Each rule shows up to {cap} affected objects, with all of their findings.</p><div class="findings">{"".join(blocks)}</div>'
        )
    else:
        findings = "<p>No findings: this snapshot passes every rule in the audit.</p>"
    if d["type"] == "derivative":
        findings = (
            "<p>This is a derivative dataset (pipeline output), so only file-integrity rules apply.</p>"
            + findings
        )
    body = "\n".join(
        [
            '<nav class="crumbs" aria-label="Breadcrumb"><a href="../index.html">OpenNeuro metadata audit</a>'
            '<a href="index.html">All datasets</a></nav>',
            "<header>",
            f'<span class="eyebrow">{e(d["id"])} &middot; snapshot {e(d["snapshot"])}</span>',
            f"<h1>{e(name) if name else e(d['id'])}</h1>",
            f'<p class="lede"><a href="{e(url)}">View {e(d["id"])} {e(d["snapshot"])} on OpenNeuro</a></p>',
            "</header>",
            f'<dl class="facts">{facts_html}</dl>',
            "<section>",
            "<h2>Findings</h2>",
            findings,
            "</section>",
            '<footer><a href="index.html">All datasets</a> &middot; <a href="../index.html">The audit</a>. '
            'Generated from <span class="mono">audit/datasets.json</span>.</footer>',
        ]
    )
    title = f"{d['id']}: {name}" if name else d["id"]
    return _doc(title, f"BIDS metadata findings for OpenNeuro {d['id']} snapshot {d['snapshot']}.", body)


def load_descriptions(path: Path = ROOT / "dbt" / "seeds" / "dq_rules.csv") -> dict[str, str]:
    with open(path, newline="") as fh:
        return {r["rule_id"]: r["description"] for r in csv.DictReader(fh)}


def render_pages(src: str = str(ROOT / "audit" / "datasets.json"), out_dir: str = str(ROOT / "docs")) -> None:
    data = json.loads(Path(src).read_text(encoding="utf-8"))
    # ids become file names and hrefs, snapshots go into URLs: check them all before writing anything
    for d in data["datasets"]:
        if not ID_RE.fullmatch(str(d.get("id"))) or not SNAPSHOT_RE.fullmatch(str(d.get("snapshot"))):
            raise ValueError(f"unexpected dataset id or snapshot: {d.get('id')!r} {d.get('snapshot')!r}")
    descriptions = load_descriptions()
    out = Path(out_dir) / "datasets"
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("ds*.html"):
        old.unlink()
    # one shared stylesheet: the audit page's tokens and look, plus what these pages add
    (out / "style.css").write_text(render.CSS.lstrip() + EXTRA_CSS, encoding="utf-8")
    (out / "index.html").write_text(index_page(data), encoding="utf-8")
    for d in data["datasets"]:
        page = dataset_page(d, descriptions, data.get("max_examples", MAX_EXAMPLES))
        (out / f"{d['id']}.html").write_text(page, encoding="utf-8")
    print(f"wrote {out}/index.html and {len(data['datasets'])} dataset pages")


if __name__ == "__main__":
    cmd, *args = sys.argv[1:] or ["render"]
    {"export": export, "render": render_pages}[cmd](*args)
