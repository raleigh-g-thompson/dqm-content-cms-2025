#!/usr/bin/env python3
"""Compile the authored issue catalog into ``known_issues.json``.

The catalog used to be authored directly as a single JSON blob
(``scripts/comparison/known_issues.json``): 56 issues, 1,228 case mappings,
and -- worst -- several markdown bodies embedded as escaped-newline JSON
strings (``body_md``, ``summary_row``, ``preamble_md``,
``cross_cutting_lessons_md``). Frankly unproofreadable in a diff: the E-22/E-23
7-column summary rows and the missing E-20 both slipped through for exactly
that reason.

4a splits that into an authored, proofreadable directory -- one issue file per
issue with TOML front matter, a flat ``cases.csv`` for the machine-scale
(measure, guid) mappings, and plain markdown for the preamble / cross-cutting
lessons. ``known_issues.json`` becomes a **compiled build artifact**: every
consumer that currently reads the JSON keeps reading it (``known_issues.py``,
``generate_engine_issues_doc.py``, ``render_catalog_issue_details.py``,
``compare_results.py`` -- all unchanged), but the file itself is now produced
by this compiler instead of being hand-edited.

Authored layout (``defect-tracking/issues/``):

    _preamble.md              catalog["preamble_md"]
    _cross_cutting_lessons.md catalog["cross_cutting_lessons_md"]
    _manifest.txt             issue-ID order (4a only; retired once a stable
                              (category, numeric-id) sort is verified)
    cases.csv                 one row per (issue_id, measure, guid) mapping
    <ID>.md                   one file per issue: TOML front matter + body

Per-issue file shape::

    +++
    id = "E-07"
    title = "convert Duration to days returns null"
    category = "engine"
    status = "**Confirmed**"
    defect_status = "workaround-applied"
    root_cause_status = "open"
    workaround = "Hand-rolled ToDays() helper"
    affected_measures = ["CMS128", "CMS156"]

    [references]
    engine_entry = "E-07"
    +++

    <body_md verbatim>

The compiler joins ``cases.csv`` rows onto each issue by ``id`` and emits the
catalog with ``json.dumps(catalog, indent=2, ensure_ascii=False) + "\\n"`` --
the exact settings proven in Phase 0 to round-trip this file byte-for-byte.

Usage:
    python3 scripts/comparison/build_catalog.py [-d defect-tracking/issues]
                                                [-o scripts/comparison/known_issues.json]
"""
import argparse
import csv
import json
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]          # dqm-content-cms-2025/
DEFAULT_ISSUES_DIR = REPO_ROOT / "defect-tracking" / "issues"
DEFAULT_OUTPUT = REPO_ROOT / "scripts" / "comparison" / "known_issues.json"

ISSUE_FIELD_ORDER = [
    "id", "title", "category", "status", "defect_status", "verified_on",
    "root_cause_status", "workaround", "references", "affected_measures",
]
# summary_row is honored if present in front matter but is emitted last in the
# JSON (matching the pre-existing key order in the committed catalog). In 4b it
# is dropped from all authored files; a residual value would still round-trip.
# top-level keys ordered as the committed catalog emits them; any key not in
# this list (e.g. the historical trailing "enriched": true) is appended last.
_CATALOG_KEY_ORDER = [
    "schema_version", "bootstrapped_from", "generated_from",
    "preamble_md", "cross_cutting_lessons_md", "issues",
]


def _write_toml_value(value) -> str:
    """Serialize one front-matter field value to TOML (single line)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
    if isinstance(value, list):
        return "[" + ", ".join(_write_toml_value(v) for v in value) + "]"
    raise TypeError(f"cannot serialize {type(value).__name__} to TOML")


def render_front_matter(meta: dict) -> str:
    """Render ``meta`` as a TOML front-matter block (no surrounding +++).

    Writes keys in insertion order so the caller controls the readable order;
    TOML parsing back into build_catalog re-emits in the canonical JSON order,
    so the front-matter layout is cosmetic.
    """
    return "\n".join(f"{k} = {_write_toml_value(v)}" for k, v in meta.items())


def parse_front_matter(text: str) -> (dict, str):
    """Split a ``+++``-delimited file into (front-matter dict, body).

    The body is returned byte-for-byte (leading ``+++`` line and trailing
    newline of the closing delimiter consumed); trailing whitespace is NOT
    stripped, so the compiled ``body_md`` matches the authored file exactly.
    """
    lines = text.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\n") != "+++":
        raise ValueError("file does not start with '+++' front matter: %r"
                         % lines[0] if lines else "")
    for i in range(1, len(lines)):
        if lines[i].rstrip("\n") == "+++":
            meta_text = "".join(lines[1:i])
            body = "".join(lines[i + 1:])
            return tomllib.loads(meta_text), body
    raise ValueError("no closing '+++' delimiter found")


def render_issue_front_matter(meta: dict) -> str:
    """Render an issue's front-matter block (no surrounding +++).

    scalar keys first, then ``summary_row`` (if present), then ``[references]``
    LAST -- TOML tables capture every following key, so the nested table must
    be the final section of the block.
    """
    lines = []
    for k in ISSUE_FIELD_ORDER:
        if k == "references":
            continue
        if k in meta:
            lines.append(f"{k} = {_write_toml_value(meta[k])}")
    if "summary_row" in meta:
        lines.append(f"summary_row = {_write_toml_value(meta['summary_row'])}")
    if "references" in meta and meta["references"]:
        lines.append("")
        lines.append("[references]")
        for rk, rv in meta["references"].items():
            lines.append(f"{rk} = {_write_toml_value(rv)}")
    return "\n".join(lines)


def read_cases(cases_path: Path) -> dict:
    """Read cases.csv -> {issue_id: [{measure, guid}, ...]} preserving row order."""
    cases: dict = {}
    with open(cases_path, "r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        seen_cols = {c.strip() for c in (reader.fieldnames or [])}
        required = {"issue_id", "measure", "guid"}
        missing = required - seen_cols
        if missing:
            raise ValueError(
                f"{cases_path} missing columns: {sorted(missing)} "
                f"(found {sorted(seen_cols)})")
        for row in reader:
            iid = row["issue_id"]
            cases.setdefault(iid, []).append(
                {"measure": row["measure"], "guid": row["guid"]})
    return cases


def read_manifest(manifest_path: Path) -> list:
    """Issue-ID order (4a only); blank lines and comments ignored."""
    order = []
    with open(manifest_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#"):
                order.append(line)
    return order


def _numeric_sort_key(issue_id: str):
    """Sort key for an issue ID: category first, then numeric part."""
    prefix, _, num = issue_id.partition("-")
    try:
        return (prefix, int(num))
    except ValueError:
        return (prefix, 0)


def load_issues(issues_dir: Path) -> dict:
    """Read every <ID>.md in issues_dir -> {id: (meta, body)}.
    _preamble.md / _cross_cutting_lessons.md / _manifest.txt excluded."""
    issues = {}
    for path in sorted(issues_dir.glob("*.md")):
        if path.name.startswith("_"):
            continue
        meta, body = parse_front_matter(path.read_text(encoding="utf-8"))
        if "id" not in meta:
            raise ValueError(f"{path.name} has no 'id' in front matter")
        if meta["id"] in issues:
            raise ValueError(f"duplicate issue id {meta['id']} in {issues_dir}")
        issues[meta["id"]] = (meta, body)
    return issues


def build_catalog(issues_dir=DEFAULT_ISSUES_DIR) -> dict:
    """Compile defect-tracking/issues/ into the known-issues catalog dict."""
    issues_dir = Path(issues_dir)
    preamble_path = issues_dir / "_preamble.md"
    cross_cutting_path = issues_dir / "_cross_cutting_lessons.md"
    manifest_path = issues_dir / "_manifest.txt"
    cases_path = issues_dir / "cases.csv"

    preamble_meta, preamble_md = parse_front_matter(
        preamble_path.read_text(encoding="utf-8"))
    cross_cutting_md = cross_cutting_path.read_text(encoding="utf-8").rstrip("\n")

    cases = read_cases(cases_path) if cases_path.exists() else {}
    issues_by_id = load_issues(issues_dir)

    # Order: 4a reads _manifest.txt (exact order from the source JSON). Once
    # _manifest.txt is retired, fall back to a stable (category, numeric id)
    # sort -- see 4b.
    ids_in_order = read_manifest(manifest_path) if manifest_path.exists() else None
    if ids_in_order is None:
        ids_in_order = sorted(issues_by_id, key=_numeric_sort_key)

    unknown = [i for i in ids_in_order if i not in issues_by_id]
    if unknown:
        raise ValueError(f"_manifest.txt lists unknown issue ids: {unknown}")

    missing = [i for i in issues_by_id if i not in set(ids_in_order)]
    if missing:
        raise ValueError(f"issue file(s) not in _manifest.txt: {missing}")

    issues = []
    for iid in ids_in_order:
        meta, body = issues_by_id[iid]
        issue = {}
        for k in ISSUE_FIELD_ORDER:
            if k == "references":
                if meta.get("references"):
                    issue[k] = meta[k]
                else:
                    issue[k] = {}
            elif k in meta:
                issue[k] = meta[k]
        issue["affected_test_cases"] = cases.get(iid, [])
        issue["body_md"] = body
        if meta.get("summary_row"):
            issue["summary_row"] = meta["summary_row"]
        issues.append(issue)

    catalog = {}
    for k in _CATALOG_KEY_ORDER:
        if k == "preamble_md":
            catalog[k] = preamble_md
        elif k == "cross_cutting_lessons_md":
            catalog[k] = cross_cutting_md
        elif k == "issues":
            catalog[k] = issues
        elif k in preamble_meta:
            catalog[k] = preamble_meta[k]
    # Trailing metadata keys (e.g. the historical "enriched": true) go last.
    for k, v in preamble_meta.items():
        if k not in _CATALOG_KEY_ORDER and k not in catalog:
            catalog[k] = v

    if "schema_version" not in catalog:
        raise ValueError("_preamble.md front matter has no schema_version")
    return catalog


def serialize(catalog: dict) -> str:
    """The exact byte representation proved byte-identical in Phase 0."""
    return json.dumps(catalog, indent=2, ensure_ascii=False) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-d", "--issues-dir", default=str(DEFAULT_ISSUES_DIR))
    parser.add_argument("-o", "--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args(argv or sys.argv[1:])

    catalog = build_catalog(args.issues_dir)
    text = serialize(catalog)
    out = Path(args.output)
    out.write_text(text, encoding="utf-8")
    print(f"Wrote {len(catalog['issues'])} issues to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())