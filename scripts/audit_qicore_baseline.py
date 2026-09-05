#!/usr/bin/env python3
"""Audit the committed QICore baseline against a freshly re-extracted source.

The committed ``scripts/comparison/qicore-2025-actual-results.csv`` is locked
"do NOT regenerate" by repository convention. But that policy does not
prevent *auditing* the file: this script compares it against a freshly
re-extracted CSV from an alternate TestCaseResult JSON set (typically a
QICore harness run).

Outputs:

* ``stdout`` summary of:
  - rows added / removed / changed.
  - per-measure drift with cell counts.
* ``--report`` path writes a Markdown breakdown including per-case examples.

This script never overwrites the locked baseline. Its single purpose is to
make the staleness visible so we can decide when to authorize a refresh.

Usage::

    python scripts/audit_qicore_baseline.py
        --baseline scripts/comparison/qicore-2025-actual-results.csv
        --fresh /tmp/qicore-bundle.csv
        --report /tmp/qicore-audit.md
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = REPO_ROOT / "scripts" / "comparison" / "qicore-2025-actual-results.csv"


def read_csv(path: Path, measures: set | None = None) -> dict[tuple[str, str, str], str]:
    """Read CSV; optionally restrict to a set of measures.

    If `measures` is given, only rows whose `measure_name` is in the set are
    retained; if `measures` is `None` all rows are returned.
    """
    out = {}
    with path.open(newline="") as fh:
        for row in csv.DictReader(fh):
            if measures is not None and row["measure_name"] not in measures:
                continue
            out[(row["measure_name"], row["guid"], row["population"])] = row["count"]
    return out


def diff(baseline: dict, fresh: dict) -> dict:
    added = sorted(set(fresh) - set(baseline))
    removed = sorted(set(baseline) - set(fresh))
    common = set(baseline) & set(fresh)
    changed = sorted(k for k in common if baseline[k] != fresh[k])
    unchanged = sorted(k for k in common if baseline[k] == fresh[k])

    per_measure_added = defaultdict(int)
    per_measure_removed = defaultdict(int)
    per_measure_changed = defaultdict(int)
    per_measure_unchanged = defaultdict(int)
    for k in added:
        per_measure_added[k[0]] += 1
    for k in removed:
        per_measure_removed[k[0]] += 1
    for k in changed:
        per_measure_changed[k[0]] += 1
    for k in unchanged:
        per_measure_unchanged[k[0]] += 1

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "unchanged": unchanged,
        "per_measure": {
            "added": dict(per_measure_added),
            "removed": dict(per_measure_removed),
            "changed": dict(per_measure_changed),
            "unchanged": dict(per_measure_unchanged),
        },
    }


def render_markdown(diff_result: dict, baseline_path: str, fresh_path: str,
                     baseline: dict, fresh: dict) -> str:
    """Render the diff as Markdown.

    `baseline_path` and `fresh_path` are display strings (used in the
    header) — the dicts are what drive the row-level "sample changes"
    table.
    """
    lines = [
        "# QICore Baseline Audit",
        "",
        f"- baseline: `{baseline_path}`",
        f"- fresh:    `{fresh_path}`",
        "",
        "## Summary",
        "",
        "| Bucket | Count |",
        "|---|---:|",
        f"| Unchanged rows | {len(diff_result['unchanged'])} |",
        f"| Added in fresh | {len(diff_result['added'])} |",
        f"| Removed vs baseline | {len(diff_result['removed'])} |",
        f"| Changed (bas != fresh) | {len(diff_result['changed'])} |",
        "",
        "## Per-measure drift",
        "",
        "| Measure | Unchanged | Added | Removed | Changed |",
        "|---|---:|---:|---:|---:|",
    ]
    measures = sorted(set(diff_result["per_measure"]["unchanged"]) |
                       set(diff_result["per_measure"]["added"]) |
                       set(diff_result["per_measure"]["removed"]) |
                       set(diff_result["per_measure"]["changed"]))
    for m in measures:
        lines.append(
            f"| {m} | {diff_result['per_measure']['unchanged'].get(m, 0)} | "
            f"{diff_result['per_measure']['added'].get(m, 0)} | "
            f"{diff_result['per_measure']['removed'].get(m, 0)} | "
            f"{diff_result['per_measure']['changed'].get(m, 0)} |"
        )
    lines.append("")
    lines.append("## Sample changes")
    lines.append("")
    lines.append("| Measure | Guid | Population | Baseline | Fresh |")
    lines.append("|---|---|---|---:|---:|")
    for m, g, p in diff_result["changed"][:50]:
        lines.append(f"| {m} | {g} | {p} | {baseline[(m, g, p)]} | {fresh[(m, g, p)]} |")
    if len(diff_result["changed"]) > 50:
        lines.append(f"\n_truncated; showing 50 of {len(diff_result['changed'])} changed rows._")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE,
                        help=f"Path to locked baseline CSV. Default: {DEFAULT_BASELINE.relative_to(REPO_ROOT)}")
    parser.add_argument("--fresh", type=Path, required=True,
                        help="Path to fresh CSV produced by extract_population_qicore.py")
    parser.add_argument("--report", type=Path, default=None,
                        help="Optional Markdown report path (otherwise prints to stdout)")
    parser.add_argument("--json", action="store_true",
                        help="Print diff as JSON to stdout (after report if --report given)")
    parser.add_argument("--measure", action="append", default=None,
                        help="Restrict comparison to given measure(s). May be passed multiple times. "
                             "Default: all measures in either CSV.")
    args = parser.parse_args()

    if not args.baseline.is_file():
        print(f"baseline not found: {args.baseline}", file=sys.stderr)
        return 1
    if not args.fresh.is_file():
        print(f"fresh csv not found: {args.fresh}", file=sys.stderr)
        return 1

    measure_filter = set(args.measure) if args.measure else None
    baseline = read_csv(args.baseline, measure_filter)
    fresh = read_csv(args.fresh, measure_filter)
    diff_result = diff(baseline, fresh)

    md = render_markdown(diff_result, str(args.baseline), str(args.fresh), baseline, fresh)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(md)
        print(f"wrote report -> {args.report}")
    else:
        print(md)

    if args.json:
        # Convert defaultdict -> dict for JSON
        summary = {
            "baseline_rows": len(baseline),
            "fresh_rows": len(fresh),
            "unchanged": len(diff_result["unchanged"]),
            "added": len(diff_result["added"]),
            "removed": len(diff_result["removed"]),
            "changed": len(diff_result["changed"]),
            "per_measure": {k: {m: v for m, v in d.items()}
                             for k, d in diff_result["per_measure"].items()},
        }
        print(json.dumps(summary, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
