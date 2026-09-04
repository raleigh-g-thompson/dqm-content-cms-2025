#!/usr/bin/env python3
"""Cross-engine shared-issue detection.

Compares the US Quality Core (CMS) engine actuals against the QI-Core actuals
(authoritative, last run of the QI-Core measures on an older engine) to
identify test-case population cells where BOTH engines deviate from expected
-- candidate shared / engine-level issues.

Data sources (all `measure_name,guid,Group_N:Population,count` CSVs):
  * expected_results.csv            (source of truth, from MeasureReport JSONs)
  * actual_results.csv              (US Quality Core engine, fresh)
  * qicore-2025-actual-results.csv  (QI-Core engine, older; do NOT regenerate)

Classification per cell (E=expected, C=CMS actual, Q=QICore actual):
  pass               C==E and Q==E
  shared             C!=E and Q!=E and C==Q            (candidate engine issue)
  shared-direction   C!=E and Q!=E and sign(C-E)==sign(Q-E) and C!=Q  (weaker)
  cms-only           C!=E and Q==E
  qicore-only        Q!=E and C==E
  conflicting        C!=E and Q!=E and opposite directions
  incomplete         either actual missing for the cell

Expected/case cells that appear in only one actual set are reported as
`incomplete` (presence differences), never as shared issues.

Population naming: expected_results.csv uses `Group_N:Measure Population
Observation` where the engines emit `Group_N:Measure Observation`; the former
is aliased to the latter so the join is 1:1.

Usage:
  python3 scripts/comparison/engine_shared_issues.py [--measure CMS69FHIRPCSBMIScreenAndFollowUp]
Default measure is CMS69 (pilot). Output goes to
  scripts/comparison/[OUTBASENAME]  (default engine_shared_issues).
"""
import argparse
import csv
import json
import os
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_MEASURE = "CMS69FHIRPCSBMIScreenAndFollowUp"

# expected_results.csv uses this name where the engines use 'Measure Observation'
POP_ALIAS = {"Measure Population Observation": "Measure Observation"}

# population-name prefix -> human bucket ordering for report columns
POP_ABBREV = {
    "Initial Population": "ini",
    "Denominator": "den",
    "Denominator Exclusion": "dexc",
    "Denominator Exception": "dxcp",
    "Numerator": "num",
    "Numerator Exclusion": "numx",
    "Numerator Observation": "numobs",
    "Denominator Observation": "denobs",
    "Measure Observation": "mobs",
    "Measure Population": "mpop",
    "Measure Population Exclusion": "mpx",
    "Measure Population Observation": "mpobs",
}


def read_csv(path):
    """Return {measure: {guid: {Group_N:Population: (count_raw, count_num)}}}."""
    data = defaultdict(lambda: defaultdict(dict))
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            meas = row["measure_name"]
            guid = row["guid"]
            pop = row["population"]
            # normalize population name for cross-file consistency
            base, _, name = pop.rpartition(":")
            if name in POP_ALIAS:
                pop = f"{base}:{POP_ALIAS[name]}"
            raw = row["count"]
            num = None
            try:
                num = int(raw)
            except (TypeError, ValueError):
                pass
            data[meas][guid][pop] = (raw, num)
    return data


def sign(x):
    if x is None:
        return 0
    return (x > 0) - (x < 0)


def classify(E, C, Q):
    """Return (bucket, (c_num, q_num, e_num)) for a cell."""
    c_num = C[1] if C else None
    q_num = Q[1] if Q else None
    e_num = E[1] if E else None
    if e_num is None:
        return "no-expected", (e_num, c_num, q_num)
    if c_num is None or q_num is None:
        return "incomplete", (e_num, c_num, q_num)
    if c_num == e_num and q_num == e_num:
        return "pass", (e_num, c_num, q_num)
    if c_num != e_num and q_num != e_num and c_num == q_num:
        return "shared", (e_num, c_num, q_num)
    if c_num != e_num and q_num != e_num and sign(c_num - e_num) == sign(q_num - e_num):
        return "shared-direction", (e_num, c_num, q_num)
    if c_num != e_num and q_num == e_num:
        return "cms-only", (e_num, c_num, q_num)
    if q_num != e_num and c_num == e_num:
        return "qicore-only", (e_num, c_num, q_num)
    return "conflicting", (e_num, c_num, q_num)


def bucket_abbrev(pop):
    name = pop.rpartition(":")[2]
    for key, ab in POP_ABBREV.items():
        if name == key or name.startswith(key):
            return ab
    return name[:8]


def render_report(measure, expected, actuals, bucket_rows):
    orders = ["shared", "shared-direction", "cms-only", "qicore-only", "conflicting", "incomplete"]
    table = "| Test Case | Population | Expected | CMS Actual | QI-Core Actual | Population | Type |\n"
    table += "|---|---|---:|---:|---:|---|---|\n"
    for bucket in orders:
        for g, p, (e, c, q) in sorted(bucket_rows[bucket]):
            table += (
                f"| [{g}](../../input/tests/measure/{measure}/{g}/) | "
                f"{p} | {e} | {c} | {q} | {bucket_abbrev(p)} | {bucket} |\n"
            )
    return table


def summarize(bucket_rows):
    totals = {b: len(rows) for b, rows in bucket_rows.items()}
    not_passing = sum(v for k, v in totals.items() if k not in ("pass", "no-expected", "incomplete"))
    shared = totals.get("shared", 0)
    pct = (100.0 * shared / not_passing) if not_passing else 0.0
    return totals, not_passing, pct


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--measure",
        default=DEFAULT_MEASURE,
        help=f"measure dir name (default {DEFAULT_MEASURE})",
    )
    ap.add_argument(
        "--output",
        default=None,
        help="base name for output file (default engine_shared_issues)",
    )
    args = ap.parse_args()

    expected_all = read_csv(SCRIPT_DIR / "expected_results.csv")
    cms_all = read_csv(SCRIPT_DIR / "actual_results.csv")
    qicore_all = read_csv(SCRIPT_DIR / "qicore-2025-actual-results.csv")

    if args.measure not in expected_all:
        raise SystemExit(f"measure not found in expected_results.csv: {args.measure}")

    expected = expected_all[args.measure]
    cms = cms_all.get(args.measure, {})
    qicore = qicore_all.get(args.measure, {})

    guids = sorted(set(expected) | set(cms) | set(qicore))
    pops = sorted({p for g in guids for p in set(expected.get(g, ())) | set(cms.get(g, ())) | set(qicore.get(g, ()))})

    bucket_rows = defaultdict(list)
    for g in guids:
        for p in pops:
            E = expected.get(g, {}).get(p)
            C = cms.get(g, {}).get(p)
            Q = qicore.get(g, {}).get(p)
            bucket, (e, c, q) = classify(E, C, Q)
            if bucket == "no-expected":
                continue
            bucket_rows[bucket].append((g, p, (e, c, q)))

    totals, not_passing, shared_pct = summarize(bucket_rows)

    out_base = args.output or "engine_shared_issues"
    out_path = SCRIPT_DIR / f"{out_base}.md"
    lines = []
    lines.append(f"# Cross-Engine Shared-Issue Detection: {args.measure}")
    lines.append("")
    lines.append("Compares US Quality Core (CMS) engine actuals vs QI-Core actuals (older engine).")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Bucket | Count |")
    lines.append("|---|---:|")
    for b in ["shared", "shared-direction", "cms-only", "qicore-only", "conflicting", "incomplete", "pass"]:
        lines.append(f"| {b} | {totals.get(b, 0)} |")
    lines.append(f"| **total cells** | {sum(totals.values())} |")
    lines.append("")
    lines.append(
        f"**Shared (exact-magnitude, both-engines-wrong) share of all not-passing cells: "
        f"{shared_pct:.1f}%** ({totals.get('shared', 0)} / {not_passing})."
    )
    lines.append("")
    lines.append("Interpretation: exact-magnitude agreement between two different engine versions")
    lines.append("on the same logical population cell is the strongest available signal of an")
    lines.append("engine-level bug shared by both engines. Cells where only one engine deviates")
    lines.append("(cms-only / qicore-only) are the disprove evidence for the shared-engine hypothesis.")
    lines.append("")
    lines.append("## Per-bucket cells")
    lines.append("")
    lines.append(render_report(args.measure, expected, {}, bucket_rows))

    with open(out_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote -> {out_path}")

    # quick stdout summary
    print(f"measure: {args.measure}")
    for b in ["shared", "shared-direction", "cms-only", "qicore-only", "conflicting", "incomplete", "pass"]:
        print(f"  {b:18} {totals.get(b, 0)}")
    print(f"  shared% of not-passing: {shared_pct:.1f}%")


if __name__ == "__main__":
    main()