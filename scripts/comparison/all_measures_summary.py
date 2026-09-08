"""Emit a per-measure bucket-count summary across all measures.

Reads expected_results.csv, actual_results.csv, qicore-2025-actual-results.csv
and produces a markdown table of (measure, total, pass, shared, shared-direction,
cms-wrong, qicore-wrong, conflicting, incomplete).

Classification is shared with engine_shared_issues.py via classification.py --
see that module for the full vocabulary. "cms-wrong"/"qicore-wrong" were
previously named "cms-only"/"qicore-only" here, which collided with an
unrelated, presence-only vocabulary used by compare_results.py.

Usage:
  python3 scripts/comparison/all_measures_summary.py
"""
import csv
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from classification import NO_EXPECTED, classify_cell
from populations import canonical_cell

def load(path):
    out = {}
    for r in csv.DictReader(open(path, newline="")):
        out[(r["measure_name"], r["guid"], r["population"])] = int(r["count"])
    return out

def normalize_key(k):
    m, g, p = k
    return (m, g, normalize(p))

def normalize(population):
    """Apply the population-name alias to a raw `Group_N:Population` string,
    returning (group, canonical_name)."""
    return tuple(canonical_cell(population).split(":", 1))


def classify(e, a_c, a_q):
    return classify_cell(e, a_c, a_q)


def build_per_measure_summary(
    expected_path=None,
    actual_path=None,
    qicore_path=None,
):
    """Build a per-measure bucket summary.

    Paths default to the canonical CSVs under ROOT.
    """
    exp = {normalize_key(k): c for k, c in load(expected_path or ROOT / "expected_results.csv").items()}
    cms = {normalize_key(k): c for k, c in load(actual_path or ROOT / "actual_results.csv").items()}
    qic = {normalize_key(k): c for k, c in load(qicore_path or ROOT / "qicore-2025-actual-results.csv").items()}

    per_measure = defaultdict(lambda: defaultdict(int))
    totals = defaultdict(int)
    all_keys = set(exp) | set(cms) | set(qic)
    for k in all_keys:
        m = k[0]
        e, a_c, a_q = exp.get(k), cms.get(k), qic.get(k)
        bucket = classify(e, a_c, a_q)
        if bucket == NO_EXPECTED:
            continue
        per_measure[m][bucket] += 1
        totals[bucket] += 1
    return per_measure, totals


def render_markdown(per_measure, totals):
    lines = [
        "# Cross-Engine Bucket Summary (all measures)",
        "",
        "| Measure | pass | shared | shared-dir | cms-wrong | qicore-wrong | conflicting | incomplete | **total cells** |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for m in sorted(per_measure):
        d = per_measure[m]
        total = sum(d.values())
        lines.append(
            f"| {m} | {d.get('pass',0)} | {d.get('shared',0)} | "
            f"{d.get('shared-direction',0)} | {d.get('cms-wrong',0)} | "
            f"{d.get('qicore-wrong',0)} | {d.get('conflicting',0)} | "
            f"{d.get('incomplete',0)} | {total} |"
        )
    lines.append("")
    lines.append("## Global totals")
    lines.append("")
    for b in ("pass", "shared", "shared-direction", "cms-wrong", "qicore-wrong",
             "conflicting", "incomplete"):
        lines.append(f"- **{b}**: {totals.get(b, 0)}")
    return "\n".join(lines) + "\n"


def main():
    per_measure, totals = build_per_measure_summary()
    md = render_markdown(per_measure, totals)
    out = ROOT / "all_measures_bucket_summary.md"
    out.write_text(md, encoding="utf-8")
    print(f"wrote -> {out}")
    for b in ("pass", "shared", "shared-direction", "cms-wrong", "qicore-wrong",
             "conflicting", "incomplete"):
        print(f"  {b}: {totals.get(b, 0)}")


if __name__ == "__main__":
    main()
