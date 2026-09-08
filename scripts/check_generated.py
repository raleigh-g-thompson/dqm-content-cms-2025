#!/usr/bin/env python3
"""Regenerate-and-diff drift detector for files marked GENERATED.

Three files in this repo carry a "do not edit by hand" banner and are supposed to
be fully reproducible from their declared source:

* scripts/comparison/known_issues.json  <- compiled from defect-tracking/issues/
   * defect-tracking/engine-issues.md      <- scripts/comparison/known_issues.json
   * scripts/comparison/catalog_issue_details.md <- known_issues.json + discrepancy_report.md
   * defect-tracking/improvement-tracking.md <- scripts/comparison/run-history.jsonl

The first of those forked from its source for months before anyone noticed --
the banner was on the last line of a 1,150-line file, so nobody scrolled far
enough to see it, and hand-edits accumulated in the working tree with no
committed recovery point. This script exists so that drift is a build failure
instead of a silent, growing gap.

Each check regenerates the file's content in memory from its source and
compares it against what's on disk. The "Generated: <timestamp>" line every
such file carries is masked before comparing -- that line is expected to
change on every real regeneration; what must NOT change is everything else.

Usage:
  python3 scripts/check_generated.py            # print per-check results, exit 1 on drift
  python3 scripts/check_generated.py --quiet     # exit code only, no output

Exit code 0 means every generated file matches its source. Non-zero means at
least one has drifted -- either someone hand-edited it (undo the edit and
change the source instead) or a generator needs to be re-run.
"""
import argparse
import difflib
import os
import re
import sys

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
_COMPARISON_DIR = os.path.join(_SCRIPTS_DIR, "comparison")
sys.path.insert(0, _SCRIPTS_DIR)
sys.path.insert(0, _COMPARISON_DIR)

import generate_engine_issues_doc as engine_issues_gen
import build_catalog
import known_issues as known_issues_lib
import render_catalog_issue_details as catalog_details_gen
import generate_improvement_tracking as improvement_tracking_gen
import run_history as run_history_lib

REPO_ROOT = os.path.dirname(_SCRIPTS_DIR)

# Matches either report's "Generated" line regardless of its markdown form
# (a bullet in catalog_issue_details.md, a table row in the discrepancy
# report style used elsewhere) so both can share one masking pass.
_TIMESTAMP_LINE_RE = re.compile(
    r"^(- Generated:|\| Generated \|).*$", re.MULTILINE)


def _mask_timestamps(text: str) -> str:
    return _TIMESTAMP_LINE_RE.sub(lambda m: m.group(1) + " <masked>", text)


def check_engine_issues_md():
    """defect-tracking/engine-issues.md, regenerated from known_issues.json."""
    catalog_path = os.path.join(
        REPO_ROOT, "scripts", "comparison", "known_issues.json")
    output_path = os.path.join(
        REPO_ROOT, "defect-tracking", "engine-issues.md")
    catalog = known_issues_lib.load_catalog(catalog_path)
    expected = engine_issues_gen.render(catalog) + "\n"
    return output_path, expected


def check_catalog_issue_details_md():
    """scripts/comparison/catalog_issue_details.md, regenerated from
    known_issues.json plus the current discrepancy_report.md (whose citation
    counts feed one column of the output -- see render_catalog_issue_details.py)."""
    known_issues_path = os.path.join(
        REPO_ROOT, "scripts", "comparison", "known_issues.json")
    discrepancy_report_path = os.path.join(
        REPO_ROOT, "scripts", "comparison", "discrepancy_report.md")
    output_path = os.path.join(
        REPO_ROOT, "scripts", "comparison", "catalog_issue_details.md")
    catalog = known_issues_lib.load_catalog(known_issues_path)
    report_lines = []
    if os.path.exists(discrepancy_report_path):
        with open(discrepancy_report_path, encoding="utf-8") as fh:
            report_lines = fh.read().splitlines()
    expected = catalog_details_gen.render_markdown(
        catalog,
        citations=catalog_details_gen.count_citations(report_lines))
    return output_path, expected


def check_improvement_tracking_md():
    """defect-tracking/improvement-tracking.md, regenerated from the append-only
    run-history log."""
    output_path = os.path.join(
        REPO_ROOT, "defect-tracking", "improvement-tracking.md")
    entries = run_history_lib.read_all(run_history_lib.DEFAULT_PATH)
    expected = improvement_tracking_gen.render(entries)
    return output_path, expected


def check_known_issues_json():
    """scripts/comparison/known_issues.json, compiled from defect-tracking/issues/."""
    issues_dir = os.path.join(REPO_ROOT, "defect-tracking", "issues")
    output_path = os.path.join(
        REPO_ROOT, "scripts", "comparison", "known_issues.json")
    return output_path, build_catalog.serialize(
        build_catalog.build_catalog(issues_dir))


# (label, check_fn) -- check_fn() returns (output_path, expected_content).
CHECKS = [
    ("known_issues.json", check_known_issues_json),
    ("engine-issues.md", check_engine_issues_md),
    ("catalog_issue_details.md", check_catalog_issue_details_md),
    ("improvement-tracking.md", check_improvement_tracking_md),
]


def run_checks(checks=CHECKS, verbose=True) -> bool:
    """Run every check; return True iff none drifted.

    A check whose output file doesn't exist yet is skipped, not failed -- a
    fresh checkout before the first pipeline run is not drift.
    """
    all_clean = True
    for name, check_fn in checks:
        output_path, expected = check_fn()
        if not os.path.exists(output_path):
            if verbose:
                print(f"SKIP  {name}: {output_path} does not exist yet")
            continue

        with open(output_path, encoding="utf-8") as fh:
            actual = fh.read()

        expected_masked = _mask_timestamps(expected)
        actual_masked = _mask_timestamps(actual)

        if expected_masked == actual_masked:
            if verbose:
                print(f"OK    {name}")
        else:
            all_clean = False
            if verbose:
                print(f"DRIFT {name}: on-disk content does not match "
                     "regeneration from its declared source")
                diff = difflib.unified_diff(
                    actual_masked.splitlines(),
                    expected_masked.splitlines(),
                    fromfile=f"{name} (on disk)",
                    tofile=f"{name} (regenerated)",
                    lineterm="",
                )
                for line in list(diff)[:40]:
                    print("  " + line)
    return all_clean


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress per-check output; exit code only.")
    args = parser.parse_args(argv)

    clean = run_checks(verbose=not args.quiet)

    if not clean and not args.quiet:
        print()
        print("One or more generated files have drifted from their source. "
             "Either undo a hand-edit and change the source instead, or "
             "re-run the relevant generator.")

    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
