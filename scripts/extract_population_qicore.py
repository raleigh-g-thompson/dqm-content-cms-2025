#!/usr/bin/env python3
"""QI-Core baseline extractor.

Parses a directory of TestCaseResult-.json files (one subdirectory per
measure) and emits the same flat CSV schema as `scripts/comparison/
qicore-2025-actual-results.csv` so it can be diff'd against the locked baseline
or, with explicit `-f/--force`, overwrite it.

The TestCaseResult JSON structure is produced by both CMS-engine and
QI-Core-engine harnesses (they share the same CR runtime). The patient data
the engine evaluates against stays the same; only the model translation /
helper-library version differs. So this extractor can run against either set
of JSONs by changing the input directory.

CSV schema: ``measure_name,guid,population,count``.

Usage:

  python scripts/extract_population_qicore.py \
      --input input/tests/results/qicore \
      --output /tmp/qicore-bundle.csv

  python scripts/extract_population_qicore.py \
      --input input/tests/results/qicore \
      --output scripts/comparison/qicore-2025-actual-results.csv \
      --force  # only after a manual review

This script reads the same Measure Resources + TestCaseResult JSONs as
``extract_population_actual.py`` and reuses the result-interpretation
logic (allowed display names, measure-criteria mapping, scoring validation).
The output is hash-by-row comparable with the existing baseline.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = REPO_ROOT / "input" / "tests" / "results"
DEFAULT_OUTPUT = REPO_ROOT / "scripts" / "comparison" / "qicore-2025-actual-results.csv"
LOCKED_PATH = DEFAULT_OUTPUT
HEADER = ("measure_name", "guid", "population", "count")
_SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()[:12]


def discover_measure_paths(input_root: Path) -> list[tuple[str, Path]]:
    """Return [(measure_name, results_dir)] sorted by name.

    A measure directory has either a flat layout (input_root/<measure>/*.json) or
    nested (input_root/qicore/<measure>/*.json) if the user picked a parent.
    """
    paths = []
    for entry in sorted(os.listdir(input_root)):
        candidate = input_root / entry
        if entry == "qicore" and candidate.is_dir():
            nested_inputs = candidate
            for sub in sorted(os.listdir(nested_inputs)):
                sub_path = nested_inputs / sub
                if sub_path.is_dir():
                    paths.append((sub, sub_path))
            continue
        if candidate.is_dir():
            paths.append((entry, candidate))
    return paths


def load_measure_criteria(measure_resource_dir: Path) -> dict:
    """Load Measure resources -> {measure_name: {group_id: {expression: population}}}."""
    from extract_population_actual import extract_measure_criteria
    criteria = {}
    if not measure_resource_dir.is_dir():
        return criteria
    for resource in sorted(os.listdir(measure_resource_dir)):
        if not resource.endswith(".json"):
            continue
        measure_path = measure_resource_dir / resource
        try:
            data = json.loads(measure_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001 - non-fatal
            continue
        name = measure_path.stem
        criteria[name] = extract_measure_criteria(data)
    return criteria


def collect_results_for_measure(
    measure_dir: Path, criteria: dict, measure_name: str
) -> list[tuple[str, str, int | str]]:
    """Walk the per-test-case JSONs and return rows (guid, population, count).

    ``measure_name`` may differ from the directory name if the TestCaseResult
    carries a different ``libraryName`` upstream.
    """
    from extract_population_actual import capture_results, MeasureSection
    if not measure_dir.is_dir():
        return []
    measure_criteria = criteria.get(measure_name, {})
    if not measure_criteria:
        return []
    sections = (
        MeasureSection(measure_name, json.loads(p.read_text(encoding="utf-8")))
        for p in sorted(measure_dir.glob("TestCaseResult-*.json"))
        if p.is_file()
    )
    populated = capture_results(sections, {measure_name: measure_criteria})
    rows: list[tuple[str, str, int | str]] = []
    for (m, guid, group), populations in sorted(populated.items()):
        for pop, value in sorted(populations.items()):
            rows.append((guid, pop, value))
    return rows


def write_csv(rows: list[tuple[str, str, int | str]], measure_name: str, out_fh):
    writer = csv.writer(out_fh)
    writer.writerow(HEADER)
    for guid, pop, value in rows:
        writer.writerow((measure_name, guid, pop, value))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                        help="Directory containing one subdirectory per measure "
                             "with TestCaseResult-*.json files. Default: input/tests/results")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help=f"Output CSV path. Default: {DEFAULT_OUTPUT.relative_to(REPO_ROOT)}")
    parser.add_argument("--resource-dir", type=Path,
                        default=REPO_ROOT / "input" / "resources" / "measure",
                        help="Directory containing Measure-CMS*.json resources. "
                             "Default: input/resources/measure")
    parser.add_argument("--measures", nargs="*", default=None,
                        help="Optional subset of measures to process (default: all). "
                             "Example: --measures CMS108FHIRVTEProphylaxis CMS190FHIRVTEProphylaxisICU")
    parser.add_argument("--measure-dir", type=Path,
                        default=REPO_ROOT / "input" / "tests" / "measure",
                        help="Directory containing test-case Measure resources. "
                             "Default: input/tests/measure")
    parser.add_argument("--force", action="store_true",
                        help=f"Allow overwriting the locked baseline ({LOCKED_PATH.name}). "
                             "Without this flag, the script refuses to overwrite.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and emit a row count / per-measure summary to stdout, "
                             "do not write any file.")
    args = parser.parse_args()

    is_locked_target = args.output.resolve() == LOCKED_PATH.resolve()
    if is_locked_target and not args.force:
        if args.dry_run:
            print(f"NOTE: --output resolves to locked baseline {LOCKED_PATH.name}; "
                  f"--dry-run skips the overwrite check.")
        else:
            print(f"refusing to overwrite locked baseline {LOCKED_PATH.name}; "
                  "rerun with --force or write to a different --output path.",
                  file=sys.stderr)
            return 2

    if not args.input.is_dir():
        print(f"input directory not found: {args.input}", file=sys.stderr)
        return 1

    measure_paths = discover_measure_paths(args.input)
    if args.measures:
        wanted = set(args.measures)
        measure_paths = [(m, p) for m, p in measure_paths if m in wanted]
        missing = wanted - {m for m, _ in measure_paths}
        if missing:
            print(f"warning: --measures requested but not found: {sorted(missing)}",
                  file=sys.stderr)

    if not measure_paths:
        print(f"no measure subdirectories found under {args.input}", file=sys.stderr)
        return 1

    criteria = load_measure_criteria(args.resource_dir)
    # Fall back to per-measure Measure resources in input/tests/measure/<measure>/MeasureReport-*.json
    # is acceptable but for the QI-Core baseline the canonical resource path is sufficient.

    totals: dict[str, int] = {}
    output_rows: list[tuple[str, str, str, int | str]] = []

    for measure_name, results_dir in measure_paths:
        per_test_case = results_dir / "test-results"
        if per_test_case.exists():
            results_dir = per_test_case
        per_measure = collect_results_for_measure(results_dir, criteria, measure_name)
        totals[measure_name] = len(per_measure)
        for guid, pop, value in per_measure:
            output_rows.append((measure_name, guid, pop, value))

    if args.dry_run:
        print(f"would write {sum(totals.values())} rows across {len(totals)} measures to {args.output}")
        for m, n in sorted(totals.items()):
            print(f"  {m}: {n} rows")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)
        for row in output_rows:
            writer.writerow(row)
    print(f"wrote {len(output_rows)} rows across {len(totals)} measures to {args.output}")
    for m, n in sorted(totals.items()):
        print(f"  {m}: {n} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
