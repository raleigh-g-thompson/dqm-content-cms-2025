#!/usr/bin/env python3
"""Parse discrepancy reports and produce structured measure-failure trackers.

Usage:
    python generate_failure_tracker.py                          # auto-discover all reports
    python generate_failure_tracker.py <input.md>               # single report, auto-name output
    python generate_failure_tracker.py <input.md> <output.md>   # single report, explicit output

In no-args mode, scans scripts/comparison/ for discrepancy_report*.md files and
generates a measure-failure-report for each one that doesn't already have one.

Output is a markdown document with:
  - Summary statistics
  - Per-measure tables listing every failing test case with Resolution column
  - Known root-cause pre-population
  - GUID index for cross-report lookup
"""

import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Known root causes — keyed by measure name
# Format: (root_cause_tag, short_description, notes)
# Tags use the convention from change-classification.md / engine-issues.md
# ---------------------------------------------------------------------------
KNOWN_ROOT_CAUSES: dict[str, list[tuple[str, str, str]]] = {
    "CMS1264FHIRECATREHQR": [
        ("§3", "Measurement Period default wrong year", "#2"),
    ],
    "CMS68FHIRDocumentationCurrentMeds": [
        ("§5.3", "recorded() ambiguous overload", "#21"),
    ],
    "CMS1173FHIRDiagnosticDelayVTE": [
        ("§5.1", "Min() over DateTime", "#24 — fully fixed"),
    ],
    "CMS128FHIRAntidepressantMgmt": [
        ("§4", "CMD dispense workaround", "#22 — fully fixed"),
    ],
    "CMS156FHIRHighRiskMedsElderly": [
        ("§4", "CMD full adaptation", "#25"),
    ],
    "CMS347FHIRStatinPreventionTxCVD": [
        ("§1", "onc→astp namespace", "#1"),
        ("§2", "UCUM units", "#3"),
        ("§3", "doNotPerform, prevalenceInterval", "#6/#10"),
    ],
    "CMS135FHIRACEIorARBorARNIforHF": [
        ("§5.11", "Unable to extract codes from fhirType Reference", "Blocked"),
    ],
    "CMS165FHIRControllingHighBP": [
        ("§5.11", "Unable to extract codes from fhirType Reference", "Blocked"),
    ],
    "CMS144FHIRHFBetaBlockerForLVSD": [
        ("§5.3", "AHAOverall Choice-type gap", "Blocked"),
    ],
    "CMS145FHIRCADBBlockerTPMIorLVSD": [
        ("content gap", "No CQL authored", "Not conversion"),
    ],
    "CMS149FHIRDementiaCognitiveAssess": [
        ("content gap", "No CQL authored", "Not conversion"),
    ],
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class TestCase:
    guid: str
    group: str
    failure_type: str  # "MR" or "mismatched"
    population: str = ""
    expected: str = ""
    actual: str = ""
    resolution: str = "_pending_"


@dataclass
class Measure:
    name: str
    total_cases: int = 0
    mr_count: int = 0
    mismatched_count: int = 0
    cases: list[TestCase] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def extract_guid_from_link(cell: str) -> str:
    """Extract the GUID from a markdown link cell."""
    m = re.search(r"\[\s*([0-9a-f-]{36})\s*\]", cell)
    return m.group(1) if m else cell.strip()


def parse_cell_lines(text: str) -> list[str]:
    """Split a cell's <br>-separated values into individual lines."""
    return [v.strip() for v in text.split("<br>") if v.strip()]


def parse_report(path: str) -> tuple[dict, dict, dict]:
    """Return (summary, passing_measures, measures_dict)."""
    text = Path(path).read_text(encoding="utf-8")
    lines = text.splitlines()

    # --- Summary stats ---
    summary: dict[str, str] = {}
    for line in lines[:12]:
        m = re.match(r"\|\s*(.+?)\s*\|\s*(.+?)\s*\|", line)
        if m:
            k, v = m.group(1).strip(), m.group(2).strip()
            if k and v and k != "Details" and k != "---":
                summary[k] = v

    # --- Passing measures (bullet list) ---
    passing: dict[str, str] = {}
    in_passing = False
    for line in lines:
        if "Measures with No Discrepancies" in line:
            in_passing = True
            continue
        if in_passing:
            if line.startswith("## "):
                break
            m = re.match(r"-\s+(\S+)\s", line)
            if m:
                passing[m.group(1)] = line

    # --- Failing measure summary table (for total_cases counts) ---
    measure_totals: dict[str, int] = {}
    in_summary_table = False
    for line in lines:
        if "Measures with Discrepancies" in line and "##" in line:
            in_summary_table = True
            continue
        if in_summary_table:
            m = re.match(
                r"\|\s*\[([A-Za-z0-9_]+)\].*?\|\s*(\d+)\s*\|", line
            )
            if m:
                measure_totals[m.group(1)] = int(m.group(2))
            elif line.startswith("#### ") or line.startswith("## "):
                break

    # --- Per-measure detail sections ---
    measures: dict[str, Measure] = {}
    current_measure: Measure | None = None
    current_failure_type: str | None = None  # "MR" or "mismatched"

    i = 0
    while i < len(lines):
        line = lines[i]

        # New measure section
        if line.startswith("#### "):
            name = line.lstrip("#").strip()
            current_measure = Measure(name=name, total_cases=measure_totals.get(name, 0))
            measures[name] = current_measure
            current_failure_type = None
            i += 1
            continue

        if current_measure is None:
            i += 1
            continue

        # Missing Results header
        mr_match = re.match(r"Missing Results \((\d+) of (\d+) test cases\)", line)
        if mr_match:
            current_measure.mr_count = int(mr_match.group(1))
            current_failure_type = "MR"
            i += 1
            continue

        # Mismatched header
        mm_match = re.match(r"Mismatched Test Cases \((\d+) of\s+of (\d+)\)", line)
        if mm_match:
            current_measure.mismatched_count = int(mm_match.group(1))
            current_failure_type = "mismatched"
            i += 1
            continue

        # Table header row — detect which type based on column content
        if line.startswith("| Test Case |"):
            if "Population" in line:
                current_failure_type = "mismatched"
            elif "Group" in line:
                current_failure_type = "MR"
            i += 1
            continue

        # Skip separator rows (e.g. | --- | --- | or |---|---|---|:---:|:---:|)
        if re.match(r"^\|[\s\-:|]+\|$", line):
            i += 1
            continue

        # Table data rows — must start with "| [ " followed by a GUID
        if current_failure_type and re.match(r"^\|\s*\[\s*[0-9a-f-]{36}", line):
            cells = [c.strip() for c in line.split("|")]
            cells = [c for c in cells if c]  # drop empty leading/trailing

            if current_failure_type == "MR":
                # | Test Case | Group |
                if len(cells) >= 2:
                    guid = extract_guid_from_link(cells[0])
                    group = cells[1].strip()
                    current_measure.cases.append(
                        TestCase(
                            guid=guid,
                            group=group,
                            failure_type="MR",
                        )
                    )
            elif current_failure_type == "mismatched":
                # | Test Case | Group | Population | Expected | Actual |
                if len(cells) >= 5:
                    guid = extract_guid_from_link(cells[0])
                    group = cells[1].strip()
                    # Population may contain <br> — keep as-is for display
                    population_raw = cells[2].strip()
                    expected_raw = cells[3].strip()
                    actual_raw = cells[4].strip()

                    # Split multi-population rows into individual entries
                    pop_lines = parse_cell_lines(population_raw)
                    exp_lines = parse_cell_lines(expected_raw)
                    act_lines = parse_cell_lines(actual_raw)

                    if len(pop_lines) > 1:
                        # Multiple population differences in one row
                        for p, e, a in zip(pop_lines, exp_lines, act_lines):
                            current_measure.cases.append(
                                TestCase(
                                    guid=guid,
                                    group=group,
                                    failure_type="mismatched",
                                    population=p,
                                    expected=e,
                                    actual=a,
                                )
                            )
                    else:
                        current_measure.cases.append(
                            TestCase(
                                guid=guid,
                                group=group,
                                failure_type="mismatched",
                                population=population_raw.replace("<br>", " "),
                                expected=expected_raw,
                                actual=actual_raw,
                            )
                        )

        # Next measure or end of file
        elif line.startswith("#### ") or line.startswith("## "):
            current_measure = None
            current_failure_type = None
            continue  # re-process this line

        i += 1

    return summary, passing, measures


# ---------------------------------------------------------------------------
# Pre-populate known root causes
# ---------------------------------------------------------------------------
def apply_known_root_causes(measures: dict[str, Measure]) -> None:
    for name, causes in KNOWN_ROOT_CAUSES.items():
        if name in measures:
            tags = ", ".join(tag for tag, _, _ in causes)
            desc = "; ".join(desc for _, desc, _ in causes)
            notes = "; ".join(n for _, _, n in causes if n)
            for case in measures[name].cases:
                case.resolution = f"{tags} — {desc}"
                if notes:
                    case.resolution += f" ({notes})"


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def generate_output(
    report_path: str,
    summary: dict,
    passing: dict,
    measures: dict[str, Measure],
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    report_name = Path(report_path).stem

    total_measures = summary.get("Total Measures", "?")
    total_cases = summary.get("Total Test Cases", "?")
    failing_count = summary.get("Measures with Discrepancies", "?")
    passing_count = str(len(passing))

    # Aggregate MR and mismatched counts across all measures
    total_mr = sum(m.mr_count for m in measures.values())
    total_mm = sum(m.mismatched_count for m in measures.values())

    # Build root-cause category summary
    rc_categories: dict[str, dict] = {}
    for m in measures.values():
        for cause in KNOWN_ROOT_CAUSES.get(m.name, []):
            tag = cause[0]
            if tag not in rc_categories:
                rc_categories[tag] = {"measures": [], "cases": 0}
            rc_categories[tag]["measures"].append(m.name)
            rc_categories[tag]["cases"] += m.mr_count + m.mismatched_count

    out: list[str] = []
    w = out.append

    w(f"# Measure Failure Tracker")
    w("")
    w(f"_Baseline: `{report_name}`_")
    w(f"_Generated: {now}_")
    w("")

    # --- Summary ---
    w("## Summary")
    w("")
    w("| Metric | Value |")
    w("|---|---|")
    w(f"| Total Measures | {total_measures} |")
    w(f"| Total Test Cases | {total_cases} |")
    w(f"| Passing Measures | {passing_count} |")
    w(f"| Failing Measures | {failing_count} |")
    w(f"| Total Failing Test Cases | {total_mr + total_mm} |")
    w(f"| Total Rows (incl. multi-pop expansion) | {sum(len(m.cases) for m in measures.values())} |")
    w("")

    w("### By Failure Type")
    w("")
    w("| Type | Entries |")
    w("|---|---|")
    w(f"| Missing Results | {total_mr} |")
    w(f"| Mismatched | {total_mm} |")
    w("")

    if rc_categories:
        w("### By Known Root Cause")
        w("")
        w("| Category | Measures | Entries | Status |")
        w("|---|---|---|---|")
        for tag, info in sorted(rc_categories.items()):
            measure_list = ", ".join(info["measures"])
            w(f"| {tag} | {len(info['measures'])} ({measure_list}) | {info['cases']} | _pending_ |")
        # Unclassified measures
        classified = set()
        for m_name, causes in KNOWN_ROOT_CAUSES.items():
            classified.add(m_name)
        unclassified = [
            m for m in measures if m not in classified
        ]
        if unclassified:
            w(f"| Not yet classified | {len(unclassified)} | {sum(m.mr_count + m.mismatched_count for m in [measures[n] for n in unclassified])} | _pending_ |")
        w("")

    # --- Per-measure detail ---
    w("---")
    w("")
    w("## Per-Measure Detail")
    w("")

    for name in sorted(measures.keys()):
        m = measures[name]
        total_failing = m.mr_count + m.mismatched_count
        w(f"### {name}")
        w(f"**Total cases**: {m.total_cases} | **Failing**: {total_failing} ({m.mr_count} MR, {m.mismatched_count} mismatched)")

        # Root cause line
        causes = KNOWN_ROOT_CAUSES.get(name, [])
        if causes:
            cause_parts = []
            for tag, desc, notes in causes:
                s = f"**{tag}** {desc}"
                if notes:
                    s += f" ({notes})"
                cause_parts.append(s)
            w(f"**Root cause**: {'; '.join(cause_parts)}")
        else:
            w(f"**Root cause**: _pending classification_")
            # Hint: if all MR, suggest it might be a single engine crash
            if m.mr_count > 0 and m.mismatched_count == 0:
                w(f"**Note**: All {m.mr_count} failing cases are Missing Results — likely a single shared root cause (engine crash or library resolution failure)")
        w("")

        # Split cases by type
        mr_cases = [c for c in m.cases if c.failure_type == "MR"]
        mm_cases = [c for c in m.cases if c.failure_type == "mismatched"]

        if mr_cases and mm_cases:
            # Mixed: render both sections
            w("#### Missing Results")
            w("| Test Case | Group | Resolution |")
            w("|---|---|---|")
            for c in mr_cases:
                w(f"| `{c.guid}` | {c.group} | {c.resolution} |")
            w("")
            w("#### Mismatched")
            w("| Test Case | Group | Population | E | A | Resolution |")
            w("|---|---|---|---|---|---|")
            for c in mm_cases:
                w(f"| `{c.guid}` | {c.group} | {c.population} | {c.expected} | {c.actual} | {c.resolution} |")
        elif mm_cases:
            w("| Test Case | Type | Group | Population | E | A | Resolution |")
            w("|---|---|---|---|---|---|---|")
            for c in mm_cases:
                w(f"| `{c.guid}` | Mismatched | {c.group} | {c.population} | {c.expected} | {c.actual} | {c.resolution} |")
        elif mr_cases:
            w("| Test Case | Type | Group | Resolution |")
            w("|---|---|---|---|")
            for c in mr_cases:
                w(f"| `{c.guid}` | Missing Results | {c.group} | {c.resolution} |")

        w("")
        w("")

    # --- GUID index ---
    w("---")
    w("")
    w("## GUID Index")
    w("")
    w("_Sorted by test case GUID for quick lookup from subsequent reports._")
    w("")
    w("| GUID | Measure | Failure Type | Resolution |")
    w("|---|---|---|---|")
    all_cases: list[tuple[str, str, str, str]] = []
    for name in sorted(measures.keys()):
        for c in measures[name].cases:
            all_cases.append((c.guid, name, c.failure_type, c.resolution))
    all_cases.sort(key=lambda x: x[0])
    for guid, mname, ftype, res in all_cases:
        w(f"| `{guid}` | {mname} | {ftype} | {res} |")
    w("")

    return "\n".join(out)


# ---------------------------------------------------------------------------
# Auto-discovery and naming
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_COMPARISON_DIR = SCRIPT_DIR / "comparison"


def derive_output_path(input_path: Path) -> Path:
    """Derive the failure-report output path from a discrepancy-report path.

    Rules:
        discrepancy_report-20260826-0959.md       → measure-failure-report-20260826-0959.md
        discrepancy_report-pre-connectathon.md     → measure-failure-report-pre-connectathon.md
        discrepancy_report.md                      → measure-failure-report.md
    """
    stem = input_path.stem  # e.g. "discrepancy_report-20260826-0959"
    prefix = "discrepancy_report"
    if stem.startswith(prefix):
        suffix = stem[len(prefix):]
        if suffix.startswith("-"):
            suffix = suffix[1:]
    else:
        suffix = stem
    if suffix:
        out_name = f"measure-failure-report-{suffix}.md"
    else:
        out_name = "measure-failure-report.md"
    return input_path.parent / out_name


def find_reports(comparison_dir: Path) -> list[Path]:
    """Find all discrepancy_report*.md files in the given directory, sorted."""
    reports = sorted(comparison_dir.glob("discrepancy_report*.md"))
    return reports


def is_failure_report(path: Path) -> bool:
    """Return True if the path looks like a measure-failure-report (not an input)."""
    return path.stem.startswith("measure-failure-report")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def process_report(input_path: Path, output_path: Path) -> None:
    """Parse a discrepancy report and write its measure-failure-report."""
    print(f"  Generating {output_path.name} from {input_path.name} ...", file=sys.stderr)

    summary, passing, measures = parse_report(str(input_path))
    apply_known_root_causes(measures)

    total_mr = sum(m.mr_count for m in measures.values())
    total_mm = sum(m.mismatched_count for m in measures.values())
    print(f"    Parsed {len(measures)} failing measures: {total_mr} MR, {total_mm} mismatched entries", file=sys.stderr)

    doc = generate_output(str(input_path), summary, passing, measures)
    output_path.write_text(doc, encoding="utf-8")
    print(f"    Written to {output_path}", file=sys.stderr)


def main():
    args = sys.argv[1:]

    if not args:
        # Auto-discover mode
        comparison_dir = DEFAULT_COMPARISON_DIR
        print(f"Discovering discrepancy reports in {comparison_dir} ...", file=sys.stderr)

        if not comparison_dir.exists():
            print(f"Directory not found: {comparison_dir}", file=sys.stderr)
            sys.exit(1)

        reports = [r for r in find_reports(comparison_dir) if not is_failure_report(r)]
        if not reports:
            print("  No discrepancy reports found.", file=sys.stderr)
            return

        for r in reports:
            print(f"  {r.name}", file=sys.stderr)

        to_process = [r for r in reports if not derive_output_path(r).exists()]
        skipped = [r for r in reports if derive_output_path(r).exists()]

        if skipped:
            print(file=sys.stderr)
            print("Already have failure reports (skipping):", file=sys.stderr)
            for r in skipped:
                print(f"  {derive_output_path(r).name}", file=sys.stderr)

        if not to_process:
            print(file=sys.stderr)
            print("All reports already have measure-failure-report files. Nothing to do.", file=sys.stderr)
            return

        print(file=sys.stderr)
        print(f"Processing {len(to_process)} report(s) ...", file=sys.stderr)
        print(file=sys.stderr)
        for report in to_process:
            process_report(report, derive_output_path(report))
            print(file=sys.stderr)

        print(f"Done — processed {len(to_process)} report(s), skipped {len(skipped)} already-generated", file=sys.stderr)
    elif len(args) == 1:
        input_path = Path(args[0])
        if not input_path.exists():
            print(f"Input not found: {input_path}", file=sys.stderr)
            sys.exit(1)
        process_report(input_path, derive_output_path(input_path))
    elif len(args) == 2:
        input_path = Path(args[0])
        if not input_path.exists():
            print(f"Input not found: {input_path}", file=sys.stderr)
            sys.exit(1)
        process_report(input_path, Path(args[1]))
    else:
        print(f"Usage: {sys.argv[0]} [<input.md> [<output.md>]]", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
