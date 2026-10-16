"""Compare expected measure results against actual CQL engine results.

Reads two CSVs of ``measure_name,guid,population,count`` rows and writes:

  * ``output_results.csv`` -- one PASS/FAIL/MISSING row per population cell
  * ``discrepancy_report.md`` -- per-measure Missing Results, Missing
    Populations, and Mismatched Test Cases

Run it with no arguments from the repository root; see ``scripts/readme.md``.

Four behaviours are worth knowing about, because each exists to stop the report
inventing discrepancies that are not there:

  * **Population names are canonicalised** (``scripts/comparison/populations.py``)
    before comparison. The expected CSV takes population names from the fixture
    MeasureReport's ``code.coding[0].display`` while the engine emits its own
    spelling, so CMS986's "Measure Population Observation" and "Measure
    Observation" are the same cell under different names. Without
    canonicalisation those rows could never match and every one read as a
    failure.
  * **CQFM measure-observation populations are excluded from scoring.** Three
    ratio measures (CMS1017, CMS871, CMS986) wire a `measure-observation` to a
    parameterized CQL function invoked once per member and then aggregated.
    This harness cannot perform that computation, so those cells are reported
    separately instead of scored. See `defect-tracking/known-issues.md`, I-62.
  * **Implementation issues resolve a failing test case.** Some test cases
    can't match their fixture MeasureReport because the measure steward's test
    data or expectation misuses FHIR or CQL semantics (class `implementation` in
    `defect-tracking/known-issues.md`; e.g. I-67). They are listed in
    ``scripts/comparison/implementation_issues.csv``. A failing cell covered by
    one scores ``IMPLEMENTATION`` instead of ``FAIL``, and a test case whose
    every failing cell is covered counts as resolved, the same as a pass. Those
    cases stay in the per-measure discrepancy tables, labelled with the issue.
  * **A ticketed known issue resolves a failing test case.** A case whose
    failure is explained by a catalogued engine, translator, content, ... issue
    is listed in ``scripts/comparison/known_issues.csv`` with the issue's GitHub
    ticket. Once the ticket is recorded, a covered failing cell scores
    ``KNOWN_ISSUE`` and the case counts as resolved. Without a ticket the case
    still fails, and the report labels it as awaiting one. Every mismatch, of
    either kind or of none, is listed under `## Measures with Discrepancies`
    with an `Issue` column and the issue's note. The ones no issue resolves are
    counted as Unattributed Discrepancies.
"""
import csv
import glob
import os
import re
import sys
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, FrozenSet, List, NamedTuple, Optional, Sequence, Set, Tuple, TypedDict

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_SCRIPTS_DIR, "comparison"))
from populations import (
    CANONICAL_POPULATIONS,
    DEFAULT_MEASURE_RESOURCE_DIR,
    canonical_cell,
    is_scored,
    load_cqfm_aggregation_exclusions,
    split_population,
)


measure_id_pattern = r"(?:CMS|CMSFHIR)(?P<measure_id>\d+)"

# Optional third input: test cases that can't match their fixture because of
# the measure steward's test data, not the engine. See the module docstring.
# Absent is fine -- no case is treated as an implementation issue.
DEFAULT_IMPLEMENTATION_ISSUES_FILE = "./scripts/comparison/implementation_issues.csv"

# The per-cell and per-test-case verdict for a failure covered by an
# implementation issue. Counts as resolved, not as a failure.
IMPLEMENTATION = "IMPLEMENTATION"

# Optional fourth input: failing test cases tied to a catalogued engine,
# translator, content, ... issue (`defect-tracking/known-issues.md`). A case
# resolves only once its issue has a GitHub ticket. Absent is fine.
DEFAULT_KNOWN_ISSUES_FILE = "./scripts/comparison/known_issues.csv"

# The per-cell and per-test-case verdict for a failure covered by a known issue
# that has a ticket. Counts as resolved. Without a ticket the cell stays FAIL.
KNOWN_ISSUE = "KNOWN_ISSUE"

MeasureDifference = namedtuple('MeasureDifference', ['measure', 'total_test_cases', 'test_cases_with_differences', 'result_deltas'])
ResultKey = namedtuple('ResultKey', ['measure_name', 'patient_guid', 'group'])
ResultDelta = namedtuple('ResultDelta', ['patient_guid', 'group', 'population', 'expected', 'actual'])
Comparison = namedtuple('Comparison', ['expected', 'actual'])
TestCaseGroupId = namedtuple('TestCaseId', ['patient_guid', 'group'])

# Canonical scored populations live in scripts/comparison/populations.py, which
# also holds the alias table reconciling the different spellings the expected and
# actual extractors emit. Kept as a module-level name for backwards compatibility
# with callers/tests that referenced it; membership tests should go through
# populations.is_scored(), which applies aliases first.
ValidMeasurePopulationTypes = sorted(CANONICAL_POPULATIONS)

class MissingPopulation(NamedTuple):
    result_key: ResultKey
    population: List[str]

class Discrepancies(NamedTuple):
    missing_results: List[ResultKey]
    missing_populations: List[MissingPopulation]
    population_differences: Dict[str, List[str]]
    measures_with_discrepancies: Set[str]

@dataclass
class MeasureDiscrepancy:
    all_test_cases: List[str] = field(default_factory=list)
    missing_results: List[ResultKey] = field(default_factory=list)
    missing_populations: List[MissingPopulation] = field(default_factory=list)
    mismatched_test_cases: Dict[TestCaseGroupId, Dict[str, Comparison]] = field(default_factory=dict)

class UnscoredCell(NamedTuple):
    measure_name: str
    patient_guid: str
    population: str
    reason: str = "unrecognized-population"
    value: str = ""


class Results(NamedTuple):
    rows: Dict[str, str]
    groups: Dict[ResultKey, Dict[str, str]]
    unscored: List[UnscoredCell] = []


def capture_results(file: str, cqfm_exclusions: Dict[str, FrozenSet[str]] = None) -> Results:
    """Read a results CSV into row- and group-keyed dicts.

    Population names are canonicalised on the way in (see
    ``scripts/comparison/populations.py``) so that keys from the expected and
    actual extractors line up even where they spell the same population
    differently -- e.g. CMS986's "Measure Population Observation" in expected
    vs "Measure Observation" in the engine's actuals. Canonicalising here, at
    the single read point, keeps every downstream key consistent.

    Cells whose population is not a scored population are collected in
    ``unscored`` instead of being silently dropped, so the report can say what
    it did not measure.

    ``cqfm_exclusions`` (``populations.load_cqfm_aggregation_exclusions()``)
    are cells that require per-member CQFM aggregation this harness cannot
    perform (see defect-tracking/known-issues.md, I-62) -- these are also routed to
    ``unscored`` (with ``reason="cqfm-aggregation-excluded"``) rather than
    ``rows``/``results``. Passing the *same* exclusion set into both
    ``capture_results`` calls for a run (expected and actual) is what keeps
    the exclusion symmetric: since the cell never enters
    ``rows``/``results`` on any side, ``capture_discrepancies_by_measure``
    never sees it as missing and the scoring functions never see it as FAIL.
    """
    cqfm_exclusions = cqfm_exclusions or {}
    rows = {}
    results = {}
    unscored: List[UnscoredCell] = []
    with open(file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['measure_name'].lower().startswith("test"):
                continue

            group, population = split_population(row["population"])
            cell = f"{group}:{population}"

            if cell in cqfm_exclusions.get(row["measure_name"], ()):
                unscored.append(UnscoredCell(
                    row["measure_name"], row["guid"], row["population"],
                    reason="cqfm-aggregation-excluded", value=row["count"]))
                continue

            key = (row["measure_name"], row["guid"], cell)
            rows[key] = row["count"]

            if population not in CANONICAL_POPULATIONS:
                unscored.append(UnscoredCell(
                    row["measure_name"], row["guid"], row["population"],
                    reason="unrecognized-population", value=row["count"]))
                continue

            result_key = ResultKey(row["measure_name"], row["guid"], group)
            result = results.setdefault(result_key, {})
            result[population] = row["count"]
    return Results(rows, results, unscored)


def side_status(expected_pops: Dict[str, str],
                side_pops: Optional[Dict[str, str]]) -> str:
    """The engine's verdict on one test case: PASS, FAIL, or MISSING.

    ``expected_pops`` and ``side_pops`` are the per-population count maps for
    one (measure, guid, group), as produced by ``capture_results(...).groups``.

    * ``MISSING`` -- the engine produced no result for this test case at all, so
      there is nothing to compare. Not the same as PASS: it is the absence of
      evidence, and is reported separately from a confirmed FAIL.
    * ``PASS`` -- every expected population equals the engine's value.
    * ``FAIL`` -- otherwise. A population this engine lacks counts as a mismatch,
      because a measure that emits no row for a scored population has not
      reproduced the expectation any more than a wrong value has.

    Comparison is string-equality on the raw counts, matching ``row_outcome`` so
    that the per-case and per-cell verdicts are decided by the same rule.
    """
    if side_pops is None:
        return "MISSING"
    for population, expected in expected_pops.items():
        side_value = side_pops.get(population)
        if side_value is None or str(expected) != str(side_value):
            return "FAIL"
    return "PASS"


class ImplementationIssue(NamedTuple):
    """One row of ``implementation_issues.csv``.

    ``group`` is optional: blank means the issue applies to every group of the
    test case, which is the usual case because a malformed resource belongs to
    the patient, not to a group.
    """
    issue_id: str
    measure_name: str
    guid: str
    group: str
    note: str


IMPLEMENTATION_ISSUE_COLUMNS = ("issue_id", "measure_name", "guid", "group", "note")


def load_implementation_issues(file: Optional[str]) -> List[ImplementationIssue]:
    """Read ``implementation_issues.csv``; an absent file means no issues.

    Columns: ``issue_id,measure_name,guid,group,note``. Values are stripped and
    blank rows are skipped. A file missing a required column, or a row with
    more fields than columns (an unquoted comma), raises ValueError rather than
    silently matching nothing.
    """
    if not file or not os.path.exists(file):
        return []
    issues: List[ImplementationIssue] = []
    with open(file, newline="") as f:
        reader = csv.DictReader(f)
        header = [h.strip() for h in (reader.fieldnames or [])]
        missing = [c for c in ("issue_id", "measure_name", "guid") if c not in header]
        if missing:
            raise ValueError(
                f"{file}: missing column(s) {', '.join(missing)}; expected "
                f"{','.join(IMPLEMENTATION_ISSUE_COLUMNS)}")
        for raw in reader:
            if None in raw:
                # csv.DictReader files surplus fields under the None key.
                raise ValueError(
                    f"{file}, line {reader.line_num}: more fields than columns; "
                    "quote any value that contains a comma")
            row = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
            if not any(row.values()):
                continue
            issues.append(ImplementationIssue(
                row["issue_id"], row["measure_name"], row["guid"],
                row.get("group", ""), row.get("note", "")))
    return issues


def implementation_issue_for(key: ResultKey, issues: Sequence):
    """The issue covering this (measure, guid, group), if any.

    Works on any rows with ``measure_name``, ``guid`` and ``group``, so it
    serves ``KnownIssue`` lists as well as ``ImplementationIssue`` ones.
    """
    for issue in issues:
        if (issue.measure_name == key.measure_name
                and issue.guid == key.patient_guid
                and issue.group in ("", key.group)):
            return issue
    return None


def cell_result_key(row_key: Tuple[str, str, str]) -> ResultKey:
    """The ResultKey of a ``Results.rows`` key ``(measure, guid, "group:population")``."""
    return ResultKey(row_key[0], row_key[1], row_key[2].split(':', 1)[0])


def failing_implementation_keys(expected_results: Dict[ResultKey, Dict[str, str]],
                                actual_results: Dict[ResultKey, Dict[str, str]],
                                issues: Sequence[ImplementationIssue]) -> Set[ResultKey]:
    """Failing (test case, group)s covered by an implementation issue.

    Decided by ``side_status``. A covered group that passes isn't included: it needs no
    explanation.
    """
    if not issues:
        return set()
    return {key for key, expected_pops in expected_results.items()
            if side_status(expected_pops, actual_results.get(key)) != "PASS"
            and implementation_issue_for(key, issues) is not None}


class KnownIssue(NamedTuple):
    """One row of ``known_issues.csv``: a failing test case tied to a catalogued issue.

    ``ticket`` is the GitHub issue (or pull request) URL tracking the fix. A
    known issue resolves a failing case only once it has one; until then the
    case stays FAIL and is reported as awaiting a ticket. ``group`` is optional,
    as in ``ImplementationIssue``.
    """
    issue_id: str
    issue_class: str
    ticket: str
    measure_name: str
    guid: str
    group: str
    note: str

    @property
    def ticketed(self) -> bool:
        return bool(self.ticket)


KNOWN_ISSUE_COLUMNS = ("issue_id", "class", "ticket", "measure_name", "guid", "group", "note")

# Root-cause classes from `defect-tracking/known-issues.md`, less `implementation`,
# which has its own file because it resolves a case without a ticket.
KNOWN_ISSUE_CLASSES = ("engine", "translator", "content", "migration", "fixture",
                       "vendored", "harness")

GITHUB_TICKET_PATTERN = re.compile(r"^https://github\.com/[^/\s]+/[^/\s]+/(?:issues|pull)/\d+$")


def load_known_issues(file: Optional[str]) -> List[KnownIssue]:
    """Read ``known_issues.csv``; an absent file means no known issues.

    Columns: ``issue_id,class,ticket,measure_name,guid,group,note``. Raises
    ValueError on a missing column, an unquoted comma, a class outside
    ``KNOWN_ISSUE_CLASSES``, a ``ticket`` that isn't a GitHub issue/PR URL (so a
    placeholder like ``TBD`` can't resolve a case), or rows of one issue that
    disagree on class or ticket (a ticket belongs to the issue, not the case).
    """
    if not file or not os.path.exists(file):
        return []
    issues: List[KnownIssue] = []
    seen: Dict[str, Tuple[str, str]] = {}
    with open(file, newline="") as f:
        reader = csv.DictReader(f)
        header = [h.strip() for h in (reader.fieldnames or [])]
        missing = [c for c in ("issue_id", "class", "ticket", "measure_name", "guid")
                   if c not in header]
        if missing:
            raise ValueError(
                f"{file}: missing column(s) {', '.join(missing)}; expected "
                f"{','.join(KNOWN_ISSUE_COLUMNS)}")
        for raw in reader:
            if None in raw:
                raise ValueError(
                    f"{file}, line {reader.line_num}: more fields than columns; "
                    "quote any value that contains a comma")
            row = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
            if not any(row.values()):
                continue
            where = f"{file}, line {reader.line_num}"
            if row["class"] not in KNOWN_ISSUE_CLASSES:
                hint = (" (list implementation issues in implementation_issues.csv)"
                        if row["class"] == "implementation" else "")
                raise ValueError(f"{where}: class {row['class']!r} is not one of "
                                 f"{', '.join(KNOWN_ISSUE_CLASSES)}{hint}")
            if row["ticket"] and not GITHUB_TICKET_PATTERN.match(row["ticket"]):
                raise ValueError(f"{where}: ticket {row['ticket']!r} is not a GitHub "
                                 "issue or pull request URL; leave it blank until one exists")
            identity = (row["class"], row["ticket"])
            if seen.setdefault(row["issue_id"], identity) != identity:
                raise ValueError(f"{where}: {row['issue_id']} rows disagree on class or "
                                 "ticket; every row of an issue must carry the same values")
            issues.append(KnownIssue(
                row["issue_id"], row["class"], row["ticket"], row["measure_name"],
                row["guid"], row.get("group", ""), row.get("note", "")))
    return issues


def failing_known_issue_keys(expected_results: Dict[ResultKey, Dict[str, str]],
                             actual_results: Dict[ResultKey, Dict[str, str]],
                             known_issues: Sequence[KnownIssue],
                             implementation_issues: Sequence[ImplementationIssue] = (),
                             ticketed: bool = True) -> Set[ResultKey]:
    """Failing (test case, group)s covered by a known issue with (or without) a ticket.

    A group an implementation issue already covers is left out: that
    explanation takes precedence, so no group is counted twice.
    """
    if not known_issues:
        return set()
    keys = set()
    for key, expected_pops in expected_results.items():
        if side_status(expected_pops, actual_results.get(key)) == "PASS":
            continue
        if implementation_issue_for(key, implementation_issues) is not None:
            continue
        issue = implementation_issue_for(key, known_issues)
        if issue is not None and issue.ticketed == ticketed:
            keys.add(key)
    return keys


def render_cqfm_exclusions_section(
        cqfm_exclusions: Dict[str, FrozenSet[str]],
        unscored_cells: List[UnscoredCell]) -> List[str]:
    """`## Populations Excluded from Automated Scoring (CQFM Aggregation)`.

    Shown whenever any measure has CQFM-aggregation populations. Without this,
    those cells would either read as a misleading FAIL or be silently scored as
    0 -- both of which invent a discrepancy that does not exist.
    """
    if not cqfm_exclusions:
        return []
    out = ["## Populations Excluded from Automated Scoring (CQFM Aggregation)\n", "\n"]
    out.append(
        "_These populations require invoking a parameterized CQL "
        "`measure-observation` function once per member of a "
        "`cqfm-criteriaReference` population and then applying "
        "`cqfm-aggregateMethod` (Sum/Count/Average) -- a computation this "
        "harness's CQL-execution path cannot perform (no scriptable/batch "
        "CQL runner; see `defect-tracking/known-issues.md`, I-62). They are "
        "intentionally excluded from pass/fail scoring rather than reported "
        "as FAIL or silently read as 0. Every other population on these "
        "measures (Initial Population, Denominator, Numerator, exclusions, "
        "etc.) is unaffected and scored normally. Use the `mcp-cql-debug` "
        "per-member probe method documented in I-62 to manually verify any "
        "of the expected values below._\n")
    out.append("\n")
    out.append("| Measure | Excluded Populations |\n")
    out.append("| --- | --- |\n")
    for measure in sort_measure_names(list(cqfm_exclusions)):
        cells = ", ".join(sorted(cqfm_exclusions[measure]))
        out.append(f"| {measure} | {cells} |\n")
    out.append("\n")

    cqfm_cells = [c for c in unscored_cells
                  if getattr(c, "reason", None) == "cqfm-aggregation-excluded"]
    if cqfm_cells:
        out.append(
            f"_{len(cqfm_cells)} cells excluded across {len(cqfm_exclusions)} "
            "measures._\n")
        out.append("\n")
    return out

def row_outcome(expected_result: str, actual_result: str) -> Tuple[str, str]:
    """Return (result, actual_display) for a single expected/actual comparison."""
    if actual_result is None or str(expected_result) != str(actual_result):
        return ("FAIL", actual_result if actual_result is not None else "MISSING")
    return ("PASS", actual_result)


def cell_outcome(key: Tuple[str, str, str], expected_result: str, actual_result: str,
                 implementation_issues: Sequence[ImplementationIssue] = (),
                 known_issues: Sequence[KnownIssue] = ()) -> Tuple[str, str]:
    """``row_outcome``, with a covered FAIL scored IMPLEMENTATION or KNOWN_ISSUE.

    An implementation issue wins over a known issue. A known issue without a
    ticket leaves the cell FAIL.
    """
    result, actual_display = row_outcome(expected_result, actual_result)
    if result == "FAIL":
        result_key = cell_result_key(key)
        if implementation_issue_for(result_key, implementation_issues):
            result = IMPLEMENTATION
        else:
            issue = implementation_issue_for(result_key, known_issues)
            if issue is not None and issue.ticketed:
                result = KNOWN_ISSUE
    return result, actual_display


def test_case_outcomes(expected_rows: Dict, actual_rows: Dict,
                       implementation_issues: Sequence[ImplementationIssue] = (),
                       known_issues: Sequence[KnownIssue] = ()) -> Dict[Tuple[str, str], str]:
    """Map (measure_name, patient_guid) -> 'PASS', 'IMPLEMENTATION', 'KNOWN_ISSUE' or 'FAIL'.

    A test case passes iff every expected valid population cell for that case
    matches the actual value; it fails once if any expected cell is wrong or
    missing. Counting distinct test cases avoids inflating the counts when a
    single root cause (e.g. a denominator bug) derails several populations of
    the same case.

    A case is IMPLEMENTATION when it has a mismatching cell and every
    mismatching cell is covered by an implementation issue. One uncovered
    mismatch, e.g. in a group the issue doesn't name, makes it FAIL. KNOWN_ISSUE
    is the same, for cells covered by a ticketed known issue; a case with both
    kinds of cell is KNOWN_ISSUE. Every verdict but FAIL counts as resolved.
    """
    rank = {"PASS": 0, IMPLEMENTATION: 1, KNOWN_ISSUE: 2, "FAIL": 3}
    outcomes: Dict[Tuple[str, str], str] = {}
    for key, expected_result in expected_rows.items():
        # key fields: [ 'measure_name', 'patient_guid', 'group:population' ]
        if not is_scored(key[2].split(':', 1)[1]):
            continue
        result, _ = cell_outcome(key, expected_result, actual_rows.get(key),
                                 implementation_issues, known_issues)
        case_key = (key[0], key[1])
        if rank[result] >= rank[outcomes.get(case_key, "PASS")]:
            outcomes[case_key] = result
    return outcomes


def generate_output(file: str, expected_rows: Dict, actual_rows: Dict,
                    implementation_issues: Sequence[ImplementationIssue] = (),
                    known_issues: Sequence[KnownIssue] = ()) -> Tuple[int, int]:
    """Write one row per expected population cell to ``file``.

    The CSV keeps per-population detail; the returned (pass, fail) counts are
    at test-case granularity (a case counts once even if several of its
    population cells mismatch — they usually share a root cause). A mismatching
    cell covered by an implementation issue is written as IMPLEMENTATION, one
    covered by a ticketed known issue as KNOWN_ISSUE, and such cases are in
    neither count; see ``implementation_case_count``/``known_issue_case_count``.
    """
    header = ["result", "measure_name", "guid", "population", "expected_result", "actual_result"]
    output = []

    for key, expected_result in expected_rows.items():
        # key fields: [ 'measure_name', 'patient_guid', 'group:population' ]
        # Unscored populations are excluded here and reported in the discrepancy
        # report's "Cells excluded from scoring" section (Results.unscored), so
        # the omission is visible rather than silent.
        if not is_scored(key[2].split(':', 1)[1]):
            continue

        result, actual_display = cell_outcome(key, expected_result, actual_rows.get(key),
                                              implementation_issues, known_issues)
        output.append([result, key[0], key[1], key[2], expected_result, actual_display])

    with open(file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(output)

    return scores(expected_rows, actual_rows, implementation_issues, known_issues)


def scores(expected_rows: Dict[str, str], actual_rows: Dict[str, str],
           implementation_issues: Sequence[ImplementationIssue] = (),
           known_issues: Sequence[KnownIssue] = ()) -> Tuple[int, int]:
    """Compute (pass, fail) counting distinct test cases (not population cells).

    IMPLEMENTATION and KNOWN_ISSUE cases are in neither count; see
    ``implementation_case_count`` and ``known_issue_case_count``.
    """
    outcomes = test_case_outcomes(expected_rows, actual_rows, implementation_issues,
                                  known_issues)
    pass_count = sum(1 for o in outcomes.values() if o == "PASS")
    fail_count = sum(1 for o in outcomes.values() if o == "FAIL")
    return (pass_count, fail_count)


def implementation_case_count(expected_rows: Dict[str, str], actual_rows: Dict[str, str],
                              implementation_issues: Sequence[ImplementationIssue],
                              known_issues: Sequence[KnownIssue] = ()) -> int:
    """Test cases resolved by an implementation issue rather than by a match."""
    outcomes = test_case_outcomes(expected_rows, actual_rows, implementation_issues,
                                  known_issues)
    return sum(1 for o in outcomes.values() if o == IMPLEMENTATION)


def known_issue_case_count(expected_rows: Dict[str, str], actual_rows: Dict[str, str],
                           implementation_issues: Sequence[ImplementationIssue],
                           known_issues: Sequence[KnownIssue]) -> int:
    """Test cases resolved by a ticketed known issue rather than by a match."""
    outcomes = test_case_outcomes(expected_rows, actual_rows, implementation_issues,
                                  known_issues)
    return sum(1 for o in outcomes.values() if o == KNOWN_ISSUE)


def create_markdown_table(headers: List[str], data: List[str], custom_separator_row: str=None) -> List[str]:
    table_rows = []

    # header row
    table_rows.append(f'| {" | ".join(headers)} |\n')

    # separator row
    table_rows.append(custom_separator_row if custom_separator_row else f'| {" | ".join(["---"] * len(headers))} |\n')

    # data rows
    for row_data in data:
        table_rows.append("| " + " | ".join(map(str, row_data)) + " |\n")
    table_rows.append('\n\n')
    return table_rows


def sort_measure_names(measure_names: List[str]) -> List[str]:
    measures_with_numbers_in_name = []
    for measure_name in measure_names:
        match = re.match(measure_id_pattern, measure_name)
        if match:
            measures_with_numbers_in_name.append(f'{match.group("measure_id")}---{measure_name}')
    sorted_measures_with_numbers_in_name = [m.split('---')[1] for m in sorted(measures_with_numbers_in_name, key=lambda x: int(x.split('---')[0]))]
    return sorted_measures_with_numbers_in_name + \
        [m for m in sorted([m for m in measure_names if m not in sorted_measures_with_numbers_in_name])]

def sort_populations(populations: List[str]) -> List[str]:
    order = {
        'initial population': 1,
        'denominator': 2,
        'denominator exclusion': 3,
        'denominator exception': 4,
        'numerator': 5,
        'numerator exclusion': 6}
    return sorted(populations, key=lambda x: order[x.lower()] if x.lower() in order else 99)

def sort_by_test_case(patient_guid: str, group: str) -> Tuple[str, str]:
    """Alphabetical sort key for a test case, ordered by patient GUID then group."""
    return (patient_guid.casefold(), group)

def sort_result_keys(result_keys: List[ResultKey]) -> List[ResultKey]:
    """Sort Missing Results alphabetically by test case."""
    return sorted(result_keys, key=lambda r: sort_by_test_case(r.patient_guid, r.group))

def sort_missing_populations(missing_populations: List[MissingPopulation]) -> List[MissingPopulation]:
    """Sort Missing Populations alphabetically by test case."""
    return sorted(missing_populations,
                  key=lambda mp: sort_by_test_case(mp.result_key.patient_guid, mp.result_key.group))

def sort_mismatched_test_cases(mismatched_test_cases: Dict[TestCaseGroupId, Dict[str, Comparison]]) -> List[Tuple[TestCaseGroupId, Dict[str, Comparison]]]:
    """Sort Mismatched Test Cases alphabetically by test case."""
    return sorted(mismatched_test_cases.items(),
                  key=lambda kv: sort_by_test_case(kv[0].patient_guid, kv[0].group))

def cql_file_link(measure_name: str, custom_id: str = None) -> str:
    return f'[ {custom_id} ](../../input/cql/{measure_name}.cql)' if custom_id else f'[ {measure_name} ](../../input/cql/{measure_name}.cql)'

def measure_report_file_link(measure_name: str, patient_guid: str) -> str:
    # path relative to root directory, this is the expected location for running the script
    measure_dir = f'./input/tests/measure/{measure_name}/{patient_guid}/'
    measure_report_file = glob.glob(f'{measure_dir}/MeasureReport*.json')
    if measure_report_file:
        # path relative to this script, need to add parent directories
        return f'[ {patient_guid} ](../../{measure_report_file[0]})'
    else:
        return patient_guid

def test_results_file_link(measure_name: str, custom_id: str = None) -> str:
    return f'[ {custom_id} ](../../input/tests/results/{measure_name}.txt)' if custom_id else f'[ {measure_name} ](../../input/tests/results/{measure_name}.txt)'


NO_DISCREPANCY_TABLE_COLUMNS = 3


def render_no_discrepancy_section(non_discrepancy_measures: List[str],
                                  total_measures: int) -> List[str]:
    """`## Measures with No Discrepancies`: the clean measures, several per row.

    As a bullet list this was the tallest thing in the report: 52 measures, one
    ~200-character line each, because the measure name appears once as text and
    again inside both link paths. A 3-column grid cuts that by two thirds.

    Column-major, not row-major: alphabetical order runs *down* each column, so
    the list stays scannable like a phone book and the short final column stays
    the tail of the alphabet rather than an interleaved jump. Only the last
    column is padded.

    The measure name doubles as the CQL link label here (rather than the short
    `[cql]` used in the per-measure sections below, where the `####` heading has
    already named the measure). In a grid a bare `[cql]` identifies nothing.
    """
    if not non_discrepancy_measures:
        return []

    def cell(measure: str) -> str:
        return f'{cql_file_link(measure)} {test_results_file_link(measure, "test")}'

    column_count = NO_DISCREPANCY_TABLE_COLUMNS
    rows_needed = -(-len(non_discrepancy_measures) // column_count)
    columns = [non_discrepancy_measures[row * rows_needed:(row + 1) * rows_needed]
               for row in range(column_count)]

    lines = [f'## Measures with No Discrepancies '
             f'({len(non_discrepancy_measures)} of {total_measures})\n',
             '\n']
    lines.extend(create_markdown_table(
        [''] * column_count,
        [[cell(column[row]) if row < len(column) else ''
          for column in columns]
         for row in range(rows_needed)]))
    return lines


def capture_discrepancies_by_measure(expected_results: Dict[ResultKey, Dict[str, str]],
                                     actual_results: Dict[ResultKey, Dict[str, str]],
                                     resolved_keys: Set[ResultKey] = frozenset()) -> Dict[str, MeasureDiscrepancy]:
    """Per-measure discrepancies, for measures that have any.

    ``resolved_keys`` still count toward ``all_test_cases`` but are not
    discrepancies. The report passes none, so a resolved failure stays listed,
    labelled with its issue.
    """
    def has_discrepancy(discrepancy: MeasureDiscrepancy) -> bool:
        return discrepancy.missing_populations or \
           discrepancy.missing_results or \
           discrepancy.mismatched_test_cases

    discrepancies = {}
    for expected_results_key, expected_populations in expected_results.items():
        measure_discrepancy = discrepancies.setdefault(expected_results_key.measure_name, MeasureDiscrepancy())
        measure_discrepancy.all_test_cases.append(expected_results_key.patient_guid)
        if expected_results_key in resolved_keys:
            continue
        if expected_results_key not in actual_results:
            measure_discrepancy.missing_results.append(expected_results_key)
        else:
            actual_populations = actual_results[expected_results_key]
            # confirm all expected populations exist
            population_delta = list(set(expected_populations.keys()) - set(actual_populations.keys()))
            if population_delta:
                measure_discrepancy.missing_populations.append(MissingPopulation(expected_results_key, population_delta))
            else:
                mismatched_populations = { population: Comparison(expected_populations[population], actual_populations[population])
                     for population in expected_populations.keys() & actual_populations.keys() if expected_populations[population] != actual_populations[population]}
                if mismatched_populations:
                    measure_discrepancy.mismatched_test_cases[TestCaseGroupId(expected_results_key.patient_guid, expected_results_key.group)] = mismatched_populations
    return {measure: discrepancies[measure] for measure in sort_measure_names([k for k,v in discrepancies.items() if has_discrepancy(v)])}


def ticket_link(ticket: str) -> str:
    """`[owner/repo#123](url)` for a GitHub ticket URL; blank stays blank."""
    if not ticket:
        return ''
    owner, repo, _, number = ticket.split('/')[3:7]
    return f'[{owner}/{repo}#{number}]({ticket})'


def sort_issue_ids(issue_ids) -> List[str]:
    """`I-9` before `I-26`: numeric order, not string order."""
    def key(issue_id: str):
        match = re.search(r'\d+', issue_id)
        return (int(match.group()) if match else float('inf'), issue_id)
    return sorted(issue_ids, key=key)


def covering_issue(key: ResultKey,
                   implementation_issues: Sequence[ImplementationIssue],
                   known_issues: Sequence[KnownIssue]):
    """(issue, status) for a failing group, or (None, None) when no issue covers it.

    ``status`` is ``implementation``, ``ticketed`` or ``needs ticket``; the first
    two resolve the group. An implementation issue takes precedence over a known
    issue, as in ``cell_outcome``.
    """
    issue = implementation_issue_for(key, implementation_issues)
    if issue is not None:
        return issue, "implementation"
    issue = implementation_issue_for(key, known_issues)
    if issue is not None:
        return issue, "ticketed" if issue.ticketed else "needs ticket"
    return None, None


def render_measure_issue_notes(keys: Sequence[ResultKey],
                               implementation_issues: Sequence[ImplementationIssue],
                               known_issues: Sequence[KnownIssue]) -> List[str]:
    """The `Issues` list under a measure's tables: one bullet per issue it cites.

    Each bullet gives the issue's class, its status (implementation, the ticket,
    or **needs ticket**) and its note. Notes are written per test case, so an
    issue whose cases in this measure carry different notes gets one sub-bullet
    per note, naming the test cases (by GUID prefix) it applies to.
    """
    by_issue: Dict[str, Tuple[object, str, Dict[str, List[str]]]] = {}
    for key in sort_result_keys(list(keys)):
        issue, status = covering_issue(key, implementation_issues, known_issues)
        if issue is None:
            continue
        _, _, notes = by_issue.setdefault(issue.issue_id, (issue, status, {}))
        guids = notes.setdefault(issue.note, [])
        if key.patient_guid not in guids:
            guids.append(key.patient_guid)
    if not by_issue:
        return []

    lines = ['Issues\n', '\n']
    for issue_id in sort_issue_ids(by_issue):
        issue, status, notes = by_issue[issue_id]
        if status == "implementation":
            detail = '`implementation`'
        else:
            detail = (f'`{issue.issue_class}`, '
                      f'{ticket_link(issue.ticket) if issue.ticketed else "**needs ticket**"}')
        bullet = f'- **{issue_id}** ({detail})'
        if len(notes) == 1:
            note = next(iter(notes))
            lines.append(f'{bullet}: {note}\n' if note else f'{bullet}\n')
        else:
            lines.append(f'{bullet}\n')
            for note, guids in notes.items():
                prefixes = ', '.join(f'`{g[:8]}`' for g in guids)
                lines.append(f'  - {prefixes}: {note or "no note"}\n')
    lines.append('\n')
    return lines


def generate_comparison_report(file: str,
                               expected_results: Dict[ResultKey, Dict[str, str]],
                               actual_results: Dict[ResultKey, Dict[str, str]],
                               pass_count: int,
                               fail_count: int,
                               unscored_cells: List[UnscoredCell] = None,
                               cqfm_exclusions: Dict[str, FrozenSet[str]] = None,
                               implementation_issues: Sequence[ImplementationIssue] = (),
                               implementation_file: str = DEFAULT_IMPLEMENTATION_ISSUES_FILE,
                               implementation_count: int = 0,
                               known_issues: Sequence[KnownIssue] = (),
                               known_issues_file: str = DEFAULT_KNOWN_ISSUES_FILE,
                               known_issue_count: int = 0):
    """Write ``discrepancy_report.md``.

    ``pass_count`` / ``fail_count`` / ``implementation_count`` /
    ``known_issue_count`` are test-case counts from ``scores``,
    ``implementation_case_count`` and ``known_issue_case_count``; the four
    together are the total.
    """
    resolved_keys = failing_implementation_keys(expected_results, actual_results,
                                                implementation_issues)
    ticketed_keys = failing_known_issue_keys(expected_results, actual_results,
                                             known_issues, implementation_issues)
    awaiting_keys = failing_known_issue_keys(expected_results, actual_results,
                                             known_issues, implementation_issues,
                                             ticketed=False)
    # Every mismatching group, whatever its issue state. Resolved ones stay in
    # the tables, labelled with the issue that resolves them.
    discrepancies = capture_discrepancies_by_measure(expected_results, actual_results)
    resolved = resolved_keys | ticketed_keys
    covered = resolved | awaiting_keys
    awaiting_case_count = len({(k.measure_name, k.patient_guid) for k in awaiting_keys})

    def discrepancy_keys(measure: str, discrepancy: MeasureDiscrepancy) -> List[ResultKey]:
        return (list(discrepancy.missing_results)
                + [mp.result_key for mp in discrepancy.missing_populations]
                + [ResultKey(measure, t.patient_guid, t.group)
                   for t in discrepancy.mismatched_test_cases])

    measure_keys = {measure: discrepancy_keys(measure, d) for measure, d in discrepancies.items()}
    # Unattributed: test cases with a group no implementation issue or ticketed
    # known issue resolves -- the FAIL verdict.
    unattributed_cases = {measure: {k.patient_guid for k in keys if k not in resolved}
                          for measure, keys in measure_keys.items()}

    def issue_cell(key: ResultKey) -> str:
        """`I-73 (implementation)`, `I-26 (ticketed)`, `I-65 (needs ticket)`, or blank."""
        issue, status = covering_issue(key, implementation_issues, known_issues)
        return f'{issue.issue_id} ({status})' if issue is not None else ''

    total_cases = pass_count + fail_count + implementation_count + known_issue_count

    with open(file, "w", newline="") as f:
        f.write('# Discrepancy Report\n')
        total_measures = len(set([k.measure_name for k in expected_results.keys()]))
        def count_with_pct(count: int) -> str:
            return f'{count} ({count / total_cases * 100:.2f}%)' if total_cases else '0'

        detail_rows = [
            ['Generated', datetime.now()],
            ['Total Measures', total_measures],
            ['Total Test Cases', total_cases],
            ['Measures with Discrepancies', len(discrepancies)],
            ['Pass Count', count_with_pct(pass_count)],
        ]
        if implementation_count:
            detail_rows.append(['Implementation Issue Count', count_with_pct(implementation_count)])
        if known_issue_count:
            detail_rows.append(['Known Issue Count (ticketed)', count_with_pct(known_issue_count)])
        detail_rows.append(['Fail Count', count_with_pct(fail_count)])
        if awaiting_case_count:
            detail_rows.append(['&nbsp;&nbsp;— known issue, needs ticket',
                                count_with_pct(awaiting_case_count)])
        resolved_parts = [name for name, count in (('Implementation', implementation_count),
                                                    ('Known Issue', known_issue_count))
                          if count]
        if resolved_parts:
            detail_rows.append([f'Resolved (Pass + {" + ".join(resolved_parts)})',
                                count_with_pct(pass_count + implementation_count
                                               + known_issue_count)])
        f.writelines(create_markdown_table(['Details', 'Value'], detail_rows))
        f.writelines(create_markdown_table(
            ['Discrepancy Summary', 'Measure Count', 'Test Case Count'],
            [
                [
                    'Missing Results',
                    len(set([m for m, d in discrepancies.items() if d.missing_results])),
                    sum([len(d.missing_results) for d in discrepancies.values()])
                ],
                [
                    'Missing Populations',
                    len(set([m for m, d in discrepancies.items() if d.missing_populations])),
                    sum([len(d.missing_populations) for d in discrepancies.values()])
                ],
                [
                    'Mismatched Test Cases',
                    len(set([m for m, d in discrepancies.items() if d.mismatched_test_cases])),
                    sum([len(d.mismatched_test_cases.keys()) for d in discrepancies.values()])
                ],
                [
                    'Unattributed Discrepancies',
                    sum(1 for cases in unattributed_cases.values() if cases),
                    sum(len(cases) for cases in unattributed_cases.values())
                ]
            ],
            '|---|:---:|:---:|\n'))
        f.write('\n')
        f.write('_Note: Measures can have multiple discrepancies, so the Measures with '
                'Discrepancies count may not match the summary counts. Unattributed '
                'Discrepancies are those no implementation issue or ticketed known issue '
                'resolves._\n')
        f.write('\n')

        f.writelines(render_cqfm_exclusions_section(cqfm_exclusions or {},
                                                    unscored_cells or []))

        non_discrepancy_measures = [
            m for m in sort_measure_names(list(set([k.measure_name for k in expected_results.keys()])))
            if m not in discrepancies]
        f.writelines(render_no_discrepancy_section(
            non_discrepancy_measures, total_measures))

        if discrepancies:
            f.write(f'## Measures with Discrepancies '
                    f'({len(discrepancies)} of {total_measures})\n')
            f.write('\n')
            f.write('_Every measure with a test case that doesn\'t match its fixture '
                    'MeasureReport. The `Issue` column names the issue that explains a '
                    'test case: `implementation` (the measure steward\'s test data or '
                    'expectation, see `defect-tracking/known-issues.md`) and `ticketed` '
                    'resolve it, the same as a pass; `needs ticket` does not yet. A test '
                    'case nothing resolves is counted as Unattributed._\n')
            f.write('\n')
            summary_headers = ['Measure', 'Total Test Cases', 'Missing Results',
                               'Missing Populations', 'Mismatched Test Cases', 'Unattributed']
            summary_separator = '|---|:---:|:---:|:---:|:---:|:---:|\n'
            summary_rows = []
            for measure, discrepancy in discrepancies.items():
                row = [
                    f'[{measure}](#{measure.lower()})',
                    len(discrepancy.all_test_cases),
                    len(discrepancy.missing_results),
                    len(discrepancy.missing_populations),
                    f'{len(discrepancy.mismatched_test_cases)/len(discrepancy.all_test_cases)*100:.2f}%   '
                    f'({len(discrepancy.mismatched_test_cases)})',
                    len(unattributed_cases[measure])
                ]
                summary_rows.append(row)
            f.writelines(create_markdown_table(summary_headers, summary_rows,
                                               summary_separator))
            f.write('\n')

            for measure, discrepancy in discrepancies.items():
                f.write(f'#### {measure}\n')
                f.write(f'{cql_file_link(measure, "[cql]")} '
                        f'{test_results_file_link(measure, "[test results]")}\n\n')
                # The column appears only on measures with a row an issue covers.
                show_issue = any(k in covered for k in measure_keys[measure])

                if discrepancy.missing_results:
                    f.write(f'Missing Results ({len(discrepancy.missing_results)} of '
                            f'{len(discrepancy.all_test_cases)} test cases)\n')
                    missing_results_headers = ['Test Case', 'Group']
                    if show_issue:
                        missing_results_headers.append('Issue')
                    missing_results_rows = []
                    for missing_key in sort_result_keys(list(discrepancy.missing_results)):
                        row = [measure_report_file_link(missing_key.measure_name,
                                                        missing_key.patient_guid),
                               missing_key.group]
                        if show_issue:
                            row.append(issue_cell(missing_key))
                        missing_results_rows.append(row)
                    f.writelines(create_markdown_table(
                        missing_results_headers, missing_results_rows))

                if discrepancy.missing_populations:
                    f.write(f'Missing Populations ({len(discrepancy.missing_populations)} of '
                            f'{len(discrepancy.all_test_cases)} test cases)\n')
                    f.writelines(create_markdown_table(
                        ['Test Case', 'Group', 'Population']
                        + (['Issue'] if show_issue else []),
                        [[
                            measure_report_file_link(missing_id.measure_name, missing_id.patient_guid),
                            missing_id.group,
                            ','.join(populations)
                        ] + ([issue_cell(missing_id)] if show_issue else [])
                            for (missing_id, populations) in sort_missing_populations(
                            list(discrepancy.missing_populations))]))

                if discrepancy.mismatched_test_cases:
                    f.write(f'Mismatched Test Cases ({len(discrepancy.mismatched_test_cases)} of '
                            f'{len(discrepancy.all_test_cases)})\n')
                    mismatched_headers = ['Test Case', 'Group', 'Population', 'Expected', 'Actual']
                    mismatched_separator = '|---|---|---|:---:|:---:|\n'
                    if show_issue:
                        mismatched_headers.append('Issue')
                        mismatched_separator = mismatched_separator[:-1] + '---|\n'
                    mismatched_rows = []
                    for test_group_id, populations in sort_mismatched_test_cases(
                            discrepancy.mismatched_test_cases):
                        ordered = sort_populations(populations.keys())
                        row = [
                            measure_report_file_link(measure, test_group_id.patient_guid),
                            test_group_id.group,
                            '<br>'.join(ordered),
                            '<br>'.join([populations[p].expected for p in ordered]),
                            '<br>'.join([populations[p].actual for p in ordered])]
                        if show_issue:
                            row.append(issue_cell(ResultKey(
                                measure, test_group_id.patient_guid, test_group_id.group)))
                        mismatched_rows.append(row)
                    f.writelines(create_markdown_table(
                        mismatched_headers, mismatched_rows, mismatched_separator))

                f.writelines(render_measure_issue_notes(
                    measure_keys[measure], implementation_issues, known_issues))
                f.write('\n')

        f.write('\n_Known issues are listed in '
                '`defect-tracking/known-issues.md`; the test cases they explain, '
                f'and their tickets, in `{known_issues_file}` and '
                f'`{implementation_file}`._\n')


def main(expected_file: str, actual_file: str, output_file: str,
         comparison_report: str, measure_resource_dir: str = None,
         implementation_file: str = DEFAULT_IMPLEMENTATION_ISSUES_FILE,
         known_issues_file: str = DEFAULT_KNOWN_ISSUES_FILE):
    # Exclusions are computed once and applied symmetrically to expected and
    # actual, so a cell is either scored on both sides or neither.
    cqfm_exclusions = load_cqfm_aggregation_exclusions(
        measure_resource_dir or DEFAULT_MEASURE_RESOURCE_DIR)

    expected_results = capture_results(expected_file, cqfm_exclusions)
    actual_results = capture_results(actual_file, cqfm_exclusions)

    implementation_issues = load_implementation_issues(implementation_file)
    print(f"Implementation issues: {len(implementation_issues)} "
          f"from {implementation_file!r}")

    known_issues = load_known_issues(known_issues_file)
    print(f"Known issues: {len(known_issues)} case rows "
          f"({sum(1 for i in known_issues if i.ticketed)} ticketed) "
          f"from {known_issues_file!r}")

    pass_count, fail_count = generate_output(output_file, expected_results.rows,
                                             actual_results.rows, implementation_issues,
                                             known_issues)
    implementation_count = implementation_case_count(
        expected_results.rows, actual_results.rows, implementation_issues, known_issues)
    known_count = known_issue_case_count(
        expected_results.rows, actual_results.rows, implementation_issues, known_issues)
    total = pass_count + fail_count + implementation_count + known_count

    def pct(count: int) -> str:
        return f"{(count / total * 100) if total else 0.0:.2f}%"

    print(f"PASS (test cases): {pass_count} ({pct(pass_count)})")
    if implementation_count:
        print(f"IMPLEMENTATION (test cases): {implementation_count} ({pct(implementation_count)})")
    if known_count:
        print(f"KNOWN_ISSUE (test cases): {known_count} ({pct(known_count)})")
    print(f"FAIL (test cases): {fail_count} ({pct(fail_count)})")

    unscored = list(expected_results.unscored) + list(actual_results.unscored)
    generate_comparison_report(comparison_report, expected_results.groups,
                               actual_results.groups, pass_count, fail_count,
                               unscored_cells=unscored,
                               cqfm_exclusions=cqfm_exclusions,
                               implementation_issues=implementation_issues,
                               implementation_file=implementation_file,
                               implementation_count=implementation_count,
                               known_issues=known_issues,
                               known_issues_file=known_issues_file,
                               known_issue_count=known_count)


def parse_args(argv: List[str]) -> Tuple[str, str, str, str, Optional[str]]:
    """Map `argv` (without the program name) to main()'s arguments.

    Returns (expected, actual, output, report, measure_resource_dir).

    The flag is removed from the positional list, so the two kinds of argument can
    appear in any order. Values pass through verbatim -- no URI conversion, no
    path normalisation -- because Windows and POSIX callers both use this and
    `open()` handles either spelling.
    """
    args = list(argv)
    measure_resource_dir = None

    flag = "--measure-resource-dir"
    if flag in args:
        idx = args.index(flag)
        if idx + 1 < len(args):
            measure_resource_dir = args[idx + 1]
        args = args[:idx] + args[idx + 2:]

    defaults = ["./scripts/comparison/expected_results.csv",
                "./scripts/comparison/actual_results.csv",
                "./scripts/comparison/output_results.csv",
                "./scripts/comparison/discrepancy_report.md"]
    positional = (args + defaults[len(args):])[:4]
    return (positional[0], positional[1], positional[2], positional[3],
            measure_resource_dir)


if __name__ == '__main__':
    (expected_file, actual_file, output_file, comparison_report,
     measure_resource_dir) = parse_args(sys.argv[1:])

    main(expected_file, actual_file, output_file, comparison_report,
         measure_resource_dir=measure_resource_dir)
