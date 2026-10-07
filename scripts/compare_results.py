"""Compare expected measure results against actual CQL engine results.

Reads two CSVs of ``measure_name,guid,population,count`` rows and writes:

  * ``output_results.csv`` -- one PASS/FAIL/MISSING row per population cell
  * ``discrepancy_report.md`` -- per-measure Missing Results, Missing
    Populations, and Mismatched Test Cases

Run it with no arguments from the repository root; see ``scripts/readme.md``.

Optionally reads a *third* CSV, ``scripts/comparison/qicore_actual_results.csv``
(a copy of the QI-Core project's ``actual_results.csv``, same schema), to answer
the question the two-input report cannot: when a test case fails here, does it
also fail on QI-Core? A FAIL on both sides is parity; a FAIL that QI-Core passes
is a regression introduced by the QI-Core -> USQualityCore migration. Per failing
test case that status becomes extra columns; a `## QI-Core Parity` section scores
*every* test case both ways, so the report also covers the reverse case CMS
passes and QI-Core fails, and totals how far apart the two engines are. If the
file is absent the report renders exactly as it does without it.

Three behaviours are worth knowing about, because each exists to stop the report
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
"""
import csv
import glob
import os
import re
import sys
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, FrozenSet, List, NamedTuple, Optional, Set, Tuple, TypedDict

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

# Optional third input: a copy of the QI-Core project's actual_results.csv. See
# the module docstring for what it adds. Absent is fine -- see main().
DEFAULT_QICORE_ACTUAL_FILE = "./scripts/comparison/qicore_actual_results.csv"

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
    vs "Measure Observation" in both engines' actuals. Canonicalising here, at
    the single read point, keeps every downstream key consistent.

    Cells whose population is not a scored population are collected in
    ``unscored`` instead of being silently dropped, so the report can say what
    it did not measure.

    ``cqfm_exclusions`` (``populations.load_cqfm_aggregation_exclusions()``)
    are cells that require per-member CQFM aggregation this harness cannot
    perform (see defect-tracking/known-issues.md, I-62) -- these are also routed to
    ``unscored`` (with ``reason="cqfm-aggregation-excluded"``) rather than
    ``rows``/``results``. Passing the *same* exclusion set into every
    ``capture_results`` call for a run (expected, actual, qicore-actual) is
    what keeps the exclusion symmetric: since the cell never enters
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


# ---------------------------------------------------------------------------
# QI-Core parity: this project's engine output vs the QI-Core engine output.
#
# `scripts/comparison/qicore_actual_results.csv` is a copy of the QI-Core
# project's `scripts/comparison/actual_results.csv`. Scoring it against the
# *same* fixture expectations this report already uses answers whether a failing
# test case is a parity gap (fails on both engines) or a migration regression
# (passes on QI-Core). Both engines' disagreements on any cell, passing or
# failing, are visible in `output_results.csv` alongside the expected/actual
# pair; this section adds only the per-case verdict.
#
# Rendered per ResultKey -- the same granularity as the report's Missing Results,
# Missing Populations, and Mismatched Test Cases tables -- so a cell-level
# comparison is never summarised into a misleading case-level PASS.
# ---------------------------------------------------------------------------

def side_status(expected_pops: Dict[str, str],
                side_pops: Optional[Dict[str, str]]) -> str:
    """One engine's verdict on one test case: PASS, FAIL, or MISSING.

    ``expected_pops`` and ``side_pops`` are the per-population count maps for
    one (measure, guid, group), as produced by ``capture_results(...).groups``.
    Deliberately side-agnostic: the UQC engine and QI-Core are scored by this
    one function, so the parity section and the UQC verdict columns can never
    disagree about the same case.

    * ``MISSING`` -- this engine produced no result for this test case at all, so
      there is nothing to compare. Not the same as PASS: it is the absence of
      evidence, and is reported separately from a confirmed FAIL.
    * ``PASS`` -- every expected population equals this engine's value. When the
      UQC engine fails such a case, the failure is a regression, not a parity
      gap.
    * ``FAIL`` -- otherwise. A population this engine lacks counts as a mismatch,
      because a measure that emits no row for a scored population has not
      reproduced the expectation any more than a wrong value has.

    Comparison is string-equality on the raw counts, matching ``row_outcome`` so
    that the two engines' verdicts are decided by the same rule.
    """
    if side_pops is None:
        return "MISSING"
    for population, expected in expected_pops.items():
        side_value = side_pops.get(population)
        if side_value is None or str(expected) != str(side_value):
            return "FAIL"
    return "PASS"


def qicore_status(expected_pops: Dict[str, str],
                  qicore_pops: Optional[Dict[str, str]]) -> str:
    """QI-Core's verdict on one test case. See ``side_status``, which decides it."""
    return side_status(expected_pops, qicore_pops)


VERDICTS = ("PASS", "FAIL", "MISSING")


def plural(count: int, singular: str, suffix: str = "s") -> str:
    """`1 test case` / `2 test cases` -- the report states these counts as prose."""
    return f"{count} {singular}{'' if count == 1 else suffix}"


class QiCoreParity(NamedTuple):
    """Whole-corpus UQC-vs-QI-Core comparison, for the `## QI-Core Parity` section.

    **Unit of account: a test case is a ``ResultKey``**, i.e. one
    ``(measure, guid, group)`` triple -- the same unit the per-measure tables
    count. The report header's ``Total Test Cases`` is a *different* unit, one
    per ``(measure, guid)``, so on measures with several groups per patient the
    cross-tab here totals more than that header does. ``cross_tab`` sums to the
    number of expected ``ResultKey``s; the renderer states both figures so the
    two can be reconciled rather than silently compared.

    ``cross_tab`` maps ``(cms_verdict, qicore_verdict)`` to the number of test
    cases, over every ``ResultKey`` in the expected results.

    The two asymmetric buckets, each keyed by measure:

    * ``cms_only_failures`` -- CMS fails, QI-Core reproduces the fixture. The
      migration is implicated and the fix is owed here. Also summarised per
      measure by the ``CMS Fail / QI-Core OK`` column.
    * ``qicore_only_failures`` -- CMS reproduces the fixture, QI-Core does not.
      Invisible in a report driven by UQC failures, and not a CMS defect; on
      every such case in this corpus the two repositories' fixture expectations
      agree and only the engine output differs (catalogued as I-01). Computed for
      a complete cross-product, but note the report renders it nowhere: its size
      shows up only as the cross-tab's ``PASS``/``FAIL`` and ``PASS``/``MISSING``
      cells.

    ``*_cells`` counts just those buckets' disagreeing population cells.

    ``disagreeing_cases`` / ``disagreeing_cells`` / ``total_cells`` count the raw
    engine-to-engine disagreement, which is a *different and larger* number than
    either asymmetric bucket: two engines can disagree about a case both of them
    get wrong the same way.
    """
    cross_tab: Dict[Tuple[str, str], int]
    cms_only_failures: Dict[str, List[ResultKey]]
    cms_only_failure_cells: Dict[str, int]
    qicore_only_failures: Dict[str, List[ResultKey]]
    qicore_only_failure_cells: Dict[str, int]
    disagreeing_cases: Set[ResultKey]
    disagreeing_cells: int
    total_cells: int

    @property
    def disagreeing_case_count(self) -> int:
        return len(self.disagreeing_cases)

    @property
    def cms_only_failure_case_count(self) -> int:
        return sum(len(keys) for keys in self.cms_only_failures.values())

    @property
    def cms_only_failure_measure_count(self) -> int:
        return len(self.cms_only_failures)

    @property
    def qicore_only_failure_case_count(self) -> int:
        return sum(len(keys) for keys in self.qicore_only_failures.values())

    @property
    def qicore_only_failure_measure_count(self) -> int:
        return len(self.qicore_only_failures)


def qi_core_parity(expected_results: Dict[ResultKey, Dict[str, str]],
                   actual_results: Dict[ResultKey, Dict[str, str]],
                   qicore_results: Dict[ResultKey, Dict[str, str]]) -> QiCoreParity:
    """Compare the two engines against the expectations and against each other.

    Both verdicts come from ``side_status``, so they are decided by the same rule
    as the report's existing UQC columns. A cell counts as a disagreement only if
    the two engines' values differ on a population the fixture actually authored
    -- deliberately not a dict-equality test, because CMS and QI-Core emit
    different *extra* populations on some measures, and scoring those would
    inflate the count with cells nobody wrote an expectation for.
    """
    cross_tab: Dict[Tuple[str, str], int] = {}
    cms_only: Dict[str, List[ResultKey]] = {}
    cms_only_cells: Dict[str, int] = {}
    qicore_only: Dict[str, List[ResultKey]] = {}
    qicore_only_cells: Dict[str, int] = {}
    disagreeing_cases: Set[ResultKey] = set()
    disagreeing_cells = 0
    total_cells = 0

    for key, expected_pops in expected_results.items():
        cms_pops = actual_results.get(key)
        qicore_pops = qicore_results.get(key)
        cms = side_status(expected_pops, cms_pops)
        qicore = side_status(expected_pops, qicore_pops)
        cross_tab[(cms, qicore)] = cross_tab.get((cms, qicore), 0) + 1

        differing = [p for p, expected in expected_pops.items()
                     if (cms_pops or {}).get(p) != (qicore_pops or {}).get(p)]
        total_cells += len(expected_pops)
        if differing:
            disagreeing_cases.add(key)
            disagreeing_cells += len(differing)

        # PASS is a strict verdict, so `!= "PASS"` folds MISSING in: an engine
        # that emitted nothing has not reproduced the fixture either.
        if cms != "PASS" and qicore == "PASS":
            cms_only.setdefault(key.measure_name, []).append(key)
            cms_only_cells[key.measure_name] = \
                cms_only_cells.get(key.measure_name, 0) + len(differing)
        elif cms == "PASS" and qicore != "PASS":
            qicore_only.setdefault(key.measure_name, []).append(key)
            qicore_only_cells[key.measure_name] = \
                qicore_only_cells.get(key.measure_name, 0) + len(differing)

    return QiCoreParity(cross_tab, cms_only, cms_only_cells,
                        qicore_only, qicore_only_cells,
                        disagreeing_cases, disagreeing_cells, total_cells)


def render_qi_core_parity_section(parity: QiCoreParity,
                                  qicore_file: str,
                                  patient_case_count: int,
                                  anchorable_measures: Set[str] = frozenset()) -> List[str]:
    """`## QI-Core Parity`: the whole-corpus UQC-vs-QI-Core view.

    The per-failing-case QI-Core columns cannot answer "how far apart are these
    two engines", because they only describe cases UQC already fails. This section
    scores every test case both ways, giving the verdict cross-tab, the total
    disagreement, and a per-measure / per-test-case breakdown of the one bucket
    that is actionable here: a summary table, then one nested bullet per GUID
    (linked to its fixture MeasureReport) listing that GUID's failing groups.

    The reverse bucket -- cases CMS passes and QI-Core does not -- is computed
    but **not rendered anywhere**: no header row, no section, no per-measure
    table. It is not CMS work, and on every such case in this corpus the two
    repositories' fixture expectations agree and only the engine output differs,
    so it is QI-Core-side staleness (I-01) and a reason to refresh the QI-Core
    snapshot. Its size is still visible in the cross-tab (`PASS`/`FAIL` plus
    `PASS`/`MISSING`); ``qicore_only_failures`` is retained because it makes the
    parity record symmetric rather than pre-filtered, not because something
    currently prints it.

    ``anchorable_measures`` is the set of measures that get their own ``####``
    section further down. Measures that pass UQC never get one, so linking to
    them unconditionally would emit dead anchors; those are listed as plain text
    instead.

    ``patient_case_count`` is the header's ``Total Test Cases``, which counts in
    the other unit (see ``QiCoreParity``). Passed in rather than recomputed
    because the header owns that count and the two are easy to conflate.
    """
    def measure_ref(measure: str) -> str:
        return (f'[{measure}](#{measure.lower()})'
                if measure in anchorable_measures else measure)

    lines: List[str] = []
    lines.append('## QI-Core Parity\n')
    lines.append(
        '_Every test case scored below, including the ones UQC passes. '
        f'QI-Core results come from `{qicore_file}`; verdicts are scored against '
        'the same fixture MeasureReport expectations used for the UQC columns. '
        'A case where the two engines disagree is not by itself evidence that '
        'either is wrong -- read the cross-tab first, then the two asymmetric '
        'buckets._\n')
    lines.append('\n')

    lines.append(
        '_**Unit of account.** A test case here is one '
        '(measure, GUID, group) triple, the same unit the per-measure tables '
        f'use, so these counts total {sum(parity.cross_tab.values())} rather '
        f'than the header\'s {plural(patient_case_count, "test case")} -- that '
        'one counts each patient once per measure. On measures with several '
        'groups per patient the two differ; reconcile by group, not by '
        'totals._\n')
    lines.append('\n')

    lines.append(
        f'CMS and QI-Core disagree on '
        f'{plural(parity.disagreeing_case_count, "test case")} '
        f'({parity.disagreeing_cells} of {parity.total_cells} population cells).\n')
    lines.append('\n')

    lines.append('### Verdicts by Engine\n')
    lines.append('\n')
    lines.extend(create_markdown_table(
        ['CMS verdict \\ QI-Core verdict'] + list(VERDICTS),
        [[cms] + [parity.cross_tab.get((cms, qicore), 0) for qicore in VERDICTS]
         for cms in VERDICTS],
        '|---|:---:|:---:|:---:|\n'))
    lines.append(
        '_The two asymmetric buckets point in opposite directions, and only one '
        'of them is work for this repo. `FAIL`/`PASS` '
        f'({plural(parity.cms_only_failure_case_count, "case")}, '
        f'{plural(parity.cms_only_failure_measure_count, "measure")}) is a '
        'migration regression to fix here; it is broken out per measure and per '
        'test case below, and per measure by the `CMS Fail / QI-Core OK` column '
        'further down. `PASS`/`FAIL` '
        f'({plural(parity.cross_tab.get(("PASS", "FAIL"), 0), "case")}) plus '
        f'`PASS`/`MISSING` '
        f'({plural(parity.cross_tab.get(("PASS", "MISSING"), 0), "case")}) is '
        'the reverse: those cases match their fixture expectations here and not '
        'on QI-Core. Both repositories agree on the expected values for them, so '
        'the difference is engine output rather than content and QI-Core\'s copy '
        'of the results is the stale side -- catalogued as I-01. Counted, not '
        'listed: it is a refresh signal for the QI-Core baseline, not CMS work. '
        '`MISSING` means that engine emitted no result for the case at all and '
        'counts as non-PASS in both directions._\n')
    lines.append('\n')

    lines.append(
        f'### CMS Fails, QI-Core Reproduces '
        f'({plural(parity.cms_only_failure_case_count, "test case")}, '
        f'{plural(parity.cms_only_failure_measure_count, "measure")})\n')
    lines.append('\n')
    if not parity.cms_only_failures:
        lines.append('_No test case falls in this bucket._\n')
        lines.append('\n')
    else:
        lines.append(
            '_These are the actionable cases: the fixture expectations are '
            'reproduced on QI-Core and not here, so the QI-Core -> USQualityCore '
            'migration is implicated. Per test case:_\n')
        lines.append('\n')
        lines.extend(create_markdown_table(
            ['Measure', 'Test Cases', 'Population Cells'],
            [[measure_ref(measure),
              len(parity.cms_only_failures[measure]),
              parity.cms_only_failure_cells.get(measure, 0)]
             for measure in sort_measure_names(list(parity.cms_only_failures))],
            '|---|:---:|:---:|\n'))
        for measure in sort_measure_names(list(parity.cms_only_failures)):
            keys = sort_result_keys(parity.cms_only_failures[measure])
            # One bullet per GUID, its failing groups after it. A multi-group
            # patient would otherwise repeat once per group (CMS347: 79 entries
            # for 24 GUIDs).
            groups_by_guid: Dict[str, List[str]] = {}
            for key in keys:
                groups_by_guid.setdefault(key.patient_guid, []).append(key.group)
            count = (f'{len(keys)}' if len(keys) == len(groups_by_guid) else
                     f'{len(keys)} test cases across {len(groups_by_guid)} GUIDs')
            lines.append(f'- **{measure}** ({count})\n')
            for guid, groups in groups_by_guid.items():
                lines.append(f'  - {measure_report_file_link(measure, guid)} '
                             f'({", ".join(groups)})\n')
    lines.append('\n')
    return lines


def render_qicore_note(qicore_results: Optional[Results], qicore_file: str) -> List[str]:
    """Provenance note printed once, under the discrepancy-measure summary.

    Stated up front because the QI-Core columns are easy to misread: the status
    is scored against this report's fixture expectations, not against the UQC
    engine's output, and MISSING counts toward "also failing".
    """
    if qicore_results is None:
        return []
    return [
        "_QI-Core columns compare against "
        f"`{qicore_file}` (a copy of the QI-Core project's `actual_results.csv`). "
        "Status is scored against the same fixture MeasureReport expectations used "
        "for the UQC columns, not against the UQC engine's output: `PASS` means "
        "QI-Core reproduced every expected population, so a UQC failure on the "
        "same case is a migration regression rather than a parity gap; `FAIL` "
        "means QI-Core is wrong the same way; `MISSING` means QI-Core produced no "
        "result for that case at all. The `QI-Core Also Failing` column counts "
        "`MISSING` as non-PASS -- it is an absence of evidence, not a confirmed "
        "pass -- so read it as \"not reproduced on QI-Core\" rather than "
        "\"confirmed broken on QI-Core\". `CMS Fail / QI-Core OK` is the same "
        "denominator read the other way: of this measure's failing cases, how many "
        "QI-Core reproduces. `0 of m` marks a measure as fully actionable here. "
        "The denominators count failing (test case, group) pairs, matching the "
        "tables below, while `Fail Count` counts each test case once across "
        "groups, so the two totals need not match._\n",
        "\n",
        "_These columns only cover cases UQC already fails, so they cannot "
        "answer \"how far apart are these two engines\". The `## QI-Core Parity` "
        "section scores every test case both ways; note that its total "
        "disagreement count is larger than the sum of the two asymmetric "
        "buckets, because two engines can disagree about a case both of them "
        "get wrong the same way._\n",
        "\n",
    ]


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


def test_case_outcomes(expected_rows: Dict, actual_rows: Dict) -> Dict[Tuple[str, str], str]:
    """Map (measure_name, patient_guid) -> 'PASS' or 'FAIL'.

    A test case passes iff every expected valid population cell for that case
    matches the actual value; it fails once if any expected cell is wrong or
    missing. Counting distinct test cases avoids inflating the counts when a
    single root cause (e.g. a denominator bug) derails several populations of
    the same case.
    """
    outcomes: Dict[Tuple[str, str], str] = {}
    for key, expected_result in expected_rows.items():
        # key fields: [ 'measure_name', 'patient_guid', 'group:population' ]
        if not is_scored(key[2].split(':', 1)[1]):
            continue
        actual_result = actual_rows.get(key)
        result, _ = row_outcome(expected_result, actual_result)
        case_key = (key[0], key[1])
        if result == "PASS":
            outcomes.setdefault(case_key, "PASS")
        else:
            outcomes[case_key] = "FAIL"
    return outcomes


def generate_output(file: str, expected_rows: Dict, actual_rows: Dict) -> Tuple[int, int]:
    """Write one row per expected population cell to ``file``.

    The CSV keeps per-population detail; the returned (pass, fail) counts are
    at test-case granularity (a case counts once even if several of its
    population cells mismatch — they usually share a root cause).
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

        actual_result = actual_rows.get(key)
        result, actual_display = row_outcome(expected_result, actual_result)
        output.append([result, key[0], key[1], key[2], expected_result, actual_display])

    with open(file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(output)

    return scores(expected_rows, actual_rows)


def scores(expected_rows: Dict[str, str], actual_rows: Dict[str, str]) -> Tuple[int, int]:
    """Compute (pass, fail) counting distinct test cases (not population cells)."""
    outcomes = test_case_outcomes(expected_rows, actual_rows)
    pass_count = sum(1 for o in outcomes.values() if o == "PASS")
    fail_count = sum(1 for o in outcomes.values() if o == "FAIL")
    return (pass_count, fail_count)


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


def capture_discrepancies_by_measure(expected_results: Dict[ResultKey, Dict[str, str]], actual_results: Dict[ResultKey, Dict[str, str]]) -> Dict[str, MeasureDiscrepancy]:
    def has_discrepancy(discrepancy: MeasureDiscrepancy) -> bool:
        return discrepancy.missing_populations or \
           discrepancy.missing_results or \
           discrepancy.mismatched_test_cases

    discrepancies = {}
    for expected_results_key, expected_populations in expected_results.items():
        measure_discrepancy = discrepancies.setdefault(expected_results_key.measure_name, MeasureDiscrepancy())
        measure_discrepancy.all_test_cases.append(expected_results_key.patient_guid)
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


def measure_discrepancy_keys(discrepancy: MeasureDiscrepancy,
                             measure_name: str) -> Set[ResultKey]:
    """Every failing (test case, group) of one measure, as ResultKeys.

    The three discrepancy kinds are kept mutually exclusive by
    `capture_discrepancies_by_measure` (a case lands in exactly one of them), so
    this is the measure's full failing set at the granularity the report's
    tables use.
    """
    keys = set(discrepancy.missing_results)
    keys |= {mp.result_key for mp in discrepancy.missing_populations}
    keys |= {ResultKey(measure_name, tg.patient_guid, tg.group)
             for tg in discrepancy.mismatched_test_cases}
    return keys


def qi_core_also_failing_count(failing_keys: Set[ResultKey],
                               expected_results: Dict[ResultKey, Dict[str, str]],
                               qicore_results: Dict[ResultKey, Dict[str, str]]) -> Tuple[int, int]:
    """(n, m) for the summary table: failing cases QI-Core also fails, of all failing.

    `MISSING` counts toward `n` -- QI-Core not reproducing a case is exactly what
    the reader needs to see, whether because it disagrees or because it never
    ran. Returns `(0, 0)` when the measure has no failures.
    """
    total = len(failing_keys)
    also_failing = sum(
        1 for key in failing_keys
        if qicore_status(expected_results.get(key, {}),
                         qicore_results.get(key)) != "PASS")
    return also_failing, total


def generate_comparison_report(file: str,
                               expected_results: Dict[ResultKey, Dict[str, str]],
                               actual_results: Dict[ResultKey, Dict[str, str]],
                               pass_count: int,
                               fail_count: int,
                               unscored_cells: List[UnscoredCell] = None,
                               cqfm_exclusions: Dict[str, FrozenSet[str]] = None,
                               qicore_results: Optional[Results] = None,
                               qicore_file: str = DEFAULT_QICORE_ACTUAL_FILE):
    discrepancies = capture_discrepancies_by_measure(expected_results, actual_results)
    total_cases = pass_count + fail_count
    qicore_groups = qicore_results.groups if qicore_results is not None else None
    parity = qi_core_parity(expected_results, actual_results, qicore_groups) \
        if qicore_groups is not None else None

    with open(file, "w", newline="") as f:
        f.write('# Discrepancy Report\n')
        total_measures = len(set([k.measure_name for k in expected_results.keys()]))
        detail_rows = [
            ['Generated', datetime.now()],
            ['Total Measures', total_measures],
            ['Total Test Cases', total_cases],
            ['Measures with Discrepancies', len(discrepancies)],
            ['Pass Count', f'{pass_count} ({pass_count / total_cases * 100:.2f}%)' if total_cases else '0'],
            ['Fail Count', f'{fail_count} ({fail_count / total_cases * 100:.2f}%)' if total_cases else '0'],
        ]
        if parity is not None:
            # One QI-Core row only. The total disagreement and the reverse
            # bucket are both reported inside `## QI-Core Parity` -- the first in
            # its opening line, the second as the cross-tab's PASS/FAIL and
            # PASS/MISSING cells -- so repeating them here added header noise
            # without adding a number.
            detail_rows.append([
                'CMS Fail / QI-Core OK',
                f'{plural(parity.cms_only_failure_case_count, "test case")} '
                f'({plural(parity.cms_only_failure_measure_count, "measure")})'])
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
                ]
            ],
            '|---|:---:|:---:|\n'))
        f.write('\n')
        f.write('_Note: Measures can have multiple discrepancies, so the Measures with '
                'Discrepancies count may not match the summary counts._\n')
        f.write('\n')

        f.writelines(render_cqfm_exclusions_section(cqfm_exclusions or {},
                                                    unscored_cells or []))

        if parity is not None:
            f.writelines(render_qi_core_parity_section(
                parity, qicore_file, total_cases, set(discrepancies)))

        non_discrepancy_measures = [
            m for m in sort_measure_names(list(set([k.measure_name for k in expected_results.keys()])))
            if m not in discrepancies]
        f.writelines(render_no_discrepancy_section(
            non_discrepancy_measures, total_measures))

        if discrepancies:
            f.write(f'## Measures with Discrepancies '
                    f'({len(discrepancies)} of {total_measures})\n')
            summary_headers = ['Measure', 'Total Test Cases', 'Missing Results',
                               'Missing Populations', 'Mismatched Test Cases']
            summary_separator = '|---|:---:|:---:|:---:|:---:|\n'
            if qicore_groups is not None:
                summary_headers.append('QI-Core Also Failing')
                summary_headers.append('CMS Fail / QI-Core OK')
                summary_separator = '|---|:---:|:---:|:---:|:---:|:---:|:---:|\n'
            summary_rows = []
            for measure, discrepancy in discrepancies.items():
                failing_keys = measure_discrepancy_keys(discrepancy, measure)
                row = [
                    f'[{measure}](#{measure.lower()})',
                    len(discrepancy.all_test_cases),
                    len(discrepancy.missing_results),
                    len(discrepancy.missing_populations),
                    f'{len(discrepancy.mismatched_test_cases)/len(discrepancy.all_test_cases)*100:.2f}%   '
                    f'({len(discrepancy.mismatched_test_cases)})'
                ]
                if qicore_groups is not None:
                    also_failing, failing_total = qi_core_also_failing_count(
                        failing_keys, expected_results, qicore_groups)
                    row.append(f'{also_failing} of {failing_total}')
                    # The other direction: of this measure's failures, how many
                    # QI-Core reproduces? Reading 0 of m here is what marks a
                    # measure as fully actionable in this repo.
                    regressions = sum(
                        1 for key in failing_keys
                        if side_status(expected_results.get(key, {}),
                                       qicore_groups.get(key)) == "PASS")
                    row.append(f'{regressions} of {failing_total}')
                summary_rows.append(row)
            f.writelines(create_markdown_table(summary_headers, summary_rows,
                                               summary_separator))
            f.write('\n')
            f.writelines(render_qicore_note(qicore_results, qicore_file))

            for measure, discrepancy in discrepancies.items():
                f.write(f'#### {measure}\n')
                f.write(f'{cql_file_link(measure, "[cql]")} '
                        f'{test_results_file_link(measure, "[test results]")}\n\n')

                if discrepancy.missing_results:
                    f.write(f'Missing Results ({len(discrepancy.missing_results)} of '
                            f'{len(discrepancy.all_test_cases)} test cases)\n')
                    missing_results_headers = ['Test Case', 'Group']
                    if qicore_groups is not None:
                        missing_results_headers.append('QI-Core')
                    missing_results_rows = []
                    for missing_key in sort_result_keys(list(discrepancy.missing_results)):
                        row = [measure_report_file_link(missing_key.measure_name,
                                                        missing_key.patient_guid),
                               missing_key.group]
                        if qicore_groups is not None:
                            row.append(qicore_status(
                                expected_results.get(missing_key, {}),
                                qicore_groups.get(missing_key)))
                        missing_results_rows.append(row)
                    f.writelines(create_markdown_table(
                        missing_results_headers, missing_results_rows))

                if discrepancy.missing_populations:
                    f.write(f'Missing Populations ({len(discrepancy.missing_populations)} of '
                            f'{len(discrepancy.all_test_cases)} test cases)\n')
                    f.writelines(create_markdown_table(
                        ['Test Case', 'Group', 'Population'],
                        [[
                            measure_report_file_link(missing_id.measure_name, missing_id.patient_guid),
                            missing_id.group,
                            ','.join(populations)
                        ] for (missing_id, populations) in sort_missing_populations(
                            list(discrepancy.missing_populations))]))

                if discrepancy.mismatched_test_cases:
                    f.write(f'Mismatched Test Cases ({len(discrepancy.mismatched_test_cases)} of '
                            f'{len(discrepancy.all_test_cases)})\n')
                    mismatched_headers = ['Test Case', 'Group', 'Population', 'Expected', 'Actual']
                    mismatched_separator = '|---|---|---|:---:|:---:|\n'
                    if qicore_groups is not None:
                        mismatched_headers += ['QI-Core', 'QI-Core Actual']
                        mismatched_separator = '|---|---|---|:---:|:---:|:---:|:---:|\n'
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
                        if qicore_groups is not None:
                            result_key = ResultKey(measure, test_group_id.patient_guid,
                                                   test_group_id.group)
                            row.append(qicore_status(expected_results.get(result_key, {}),
                                                    qicore_groups.get(result_key)))
                            qicore_pops = qicore_groups.get(result_key)
                            # Aligned with the Population/Expected/Actual lists
                            # above, position for position. A population QI-Core
                            # has no row for shows MISSING rather than being
                            # dropped, so the columns stay row-aligned.
                            row.append('<br>'.join(
                                [str(qicore_pops.get(p, 'MISSING')) if qicore_pops is not None
                                 else 'MISSING'
                                 for p in ordered]))
                        mismatched_rows.append(row)
                    f.writelines(create_markdown_table(
                        mismatched_headers, mismatched_rows, mismatched_separator))
                    f.write('\n')

        f.write('\n_Known issues are tracked by hand in '
                '`defect-tracking/known-issues.md`; open judgment calls are in '
                '`defect-tracking/CONNECTATHON-BREADCRUMBS.md`._\n')


def main(expected_file: str, actual_file: str, output_file: str,
         comparison_report: str, measure_resource_dir: str = None,
         qicore_file: str = DEFAULT_QICORE_ACTUAL_FILE):
    # Exclusions are computed once and applied symmetrically to expected,
    # actual, and QI-Core, so a cell is either scored on every side or none.
    cqfm_exclusions = load_cqfm_aggregation_exclusions(
        measure_resource_dir or DEFAULT_MEASURE_RESOURCE_DIR)

    expected_results = capture_results(expected_file, cqfm_exclusions)
    actual_results = capture_results(actual_file, cqfm_exclusions)

    # Optional third input. Absent file is not an error: the report renders
    # exactly as it does without QI-Core, so a QI-Core-less checkout works.
    if qicore_file and os.path.exists(qicore_file):
        qicore_results = capture_results(qicore_file, cqfm_exclusions)
        print(f"QI-Core actual results: {qicore_file}")
    else:
        qicore_results = None
        print(f"QI-Core actual results: none found at {qicore_file!r} "
              "(QI-Core columns omitted)")

    pass_count, fail_count = generate_output(output_file, expected_results.rows,
                                             actual_results.rows)
    total = pass_count + fail_count
    pass_pct = (pass_count / total * 100) if total else 0.0
    print(f"PASS (test cases): {pass_count} ({pass_pct:.2f})%")
    print(f"FAIL (test cases): {fail_count} ({(100 - pass_pct):.2f})%")

    unscored = list(expected_results.unscored) + list(actual_results.unscored)
    # qicore_results.unscored is deliberately not folded in: the CQFM section's
    # "N cells excluded" count describes the scored UQC comparison, and adding a
    # third side's cells would inflate it without changing what was excluded.
    generate_comparison_report(comparison_report, expected_results.groups,
                               actual_results.groups, pass_count, fail_count,
                               unscored_cells=unscored,
                               cqfm_exclusions=cqfm_exclusions,
                               qicore_results=qicore_results,
                               qicore_file=qicore_file)


def parse_args(argv: List[str]) -> Tuple[str, str, str, str, str, str]:
    """Map `argv` (without the program name) to main()'s arguments.

    Returns (expected, actual, output, report, measure_resource_dir, qicore).

    Flags are removed from the positional list, so the two kinds of argument can
    appear in any order. Values pass through verbatim -- no URI conversion, no
    path normalisation -- because Windows and POSIX callers both use this and
    `open()` handles either spelling.
    """
    args = list(argv)
    measure_resource_dir = None
    qicore_file = DEFAULT_QICORE_ACTUAL_FILE

    for flag, default in (("--measure-resource-dir", None),
                          ("--qicore-actual", DEFAULT_QICORE_ACTUAL_FILE)):
        if flag in args:
            idx = args.index(flag)
            if idx + 1 < len(args):
                if flag == "--qicore-actual":
                    qicore_file = args[idx + 1]
                else:
                    measure_resource_dir = args[idx + 1]
            args = args[:idx] + args[idx + 2:]

    defaults = ["./scripts/comparison/expected_results.csv",
                "./scripts/comparison/actual_results.csv",
                "./scripts/comparison/output_results.csv",
                "./scripts/comparison/discrepancy_report.md"]
    positional = (args + defaults[len(args):])[:4]
    return (positional[0], positional[1], positional[2], positional[3],
            measure_resource_dir, qicore_file)


if __name__ == '__main__':
    (expected_file, actual_file, output_file, comparison_report,
     measure_resource_dir, qicore_file) = parse_args(sys.argv[1:])

    main(expected_file, actual_file, output_file, comparison_report,
         measure_resource_dir=measure_resource_dir, qicore_file=qicore_file)
