import os
import csv
import glob
import re
import shutil
import sys
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, NamedTuple, Set, Tuple, TypedDict

_SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _SCRIPTS_DIR)
sys.path.insert(0, os.path.join(_SCRIPTS_DIR, "comparison"))
import attribution
import classification
import clock
import known_issues as known_issues_lib
import render_catalog_issue_details as render_catalog_lib
import run_history
from populations import (
    CANONICAL_POPULATIONS,
    canonical_cell,
    is_scored,
    split_population,
)

measure_id_pattern = r"(?:CMS|CMSFHIR)(?P<measure_id>\d+)"

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


class Results(NamedTuple):
    rows: Dict[str, str]
    groups: Dict[ResultKey, Dict[str, str]]
    unscored: List[UnscoredCell] = []


def capture_results(file: str) -> Results:
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
    """
    rows = {}
    results = {}
    unscored: List[UnscoredCell] = []
    with open(file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['measure_name'].lower().startswith("test"):
                continue

            group, population = split_population(row["population"])
            key = (row["measure_name"], row["guid"], f"{group}:{population}")
            rows[key] = row["count"]

            if population not in CANONICAL_POPULATIONS:
                unscored.append(UnscoredCell(
                    row["measure_name"], row["guid"], row["population"]))
                continue

            result_key = ResultKey(row["measure_name"], row["guid"], group)
            result = results.setdefault(result_key, {})
            result[population] = row["count"]
    return Results(rows, results, unscored)


# ---------------------------------------------------------------------------
# Cross-engine diffs: this project's engine output vs the QI-Core engine output.
# The QI-Core project's `actual_results.csv` is copied in as the source of truth
# for QI-Core measures; comparing it against this project's own `actual_results`
# surfaces every population where the two engines disagree.
# ---------------------------------------------------------------------------
EngineDiffClass = namedtuple('EngineDiffClass', ['label', 'detail'])

def diff_actual_results(cms_rows: Dict, qicore_rows: Dict) -> Dict[str, "classification.PresenceBucket"]:
    """Compare two engines' per-case actual results.

    ``cms_rows`` / ``qicore_rows`` are the ``Results.rows`` dicts from
    ``capture_results``, keyed ``(measure_name, patient_guid, population)`` where
    population is the full ``Group_N:PopulationType`` string.

    Expected results are not consulted -- this is a two-way presence/value
    comparison of the two engines' raw output. See classification.py for the
    full vocabulary; in short, ``missing_in_cms`` is a row QI-Core produced that
    CMS did not, and ``missing_in_qicore`` is the reverse (these were previously
    named "qicore_only"/"cms_only", which collided with an unrelated,
    expected-relative vocabulary used elsewhere -- see classification.py).

    Returns ``{measure: PresenceBucket}`` (a NamedTuple with ``.mismatch``,
    ``.missing_in_cms``, ``.missing_in_qicore``, ``.match``).
    """
    return classification.classify_presence(cms_rows, qicore_rows)


def render_engine_diff_section(engine_diff: Dict[str, Dict]) -> List[str]:
    """Render the Engine Diff (CMS vs QI-Core) markdown section.

    Ordered by measure (numeric), listing per-measure mismatch/cms-only/qicore-only
    tallies and the individual differing population rows.
    """
    if not engine_diff:
        return []

    def row_total(m):
        d = engine_diff[m]
        return len(d.mismatch) + len(d.missing_in_qicore) + len(d.missing_in_cms)

    non_empty = [m for m in engine_diff if row_total(m) > 0]
    out = ["## Engine Diff: CMS vs QI-Core (qicore-2025)\n",
           "\n"]
    out.append("_Where the CMS engine's actual results differ from the QI-Core engine's "
               "(source of truth) on the same test case and population. \"Missing in CMS\" "
               "rows are populations the QI-Core engine produced that CMS did not; "
               "\"Missing in QI-Core\" is the reverse._\n")
    out.append("\n")
    out.append(f"| Measure | Mismatch | Missing in CMS | Missing in QI-Core |\n")
    out.append("| --- | ---: | ---: | ---: |\n")
    total_mm = sum(len(engine_diff[m].mismatch) for m in engine_diff)
    total_missing_cms = sum(len(engine_diff[m].missing_in_cms) for m in engine_diff)
    total_missing_qi = sum(len(engine_diff[m].missing_in_qicore) for m in engine_diff)
    for measure in sort_measure_names(non_empty):
        d = engine_diff[measure]
        out.append(f"| {measure} | {len(d.mismatch)} | {len(d.missing_in_cms)} | {len(d.missing_in_qicore)} |\n")
    out.append(f"\n")
    out.append(f"| **Total** | **{total_mm}** | **{total_missing_cms}** | **{total_missing_qi}** |\n")
    out.append("\n")

    def population_label(pop_key):
        group, _, pop = pop_key.partition(":")
        return pop if pop else pop_key

    for measure in sort_measure_names(non_empty):
        d = engine_diff[measure]
        out.append(f"### {measure}\n\n")
        rows = []
        for key, cms_cnt, qi_cnt in sorted(d.mismatch, key=lambda t: sort_by_test_case(t[0][1], t[0][2])):
            _m, guid, pop = key
            rows.append([guid, population_label(pop), cms_cnt, qi_cnt, classification.MISMATCH])
        for key in sorted(d.missing_in_cms, key=lambda t: sort_by_test_case(t[1], t[2])):
            _m, guid, pop = key
            rows.append([guid, population_label(pop), "—", "—", classification.MISSING_IN_CMS])
        for key in sorted(d.missing_in_qicore, key=lambda t: sort_by_test_case(t[1], t[2])):
            _m, guid, pop = key
            rows.append([guid, population_label(pop), "—", "—", classification.MISSING_IN_QICORE])
        if rows:
            out.append("| Test Case | Population | CMS Actual | QI-Core Actual | Type |\n")
            out.append("|---|---|---:|---:|---|\n")
            for guid, pop, cms_c, qi_c, kind in rows:
                out.append(f"| {guid} | {pop} | {cms_c} | {qi_c} | {kind} |\n")
            out.append("\n")
    return out


def write_engine_diff_csv(engine_diff: Dict[str, Dict], out_path: str) -> None:
    """Write the engine diff to a CSV for machine consumption.

    Emits one row per differing population. ``measure_name,guid,population`` are
    the cross-repo key; ``cms_count``/``qicore_count`` are the two engines' actuals
    (empty for rows present in only one); ``diff_type`` is one of
    mismatch|missing-in-cms|missing-in-qicore (see classification.py -- these
    were previously "cms-only"/"qicore-only", renamed to stop colliding with the
    unrelated, expected-relative vocabulary used by engine_shared_issues.py).
    """
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["measure_name", "guid", "population", "cms_count", "qicore_count", "diff_type"])
        for measure in sorted(engine_diff):
            d = engine_diff[measure]
            for key, cms_cnt, qi_cnt in d.mismatch:
                _m, guid, pop = key
                writer.writerow([measure, guid, pop, cms_cnt, qi_cnt, classification.MISMATCH])
            for key in d.missing_in_cms:
                _m, guid, pop = key
                writer.writerow([measure, guid, pop, "", "", classification.MISSING_IN_CMS])
            for key in d.missing_in_qicore:
                _m, guid, pop = key
                writer.writerow([measure, guid, pop, "", "", classification.MISSING_IN_QICORE])


def row_outcome(expected_result: str, actual_result: str) -> Tuple[str, str]:
    """Return (result, actual_display) for a single expected/actual comparison."""
    if actual_result is None or str(expected_result) != str(actual_result):
        return ("FAIL", actual_result if actual_result is not None else "MISSING")
    return ("PASS", actual_result)


def qicore_row_outcome(expected_rows: Dict, qicore_rows: Dict, measure: str, patient_guid: str, group: str, population: str) -> str:
    """Return 'PASS', 'FAIL', or 'N/A' for a single group+population in QICore.

    PASS when QICore matches expected; FAIL when QICore produced a differing
    value; N/A when expected is absent or QICore produced no value for that
    (measure, guid, group:population) row.
    """
    key = (measure, patient_guid, f"{group}:{population}")
    expected_result = expected_rows.get(key)
    if expected_result is None:
        return 'N/A'
    actual = qicore_rows.get(key)
    if actual is None:
        return 'N/A'
    result, _ = row_outcome(expected_result, actual)
    return result


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


def scores_by_measure(expected_rows: Dict[str, str], actual_rows: Dict[str, str]) -> Dict[str, Tuple[int, int]]:
    """Return dict of measure_name -> (pass_count, fail_count) counting distinct
    test cases per measure (a case fails once even if several populations are off)."""
    by_measure: Dict[str, Tuple[int, int]] = {}
    for (measure, _guid), outcome in test_case_outcomes(expected_rows, actual_rows).items():
        p, f = by_measure.get(measure, (0, 0))
        if outcome == "PASS":
            by_measure[measure] = (p + 1, f)
        else:
            by_measure[measure] = (p, f + 1)
    return by_measure


def test_case_guid_count(discrepancy: 'MeasureDiscrepancy') -> int:
    """Number of distinct patient GUIDs seen across all groups of a measure."""
    return len(set(discrepancy.all_test_cases))


def mismatched_test_case_guids(discrepancy: 'MeasureDiscrepancy') -> Set[str]:
    """Distinct patient GUIDs that have at least one mismatched group."""
    return {test_group_id.patient_guid for test_group_id in discrepancy.mismatched_test_cases}


def exclude_pending_rows(expected_rows: Dict, actual_rows: Dict, pending: Set) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Return (expected, actual) rows with resolution-pending cases removed.

    ``pending`` is a set of (measure_name, patient_guid) flagged by an unresolved
    known issue; all groups of a pending case are dropped from both dicts.
    """
    filt_expected = {}
    filt_actual = {}
    for key, value in expected_rows.items():
        if (key[0], key[1]) in pending:
            continue
        filt_expected[key] = value
    for key, value in actual_rows.items():
        if (key[0], key[1]) in pending:
            continue
        filt_actual[key] = value
    return filt_expected, filt_actual

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

def known_issue_label(issues, measure_name: str, patient_guid: str) -> str:
    """Return a markdown label of known-issue IDs affecting a case, or '—'.

    Only unresolved issues whose enumerated affected_test_cases include the case
    are reported; resolved/historical issues do not appear (their cases pass).
    """
    labels = []
    for issue in issues:
        if issue.get("resolved", False):
            continue
        for case in issue.get("affected_test_cases", []):
            if case.get("measure") == measure_name and case.get("guid") == patient_guid:
                labels.append(f"{issue['id']} — resolution pending")
                break
    return "<br>".join(labels) if labels else "—"


def generate_comparison_report(file: str, expected_results: Dict[ResultKey, Dict[str, str]], actual_results: Dict[ResultKey, Dict[str, str]], pass_count: int, fail_count: int, issues: List[dict] = None, expected_rows: Dict[str, str] = None, actual_rows: Dict[str, str] = None, engine_diff: Dict[str, Dict] = None, qicore_rows: Dict[str, str] = None, qicore_groups: Dict[ResultKey, Dict[str, str]] = None, unscored_cells: List = None, now: datetime = None):
    discrepancies = capture_discrepancies_by_measure(expected_results, actual_results)
    issues = issues or []
    expected_keys = (list(expected_rows.keys()) if expected_rows is not None
                     else list(expected_results.keys()))
    pending = known_issues_lib.pending_case_set({"issues": issues})
    pending_issues = known_issues_lib.pending_issues({"issues": issues})

    pending_case_count = len({(k[0], k[1]) for k in expected_keys}
                             & pending) if expected_keys else 0

    # Attribution ledger replaces the old dual score. The previous
    # "excl. resolution-pending" figure subtracted every case cited by an open
    # issue -- but most of those cases had started passing, so it removed ~4
    # passing cases for every failing one and reported a 100.00% that could not
    # be reached by failure. The ledger keeps one denominator and reports how
    # many failures have a written cause instead. See attribution.py.
    ledger = None
    if expected_rows is not None and actual_rows is not None:
        ledger = attribution.build_ledger(
            expected_rows,
            actual_rows,
            {"issues": issues},
            test_case_outcomes(expected_rows, actual_rows),
            unscored_cells or (),
        )

    # QICore pass/fail counts (computed from expected vs QICore actuals).
    qicore_discrepancies: Dict[str, MeasureDiscrepancy] = {}
    qicore_pass_by_measure: Dict[str, Tuple[int, int]] = {}
    qicore_total_pass = 0
    qicore_total_fail = 0
    if qicore_groups is not None and qicore_rows is not None:
        qicore_discrepancies = capture_discrepancies_by_measure(expected_results, qicore_groups)
        qicore_pass_by_measure = scores_by_measure(expected_rows, qicore_rows)
        qicore_total_pass = sum(p for p, _ in qicore_pass_by_measure.values())
        qicore_total_fail = sum(f for _, f in qicore_pass_by_measure.values())

    def pad(_pass, _fail):
        denom = _pass + _fail
        return f'{_pass} ({_pass / denom * 100:.2f}%)' if denom else f'{_pass} (0.00%)'

    with open(file, "w", newline="") as f:
        f.write('# Discrepancy Report\n')
        summary_rows = [
                ['Generated', clock.resolve(now)],
                ['Total Measures', len(set([result_key.measure_name for result_key in expected_results.keys()]))],
                ['Total Test Cases', len(set([(result_key.measure_name, result_key.patient_guid) for result_key in expected_results.keys()]))],
                ['Measures with Discrepancies', len(discrepancies)],
                ['Known Issues (resolution pending)', f'{len(pending_issues)} issues / {pending_case_count} test cases'],
                ['Passing Test Cases', pad(pass_count, fail_count)],
                ['Failing Test Cases', pad(fail_count, pass_count)],
        ]
        if ledger is not None:
            summary_rows.extend([
                ['&nbsp;&nbsp;— attributed to an open known issue',
                 f'{ledger.attributed_failures}'],
                ['&nbsp;&nbsp;— **UNATTRIBUTED** (regression / untriaged)',
                 f'**{ledger.unattributed_count}**'],
            ])
        if qicore_groups is not None:
            summary_rows.extend([
                ['QICore Passing Test Cases', pad(qicore_total_pass, qicore_total_fail)],
                ['QICore Failing Test Cases', pad(qicore_total_fail, qicore_total_pass)],
                ['QICore Measures with Discrepancies', len(qicore_discrepancies)],
            ])
        f.writelines(create_markdown_table(
            ['Details', 'Value'],
            summary_rows
        ))
        if ledger is not None:
            f.write('\n')
            f.write('\n'.join(attribution.render_health_section(ledger)))
            f.write('\n')
        if pending_issues:
            f.write('\n## Known Issues (resolution-pending)\n\n')
            f.writelines(create_markdown_table(
                ['ID', 'Issue', 'Category', 'Status', 'Affected measures', 'Tracked test cases'],
                [
                    [
                        issue["id"],
                        issue.get('title', ''),
                        issue.get('category', ''),
                        issue.get('status', ''),
                        ', '.join(dict.fromkeys(issue.get('affected_measures', []))),
                        str(len(issue.get('affected_test_cases', []))),
                    ] for issue in pending_issues
                ],
                '|---|------|---------|-------|-----|------|\n'))
        f.writelines(create_markdown_table(
            ['Discrepancy Summary', 'Measure Count', 'Test Case Count'],
            [
                [
                    'Missing Results', 
                    len(set([measure for measure, discrepancy  in discrepancies.items() if discrepancy.missing_results])),
                    sum([len(discrepancy.missing_results) for discrepancy in discrepancies.values()])
                ],
                [
                    'Missing Populations', 
                    len(set([measure for measure, discrepancy  in discrepancies.items() if discrepancy.missing_populations])),
                    sum([len(discrepancy.missing_populations) for discrepancy in discrepancies.values()])
                ],
                [
                    'Mismatched Test Cases', 
                    len(set([measure for measure, discrepancy  in discrepancies.items() if discrepancy.mismatched_test_cases])),
                    sum([len(mismatched_test_case_guids(discrepancy)) for discrepancy in discrepancies.values()])
                ]
            ],
            '|---|:---:|:---:|\n'))
        f.write('\n')
        f.write('_Note: Measures can have multiple discrepancies, so the Measures with Discrepancies count may not match the summary counts._\n')

        # Per-measure comparison table (CMS vs QICore)
        if qicore_groups is not None:
            all_measures = sort_measure_names(list(set([k.measure_name for k in expected_results.keys()])))
            cms_pass_by_measure = scores_by_measure(expected_rows, actual_rows) if expected_rows and actual_rows else {}
            comparison_rows = []
            for measure in all_measures:
                cms_p, cms_f = cms_pass_by_measure.get(measure, (0, 0))
                qi_p, qi_f = qicore_pass_by_measure.get(measure, (0, 0))
                cms_has_discrepancy = measure in discrepancies
                qi_has_discrepancy = measure in qicore_discrepancies
                if cms_has_discrepancy and qi_has_discrepancy:
                    note = 'Both have discrepancies'
                elif not cms_has_discrepancy and not qi_has_discrepancy:
                    note = 'Match — both pass'
                elif not cms_has_discrepancy and qi_has_discrepancy:
                    note = 'CMS passes, QICore has discrepancies'
                else:
                    note = 'CMS has discrepancies, QICore passes'
                comparison_rows.append([
                    measure,
                    f'{cms_p} / {cms_f}',
                    f'{qi_p} / {qi_f}',
                    note,
                ])
            if comparison_rows:
                f.write('## CMS vs QICore Comparison\n\n')
                f.writelines(create_markdown_table(
                    ['Measure', 'CMS Passing / Failing Test Cases', 'QICore Passing / Failing Test Cases', 'Notes'],
                    comparison_rows,
                    '|---|:---:|:---:|---|\n'))

        non_discrepancy_measures = [measure_name for measure_name in sort_measure_names(list(set([k.measure_name for k in expected_results.keys()]))) if measure_name not in discrepancies]
        if non_discrepancy_measures or (qicore_groups is not None and qicore_discrepancies is not None):
            f.write('## Measures with No Discrepancies\n\n')
            # CMS measures with no discrepancies
            f.write(f'### CMS Measures ({len(non_discrepancy_measures)})\n')
            if non_discrepancy_measures:
                for measure in non_discrepancy_measures:
                    qi_note = ''
                    if qicore_groups is not None:
                        if measure in qicore_discrepancies:
                            qi_note = ' — QICore has discrepancies'
                        else:
                            qi_note = ' — matches QICore'
                    f.write(f'- {measure} {cql_file_link(measure,"[cql]")} {test_results_file_link(measure,"[test results]")}{qi_note}\n')
            else:
                f.write('_None_\n')

            # QICore measures with no discrepancies (sub-section)
            if qicore_groups is not None:
                qicore_non_discrepancy = [m for m in sort_measure_names(list(set([k.measure_name for k in expected_results.keys()]))) if m not in qicore_discrepancies]
                f.write(f'\n### QICore Measures ({len(qicore_non_discrepancy)})\n')
                if qicore_non_discrepancy:
                    for measure in qicore_non_discrepancy:
                        cms_note = ''
                        if measure not in discrepancies:
                            cms_note = ' — also passes in CMS'
                        else:
                            cms_note = ' — CMS has discrepancies'
                        f.write(f'- {measure} {cql_file_link(measure,"[cql]")} {test_results_file_link(measure,"[test results]")}{cms_note}\n')
                else:
                    f.write('_None_\n')

        if discrepancies:
            f.write(f'## Measures with Discrepancies ({len(discrepancies)})\n')
            f.writelines(create_markdown_table(
                ['Measure', 'Total Test Cases', 'Missing Results', 'Missing Populations', 'Mismatched Test Cases', 'QICore Passing / Failing Test Cases', 'QICore Status'],
                [
                    [
                        f'[{measure}](#{measure.lower()})',
                        test_case_guid_count(discrepancy),
                        len(discrepancy.missing_results),
                        len(discrepancy.missing_populations),
                        f'{len(mismatched_test_case_guids(discrepancy))/test_case_guid_count(discrepancy)*100:.2f}%   ({len(mismatched_test_case_guids(discrepancy))})',
                        f'{qicore_pass_by_measure.get(measure, (0, 0))[0]} / {qicore_pass_by_measure.get(measure, (0, 0))[1]}',
                        'passes' if measure not in qicore_discrepancies else f'has discrepancies ({len(mismatched_test_case_guids(qicore_discrepancies[measure]))})',
                    ] for measure, discrepancy in discrepancies.items()
                ],
                '|---|:---:|:---:|:---:|:---:|:---:|---|\n'))
            f.write('\n')

            for measure, discrepancy in discrepancies.items():
                f.write(f'#### {measure}\n')
                f.write(f'{cql_file_link(measure, '[cql]')} {test_results_file_link(measure, '[test results]')}\n\n')
                if qicore_groups is not None:
                    qi_p, qi_f = qicore_pass_by_measure.get(measure, (0, 0))
                    if measure in qicore_discrepancies:
                        qi_mismatch = len(mismatched_test_case_guids(qicore_discrepancies[measure]))
                        qi_missing = len(qicore_discrepancies[measure].missing_results)
                        qi_status = f'has discrepancies ({qi_mismatch} mismatched, {qi_missing} missing)'
                    else:
                        qi_status = 'passes'
                    f.write(f'QICore: {qi_p} / {qi_f} — {qi_status}\n\n')

                if discrepancy.missing_results:
                    f.write(f'Missing Results ({len(discrepancy.missing_results)} of {test_case_guid_count(discrepancy)} test cases)\n')
                    f.writelines(create_markdown_table(
                        ['Test Case', 'Group', 'Known Issue'],
                        [[
                            measure_report_file_link(missing_id.measure_name, missing_id.patient_guid),
                            missing_id.group,
                            known_issue_label(issues, missing_id.measure_name, missing_id.patient_guid)
                         ] for missing_id in sort_result_keys(discrepancy.missing_results)]))
            
                if discrepancy.missing_populations:
                    f.write(f'Missing Populations ({len(discrepancy.missing_populations)} of {test_case_guid_count(discrepancy)} test cases)\n')
                    f.writelines(create_markdown_table(
                        ['Test Case', 'Group', 'Population'],
                        [[
                            measure_report_file_link(missing_id.measure_name, missing_id.patient_guid),
                            missing_id.group,
                            ','.join(populations)] for (missing_id, populations) in sort_missing_populations(discrepancy.missing_populations)]))
            
                if discrepancy.mismatched_test_cases:
                    f.write(f'Mismatched Test Cases ({len(mismatched_test_case_guids(discrepancy))} of {test_case_guid_count(discrepancy)} test cases)\n')
                    if qicore_groups is not None:
                        f.writelines(create_markdown_table(
                            ['Test Case', 'Group', 'Population', 'Expected', 'Actual', 'Known Issue', 'QICore'],
                            [[
                                measure_report_file_link(measure, test_group_id.patient_guid),
                                test_group_id.group,
                                '<br>'.join([population for population in sort_populations(populations.keys())]),
                                '<br>'.join([populations[population].expected for population in sort_populations(populations.keys())]),
                                '<br>'.join([populations[population].actual for population in sort_populations(populations.keys())]),
                                known_issue_label(issues, measure, test_group_id.patient_guid),
                                '<br>'.join([qicore_row_outcome(expected_rows, qicore_rows, measure, test_group_id.patient_guid, test_group_id.group, population) for population in sort_populations(populations.keys())]),
                             ] for test_group_id, populations in sort_mismatched_test_cases(discrepancy.mismatched_test_cases)],
                            '|---|---|---|:---:|:---:|---|:---:|\n'))
                    else:
                        f.writelines(create_markdown_table(
                            ['Test Case', 'Group', 'Population', 'Expected', 'Actual', 'Known Issue'],
                            [[
                                measure_report_file_link(measure, test_group_id.patient_guid),
                                test_group_id.group,
                                '<br>'.join([population for population in sort_populations(populations.keys())]),
                                '<br>'.join([populations[population].expected for population in sort_populations(populations.keys())]),
                                '<br>'.join([populations[population].actual for population in sort_populations(populations.keys())]),
                                known_issue_label(issues, measure, test_group_id.patient_guid)
                             ] for test_group_id, populations in sort_mismatched_test_cases(discrepancy.mismatched_test_cases)],
                            '|---|---|---|:---:|:---:|---|\n'))

        if engine_diff:
            f.writelines(render_engine_diff_section(engine_diff))

        f.write('\n---\n')
        f.write('\n_See [catalog_issue_details.md](./catalog_issue_details.md) for per-issue detail on every catalog issue cited above._\n')

def archive_report(report_path: str, now: datetime = None) -> str:
    """Copy the generated discrepancy report to _archive/ with a timestamp.

    Returns the archive path written (or None if the report file is absent).
    """
    src = Path(report_path)
    if not src.exists():
        return None
    archive_dir = src.parent / "_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = clock.resolve(now).strftime("%Y%m%d-%H%M")
    dest = archive_dir / f"{src.stem}-{stamp}{src.suffix}"
    shutil.copy2(src, dest)
    return str(dest)


def main(expected_file: str, actual_file: str, output_file: str, comparison_report: str, known_issues_file: str = None, qicore_actual_file: str = None, qicore_diff_csv: str = None, now: datetime = None, run_history_path: str = None):
    # Resolved once so every timestamp this run produces -- the report's
    # "Generated" row, the archive filename, catalog_issue_details.md's
    # "Generated" line, and the run-history entry -- agrees. Passing an
    # explicit `now` (or setting SOURCE_DATE_EPOCH) makes the whole run
    # deterministic, which is what lets check_generated.py regenerate-and-diff.
    now = clock.resolve(now)

    expected_results = capture_results(expected_file)
    actual_results = capture_results(actual_file)

    issues = known_issues_lib.load_catalog(known_issues_file).get("issues", [])

    # Cross-engine diff (additive, informational): compare this project's actuals
    # against the QI-Core engine's actuals (source of truth for QI-Core measures).
    engine_diff = None
    if qicore_actual_file and os.path.exists(qicore_actual_file):
        qicore_results = capture_results(qicore_actual_file)
        engine_diff = diff_actual_results(actual_results[0], qicore_results[0])
        if qicore_diff_csv:
            write_engine_diff_csv(engine_diff, qicore_diff_csv)

    pass_fail_count = generate_output(output_file, expected_results[0], actual_results[0])
    pass_pct = pass_fail_count[0] / (pass_fail_count[0] + pass_fail_count[1]) * 100
    print(f"PASS (test cases): {pass_fail_count[0]} ({pass_pct:.2f})%")
    print(f"FAIL (test cases): {pass_fail_count[1]} ({(100 - pass_pct):.2f})%")

    ledger = attribution.build_ledger(
        expected_results[0], actual_results[0], {"issues": issues},
        test_case_outcomes(expected_results[0], actual_results[0]),
        expected_results.unscored)
    if issues:
        print(f"  attributed to an open known issue: {ledger.attributed_failures}")
        print(f"  UNATTRIBUTED (regression/untriaged): {ledger.unattributed_count}")
        if ledger.stale_count:
            print(f"  stale attributions (cited case now passes): {ledger.stale_count}")
        if ledger.phantom_count:
            print(f"  phantom attributions (case not in expected): {ledger.phantom_count}")
        if ledger.unscored_cases:
            print(f"  test cases not fully measured: {len(ledger.unscored_cases)}")


    generate_comparison_report(comparison_report, expected_results[1], actual_results[1], pass_fail_count[0], pass_fail_count[1], issues, expected_results[0], actual_results[0], engine_diff, qicore_results[0] if qicore_actual_file and os.path.exists(qicore_actual_file) else None, qicore_results[1] if qicore_actual_file and os.path.exists(qicore_actual_file) else None, expected_results.unscored, now)

    archived = archive_report(comparison_report, now)
    if archived:
        print(f"Archived report -> {archived}")

    details_out = os.path.join(os.path.dirname(comparison_report), "catalog_issue_details.md")
    render_catalog_lib.write_catalog_details(
        known_issues_path=known_issues_file,
        discrepancy_report_path=comparison_report,
        output_path=details_out,
        now=now,
    )

    run_history.record({
        "timestamp": now.isoformat(),
        "total_cases": ledger.total_cases,
        "passing": ledger.passing,
        "failing": ledger.failing,
        "attributed_failures": ledger.attributed_failures,
        "unattributed_failures": ledger.unattributed_count,
        "stale_attributions": ledger.stale_count,
        "phantom_attributions": ledger.phantom_count,
        "unscored_cases": len(ledger.unscored_cases),
    }, path=run_history_path)
    print(f"Wrote catalog issue details -> {details_out}")

if __name__ == '__main__':
    expected_file = "./scripts/comparison/expected_results.csv"
    actual_file = "./scripts/comparison/actual_results.csv"
    output_file = "./scripts/comparison/output_results.csv"
    comparison_report = "./scripts/comparison/discrepancy_report.md"
    known_issues_file = "./scripts/comparison/known_issues.json"
    qicore_actual_file = "./scripts/comparison/qicore-2025-actual-results.csv"
    qicore_diff_csv = "./scripts/comparison/qicore_engine_diff.csv"

    args = sys.argv[1:]
    if "--known-issues" in args:
        idx = args.index("--known-issues")
        if idx + 1 < len(args):
            known_issues_file = args[idx + 1]
        args = args[:idx] + args[idx + 2:]
    if "--qicore-actual-results" in args:
        idx = args.index("--qicore-actual-results")
        if idx + 1 < len(args):
            qicore_actual_file = args[idx + 1]
        args = args[:idx] + args[idx + 2:]
    if "--qicore-diff-csv" in args:
        idx = args.index("--qicore-diff-csv")
        if idx + 1 < len(args):
            qicore_diff_csv = args[idx + 1]
        args = args[:idx] + args[idx + 2:]
    if args:
        # positional: expected actual output report [known-issues]
        expected_file = args[0]
        if len(args) > 1:
            actual_file = args[1]
        if len(args) > 2:
            output_file = args[2]
        if len(args) > 3:
            comparison_report = args[3]
        if len(args) > 4:
            known_issues_file = args[4]

    main(expected_file, actual_file, output_file, comparison_report, known_issues_file, qicore_actual_file, qicore_diff_csv)
