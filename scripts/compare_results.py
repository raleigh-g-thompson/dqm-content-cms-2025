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
import known_issues as known_issues_lib

measure_id_pattern = r"(?:CMS|CMSFHIR)(?P<measure_id>\d+)"

MeasureDifference = namedtuple('MeasureDifference', ['measure', 'total_test_cases', 'test_cases_with_differences', 'result_deltas'])
ResultKey = namedtuple('ResultKey', ['measure_name', 'patient_guid', 'group'])
ResultDelta = namedtuple('ResultDelta', ['patient_guid', 'group', 'population', 'expected', 'actual'])
Comparison = namedtuple('Comparison', ['expected', 'actual'])
TestCaseGroupId = namedtuple('TestCaseId', ['patient_guid', 'group'])

# source: https://terminology.hl7.org/CodeSystem-measure-population.html
ValidMeasurePopulationTypes = [
    'Initial Population',
    'Numerator',
    'Numerator Exclusion',
    'Numerator Observations',
    'Denominator',
    'Denominator Exclusion',
    'Denominator-exclusion',
    'Denominator-exception',
    'Denominator Exception',
    'Denominator Observations',
    'Measure Population',
    'Measure Population Exclusion'
]

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

class Results(NamedTuple):
    rows: Dict[str, str]
    groups: Dict[ResultKey, Dict[str, str]]

def capture_results(file: str) -> Results:
    rows = {}
    results = {}
    with open(file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row['measure_name'].lower().startswith("test"):
                key = (row["measure_name"], row["guid"], row["population"])
                rows[key] = row["count"]

                group_and_population = row["population"].split(':')
                if group_and_population[1] not in ValidMeasurePopulationTypes:
                    continue

                result_key = ResultKey(row["measure_name"], row["guid"], group_and_population[0])
                result = results.setdefault(result_key, {})
                result[group_and_population[1]] = row["count"]
    return Results(rows, results)

def row_outcome(expected_result: str, actual_result: str) -> Tuple[str, str]:
    """Return (result, actual_display) for a single expected/actual comparison."""
    if actual_result is None or str(expected_result) != str(actual_result):
        return ("FAIL", actual_result if actual_result is not None else "MISSING")
    return ("PASS", actual_result)


def generate_output(file: str, expected_rows: Dict, actual_rows: Dict) -> Tuple[int, int]:
    header = ["result", "measure_name", "guid", "population", "expected_result", "actual_result"]
    output = []

    pass_count = 0
    fail_count = 0

    for key, expected_result in expected_rows.items():
        # key fields: [ 'measure_name', 'patient_guid', 'group' ]
        # verify the population
        if key[2].split(':')[1] not in ValidMeasurePopulationTypes:
            # TODO: include 'bad' population in report so user know why population wasn't used in report
            continue

        actual_result = actual_rows.get(key)
        result, actual_display = row_outcome(expected_result, actual_result)
        output.append([result, key[0], key[1], key[2], expected_result, actual_display])
        if result == "PASS":
            pass_count += 1
        else:
            fail_count += 1

    with open(file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(output)

    return (pass_count, fail_count)


def scores(expected_rows: Dict[str, str], actual_rows: Dict[str, str]) -> Tuple[int, int]:
    """Compute (pass, fail) over expected rows compared against actual rows."""
    pass_count = 0
    fail_count = 0
    for key, expected_result in expected_rows.items():
        if key[2].split(':')[1] not in ValidMeasurePopulationTypes:
            continue
        actual_result = actual_rows.get(key)
        result, _ = row_outcome(expected_result, actual_result)
        if result == "PASS":
            pass_count += 1
        else:
            fail_count += 1
    return (pass_count, fail_count)


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


def generate_comparison_report(file: str, expected_results: Dict[ResultKey, Dict[str, str]], actual_results: Dict[ResultKey, Dict[str, str]], pass_count: int, fail_count: int, issues: List[dict] = None, expected_rows: Dict[str, str] = None, actual_rows: Dict[str, str] = None):
    discrepancies = capture_discrepancies_by_measure(expected_results, actual_results)
    issues = issues or []
    expected_keys = (list(expected_rows.keys()) if expected_rows is not None
                     else list(expected_results.keys()))
    pending = known_issues_lib.pending_case_set({"issues": issues})
    pending_issues = known_issues_lib.pending_issues({"issues": issues})

    # Dual scores: all vs excluding resolution-pending cases.
    if expected_rows is not None and actual_rows is not None:
        excl_expected, excl_actual = exclude_pending_rows(expected_rows, actual_rows, pending)
        excl_pass, excl_fail = scores(excl_expected, excl_actual)
    else:
        excl_pass, excl_fail = pass_count, fail_count
    pending_case_count = len({(k[0], k[1]) for k in expected_keys}
                             & pending) if expected_keys else 0

    def pad(_pass, _fail):
        denom = _pass + _fail
        return f'{_pass} ({_pass / denom * 100:.2f}%)' if denom else f'{_pass} (0.00%)'

    with open(file, "w", newline="") as f:
        f.write('# Discrepancy Report\n')
        f.writelines(create_markdown_table(
            ['Details', 'Value'],
            [
                ['Generated', datetime.now()],
                ['Total Measures', len(set([result_key.measure_name for result_key in expected_results.keys()]))],
                ['Total Test Cases', len(set([(result_key.measure_name, result_key.patient_guid) for result_key in expected_results.keys()]))],
                ['Measures with Discrepancies', len(discrepancies)],
                ['Known Issues (resolution pending)', f'{len(pending_issues)} issues / {pending_case_count} test cases'],
                ['Pass Count (all)', pad(pass_count, fail_count)],
                ['Fail Count (all)', pad(fail_count, pass_count)],
                ['Pass Count (excl. resolution-pending)', pad(excl_pass, excl_fail)],
                ['Fail Count (excl. resolution-pending)', pad(excl_fail, excl_pass)],
            ]
        ))
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
                    sum([len(discrepancy.mismatched_test_cases.keys()) for discrepancy in discrepancies.values()])
                ]
            ],
            '|---|:---:|:---:|\n'))
        f.write('\n')
        f.write('_Note: Measures can have multiple discrepancies, so the Measures with Discrepancies count may not match the summary counts._\n')

        non_discrepancy_measures = [measure_name for measure_name in sort_measure_names(list(set([k.measure_name for k in expected_results.keys()]))) if measure_name not in discrepancies]
        if non_discrepancy_measures:
            f.write(f'## Measures with No Discrepancies ({len(non_discrepancy_measures)})\n')
            for measure in non_discrepancy_measures:
                f.write(f'- {measure} {cql_file_link(measure,'[cql]')} {test_results_file_link(measure,'[test results]')}\n')

        if discrepancies:
            f.write(f'## Measures with Discrepancies ({len(discrepancies)})\n')
            f.writelines(create_markdown_table(
                ['Measure', 'Total Test Cases', 'Missing Results', 'Missing Populations', 'Mismatched Test Cases'],
                [
                    [
                        f'[{measure}](#{measure.lower()})',
                        len(discrepancy.all_test_cases),
                        len(discrepancy.missing_results),
                        len(discrepancy.missing_populations),
                        f'{len(discrepancy.mismatched_test_cases)/len(discrepancy.all_test_cases)*100:.2f}%   ({len(discrepancy.mismatched_test_cases)})'
                    ] for measure, discrepancy in discrepancies.items()
                ],
                '|---|:---:|:---:|:---:|:---:|\n'))
            f.write('\n')

            for measure, discrepancy in discrepancies.items():
                f.write(f'#### {measure}\n')
                f.write(f'{cql_file_link(measure, '[cql]')} {test_results_file_link(measure, '[test results]')}\n\n')

                if discrepancy.missing_results:
                    f.write(f'Missing Results ({len(discrepancy.missing_results)} of {len(discrepancy.all_test_cases)} test cases)\n')
                    f.writelines(create_markdown_table(
                        ['Test Case', 'Group', 'Known Issue'],
                        [[
                            measure_report_file_link(missing_id.measure_name, missing_id.patient_guid),
                            missing_id.group,
                            known_issue_label(issues, missing_id.measure_name, missing_id.patient_guid)
                         ] for missing_id in sort_result_keys(discrepancy.missing_results)]))
            
                if discrepancy.missing_populations:
                    f.write(f'Missing Populations ({len(discrepancy.missing_populations)} of {len(discrepancy.all_test_cases)} test cases)\n')
                    f.writelines(create_markdown_table(
                        ['Test Case', 'Group', 'Population'],
                        [[
                            measure_report_file_link(missing_id.measure_name, missing_id.patient_guid),
                            missing_id.group,
                            ','.join(populations)] for (missing_id, populations) in sort_missing_populations(discrepancy.missing_populations)]))
            
                if discrepancy.mismatched_test_cases:
                    f.write(f'Mismatched Test Cases ({len(discrepancy.mismatched_test_cases)} of  of {len(discrepancy.all_test_cases)})\n')
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

def archive_report(report_path: str) -> str:
    """Copy the generated discrepancy report to _archive/ with a timestamp.

    Returns the archive path written (or None if the report file is absent).
    """
    src = Path(report_path)
    if not src.exists():
        return None
    archive_dir = src.parent / "_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M")
    dest = archive_dir / f"{src.stem}-{stamp}{src.suffix}"
    shutil.copy2(src, dest)
    return str(dest)


def main(expected_file: str, actual_file: str, output_file: str, comparison_report: str, known_issues_file: str = None):
    expected_results = capture_results(expected_file)
    actual_results = capture_results(actual_file)

    issues = known_issues_lib.load_catalog(known_issues_file).get("issues", [])

    pass_fail_count = generate_output(output_file, expected_results[0], actual_results[0])
    pass_pct = pass_fail_count[0] / (pass_fail_count[0] + pass_fail_count[1]) * 100
    print(f"PASS: {pass_fail_count[0]} ({pass_pct:.2f})%")
    print(f"FAIL: {pass_fail_count[1]} ({(100 - pass_pct):.2f})%")
    if issues:
        pending = known_issues_lib.pending_case_set({"issues": issues})
        excl_expected, excl_actual = exclude_pending_rows(expected_results[0], actual_results[0], pending)
        p, fl = scores(excl_expected, excl_actual)
        denom = p + fl
        print(f"PASS (excl. resolution-pending): {p} ({p / denom * 100:.2f}%)" if denom else f"PASS (excl. resolution-pending): {p}")
        print(f"FAIL (excl. resolution-pending): {fl} ({(100 - p / denom * 100) if denom else 0:.2f})%")
    
    generate_comparison_report(comparison_report, expected_results[1], actual_results[1], pass_fail_count[0], pass_fail_count[1], issues, expected_results[0], actual_results[0])

    archived = archive_report(comparison_report)
    if archived:
        print(f"Archived report -> {archived}")

if __name__ == '__main__':
    expected_file = "./scripts/comparison/expected_results.csv"
    actual_file = "./scripts/comparison/actual_results.csv"
    output_file = "./scripts/comparison/output_results.csv"
    comparison_report = "./scripts/comparison/discrepancy_report.md"
    known_issues_file = "./scripts/comparison/known_issues.json"

    args = sys.argv[1:]
    if "--known-issues" in args:
        idx = args.index("--known-issues")
        if idx + 1 < len(args):
            known_issues_file = args[idx + 1]
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

    main(expected_file, actual_file, output_file, comparison_report, known_issues_file)
