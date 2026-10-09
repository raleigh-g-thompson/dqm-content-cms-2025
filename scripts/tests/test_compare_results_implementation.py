"""Tests for scoring test cases resolved by an implementation issue.

A case the measure steward's test data can't satisfy (class `implementation` in
`defect-tracking/known-issues.md`) is listed in `implementation_issues.csv`.
An issue explains the failure as well as a match would, so its failing cells
score IMPLEMENTATION instead of FAIL and the case counts as resolved. These
tests pin that rule and the guarantee that it never hides a failure the issue
doesn't cover.
"""
import csv
import os
import shutil
import tempfile
import unittest

from scripts.compare_results import (
    IMPLEMENTATION,
    ImplementationIssue,
    capture_results,
    cell_outcome,
    failing_implementation_keys,
    generate_output,
    implementation_case_count,
    main,
    scores,
)
# Imported under another name: pytest would collect a bare `test_case_outcomes`.
from scripts.compare_results import test_case_outcomes as case_outcomes

MEASURE = "CMS999FHIRTestMeasure"
OTHER = "CMS998FHIROtherMeasure"
GUID_A = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
GUID_B = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
GUID_C = "cccccccc-3333-4333-8333-cccccccccccc"

CSV_HEADER = "measure_name,guid,population,count\n"
ISSUES_HEADER = "issue_id,measure_name,guid,group,note\n"


def _csv(*rows):
    return CSV_HEADER + "".join(f"{r}\n" for r in rows)


def _issue(guid, group="", issue_id="I-73", measure=MEASURE):
    return ImplementationIssue(issue_id, measure, guid, group, "Steward note.")


class ImplementationScoringTest(unittest.TestCase):
    """Cell and test-case verdicts, without touching the filesystem for output."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def _rows(self, *rows):
        path = os.path.join(self.tmp, f"{len(os.listdir(self.tmp))}.csv")
        with open(path, "w", newline="") as f:
            f.write(_csv(*rows))
        return capture_results(path)

    def test_covered_failing_cell_scores_implementation(self):
        key = (MEASURE, GUID_A, "Group_1:Denominator")
        self.assertEqual(cell_outcome(key, "2", "1", [_issue(GUID_A)]), (IMPLEMENTATION, "1"))

    def test_covered_missing_cell_scores_implementation(self):
        key = (MEASURE, GUID_A, "Group_1:Denominator")
        self.assertEqual(cell_outcome(key, "1", None, [_issue(GUID_A)]),
                         (IMPLEMENTATION, "MISSING"))

    def test_covered_passing_cell_stays_pass(self):
        key = (MEASURE, GUID_A, "Group_1:Denominator")
        self.assertEqual(cell_outcome(key, "1", "1", [_issue(GUID_A)]), ("PASS", "1"))

    def test_uncovered_failing_cell_stays_fail(self):
        key = (MEASURE, GUID_B, "Group_1:Denominator")
        self.assertEqual(cell_outcome(key, "1", "0", [_issue(GUID_A)]), ("FAIL", "0"))

    def test_group_scoped_issue_covers_only_its_group(self):
        issues = [_issue(GUID_A, group="Group_2")]
        self.assertEqual(cell_outcome((MEASURE, GUID_A, "Group_1:Numerator"), "1", "0", issues)[0],
                         "FAIL")
        self.assertEqual(cell_outcome((MEASURE, GUID_A, "Group_2:Numerator"), "1", "0", issues)[0],
                         IMPLEMENTATION)

    def test_issue_for_other_measure_does_not_cover(self):
        key = (MEASURE, GUID_A, "Group_1:Denominator")
        self.assertEqual(cell_outcome(key, "1", "0", [_issue(GUID_A, measure=OTHER)])[0], "FAIL")

    def test_case_outcomes_and_counts(self):
        # GUID_A: every failing group covered -> IMPLEMENTATION.
        # GUID_B: Group_2 covered but Group_1 fails uncovered -> FAIL.
        # GUID_C: passes, though covered -> PASS.
        expected = self._rows(
            f"{MEASURE},{GUID_A},Group_1:Denominator,2",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{MEASURE},{GUID_B},Group_2:Denominator,1",
            f"{MEASURE},{GUID_C},Group_1:Denominator,1")
        actual = self._rows(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,0",
            f"{MEASURE},{GUID_B},Group_2:Denominator,0",
            f"{MEASURE},{GUID_C},Group_1:Denominator,1")
        issues = [_issue(GUID_A), _issue(GUID_B, group="Group_2"), _issue(GUID_C)]
        self.assertEqual(case_outcomes(expected.rows, actual.rows, issues), {
            (MEASURE, GUID_A): IMPLEMENTATION,
            (MEASURE, GUID_B): "FAIL",
            (MEASURE, GUID_C): "PASS",
        })
        self.assertEqual(scores(expected.rows, actual.rows, issues), (1, 1))
        self.assertEqual(implementation_case_count(expected.rows, actual.rows, issues), 1)
        # Group-level: GUID_B's covered Group_2 is resolved, its Group_1 is not.
        self.assertEqual(
            {(k.patient_guid, k.group) for k in
             failing_implementation_keys(expected.groups, actual.groups, issues)},
            {(GUID_A, "Group_1"), (GUID_B, "Group_2")})

    def test_without_issues_scoring_is_unchanged(self):
        expected = self._rows(f"{MEASURE},{GUID_A},Group_1:Denominator,2")
        actual = self._rows(f"{MEASURE},{GUID_A},Group_1:Denominator,1")
        self.assertEqual(scores(expected.rows, actual.rows), (0, 1))
        self.assertEqual(implementation_case_count(expected.rows, actual.rows, []), 0)
        self.assertEqual(failing_implementation_keys(expected.groups, actual.groups, []), set())

    def test_generate_output_writes_implementation_rows(self):
        expected = self._rows(f"{MEASURE},{GUID_A},Group_1:Denominator,2",
                              f"{MEASURE},{GUID_A},Group_1:Numerator,1")
        actual = self._rows(f"{MEASURE},{GUID_A},Group_1:Denominator,1",
                            f"{MEASURE},{GUID_A},Group_1:Numerator,1")
        out = os.path.join(self.tmp, "out.csv")
        self.assertEqual(generate_output(out, expected.rows, actual.rows, [_issue(GUID_A)]), (0, 0))
        with open(out, newline="") as f:
            results = {r["population"]: r["result"] for r in csv.DictReader(f)}
        self.assertEqual(results, {"Group_1:Denominator": IMPLEMENTATION,
                                   "Group_1:Numerator": "PASS"})


class ImplementationReportTest(unittest.TestCase):
    """End to end through main(), with no QI-Core file."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def _write(self, name, text):
        path = os.path.join(self.tmp, name)
        with open(path, "w", newline="") as f:
            f.write(text)
        return path

    def _run(self, expected_rows, actual_rows, issues_text):
        e = self._write("e.csv", _csv(*expected_rows))
        a = self._write("a.csv", _csv(*actual_rows))
        impl = self._write("impl.csv", issues_text)
        self.output = os.path.join(self.tmp, "o.csv")
        report = os.path.join(self.tmp, "r.md")
        main(e, a, self.output, report, measure_resource_dir=self.tmp,
             qicore_file=os.path.join(self.tmp, "no-qicore.csv"),
             implementation_file=impl)
        with open(report) as f:
            return f.read()

    def test_measure_with_only_implementation_failures_has_no_discrepancies(self):
        text = self._run(
            [f"{MEASURE},{GUID_A},Group_1:Initial Population,2",
             f"{MEASURE},{GUID_A},Group_1:Denominator,2",
             f"{MEASURE},{GUID_B},Group_1:Initial Population,1",
             f"{MEASURE},{GUID_B},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Initial Population,1",
             f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_B},Group_1:Initial Population,1",
             f"{MEASURE},{GUID_B},Group_1:Denominator,1"],
            ISSUES_HEADER + f'I-73,{MEASURE},{GUID_A},,"One Claim, two encounters."\n')
        self.assertIn("| Total Test Cases | 2 |", text)
        self.assertIn("| Measures with Discrepancies | 0 |", text)
        self.assertIn("| Pass Count | 1 (50.00%) |", text)
        self.assertIn("| Implementation Issue Count | 1 (50.00%) |", text)
        self.assertIn("| Fail Count | 0 (0.00%) |", text)
        self.assertIn("| Resolved (Pass + Implementation) | 2 (100.00%) |", text)
        self.assertIn("## Implementation Issues (1 test case, 1 measure)", text)
        self.assertIn(f"  - {GUID_A} (Group_1: Initial Population 2 → 1, Denominator 2 → 1): "
                      "**I-73**. One Claim, two encounters.\n", text)
        self.assertIn("## Measures with No Discrepancies (1 of 1)", text)
        self.assertNotIn("## Measures with Discrepancies", text)
        self.assertNotIn("QI-Core Parity", text)
        with open(self.output, newline="") as f:
            results = {(r["guid"], r["population"]): r["result"] for r in csv.DictReader(f)}
        self.assertEqual(results[(GUID_A, "Group_1:Denominator")], IMPLEMENTATION)
        self.assertEqual(results[(GUID_B, "Group_1:Denominator")], "PASS")

    def test_uncovered_group_keeps_case_failing(self):
        text = self._run(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_A},Group_2:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,0",
             f"{MEASURE},{GUID_A},Group_2:Denominator,0"],
            ISSUES_HEADER + f"I-72,{MEASURE},{GUID_A},Group_2,Only Group_2.\n")
        self.assertIn("| Fail Count | 1 (100.00%) |", text)
        self.assertNotIn("Implementation Issue Count", text)
        # Group_2 is explained and listed; Group_1 stays a discrepancy.
        self.assertIn("## Implementation Issues (1 test case, 1 measure)", text)
        self.assertIn(f"  - {GUID_A} (Group_2: Denominator 1 → 0): **I-72**. Only Group_2.\n", text)
        self.assertIn("Mismatched Test Cases (1 of 2)", text)
        self.assertRegex(text, rf"\| {GUID_A} \| Group_1 \| Denominator \| 1 \| 0 \|")

    def test_no_failing_covered_case_renders_no_section(self):
        text = self._run(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            ISSUES_HEADER + f"I-73,{MEASURE},{GUID_A},,Already fixed.\n")
        self.assertNotIn("Implementation Issue", text)
        self.assertIn("| Pass Count | 1 (100.00%) |", text)


if __name__ == "__main__":
    unittest.main()
