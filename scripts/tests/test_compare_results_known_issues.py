"""Tests for scoring test cases explained by a ticketed known issue.

A failing case explained by a catalogued issue (`defect-tracking/known-issues.md`)
is listed in `known_issues.csv`. It counts as resolved only once its issue has a
GitHub ticket: its failing cells then score KNOWN_ISSUE instead of FAIL. Without
a ticket the case stays FAIL and the report labels it as needing one. These
tests pin that rule, the loader's validation, and the report rendering.
"""
import csv
import os
import shutil
import tempfile
import unittest

from scripts.compare_results import (
    IMPLEMENTATION,
    KNOWN_ISSUE,
    ImplementationIssue,
    KnownIssue,
    capture_results,
    cell_outcome,
    failing_known_issue_keys,
    known_issue_case_count,
    load_known_issues,
    main,
    scores,
    sort_issue_ids,
    ticket_link,
)
# Imported under another name: pytest would collect a bare `test_case_outcomes`.
from scripts.compare_results import test_case_outcomes as case_outcomes

MEASURE = "CMS999FHIRTestMeasure"
GUID_A = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
GUID_B = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
GUID_C = "cccccccc-3333-4333-8333-cccccccccccc"
TICKET = "https://github.com/cqframework/clinical_quality_language/issues/1855"

CSV_HEADER = "measure_name,guid,population,count\n"
KNOWN_HEADER = "issue_id,class,ticket,measure_name,guid,group,note\n"
IMPL_HEADER = "issue_id,measure_name,guid,group,note\n"


def _csv(*rows):
    return CSV_HEADER + "".join(f"{r}\n" for r in rows)


def _known(guid, ticket=TICKET, group="", issue_id="I-26", issue_class="engine"):
    return KnownIssue(issue_id, issue_class, ticket, MEASURE, guid, group, "Engine note.")


class KnownIssueLoaderTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def _load(self, text):
        path = os.path.join(self.tmp, "known.csv")
        with open(path, "w", newline="") as f:
            f.write(text)
        return load_known_issues(path)

    def test_absent_file_means_no_issues(self):
        self.assertEqual(load_known_issues(os.path.join(self.tmp, "none.csv")), [])
        self.assertEqual(load_known_issues(None), [])

    def test_reads_ticketed_and_unticketed_rows(self):
        issues = self._load(KNOWN_HEADER
                            + f"I-18,translator,{TICKET},{MEASURE},{GUID_A},,\n"
                            + f'I-26,engine,,{MEASURE},{GUID_B},Group_1,"Reference, not code."\n')
        self.assertEqual([(i.issue_id, i.ticketed, i.group, i.note) for i in issues],
                         [("I-18", True, "", ""),
                          ("I-26", False, "Group_1", "Reference, not code.")])

    def test_pull_request_url_is_a_ticket(self):
        url = "https://github.com/cqframework/clinical_quality_language/pull/1857"
        issues = self._load(KNOWN_HEADER + f"I-69,engine,{url},{MEASURE},{GUID_A},,\n")
        self.assertTrue(issues[0].ticketed)

    def test_placeholder_ticket_is_rejected(self):
        for ticket in ("TBD", "#1855", "https://example.com/issues/1",
                       "https://github.com/cqframework/clinical_quality_language"):
            with self.subTest(ticket=ticket), self.assertRaisesRegex(ValueError, "GitHub"):
                self._load(KNOWN_HEADER + f"I-26,engine,{ticket},{MEASURE},{GUID_A},,\n")

    def test_implementation_class_points_to_its_own_file(self):
        with self.assertRaisesRegex(ValueError, "implementation_issues.csv"):
            self._load(KNOWN_HEADER + f"I-73,implementation,,{MEASURE},{GUID_A},,\n")

    def test_unknown_class_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "not one of"):
            self._load(KNOWN_HEADER + f"I-26,runtime,,{MEASURE},{GUID_A},,\n")

    def test_rows_of_one_issue_must_share_the_ticket(self):
        with self.assertRaisesRegex(ValueError, "I-26 rows disagree"):
            self._load(KNOWN_HEADER
                       + f"I-26,engine,{TICKET},{MEASURE},{GUID_A},,\n"
                       + f"I-26,engine,,{MEASURE},{GUID_B},,\n")

    def test_missing_column_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "missing column.*ticket"):
            self._load(IMPL_HEADER + f"I-26,{MEASURE},{GUID_A},,\n")

    def test_unquoted_comma_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "more fields than columns"):
            self._load(KNOWN_HEADER + f"I-26,engine,,{MEASURE},{GUID_A},,a,b\n")


class KnownIssueScoringTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def _rows(self, *rows):
        path = os.path.join(self.tmp, f"{len(os.listdir(self.tmp))}.csv")
        with open(path, "w", newline="") as f:
            f.write(_csv(*rows))
        return capture_results(path)

    def test_ticketed_failing_cell_scores_known_issue(self):
        key = (MEASURE, GUID_A, "Group_1:Denominator")
        self.assertEqual(cell_outcome(key, "1", None, (), [_known(GUID_A)]),
                         (KNOWN_ISSUE, "MISSING"))

    def test_unticketed_failing_cell_stays_fail(self):
        key = (MEASURE, GUID_A, "Group_1:Denominator")
        self.assertEqual(cell_outcome(key, "1", "0", (), [_known(GUID_A, ticket="")]),
                         ("FAIL", "0"))

    def test_passing_cell_stays_pass(self):
        key = (MEASURE, GUID_A, "Group_1:Denominator")
        self.assertEqual(cell_outcome(key, "1", "1", (), [_known(GUID_A)]), ("PASS", "1"))

    def test_implementation_issue_takes_precedence(self):
        key = (MEASURE, GUID_A, "Group_1:Denominator")
        impl = [ImplementationIssue("I-73", MEASURE, GUID_A, "", "")]
        self.assertEqual(cell_outcome(key, "1", "0", impl, [_known(GUID_A)])[0],
                         IMPLEMENTATION)

    def test_case_outcomes_and_counts(self):
        # GUID_A: ticketed -> KNOWN_ISSUE. GUID_B: unticketed -> FAIL.
        # GUID_C: Group_1 implementation, Group_2 ticketed -> KNOWN_ISSUE.
        expected = self._rows(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{MEASURE},{GUID_C},Group_1:Denominator,1",
            f"{MEASURE},{GUID_C},Group_2:Denominator,1")
        actual = self._rows(
            f"{MEASURE},{GUID_A},Group_1:Denominator,0",
            f"{MEASURE},{GUID_B},Group_1:Denominator,0",
            f"{MEASURE},{GUID_C},Group_1:Denominator,0",
            f"{MEASURE},{GUID_C},Group_2:Denominator,0")
        impl = [ImplementationIssue("I-73", MEASURE, GUID_C, "Group_1", "")]
        known = [_known(GUID_A), _known(GUID_C, group="Group_2"),
                 _known(GUID_B, ticket="", issue_id="I-63")]
        self.assertEqual(case_outcomes(expected.rows, actual.rows, impl, known), {
            (MEASURE, GUID_A): KNOWN_ISSUE,
            (MEASURE, GUID_B): "FAIL",
            (MEASURE, GUID_C): KNOWN_ISSUE,
        })
        self.assertEqual(scores(expected.rows, actual.rows, impl, known), (0, 1))
        self.assertEqual(known_issue_case_count(expected.rows, actual.rows, impl, known), 2)
        self.assertEqual(
            {(k.patient_guid, k.group) for k in
             failing_known_issue_keys(expected.groups, actual.groups, known, impl)},
            {(GUID_A, "Group_1"), (GUID_C, "Group_2")})
        self.assertEqual(
            {(k.patient_guid, k.group) for k in
             failing_known_issue_keys(expected.groups, actual.groups, known, impl,
                                      ticketed=False)},
            {(GUID_B, "Group_1")})


class KnownIssueReportTest(unittest.TestCase):
    """End to end through main(), with no QI-Core file and no implementation issues."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def _write(self, name, text):
        path = os.path.join(self.tmp, name)
        with open(path, "w", newline="") as f:
            f.write(text)
        return path

    def _run(self, expected_rows, actual_rows, known_text):
        e = self._write("e.csv", _csv(*expected_rows))
        a = self._write("a.csv", _csv(*actual_rows))
        known = self._write("known.csv", known_text)
        self.output = os.path.join(self.tmp, "o.csv")
        report = os.path.join(self.tmp, "r.md")
        main(e, a, self.output, report, measure_resource_dir=self.tmp,
             qicore_file=os.path.join(self.tmp, "no-qicore.csv"),
             implementation_file=os.path.join(self.tmp, "no-impl.csv"),
             known_issues_file=known)
        with open(report) as f:
            return f.read()

    def test_ticketed_issue_resolves_the_case(self):
        text = self._run(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_B},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_B},Group_1:Denominator,1"],
            KNOWN_HEADER + f"I-18,translator,{TICKET},{MEASURE},{GUID_A},,Overload ambiguity.\n")
        self.assertIn("| Known Issue Count (ticketed) | 1 (50.00%) |", text)
        self.assertIn("| Fail Count | 0 (0.00%) |", text)
        self.assertIn("| Resolved (Pass + Known Issue) | 2 (100.00%) |", text)
        self.assertNotIn("needs ticket", text)
        self.assertIn("## Known Issues (1 test case, 1 issue)", text)
        self.assertIn("| I-18 | `translator` | "
                      f"[cqframework/clinical_quality_language#1855]({TICKET}) | 1 | "
                      "Resolved (ticketed) |", text)
        self.assertIn(f"  - {MEASURE} {GUID_A} (Group_1: Denominator 1 → MISSING): "
                      "Overload ambiguity.\n", text)
        self.assertIn("| Measures with Discrepancies | 0 |", text)
        with open(self.output, newline="") as f:
            results = {r["guid"]: r["result"] for r in csv.DictReader(f)}
        self.assertEqual(results, {GUID_A: KNOWN_ISSUE, GUID_B: "PASS"})

    def test_unticketed_issue_stays_failing_and_is_labelled(self):
        text = self._run(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_B},Group_1:Denominator,1",
             f"{MEASURE},{GUID_C},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_B},Group_1:Denominator,0",
             f"{MEASURE},{GUID_C},Group_1:Denominator,0"],
            KNOWN_HEADER + f"I-26,engine,,{MEASURE},{GUID_A},,\n"
            + f"I-26,engine,,{MEASURE},{GUID_B},,\n")
        self.assertIn("| Fail Count | 3 (100.00%) |", text)
        self.assertIn("| &nbsp;&nbsp;— known issue, needs ticket | 2 (66.67%) |", text)
        self.assertNotIn("Known Issue Count (ticketed)", text)
        self.assertNotIn("Resolved (", text)
        self.assertIn("| I-26 | `engine` | — | 2 | **Needs ticket** |", text)
        self.assertIn("_1 issue (I-26) needs a ticket before its test cases count as "
                      "resolved._", text)
        # Still in the discrepancy tables, with the issue named; GUID_C has none.
        self.assertIn(f"| {GUID_A} | Group_1 | I-26 (needs ticket) |", text)
        self.assertRegex(text, rf"\| {GUID_B} \| Group_1 \| Denominator \| 1 \| 0 \| "
                               r"I-26 \(needs ticket\) \|")
        self.assertRegex(text, rf"\| {GUID_C} \| Group_1 \| Denominator \| 1 \| 0 \|  \|")
        with open(self.output, newline="") as f:
            results = {r["guid"]: r["result"] for r in csv.DictReader(f)}
        self.assertEqual(set(results.values()), {"FAIL"})

    def test_no_failing_covered_case_renders_no_section(self):
        text = self._run(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            KNOWN_HEADER + f"I-26,engine,,{MEASURE},{GUID_A},,\n")
        self.assertNotIn("## Known Issues", text)
        self.assertNotIn("Known Issue", text.split("\n_Known issues are tracked")[0])


class HelpersTest(unittest.TestCase):

    def test_ticket_link(self):
        self.assertEqual(ticket_link(TICKET),
                         f"[cqframework/clinical_quality_language#1855]({TICKET})")
        self.assertEqual(ticket_link(""), "")

    def test_sort_issue_ids_is_numeric(self):
        self.assertEqual(sort_issue_ids(["I-26", "I-9", "I-100"]), ["I-9", "I-26", "I-100"])


if __name__ == "__main__":
    unittest.main()
