import os
import shutil
import tempfile
import unittest

from scripts.compare_results import (
    archive_report,
    exclude_pending_rows,
    known_issue_label,
    row_outcome,
    scores,
)

PENDING = {("m1", "g-pending")}

EXPECTED = {
    ("m1", "g-pending", "g1:Denominator"): "1",
    ("m1", "g-good", "g1:Denominator"): "1",
}
ACTUAL = {
    ("m1", "g-pending", "g1:Denominator"): "0",
    ("m1", "g-good", "g1:Denominator"): "1",
}

ISSUES = [
    {
        "id": "E-11",
        "resolved": False,
        "affected_test_cases": [
            {"measure": "m1", "guid": "g-pending"},
        ],
    },
    {
        "id": "F-01",
        "resolved": True,
        "affected_test_cases": [
            {"measure": "m1", "guid": "g-pending"},
        ],
    },
]


class RowOutcomeTest(unittest.TestCase):

    def test_match_is_pass(self):
        self.assertEqual(row_outcome("1", "1"), ("PASS", "1"))

    def test_mismatch_is_fail(self):
        self.assertEqual(row_outcome("1", "0"), ("FAIL", "0"))

    def test_missing_actual_is_fail_with_missing_display(self):
        self.assertEqual(row_outcome("1", None), ("FAIL", "MISSING"))


class ScoresTest(unittest.TestCase):

    def test_counts_pass_and_fail(self):
        p, f = scores(EXPECTED, ACTUAL)
        self.assertEqual((p, f), (1, 1))


class ExcludePendingRowsTest(unittest.TestCase):

    def test_removes_all_groups_of_pending_case(self):
        e, a = exclude_pending_rows(EXPECTED, ACTUAL, PENDING)
        self.assertNotIn(("m1", "g-pending", "g1:Denominator"), e)
        self.assertNotIn(("m1", "g-pending", "g1:Denominator"), a)
        self.assertIn(("m1", "g-good", "g1:Denominator"), e)
        self.assertEqual(len(e), 1)

    def test_exclusion_flips_fail_to_pass_score(self):
        e, a = exclude_pending_rows(EXPECTED, ACTUAL, PENDING)
        p, f = scores(e, a)
        self.assertEqual((p, f), (1, 0))


class ArchiveReportTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.report = os.path.join(self.tmp, "discrepancy_report.md")
        with open(self.report, "w", encoding="utf-8") as fh:
            fh.write("# discrepancy")

    def test_creates_timestamped_copy_in_archive(self):
        self.setUp()
        dest = archive_report(self.report)
        self.assertIsNot(dest, None)
        self.assertTrue(dest.startswith(os.path.join(self.tmp, "_archive")))
        self.assertTrue(os.path.basename(dest).startswith("discrepancy_report-"))
        self.assertTrue(dest.endswith(".md"))
        with open(dest, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "# discrepancy")

    def test_missing_report_returns_none(self):
        self.assertIsNone(archive_report(os.path.join(self.tmp, "nope.md")))


class KnownIssueLabelTest(unittest.TestCase):

    def test_unresolved_case_labeled(self):
        self.assertEqual(known_issue_label(ISSUES, "m1", "g-pending"),
                         "E-11 — resolution pending")

    def test_resolved_issue_not_labeled(self):
        # g-pending is on both E-11 (unresolved) and F-01 (resolved); only the
        # unresolved one is reported.
        self.assertEqual(known_issue_label(ISSUES, "m1", "g-pending"),
                         "E-11 — resolution pending")

    def test_no_match_returns_emdash(self):
        self.assertEqual(known_issue_label(ISSUES, "m1", "g-unknown"), "—")

    def test_member_dash_when_only_resolved(self):
        issues = [ISSUES[1]]
        self.assertEqual(known_issue_label(issues, "m1", "g-pending"), "—")


if __name__ == "__main__":
    unittest.main()
