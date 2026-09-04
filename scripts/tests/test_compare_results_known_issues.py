import os
import shutil
import tempfile
import unittest

from scripts.compare_results import (
    archive_report,
    diff_actual_results,
    exclude_pending_rows,
    known_issue_label,
    qicore_row_outcome,
    render_engine_diff_section,
    row_outcome,
    scores,
    scores_by_measure,
    test_case_outcomes,
    write_engine_diff_csv,
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

    def test_counts_case_once_when_multiple_populations_mismatch(self):
        expected = {
            ("m1", "g-a", "g1:Denominator"): "1",
            ("m1", "g-a", "g1:Numerator"): "1",
            ("m1", "g-b", "g1:Denominator"): "1",
        }
        actual = {
            ("m1", "g-a", "g1:Denominator"): "0",
            ("m1", "g-a", "g1:Numerator"): "0",
            ("m1", "g-b", "g1:Denominator"): "1",
        }
        p, f = scores(expected, actual)
        self.assertEqual((p, f), (1, 1))

    def test_scores_by_measure_counts_distinct_test_cases(self):
        expected = {
            ("m1", "g-a", "g1:Denominator"): "1",
            ("m1", "g-a", "g1:Numerator"): "1",
            ("m1", "g-b", "g1:Denominator"): "1",
            ("m2", "g-c", "g1:Denominator"): "1",
        }
        actual = {
            ("m1", "g-a", "g1:Denominator"): "0",
            ("m1", "g-a", "g1:Numerator"): "0",
            ("m1", "g-b", "g1:Denominator"): "1",
            ("m2", "g-c", "g1:Denominator"): "0",
        }
        self.assertEqual(scores_by_measure(expected, actual), {"m1": (1, 1), "m2": (0, 1)})

    def test_test_case_outcomes_marks_case_fail_until_a_pass_cell(self):
        expected = {
            ("m1", "g-a", "g1:Denominator"): "1",
            ("m1", "g-a", "g1:Numerator"): "1",
        }
        actual = {
            ("m1", "g-a", "g1:Denominator"): "0",
            ("m1", "g-a", "g1:Numerator"): "1",
        }
        self.assertEqual(test_case_outcomes(expected, actual), {("m1", "g-a"): "FAIL"})


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


class EngineDiffTest(unittest.TestCase):

    def setUp(self):
        self.cms = {
            ("m1", "g-a", "g1:Denominator"): "1",
            ("m1", "g-b", "g1:Numerator"): "1",
            ("m1", "g-c", "g1:Denominator"): "0",
        }
        self.qi = {
            ("m1", "g-a", "g1:Denominator"): "1",   # match
            ("m1", "g-b", "g1:Numerator"): "0",     # mismatch
            ("m1", "g-d", "g1:Numerator"): "1",     # qicore-only
        }

    def test_classifies_match_mismatch_cms_only_qicore_only(self):
        diff = diff_actual_results(self.cms, self.qi)
        self.assertIn("m1", diff)
        self.assertEqual(diff["m1"]["match"], 1)  # g-a
        self.assertEqual(diff["m1"]["mismatch"], [(("m1", "g-b", "g1:Numerator"), "1", "0")])
        self.assertEqual(diff["m1"]["cms_only"], [("m1", "g-c", "g1:Denominator")])
        self.assertEqual(diff["m1"]["qicore_only"], [("m1", "g-d", "g1:Numerator")])

    def test_qicore_is_reference(self):
        # A row present only in QI-Core is qicore-only (missing CMS population),
        # and a row present only in CMS is cms-only.
        diff = diff_actual_results(self.cms, self.qi)
        self.assertEqual(len(diff["m1"]["qicore_only"]), 1)
        self.assertEqual(len(diff["m1"]["cms_only"]), 1)
        self.assertEqual(len(diff["m1"]["mismatch"]), 1)

    def test_empty_inputs(self):
        self.assertEqual(diff_actual_results({}, {}), {})

    def test_identical_rows_all_match(self):
        rows = {("m1", "g-a", "g1:Denominator"): "1",
                ("m1", "g-b", "g1:Numerator"): "0"}
        diff = diff_actual_results(rows, dict(rows))
        self.assertEqual(diff["m1"]["match"], 2)
        self.assertEqual(diff["m1"]["mismatch"], [])
        self.assertEqual(diff["m1"]["cms_only"], [])
        self.assertEqual(diff["m1"]["qicore_only"], [])

    def test_render_section_contains_measure_and_totals(self):
        diff = diff_actual_results(self.cms, self.qi)
        section = "".join(render_engine_diff_section(diff))
        self.assertIn("## Engine Diff", section)
        self.assertIn("m1", section)
        self.assertIn("g-b", section)
        self.assertRegex(section, r"\*\*1\*\*")  # total columns present

    def test_render_empty_returns_nothing(self):
        self.assertEqual(render_engine_diff_section({}), [])

    def test_write_engine_diff_csv(self):
        diff = diff_actual_results(self.cms, self.qi)
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        path = os.path.join(tmp, "diff.csv")
        write_engine_diff_csv(diff, path)
        with open(path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        self.assertEqual(lines[0],
                         "measure_name,guid,population,cms_count,qicore_count,diff_type")
        self.assertEqual(len(lines), 4)  # header + 1 mismatch + 1 cms-only + 1 qicore-only
        self.assertIn("m1,g-b,g1:Numerator,1,0,mismatch", lines)
        self.assertIn("m1,g-c,g1:Denominator,,,cms-only", lines)
        self.assertIn("m1,g-d,g1:Numerator,,,qicore-only", lines)


class QICoreRowOutcomeTest(unittest.TestCase):

    def setUp(self):
        self.expected = {
            ("m1", "g-a", "g1:Denominator"): "1",
            ("m1", "g-b", "g1:Numerator"): "1",
        }
        self.qicore = {
            ("m1", "g-a", "g1:Denominator"): "1",   # match -> PASS
            ("m1", "g-b", "g1:Numerator"): "0",     # mismatch -> FAIL
        }

    def test_pass_when_qicore_matches_expected(self):
        self.assertEqual(
            qicore_row_outcome(self.expected, self.qicore, "m1", "g-a", "g1", "Denominator"),
            "PASS",
        )

    def test_fail_when_qicore_mismatches_expected(self):
        self.assertEqual(
            qicore_row_outcome(self.expected, self.qicore, "m1", "g-b", "g1", "Numerator"),
            "FAIL",
        )

    def test_na_when_qicore_has_no_result(self):
        self.assertEqual(
            qicore_row_outcome(self.expected, self.qicore, "m1", "g-unknown", "g1", "Denominator"),
            "N/A",
        )

    def test_na_when_expected_key_absent(self):
        self.assertEqual(
            qicore_row_outcome(self.expected, self.qicore, "m1", "g-a", "g1", "BogusPopulation"),
            "N/A",
        )

    def test_na_when_qicore_rows_empty(self):
        self.assertEqual(
            qicore_row_outcome(self.expected, {}, "m1", "g-a", "g1", "Denominator"),
            "N/A",
        )


if __name__ == "__main__":
    unittest.main()
