"""End-to-end test for the run_reports.py entry point.

This is the "one daily command" that replaced running compare_results.py and
generate_engine_issues_doc.py separately -- the split invocation is exactly
how engine-issues.md and known_issues.json forked in the first place (the
person who edited the catalog had no reason to know a second script needed
running). The test proves the combined command produces a report, regenerates
the tracker, and leaves both consistent with each other on a minimal fixture.
"""
import csv
import json
import os
import tempfile
import unittest

from scripts.run_reports import main


def _write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["measure_name", "guid", "population", "count"])
        w.writerows(rows)


class RunReportsEndToEndTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.expected = os.path.join(self.tmp, "expected.csv")
        self.actual = os.path.join(self.tmp, "actual.csv")
        self.output = os.path.join(self.tmp, "output_results.csv")
        self.report = os.path.join(self.tmp, "discrepancy_report.md")
        self.known_issues = os.path.join(self.tmp, "known_issues.json")
        self.engine_issues_out = os.path.join(self.tmp, "engine-issues.md")
        self.improvement_tracking_out = os.path.join(self.tmp, "improvement-tracking.md")

        _write_csv(self.expected, [
            ("CMS1", "g1", "Group_1:Initial Population", "1"),
            ("CMS1", "g1", "Group_1:Denominator", "1"),
            ("CMS1", "g1", "Group_1:Numerator", "1"),
        ])
        _write_csv(self.actual, [
            ("CMS1", "g1", "Group_1:Initial Population", "1"),
            ("CMS1", "g1", "Group_1:Denominator", "1"),
            ("CMS1", "g1", "Group_1:Numerator", "1"),
        ])
        with open(self.known_issues, "w", encoding="utf-8") as fh:
            json.dump({
                "schema_version": 1,
                "preamble_md": "# Engine / Translator Issues Tracker\n",
                "cross_cutting_lessons_md": "## Cross-Cutting Lessons\n",
                "issues": [],
            }, fh)

    def _run(self, **overrides):
        # --run-history-path is mandatory in every call here: without it,
        # compare_results.main() falls back to the real
        # scripts/comparison/run-history.jsonl, and this test suite would
        # pollute the repo's actual run history with fixture-sized fake runs
        # every time it executes (caught in review -- see the retrofit note
        # in run_reports.py's --run-history-path help text).
        # --skip-catalog-build keeps these tests hermetic: they exercise the
        # report pipeline on a fixture known_issues.json, not the real
        # defect-tracking/issues/ tree (the compile wiring has its own test,
        # test_catalog_is_compiled_before_compare_results).
        args = [
            "--expected", self.expected,
            "--actual", self.actual,
            "--output", self.output,
            "--report", self.report,
            "--known-issues", self.known_issues,
            "--qicore-actual", os.path.join(self.tmp, "nonexistent-qicore.csv"),
            "--qicore-diff-csv", os.path.join(self.tmp, "qicore_diff.csv"),
            "--engine-issues-output", self.engine_issues_out,
            "--improvement-tracking-output", self.improvement_tracking_out,
            "--run-history-path", os.path.join(self.tmp, "run-history.jsonl"),
            "--skip-catalog-build",
        ]
        return main(args)

    def test_exits_zero_on_a_clean_run(self):
        self.assertEqual(self._run(), 0)

    def test_writes_all_four_outputs(self):
        self._run()
        self.assertTrue(os.path.exists(self.output))
        self.assertTrue(os.path.exists(self.report))
        self.assertTrue(os.path.exists(self.engine_issues_out))
        self.assertTrue(os.path.exists(self.improvement_tracking_out))

    def test_improvement_tracking_is_generated_from_the_run_history(self):
        """Step 2 must regenerate improvement-tracking.md from the log that
        step 1 appended to -- not from the repo's real run-history.jsonl."""
        self._run()
        with open(self.improvement_tracking_out, encoding="utf-8") as fh:
            content = fh.read()
        self.assertTrue(content.startswith(
            "<!-- GENERATED from scripts/comparison/run-history.jsonl"))
        self.assertIn("Runs recorded: 1", content)

    def test_engine_issues_doc_is_generated_from_the_same_catalog(self):
        """The exact failure mode this script exists to prevent: the doc and
        the catalog must never be produced by only one of the two steps."""
        self._run()
        with open(self.engine_issues_out, encoding="utf-8") as fh:
            content = fh.read()
        self.assertTrue(content.startswith(
            "<!-- GENERATED from scripts/comparison/known_issues.json"))

    def test_report_reflects_a_passing_case(self):
        self._run()
        with open(self.report, encoding="utf-8") as fh:
            content = fh.read()
        self.assertIn("Passing Test Cases", content)
        self.assertIn("1 (100.00%)", content)

    def test_skip_drift_check_flag_still_writes_output(self):
        args = [
            "--expected", self.expected,
            "--actual", self.actual,
            "--output", self.output,
            "--report", self.report,
            "--known-issues", self.known_issues,
            "--qicore-actual", os.path.join(self.tmp, "nonexistent-qicore.csv"),
            "--qicore-diff-csv", os.path.join(self.tmp, "qicore_diff.csv"),
            "--engine-issues-output", self.engine_issues_out,
            "--improvement-tracking-output", self.improvement_tracking_out,
            "--run-history-path", os.path.join(self.tmp, "run-history.jsonl"),
            "--skip-drift-check",
        ]
        self.assertEqual(main(args), 0)
        self.assertTrue(os.path.exists(self.report))

    def test_catalog_is_compiled_before_compare_results(self):
        """Step 0 must compile known_issues.json from the authored issue tree,
        so compare_results consumes the freshly-compiled catalog even when the
        on-disk JSON was stale or absent."""
        issues_dir = os.path.join(self.tmp, "issues")
        os.makedirs(issues_dir)
        with open(os.path.join(issues_dir, "_preamble.md"), "w", encoding="utf-8") as fh:
            fh.write("+++\nschema_version = 1\ngenerated_from = \"test/\"\n+++\n"
                     "# Preamble\n")
        with open(os.path.join(issues_dir, "_cross_cutting_lessons.md"), "w",
                  encoding="utf-8") as fh:
            fh.write("## Cross-Cutting\n")
        with open(os.path.join(issues_dir, "_manifest.txt"), "w", encoding="utf-8") as fh:
            fh.write("# test manifest\nT-01\n")
        with open(os.path.join(issues_dir, "cases.csv"), "w", newline="",
                  encoding="utf-8") as fh:
            fh.write("issue_id,measure,guid\n")
        with open(os.path.join(issues_dir, "T-01.md"), "w", encoding="utf-8") as fh:
            fh.write(
                "+++\n"
                "id = \"T-01\"\n"
                "title = \"Test issue\"\n"
                "category = \"engine\"\n"
                "status = \"**Confirmed**\"\n"
                "defect_status = \"confirmed\"\n"
                "root_cause_status = \"open\"\n"
                "affected_measures = []\n"
                "+++\n"
                "### T-01: test body\n")

        args = [
            "--expected", self.expected,
            "--actual", self.actual,
            "--output", self.output,
            "--report", self.report,
            "--known-issues", self.known_issues,
            "--qicore-actual", os.path.join(self.tmp, "nonexistent-qicore.csv"),
            "--qicore-diff-csv", os.path.join(self.tmp, "qicore_diff.csv"),
            "--engine-issues-output", self.engine_issues_out,
            "--improvement-tracking-output", self.improvement_tracking_out,
            "--run-history-path", os.path.join(self.tmp, "run-history.jsonl"),
            "--issues-dir", issues_dir,
        ]
        self.assertEqual(main(args), 0)

        with open(self.known_issues, encoding="utf-8") as fh:
            catalog = json.load(fh)
        self.assertEqual(catalog["schema_version"], 1)
        self.assertEqual(catalog["generated_from"], "test/")
        self.assertEqual([i["id"] for i in catalog["issues"]], ["T-01"])
        self.assertEqual(catalog["issues"][0]["body_md"], "### T-01: test body\n")

    def test_run_history_is_written_to_the_override_path_not_the_real_one(self):
        """Regression guard for the pollution bug this test file caused on
        first write: a run with a temp --run-history-path must never touch
        scripts/comparison/run-history.jsonl."""
        from scripts.comparison.run_history import DEFAULT_PATH
        from scripts.comparison.generate_improvement_tracking import DEFAULT_OUTPUT
        before = (DEFAULT_PATH.read_text(encoding="utf-8")
                 if DEFAULT_PATH.exists() else None)
        improvement_before = (DEFAULT_OUTPUT.read_text(encoding="utf-8")
                              if DEFAULT_OUTPUT.exists() else None)
        self._run()
        after = (DEFAULT_PATH.read_text(encoding="utf-8")
                 if DEFAULT_PATH.exists() else None)
        improvement_after = (DEFAULT_OUTPUT.read_text(encoding="utf-8")
                             if DEFAULT_OUTPUT.exists() else None)
        self.assertEqual(before, after)
        self.assertEqual(improvement_before, improvement_after)
        self.assertTrue(os.path.exists(
            os.path.join(self.tmp, "run-history.jsonl")))


if __name__ == "__main__":
    unittest.main()
