"""End-to-end test for the run_reports.py entry point.

This is the "one daily command" that replaced running compare_results.py and
generate_engine_issues_doc.py separately -- the split invocation is exactly
how engine-issues.md and known_issues.json forked in the first place (the
person who edited the catalog had no reason to know a second script needed
running). The test proves the combined command produces a report, regenerates
the tracker, and leaves both consistent with each other on a minimal fixture.

Since the extraction step (0/6) was folded in, every fixture-driven test
passes --skip-extract so the hand-written actual.csv isn't clobbered by
extract_population_actual.main() reading the real ./input/tests/results/
during a pytest run. The dedicated test_extract_feeds_compare_from_fresh_results
test exercises the extraction wiring end-to-end with a temp results dir.
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
        self.cql_changes = os.path.join(self.tmp, "cql-changes.jsonl")
        self.updated_cql_out = os.path.join(self.tmp, "updated-cql.md")

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
        # --skip-extract keeps these tests hermetic against step 0/6: without
        # it the orchestrator would read the real ./input/tests/results/
        # during a pytest run and overwrite the hand-written --actual fixture.
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
            "--cql-changes-path", self.cql_changes,
            "--updated-cql-output", self.updated_cql_out,
            "--cql-changes-path", self.cql_changes,
            "--updated-cql-output", self.updated_cql_out,
            "--skip-catalog-build",
            "--skip-extract",
        ]
        return main(args)

    def test_exits_zero_on_a_clean_run(self):
        self.assertEqual(self._run(), 0)

    def test_writes_all_five_outputs(self):
        self._run()
        self.assertTrue(os.path.exists(self.output))
        self.assertTrue(os.path.exists(self.report))
        self.assertTrue(os.path.exists(self.engine_issues_out))
        self.assertTrue(os.path.exists(self.improvement_tracking_out))
        self.assertTrue(os.path.exists(self.updated_cql_out))

    def test_updated_cql_is_generated_from_the_cql_change_log(self):
        """Step 4 must regenerate updated-cql.md from the cql-changes log the
        caller pointed at -- not the repo's real cql-changes.jsonl (which is
        exactly the hermeticity this test would violate if the wiring drifted)."""
        with open(self.cql_changes, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"id": "CQL-001", "date": "2026-09-09",
                                 "measure": "CMS190FHIRVTEProphylaxisICU",
                                 "title": "seed entry"}) + "\n")
        self._run()
        with open(self.updated_cql_out, encoding="utf-8") as fh:
            content = fh.read()
        self.assertTrue(content.startswith(
            "<!-- GENERATED from defect-tracking/cql-changes.jsonl"))
        self.assertIn("Entries recorded: 1", content)
        self.assertIn("CQL-001", content)

    def test_improvement_tracking_is_generated_from_the_run_history(self):
        """Step 3 must regenerate improvement-tracking.md from the log that
        step 2 appended to -- not from the repo's real run-history.jsonl."""
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
            "--cql-changes-path", self.cql_changes,
            "--updated-cql-output", self.updated_cql_out,
            "--skip-catalog-build",
            "--skip-extract",
            "--skip-drift-check",
        ]
        self.assertEqual(main(args), 0)
        self.assertTrue(os.path.exists(self.report))

    def test_catalog_is_compiled_before_compare_results(self):
        """Step 1 must compile known_issues.json from the authored issue tree,
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
            "--cql-changes-path", self.cql_changes,
            "--updated-cql-output", self.updated_cql_out,
            "--issues-dir", issues_dir,
            "--skip-extract",
        ]
        self.assertEqual(main(args), 0)

        with open(self.known_issues, encoding="utf-8") as fh:
            catalog = json.load(fh)
        self.assertEqual(catalog["schema_version"], 1)
        self.assertEqual(catalog["generated_from"], "test/")
        self.assertEqual([i["id"] for i in catalog["issues"]], ["T-01"])
        self.assertEqual(catalog["issues"][0]["body_md"], "### T-01: test body\n")

    def test_extract_feeds_compare_from_fresh_results(self):
        """Step 0/6 must read input/tests/results/ and write --actual before
        compare consumes it -- the orchestrator is the contract that ties
        extraction to the rest of the pipeline."""
        measure = "CMS74FHIRDentalCariesPrevention"
        guid = "11111111-1111-4111-8111-111111111111"
        results_dir = os.path.join(self.tmp, "results")
        measure_subdir = os.path.join(results_dir, measure)
        os.makedirs(measure_subdir)
        trace_path = os.path.join(measure_subdir, f"{guid}.txt")
        with open(trace_path, "w", encoding="utf-8") as fh:
            fh.write(
                "CQL: /tmp/cql\n"
                "Terminology: /tmp/valueset\n"
                "Test cases:\n"
                f"{guid} - /tmp/{measure}/{guid}\n"
                "\n"
                "Initial Population=true\n"
                "Denominator=true\n"
                "Numerator=true\n"
                f"Patient=Patient(id={guid})\n"
            )

        extracted_actual = os.path.join(self.tmp, "extracted_actual.csv")
        # Pre-populate --actual with junk so the assertion proves extraction
        # overwrote it rather than the orchestrator leaving it untouched.
        with open(extracted_actual, "w", encoding="utf-8") as fh:
            fh.write("measure_name,guid,population,count\nUNRELATED,0,Group_1:Initial Population,0\n")

        _write_csv(self.expected, [
            (measure, guid, "Group_1:Initial Population", "1"),
            (measure, guid, "Group_1:Denominator", "1"),
            (measure, guid, "Group_1:Numerator", "1"),
        ])

        args = [
            "--expected", self.expected,
            "--actual", extracted_actual,
            "--output", os.path.join(self.tmp, "output_results.csv"),
            "--report", os.path.join(self.tmp, "discrepancy_report.md"),
            "--known-issues", self.known_issues,
            "--qicore-actual", os.path.join(self.tmp, "nonexistent-qicore.csv"),
            "--qicore-diff-csv", os.path.join(self.tmp, "qicore_diff.csv"),
            "--engine-issues-output", self.engine_issues_out,
            "--improvement-tracking-output", self.improvement_tracking_out,
            "--run-history-path", os.path.join(self.tmp, "run-history.jsonl"),
            "--cql-changes-path", self.cql_changes,
            "--updated-cql-output", self.updated_cql_out,
            "--results-dir", results_dir,
            "--skip-catalog-build",
            "--skip-drift-check",
        ]
        self.assertEqual(main(args), 0)

        # The extracted CSV must contain a row for our measure, proving the
        # orchestrator wrote it via extract_population_actual (the hand-
        # written UNRELATED row above would fail this assertion if extraction
        # had been skipped).
        with open(extracted_actual, encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        measure_rows = [r for r in rows if r["measure_name"] == measure]
        self.assertGreater(len(measure_rows), 0)
        populations = {(r["guid"], r["population"]) for r in measure_rows}
        self.assertIn((guid, "Group_1:Initial Population"), populations)
        self.assertIn((guid, "Group_1:Denominator"), populations)
        self.assertIn((guid, "Group_1:Numerator"), populations)

        self.assertTrue(os.path.exists(os.path.join(self.tmp, "discrepancy_report.md")))

    def test_extract_skipped_when_results_dir_is_empty(self):
        """When --results-dir is empty or absent the orchestrator must NOT
        overwrite --actual; it prints a notice and proceeds using whatever
        is there. This guards the 'no fresh CQL run yet' state."""
        actual = os.path.join(self.tmp, "preserved_actual.csv")
        _write_csv(actual, [("CMS1", "g1", "Group_1:Initial Population", "1")])

        empty_results_dir = os.path.join(self.tmp, "empty_results")
        os.makedirs(empty_results_dir)

        args = [
            "--expected", self.expected,
            "--actual", actual,
            "--output", os.path.join(self.tmp, "output_results.csv"),
            "--report", os.path.join(self.tmp, "discrepancy_report.md"),
            "--known-issues", self.known_issues,
            "--qicore-actual", os.path.join(self.tmp, "nonexistent-qicore.csv"),
            "--qicore-diff-csv", os.path.join(self.tmp, "qicore_diff.csv"),
            "--engine-issues-output", self.engine_issues_out,
            "--improvement-tracking-output", self.improvement_tracking_out,
            "--run-history-path", os.path.join(self.tmp, "run-history.jsonl"),
            "--cql-changes-path", self.cql_changes,
            "--updated-cql-output", self.updated_cql_out,
            "--results-dir", empty_results_dir,
            "--skip-catalog-build",
            "--skip-drift-check",
        ]
        self.assertEqual(main(args), 0)

        with open(actual, encoding="utf-8") as fh:
            content = fh.read()
        self.assertIn("CMS1,g1,Group_1:Initial Population,1", content)

    def test_detailed_flag_writes_detailed_report_sibling(self):
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
            "--updated-cql-output", self.updated_cql_out,
            "--skip-catalog-build",
            "--skip-extract",
            "--detailed",
        ]
        self.assertEqual(main(args), 0)
        self.assertTrue(os.path.exists(self.report))
        detailed = os.path.join(self.tmp, "discrepancy_report-detailed.md")
        self.assertTrue(os.path.exists(detailed))

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
