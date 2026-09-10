"""Tests for the `--detailed` discrepancy-report variant.

The default (slim) report drops three sections that only add noise to a
one-page summary -- `## Attribution Health` (with its Stale attributions and
Repro-scaffold references subsections), `## Known Issues (resolution-pending)`,
and `## Measures with No Discrepancies`. `--detailed` keeps the full report
"like we have now", written so both files can coexist:
``discrepancy_report.md`` (slim) + ``discrepancy_report-detailed.md`` (full).
"""
import csv
import json
import os
import tempfile
import unittest

from scripts.compare_results import (
    _archive_stamp_key,
    archive_report,
    capture_results,
    detailed_report_path,
    generate_comparison_report,
    main,
)


def _write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["measure_name", "guid", "population", "count"])
        w.writerows(rows)


_PASS_ROWS = [
    ("CMS1", "g-pass", "Group_1:Initial Population", "1"),
    ("CMS1", "g-pass", "Group_1:Denominator", "1"),
    ("CMS1", "g-pass", "Group_1:Numerator", "1"),
]
_FAIL_ROWS = [
    ("CMS2", "g-fail", "Group_1:Initial Population", "1"),
    ("CMS2", "g-fail", "Group_1:Denominator", "1"),
    ("CMS2", "g-fail", "Group_1:Numerator", "1"),
]

_KISSUES = {
    "schema_version": 1,
    "preamble_md": "# Engine / Translator Issues Tracker\n",
    "cross_cutting_lessons_md": "## Cross-Cutting Lessons\n",
    "issues": [
        {
            "id": "E-99",
            "title": "Fixture issue",
            "category": "engine",
            "status": "**Under investigation**",
            "defect_status": "suspected",
            "affected_measures": ["CMS1"],
            "affected_test_cases": [{"measure": "CMS1", "guid": "g-pass"}],
        }
    ],
}


class DetailedReportFixtureTest(unittest.TestCase):
    """Unit tests for generate_comparison_report's `detailed` gate."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.slim = os.path.join(self.tmp, "discrepancy_report.md")
        self.full = os.path.join(self.tmp, "discrepancy_report-detailed.md")

        expected_csv = os.path.join(self.tmp, "expected.csv")
        actual_csv = os.path.join(self.tmp, "actual.csv")
        _write_csv(expected_csv, _PASS_ROWS + _FAIL_ROWS)
        _write_csv(actual_csv, _PASS_ROWS + [
            (m, g, p, "0") for m, g, p, _ in _FAIL_ROWS
        ])

        self.expected = capture_results(expected_csv)
        self.actual = capture_results(actual_csv)
        self.issues = _KISSUES["issues"]

    def _render(self, path, detailed):
        return generate_comparison_report(
            path,
            self.expected.groups,
            self.actual.groups,
            pass_count=3,
            fail_count=3,
            issues=self.issues,
            expected_rows=self.expected.rows,
            actual_rows=self.actual.rows,
            engine_diff=None,
            qicore_rows=None,
            qicore_groups=None,
            unscored_cells=self.expected.unscored,
            detailed=detailed,
        )

    def read(self, path):
        with open(path, encoding="utf-8") as fh:
            return fh.read()

    def test_slim_report_omits_the_three_sections(self):
        self._render(self.slim, detailed=False)
        text = self.read(self.slim)
        self.assertIn("## Measures with Discrepancies", text)
        self.assertNotIn("## Attribution Health", text)
        self.assertNotIn("## Known Issues (resolution-pending)", text)
        self.assertNotIn("## Measures with No Discrepancies", text)
        self.assertNotIn("## CMS vs QICore Comparison", text)

    def test_detailed_report_includes_the_three_sections(self):
        self._render(self.full, detailed=True)
        text = self.read(self.full)
        self.assertIn("## Attribution Health", text)
        self.assertIn("## Known Issues (resolution-pending)", text)
        self.assertIn("## Measures with No Discrepancies", text)


class DetailedReportQICoreTest(unittest.TestCase):
    """`## CMS vs QICore Comparison` follows the same detailed gate."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.slim = os.path.join(self.tmp, "discrepancy_report.md")
        self.full = os.path.join(self.tmp, "discrepancy_report-detailed.md")

        expected_csv = os.path.join(self.tmp, "expected.csv")
        actual_csv = os.path.join(self.tmp, "actual.csv")
        _write_csv(expected_csv, _PASS_ROWS + _FAIL_ROWS)
        _write_csv(actual_csv, _PASS_ROWS + [
            (m, g, p, "0") for m, g, p, _ in _FAIL_ROWS
        ])

        self.expected = capture_results(expected_csv)
        self.actual = capture_results(actual_csv)
        # Mirror CMS groups so the QI-Core side has something to compare against.
        self.qicore = capture_results(actual_csv)

    def _render(self, path, detailed):
        return generate_comparison_report(
            path,
            self.expected.groups,
            self.actual.groups,
            pass_count=3,
            fail_count=3,
            issues=[],
            expected_rows=self.expected.rows,
            actual_rows=self.actual.rows,
            engine_diff=None,
            qicore_rows=self.qicore.rows,
            qicore_groups=self.qicore.groups,
            unscored_cells=self.expected.unscored,
            detailed=detailed,
        )

    def test_slim_report_omits_cms_vs_qicore_comparison(self):
        self._render(self.slim, detailed=False)
        with open(self.slim, encoding="utf-8") as fh:
            self.assertNotIn("## CMS vs QICore Comparison", fh.read())

    def test_detailed_report_includes_cms_vs_qicore_comparison(self):
        self._render(self.full, detailed=True)
        with open(self.full, encoding="utf-8") as fh:
            self.assertIn("## CMS vs QICore Comparison", fh.read())


class DetailedReportMainTest(unittest.TestCase):
    """main(detailed=True) writes slim + -detailed sibling and archives both."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.expected_csv = os.path.join(self.tmp, "expected.csv")
        self.actual_csv = os.path.join(self.tmp, "actual.csv")
        self.output_csv = os.path.join(self.tmp, "output.csv")
        self.report = os.path.join(self.tmp, "discrepancy_report.md")
        self.known_issues = os.path.join(self.tmp, "known_issues.json")
        self.run_history = os.path.join(self.tmp, "run-history.jsonl")

        _write_csv(self.expected_csv, _PASS_ROWS + _FAIL_ROWS)
        _write_csv(self.actual_csv, _PASS_ROWS + [
            (m, g, p, "0") for m, g, p, _ in _FAIL_ROWS
        ])
        with open(self.known_issues, "w", encoding="utf-8") as fh:
            json.dump(_KISSUES, fh)

    def test_main_writes_both_reports_when_detailed(self):
        main(
            self.expected_csv,
            self.actual_csv,
            self.output_csv,
            self.report,
            self.known_issues,
            qicore_actual_file=os.path.join(self.tmp, "missing_qicore.csv"),
            run_history_path=self.run_history,
            detailed=True,
        )
        self.assertTrue(os.path.exists(self.report))
        full = detailed_report_path(self.report)
        self.assertTrue(os.path.exists(full))
        with open(self.report, encoding="utf-8") as fh:
            slim_text = fh.read()
        self.assertNotIn("## Attribution Health", slim_text)
        with open(full, encoding="utf-8") as fh:
            full_text = fh.read()
        self.assertIn("## Attribution Health", full_text)

    def test_archive_ranks_detailed_archives_by_embedded_stamp(self):
        archive_dir = os.path.join(self.tmp, "_archive")
        os.makedirs(archive_dir, exist_ok=True)
        for name in ("discrepancy_report-detailed-20260902-1200.md",
                     "discrepancy_report-detailed-20260901-0900.md"):
            with open(os.path.join(archive_dir, name), "w",
                      encoding="utf-8") as fh:
                fh.write(name)
        keys = sorted(
            (_archive_stamp_key(n), n) for n in os.listdir(archive_dir))
        self.assertEqual([key for key, _ in keys],
                         ["20260901-0900", "20260902-1200"])


if __name__ == "__main__":
    unittest.main()