import os
import tempfile
import unittest

from pathlib import Path

from scripts.generate_failure_tracker import (
    Measure,
    TestCase,
    apply_known_root_causes,
    derive_output_path,
    resolution_for_case,
    split_reports_to_process,
)

ISSUES = [
    {
        "id": "E-11",
        "title": "`Unable to extract codes`",
        "category": "engine",
        "resolved": False,
        "affected_test_cases": [
            {"measure": "CMS123", "guid": "guid-1"},
        ],
    },
    {
        "id": "F-01",
        "title": "Historical fixture issue",
        "category": "fixture",
        "resolved": True,
        "affected_test_cases": [
            {"measure": "CMS123", "guid": "guid-1"},
        ],
    },
]


def measure(name, cases):
    m = Measure(name=name)
    m.cases = [TestCase(guid=g, group="Group_1", failure_type="MR") for g in cases]
    return m


class ResolutionForCaseTest(unittest.TestCase):

    def test_unresolved_issue_labels_pending(self):
        self.assertEqual(
            resolution_for_case(ISSUES, "CMS123", "guid-1"),
            "E-11 — `Unable to extract codes` (resolution pending)",
        )

    def test_resolved_only_labels_historical(self):
        issues = [ISSUES[1]]
        self.assertEqual(
            resolution_for_case(issues, "CMS123", "guid-1"),
            "F-01 (resolved — historical)",
        )

    def test_unmatched_case_is_unclassified(self):
        self.assertEqual(
            resolution_for_case(ISSUES, "CMS123", "guid-x"),
            "_pending_ (unclassified)",
        )


class ApplyKnownRootCausesTest(unittest.TestCase):

    def test_catalog_drives_resolution_when_issues_given(self):
        measures = {"CMS123": measure("CMS123", ["guid-1", "guid-unmatched"])}
        apply_known_root_causes(measures, ISSUES)
        by_guid = {c.guid: c.resolution for c in measures["CMS123"].cases}
        self.assertIn("E-11", by_guid["guid-1"])
        self.assertEqual(by_guid["guid-unmatched"], "_pending_ (unclassified)")

    def test_does_not_crash_with_empty_issues(self):
        measures = {"CMS123": measure("CMS123", ["guid-1"])}
        apply_known_root_causes(measures, [])
        self.assertEqual(measures["CMS123"].cases[0].resolution, "_pending_")


class SplitReportsToProcessTest(unittest.TestCase):

    def setUp(self):
        self._tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _touch(self, name):
        p = self._tmp / name
        p.write_text("", encoding="utf-8")
        return p

    def test_live_report_always_regenerated_even_when_output_exists(self):
        live = self._touch("discrepancy_report.md")
        self._touch("measure-failure-report.md")
        always, to_process, skipped = split_reports_to_process([live])
        self.assertEqual(always, [live])
        self.assertEqual(to_process, [])
        self.assertEqual(skipped, [])

    def test_timestamped_report_skipped_when_output_exists(self):
        ts = self._touch("discrepancy_report-20260902-1658.md")
        self._touch("measure-failure-report-20260902-1658.md")
        always, to_process, skipped = split_reports_to_process([ts])
        self.assertEqual(always, [])
        self.assertEqual(to_process, [])
        self.assertEqual(skipped, [ts])

    def test_timestamped_report_processed_when_output_missing(self):
        ts = self._touch("discrepancy_report-20260902-1658.md")
        always, to_process, skipped = split_reports_to_process([ts])
        self.assertEqual(to_process, [ts])
        self.assertEqual(skipped, [])

    def test_mixed_partition_and_output_derivation(self):
        live = self._touch("discrepancy_report.md")
        self._touch("measure-failure-report.md")
        existing_ts = self._touch("discrepancy_report-20260902-1658.md")
        self._touch("measure-failure-report-20260902-1658.md")
        new_ts = self._touch("discrepancy_report-20260902-1712.md")
        always, to_process, skipped = split_reports_to_process([live, existing_ts, new_ts])
        self.assertEqual(always, [live])
        self.assertEqual(to_process, [new_ts])
        self.assertEqual(skipped, [existing_ts])
        self.assertEqual(derive_output_path(live).name, "measure-failure-report.md")
        self.assertEqual(derive_output_path(new_ts).name,
                         "measure-failure-report-20260902-1712.md")


if __name__ == "__main__":
    unittest.main()
