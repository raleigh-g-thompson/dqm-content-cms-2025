import unittest

from scripts.generate_failure_tracker import (
    Measure,
    TestCase,
    apply_known_root_causes,
    resolution_for_case,
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


if __name__ == "__main__":
    unittest.main()
