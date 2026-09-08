import json
import os
import tempfile
import unittest

from scripts.comparison.known_issues import (
    DEFAULT_CATALOG_PATH,
    DEFECT_STATUS_ALL,
    affected_measure_guid_pairs,
    is_resolved,
    issues_for_case,
    load_catalog,
    pending_case_set,
    pending_issues,
    resolved_issues,
)

SAMPLE = {
    "schema_version": 1,
    "issues": [
        {
            "id": "E-11",
            "title": "`Unable to extract codes from fhirType Reference`",
            "category": "engine",
            "status": "Confirmed",
            "defect_status": "confirmed",
            "root_cause_status": "open",
            "affected_measures": ["CMS135FHIRACEIorARBorARNIforHF"],
            "affected_test_cases": [
                {"measure": "CMS135FHIRACEIorARBorARNIforHF", "guid": "guid-1"},
                {"measure": "CMS135FHIRACEIorARBorARNIforHF", "guid": "guid-2"},
            ],
        },
        {
            "id": "F-01",
            "title": "Historical fixture issue",
            "category": "fixture",
            "status": "Resolved",
            "defect_status": "fixed-upstream",
            "root_cause_status": "resolved",
            "affected_measures": ["CMS135FHIRACEIorARBorARNIforHF"],
            "affected_test_cases": [
                {"measure": "CMS135FHIRACEIorARBorARNIforHF", "guid": "guid-1"},
            ],
        },
    ],
}


def write_catalog(catalog):
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "known_issues.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh)
    return path


class LoadCatalogTest(unittest.TestCase):

    def test_load_catalog_returns_empty_for_missing_path(self):
        catalog = load_catalog("/nonexistent/path/known_issues.json")
        self.assertEqual(catalog.get("issues"), [])

    def test_load_catalog_reads_file(self):
        path = write_catalog(SAMPLE)
        self.assertEqual(load_catalog(path)["issues"][0]["id"], "E-11")

    def test_default_path_is_under_comparison(self):
        self.assertEqual(DEFAULT_CATALOG_PATH.name, "known_issues.json")


class PendingResolvedTest(unittest.TestCase):

    def test_pending_issues_excludes_resolved(self):
        ids = {i["id"] for i in pending_issues(SAMPLE)}
        self.assertEqual(ids, {"E-11"})

    def test_resolved_issues_includes_only_resolved(self):
        ids = {i["id"] for i in resolved_issues(SAMPLE)}
        self.assertEqual(ids, {"F-01"})


class AffectedPairsTest(unittest.TestCase):

    def test_affected_pairs_from_dict_cases(self):
        pairs = affected_measure_guid_pairs(SAMPLE["issues"][0])
        self.assertIn(("CMS135FHIRACEIorARBorARNIforHF", "guid-1"), pairs)

    def test_affected_pairs_from_list_cases(self):
        issue = {"affected_test_cases": [["m1", "g1"], ["m2", "g2"]]}
        self.assertEqual(affected_measure_guid_pairs(issue),
                         [("m1", "g1"), ("m2", "g2")])


class PendingCaseSetTest(unittest.TestCase):

    def test_only_unresolved_issue_cases_are_pending(self):
        pending = pending_case_set(SAMPLE)
        self.assertIn(("CMS135FHIRACEIorARBorARNIforHF", "guid-1"), pending)
        self.assertIn(("CMS135FHIRACEIorARBorARNIforHF", "guid-2"), pending)
        # guid-1 is also listed under the resolved F-01, which must NOT add it
        # again and must NOT remove it (already pending from E-11). The resolved
        # issue alone contributes nothing new.
        self.assertEqual(len(pending), 2)

    def test_empty_catalog_yields_empty_set(self):
        self.assertEqual(pending_case_set({"issues": []}), set())


class IssuesForCaseTest(unittest.TestCase):

    def test_returns_all_issues_for_case_across_resolution(self):
        matches = issues_for_case(SAMPLE, "CMS135FHIRACEIorARBorARNIforHF", "guid-1")
        ids = {i["id"] for i in matches}
        self.assertEqual(ids, {"E-11", "F-01"})

    def test_no_match_returns_empty(self):
        self.assertEqual(issues_for_case(SAMPLE, "CMS999", "guid-x"), [])


class CatalogHygieneTest(unittest.TestCase):
    """Phase 4b: every catalog issue must carry a valid authored
    ``defect_status`` enum value. The old ``resolved`` boolean was a single
    axis that could not express "worked around but not actually fixed";
    ``defect_status`` is the enum that does. Lock the catalog to it."""

    def test_repo_catalog_uses_enum_defect_status(self):
        catalog = load_catalog()
        offenders = [i["id"] for i in catalog["issues"]
                     if i.get("defect_status") not in DEFECT_STATUS_ALL]
        self.assertFalse(
            offenders,
            f"{offenders}: 'defect_status' must be one of {sorted(DEFECT_STATUS_ALL)}",
        )


class IsResolvedTest(unittest.TestCase):
    """is_resolved is the seam for "is the underlying bug actually fixed". Only
    the resolved enum values count; worked-around-but-unfixed issues stay open.
    The legacy-ish ``resolved`` boolean/string fallback still maps sensibly so
    transitional/unit-test dicts never need to know about the migration."""

    def test_fixed_upstream_is_resolved(self):
        self.assertTrue(is_resolved({"defect_status": "fixed-upstream"}))

    def test_retired_is_resolved(self):
        self.assertTrue(is_resolved({"defect_status": "retired"}))

    def test_workaround_applied_stays_open(self):
        self.assertFalse(is_resolved({"defect_status": "workaround-applied"}))

    def test_confirmed_stays_open(self):
        self.assertFalse(is_resolved({"defect_status": "confirmed"}))

    def test_suspected_stays_open(self):
        self.assertFalse(is_resolved({"defect_status": "suspected"}))

    def test_missing_defaults_to_pending(self):
        self.assertFalse(is_resolved({}))

    def test_legacy_boolean_true_maps_to_resolved(self):
        self.assertTrue(is_resolved({"resolved": True}))

    def test_legacy_boolean_false_maps_to_confirmed(self):
        self.assertFalse(is_resolved({"resolved": False}))

    def test_legacy_string_true(self):
        self.assertTrue(is_resolved({"resolved": "true"}))

    def test_legacy_string_false(self):
        self.assertFalse(is_resolved({"resolved": "false"}))

    def test_legacy_string_mixed_case(self):
        self.assertTrue(is_resolved({"resolved": "True"}))
        self.assertFalse(is_resolved({"resolved": "FALSE"}))

    def test_legacy_int_zero(self):
        self.assertFalse(is_resolved({"resolved": 0}))

    def test_legacy_int_one(self):
        self.assertTrue(is_resolved({"resolved": 1}))

    def test_legacy_string_pending_filter(self):
        catalog = {"issues": [
            {"id": "E-string", "resolved": "false"},
            {"id": "F-string", "resolved": "true"},
        ]}
        pending_ids = {i["id"] for i in pending_issues(catalog)}
        resolved_ids = {i["id"] for i in resolved_issues(catalog)}
        self.assertEqual(pending_ids, {"E-string"})
        self.assertEqual(resolved_ids, {"F-string"})


if __name__ == "__main__":
    unittest.main()
