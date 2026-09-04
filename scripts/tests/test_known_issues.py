import json
import os
import tempfile
import unittest

from scripts.comparison.known_issues import (
    DEFAULT_CATALOG_PATH,
    affected_measure_guid_pairs,
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
            "resolved": False,
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
            "resolved": True,
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
    """The catalog has historically accumulated a few `resolved` values written
    as JSON strings ("true"/"false") instead of booleans.  Because Python's
    `not "false"` is False (non-empty string is truthy), the entries end up
    in `resolved_issues` even when the author clearly meant pending.  Lock the
    catalog to boolean values so `pending_issues` reflects intent."""

    def test_repo_catalog_uses_boolean_resolved(self):
        catalog = load_catalog()
        offenders = [i["id"] for i in catalog["issues"]
                     if not isinstance(i.get("resolved"), bool)]
        self.assertFalse(
            offenders,
            f"{offenders}: 'resolved' must be a JSON boolean, not a string "
            "(truthy strings defeat pending_issues).",
        )


if __name__ == "__main__":
    unittest.main()
