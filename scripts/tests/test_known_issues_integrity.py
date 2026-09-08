"""Loss guard for the curated issue catalog.

`known_issues.json` holds ~1,200 hand-curated (measure, guid) attributions that
exist nowhere else -- they are not derivable from the CSVs.  Two ways they have
been at risk:

  * `enrich_known_issues.py` (removed) unconditionally replaced
    `affected_test_cases` with a hardcoded 4-tuple table when run.
  * The JSON is routinely reformatted wholesale, so a dropped array is invisible
    in review -- a 12k-line diff hides a 200-mapping deletion.

These tests assert *floors*, not exact values: adding issues or attributions is
expected and passes, while any net loss fails.  When you intentionally retire an
issue, lower the floor and remove the ID from ``REQUIRED_IDS`` in the same
commit, so the deletion is a reviewable, deliberate line in the diff rather than
a silent side effect.
"""
import unittest

from scripts.comparison.known_issues import (
    affected_measure_guid_pairs,
    load_catalog,
)

# Floors captured 2026-09-08. Raise when the catalog legitimately grows;
# lower ONLY alongside a deliberate retirement.
MIN_ISSUE_COUNT = 56
MIN_CASE_MAPPINGS = 1228

# Every ID known to exist as of 2026-09-08. Guards against an individual issue
# vanishing while the totals stay plausible (e.g. one deleted, one added).
# Note: E-20 is absent by design -- `git log --all -S'E-20'` returns zero
# commits across every branch and file, so the number was skipped, never used.
REQUIRED_IDS = {
    "B-01",
    "C-01", "C-02", "C-03", "C-04", "C-05", "C-06", "C-07",
    "C-08", "C-09", "C-10", "C-11", "C-12", "C-13", "C-14",
    "E-01", "E-02", "E-03", "E-04", "E-05", "E-06", "E-07", "E-08",
    "E-09", "E-10", "E-11", "E-12", "E-13", "E-14", "E-15", "E-16",
    "E-17", "E-18", "E-19", "E-21", "E-22", "E-23",
    "F-01", "F-02", "F-03", "F-04", "F-05", "F-06",
    "F-07", "F-08", "F-09", "F-10", "F-11", "F-12",
    "M-01", "M-02", "M-03", "M-04", "M-05",
    "V-01", "V-02",
}


class CatalogSizeFloorTest(unittest.TestCase):

    def test_issue_count_does_not_shrink(self):
        issues = load_catalog()["issues"]
        self.assertGreaterEqual(
            len(issues), MIN_ISSUE_COUNT,
            f"catalog shrank to {len(issues)} issues (floor {MIN_ISSUE_COUNT}). "
            "If this is a deliberate retirement, lower MIN_ISSUE_COUNT and drop "
            "the ID from REQUIRED_IDS in the same commit.",
        )

    def test_case_mapping_count_does_not_shrink(self):
        issues = load_catalog()["issues"]
        total = sum(len(affected_measure_guid_pairs(i)) for i in issues)
        self.assertGreaterEqual(
            total, MIN_CASE_MAPPINGS,
            f"case attributions dropped to {total} (floor {MIN_CASE_MAPPINGS}). "
            "These are hand-curated and not reconstructible from the CSVs.",
        )


class CatalogIdentityTest(unittest.TestCase):

    def test_no_known_id_disappears(self):
        present = {i["id"] for i in load_catalog()["issues"]}
        missing = sorted(REQUIRED_IDS - present)
        self.assertFalse(
            missing, f"issue IDs vanished from the catalog: {missing}")

    def test_ids_are_unique(self):
        ids = [i["id"] for i in load_catalog()["issues"]]
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        self.assertFalse(dupes, f"duplicate issue IDs: {dupes}")


# Known pre-existing rot, documented rather than hidden: F-10 records its 58
# cases as 58 copies of {"measure": "CMS1264FHIRECATREHQR", "guid": ""} instead
# of real GUIDs, so its true mapping count is 0 and the catalog's 1,228 total is
# really 1,170. F-10 is resolved, so these never reach the pending exclusion set
# and scoring is unaffected. Repaired when the catalog moves to cases.csv, where
# a blank guid column is visible on sight. Until then this ceiling stops the
# pattern spreading.
KNOWN_MALFORMED = {"F-10": 58}


class CaseMappingShapeTest(unittest.TestCase):

    @staticmethod
    def _malformed_by_issue():
        counts = {}
        for issue in load_catalog()["issues"]:
            bad = 0
            for case in issue.get("affected_test_cases") or []:
                if isinstance(case, dict):
                    if not case.get("measure") or not case.get("guid"):
                        bad += 1
                elif isinstance(case, (list, tuple)):
                    if len(case) != 2 or not all(case):
                        bad += 1
                else:
                    bad += 1
            if bad:
                counts[issue["id"]] = bad
        return counts

    def test_no_new_issue_has_malformed_mappings(self):
        offenders = {k: v for k, v in self._malformed_by_issue().items()
                     if k not in KNOWN_MALFORMED}
        self.assertFalse(
            offenders,
            f"attributions missing measure/guid: {offenders}. Every mapping "
            "needs a real (measure, guid) or it can never match a test case.",
        )

    def test_known_malformed_counts_do_not_grow(self):
        actual = self._malformed_by_issue()
        for issue_id, ceiling in KNOWN_MALFORMED.items():
            self.assertLessEqual(
                actual.get(issue_id, 0), ceiling,
                f"{issue_id} malformed mappings grew past the documented "
                f"ceiling of {ceiling}.",
            )


if __name__ == "__main__":
    unittest.main()
