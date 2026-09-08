"""Guard against populations silently vanishing from the scorer.

The scorer skips any expected cell whose population is not recognised. That skip
used to be unreported, and the allowlist had drifted out of sync with the data:
it listed plural spellings ("Numerator Observations") that appear in no CSV,
while the singular forms the extractors actually emit were absent. 1,031 of
24,853 expected cells (4.1%) were never scored -- across CMS986, CMS1017 and
CMS871 -- and two catalog issues (C-03, C-04) were tracking defects in rows the
scorer structurally could not fail on.

The rule these tests enforce: every population present in the real CSVs must be
either scored or *deliberately* declared unscored here. Adding a population to
the data without a decision fails the build.
"""
import csv
import os
import unittest

from scripts.comparison.populations import (
    CANONICAL_POPULATIONS,
    POPULATION_ALIASES,
    canonical_cell,
    canonical_population,
    is_scored,
    split_population,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
COMPARISON = os.path.join(REPO_ROOT, "scripts", "comparison")

CSVS = {
    "expected": os.path.join(COMPARISON, "expected_results.csv"),
    "actual": os.path.join(COMPARISON, "actual_results.csv"),
    "qicore": os.path.join(COMPARISON, "qicore-2025-actual-results.csv"),
}

# Populations deliberately excluded from scoring. Empty on purpose: as of
# 2026-09-08 every population present in the data is scored. If a genuinely
# non-scoring population appears later, add it here WITH a reason, so the
# exclusion is a reviewed decision rather than a silent skip.
INTENTIONALLY_UNSCORED: dict = {}


def populations_in(path):
    if not os.path.exists(path):
        return set()
    found = set()
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            if row["measure_name"].lower().startswith("test"):
                continue
            found.add(split_population(row["population"])[1])
    return found


class LiveDataCoverageTest(unittest.TestCase):

    def test_every_population_in_the_data_is_scored_or_declared(self):
        for label, path in CSVS.items():
            with self.subTest(csv=label):
                unknown = {
                    p for p in populations_in(path)
                    if p not in CANONICAL_POPULATIONS
                    and p not in INTENTIONALLY_UNSCORED
                }
                self.assertFalse(
                    unknown,
                    f"{label}: population(s) {sorted(unknown)} are neither "
                    "scored nor declared in INTENTIONALLY_UNSCORED. Decide "
                    "which, and record it -- do not let the scorer skip them "
                    "silently.",
                )

    def test_expected_and_actual_share_a_population_vocabulary(self):
        """The bug that hid CMS986: expected said 'Measure Population
        Observation' while both engines said 'Measure Observation', so the keys
        could never match. After canonicalisation the vocabularies must agree."""
        expected = populations_in(CSVS["expected"])
        actual = populations_in(CSVS["actual"])
        if not expected or not actual:
            self.skipTest("results CSVs not present")
        only_expected = expected - actual
        self.assertFalse(
            only_expected,
            f"populations present in expected but never in actual: "
            f"{sorted(only_expected)} -- these can only ever score as MISSING.",
        )


class AliasTest(unittest.TestCase):

    def test_every_alias_target_is_canonical(self):
        for raw, target in POPULATION_ALIASES.items():
            with self.subTest(alias=raw):
                self.assertIn(
                    target, CANONICAL_POPULATIONS,
                    f"alias {raw!r} maps to {target!r}, which is not canonical",
                )

    def test_no_alias_shadows_a_canonical_name(self):
        overlap = set(POPULATION_ALIASES) & CANONICAL_POPULATIONS
        self.assertFalse(
            overlap, f"names are both canonical and aliased: {sorted(overlap)}")

    def test_the_cms986_alias_is_present(self):
        """Load-bearing: without it CMS986's ~900 cells score as MISSING."""
        self.assertEqual(
            canonical_population("Measure Population Observation"),
            "Measure Observation")

    def test_canonicalisation_is_idempotent(self):
        for name in CANONICAL_POPULATIONS | set(POPULATION_ALIASES):
            once = canonical_population(name)
            self.assertEqual(once, canonical_population(once), name)


class SplitTest(unittest.TestCase):

    def test_splits_group_and_canonicalises(self):
        self.assertEqual(
            split_population("Group_2:Measure Population Observation"),
            ("Group_2", "Measure Observation"))

    def test_canonical_cell_roundtrip(self):
        self.assertEqual(
            canonical_cell("Group_1:Numerator Observations"),
            "Group_1:Numerator Observation")

    def test_population_without_group_raises(self):
        """A malformed population column means the extractor is broken; fail
        loudly rather than silently mangling the key."""
        with self.assertRaises(ValueError):
            split_population("Numerator")

    def test_group_names_with_colons_keep_the_remainder(self):
        self.assertEqual(
            split_population("Group_1:Odd:Name"), ("Group_1", "Odd:Name"))


class IsScoredTest(unittest.TestCase):

    def test_aliased_name_is_scored(self):
        self.assertTrue(is_scored("Measure Population Observation"))

    def test_canonical_name_is_scored(self):
        self.assertTrue(is_scored("Denominator Observation"))

    def test_unknown_name_is_not_scored(self):
        self.assertFalse(is_scored("Totally Made Up Population"))


if __name__ == "__main__":
    unittest.main()
