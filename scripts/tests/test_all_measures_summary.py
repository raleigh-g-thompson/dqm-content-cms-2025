"""Tests for all_measures_summary.py."""
import csv
import unittest
from pathlib import Path

from scripts.comparison.all_measures_summary import (
    classify,
    normalize,
    normalize_key,
    render_markdown,
    build_per_measure_summary,
)


HEADER = ("measure_name", "guid", "population", "count")


def _write(path: Path, rows: list[tuple[str, str, str, str]]):
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        for r in rows:
            w.writerow(r)


class ClassifyTest(unittest.TestCase):
    def test_pass_when_both_match_expected(self):
        self.assertEqual(classify(1, 1, 1), "pass")

    def test_shared_when_both_equal_but_different_from_expected(self):
        self.assertEqual(classify(0, 1, 1), "shared")

    def test_cms_only(self):
        self.assertEqual(classify(1, 0, 1), "cms-only")

    def test_qicore_only(self):
        self.assertEqual(classify(1, 1, 0), "qicore-only")

    def test_shared_direction(self):
        self.assertEqual(classify(0, 2, 4), "shared-direction")

    def test_conflicting(self):
        self.assertEqual(classify(0, 1, -1), "conflicting")

    def test_incomplete_when_cms_missing(self):
        self.assertEqual(classify(1, None, 1), "incomplete")

    def test_incomplete_when_qicore_missing(self):
        self.assertEqual(classify(1, 1, None), "incomplete")

    def test_not_expected_when_no_expected(self):
        self.assertEqual(classify(None, 1, 1), "not-expected")


class NormalizeTest(unittest.TestCase):
    def test_aliased_measure_observation(self):
        self.assertEqual(normalize("Group_1:Measure Population Observation"),
                         ("Group_1", "Measure Observation"))

    def test_passthrough(self):
        self.assertEqual(normalize("Group_1:Numerator"),
                         ("Group_1", "Numerator"))

    def test_normalize_key_applies_alias(self):
        self.assertEqual(normalize_key(("M", "g", "Group_1:Measure Population Observation")),
                         ("M", "g", ("Group_1", "Measure Observation")))


class RenderMarkdownTest(unittest.TestCase):
    def test_includes_measure_and_bucket_totals(self):
        per_measure = {"CMSX": {"pass": 5, "qicore-only": 3}}
        totals = {"pass": 5, "qicore-only": 3, "shared": 0}
        md = render_markdown(per_measure, totals)
        self.assertIn("| CMSX |", md)
        self.assertIn("- **pass**: 5", md)
        self.assertIn("- **qicore-only**: 3", md)


class BuildPerMeasureSummaryTest(unittest.TestCase):
    def test_aggregates_across_guides_per_measure(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            _write(tmp_path / "expected.csv", [
                ("CMSX", "g1", "Group_1:Numerator", "1"),
                ("CMSX", "g2", "Group_1:Numerator", "1"),
            ])
            _write(tmp_path / "cms.csv", [
                ("CMSX", "g1", "Group_1:Numerator", "1"),
                ("CMSX", "g2", "Group_1:Numerator", "1"),
            ])
            _write(tmp_path / "expected.csv", [
                ("CMSX", "g1", "Group_1:Numerator", "1"),
                ("CMSX", "g2", "Group_1:Numerator", "0"),
            ])
            _write(tmp_path / "cms.csv", [
                ("CMSX", "g1", "Group_1:Numerator", "1"),
                ("CMSX", "g2", "Group_1:Numerator", "1"),
            ])
            _write(tmp_path / "qic.csv", [
                ("CMSX", "g1", "Group_1:Numerator", "0"),  # qicore-only
                ("CMSX", "g2", "Group_1:Numerator", "1"),  # shared (cms=1, qic=1, expected=0)
            ])
            # Patch ROOT
            per, totals = build_per_measure_summary(
                expected_path=tmp_path / "expected.csv",
                actual_path=tmp_path / "cms.csv",
                qicore_path=tmp_path / "qic.csv",
            )
        self.assertEqual(per["CMSX"]["qicore-only"], 1)
        self.assertEqual(per["CMSX"]["shared"], 1)


if __name__ == "__main__":
    unittest.main()
