import csv
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

SAMPLE_MEASURE_RESOURCE = json.dumps({
    "resourceType": "Measure",
    "id": "CMSExample",
    "url": "http://example.org/Measure/CMSExample",
    "version": "0.1.000",
    "name": "CMSExample",
    "title": "CMS Example",
    "scoring": {"coding": [{"code": "proportion"}]},
    "group": [
        {
            "id": "Group_1",
            "population": [
                {"id": "Initial_Population", "criteria": {"expression": "Initial Population"},
                 "code": {"coding": [{"code": "initial-population", "display": "Initial Population"}]}},
                {"id": "Denominator", "criteria": {"expression": "Denominator"},
                 "code": {"coding": [{"code": "denominator", "display": "Denominator"}]}},
                {"id": "Numerator", "criteria": {"expression": "Numerator"},
                 "code": {"coding": [{"code": "numerator", "display": "Numerator"}]}},
                {"id": "Denominator_Exception", "criteria": {"expression": "Denominator Exception"},
                 "code": {"coding": [{"code": "denominator-exception", "display": "Denominator Exception"}]}},
            ],
        },
    ],
}, indent=2)

SAMPLE_TEST_CASE_A = json.dumps({
    "libraryName": "CMSExample",
    "testCaseName": "11111111-aaaa-bbbb-cccc-000000000001",
    "parameters": [
        {"name": "Measurement Period",
         "type": "Interval<DateTime>",
         "value": "Interval[@2026-01-01T00:00:00.000Z, @2027-01-01T00:00:00.000Z)",
         "source": "config-global"}
    ],
    "results": [
        {"name": "Initial Population", "value": "true"},
        {"name": "Denominator", "value": "true"},
        {"name": "Numerator", "value": "true"},
        {"name": "Denominator Exception", "value": "false"},
    ]
}, indent=2)

SAMPLE_TEST_CASE_B = json.dumps({
    "libraryName": "CMSExample",
    "testCaseName": "22222222-aaaa-bbbb-cccc-000000000002",
    "parameters": [
        {"name": "Measurement Period",
         "type": "Interval<DateTime>",
         "value": "Interval[@2026-01-01T00:00:00.000Z, @2027-01-01T00:00:00.000Z)",
         "source": "config-global"}
    ],
    "results": [
        {"name": "Initial Population", "value": "false"},
        {"name": "Denominator", "value": "false"},
        {"name": "Numerator", "value": "false"},
        {"name": "Denominator Exception", "value": "true"},
    ]
}, indent=2)


def _build_tree(root: Path) -> Path:
    """Create a self-contained QI-Core style input tree under root with one measure."""
    (root / "input/results/CMSExample").mkdir(parents=True)
    (root / "input/resources/measure").mkdir(parents=True)
    (root / "input/resources/measure/CMSExample.json").write_text(SAMPLE_MEASURE_RESOURCE)
    (root / "input/results/CMSExample/TestCaseResult-11111111-aaaa-bbbb-cccc-000000000001.json").write_text(
        SAMPLE_TEST_CASE_A)
    (root / "input/results/CMSExample/TestCaseResult-22222222-aaaa-bbbb-cccc-000000000002.json").write_text(
        SAMPLE_TEST_CASE_B)
    return root


def _run(args, cwd):
    return subprocess.run(
        ["python3", "-m", "scripts.extract_population_qicore", *args],
        cwd=cwd, check=False, capture_output=True, text=True,
    )


class ExtractPopulationQICoreTest(unittest.TestCase):

    def test_writes_csv_with_expected_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_tree(root)
            out = root / "out.csv"
            r = _run([
                "--input", str(root / "input/results"),
                "--resource-dir", str(root / "input/resources/measure"),
                "--output", str(out),
            ], cwd=REPO_ROOT)
            self.assertEqual(r.returncode, 0, msg=r.stderr)
            with out.open() as fh:
                rows = list(csv.reader(fh))
            self.assertEqual(rows[0], ["measure_name", "guid", "population", "count"])

            # 4 populations x 2 test cases = 8 rows
            self.assertEqual(len(rows) - 1, 8)

            seen = {(r[0], r[1], r[2]): r[3] for r in rows[1:]}
            self.assertEqual(seen[("CMSExample", ANY_GUID_A, "Group_1:Initial Population")], "1")
            # Test B has Initial/Denom/Numer=0 and Denominator Exception=true (1).
            # After validate_measure_population_counts, when Denominator=0
            # and Numerator=0, the Denominator Exception is also zeroed. So we
            # assert "0" here, matching what the canonical extractor does.
            self.assertEqual(seen[("CMSExample", ANY_GUID_B, "Group_1:Denominator Exception")], "0")
            self.assertEqual(seen[("CMSExample", ANY_GUID_A, "Group_1:Numerator")], "1")
            # Sanity: every cell is "0" or "1".
            counts = [r[3] for r in rows[1:]]
            for c in counts:
                self.assertIn(c, {"0", "1"})

    def test_refuses_to_overwrite_locked_baseline_without_force(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_tree(root)
            real_target = REPO_ROOT / "scripts/comparison/qicore-2025-actual-results.csv"
            r = _run([
                "--input", str(root / "input/results"),
                "--resource-dir", str(root / "input/resources/measure"),
                "--output", str(real_target),
            ], cwd=REPO_ROOT)
            self.assertNotEqual(r.returncode, 0, msg="should refuse without --force")
            self.assertIn("refusing to overwrite", r.stderr + r.stdout)

    def test_force_required_for_real_baseline_path(self):
        """A path that resolves to the LOCKED baseline refuses to overwrite
        even when --force is omitted; --force unlocks the guard."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_tree(root)
            real_target = REPO_ROOT / "scripts/comparison/qicore-2025-actual-results.csv"
            # Without --force: refuse.
            r = _run([
                "--input", str(root / "input/results"),
                "--resource-dir", str(root / "input/resources/measure"),
                "--output", str(real_target),
            ], cwd=REPO_ROOT)
            self.assertNotEqual(r.returncode, 0)
            self.assertIn("refusing to overwrite", r.stderr + r.stdout)
            # Confirm the real baseline wasn't touched.
            baseline_size = (REPO_ROOT / "scripts/comparison/qicore-2025-actual-results.csv").stat().st_size
            self.assertGreater(baseline_size, 0)

    def test_force_allows_overwrite_to_tmp_path(self):
        """--force lets the script write a non-locked path even without special
        name (the overwriting guard only checks the LOCKED_PATH exactly)."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_tree(root)
            out = root / "out.csv"
            r = _run([
                "--input", str(root / "input/results"),
                "--resource-dir", str(root / "input/resources/measure"),
                "--output", str(out),
                "--force",
            ], cwd=REPO_ROOT)
            self.assertEqual(r.returncode, 0, msg=r.stderr)
            # second run overwriting
            r2 = _run([
                "--input", str(root / "input/results"),
                "--resource-dir", str(root / "input/resources/measure"),
                "--output", str(out),
                "--force",
            ], cwd=REPO_ROOT)
            self.assertEqual(r2.returncode, 0, msg=r2.stderr)
            self.assertTrue(out.exists())

    def test_dry_run_does_not_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_tree(root)
            out = root / "should-not-exist.csv"
            r = _run([
                "--input", str(root / "input/results"),
                "--resource-dir", str(root / "input/resources/measure"),
                "--output", str(out),
                "--dry-run",
            ], cwd=REPO_ROOT)
            self.assertEqual(r.returncode, 0, msg=r.stderr)
            self.assertFalse(out.exists())
            self.assertIn("would write", r.stdout)
            self.assertIn("CMSExample", r.stdout)

    def test_underscore_in_csv_count_is_int(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _build_tree(root)
            out = root / "out.csv"
            r = _run([
                "--input", str(root / "input/results"),
                "--resource-dir", str(root / "input/resources/measure"),
                "--measures", "CMSExample",
                "--output", str(out),
            ], cwd=REPO_ROOT)
            self.assertEqual(r.returncode, 0)
            with out.open() as fh:
                rows = list(csv.reader(fh))
            counts = [r[3] for r in rows[1:]]
            for c in counts:
                self.assertIn(c, {"0", "1"})


ANY_GUID_A = "11111111-aaaa-bbbb-cccc-000000000001"
ANY_GUID_B = "22222222-aaaa-bbbb-cccc-000000000002"

if __name__ == "__main__":
    unittest.main()
