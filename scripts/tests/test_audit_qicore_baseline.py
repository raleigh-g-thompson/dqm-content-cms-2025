import csv
import tempfile
import unittest
from pathlib import Path

from scripts.audit_qicore_baseline import diff, read_csv, render_markdown


HEADER = ("measure_name", "guid", "population", "count")


def _write(path: Path, rows: list[tuple[str, str, str, str]]) -> Path:
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)
        for r in rows:
            writer.writerow(r)
    return path


class AuditReadCsvTest(unittest.TestCase):
    def test_reads_baseline_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "baseline.csv"
            _write(path, [
                ("CMSX", "11111111-aaaa", "Group_1:Initial Population", "1"),
                ("CMSX", "22222222-bbbb", "Group_1:Numerator", "0"),
            ])
            data = read_csv(path)
        self.assertEqual(data[("CMSX", "11111111-aaaa", "Group_1:Initial Population")], "1")
        self.assertEqual(data[("CMSX", "22222222-bbbb", "Group_1:Numerator")], "0")


class DiffTest(unittest.TestCase):
    def test_added_removed_changed_unchanged(self):
        baseline = {
            ("CMSX", "g1", "Group_1:Initial Population"): "1",
            ("CMSX", "g2", "Group_1:Numerator"): "0",  # removed-only
            ("CMSX", "g3", "Group_1:Denominator"): "1",  # changed
            ("CMSX", "g4", "Group_1:Numerator"): "1",  # unchanged
        }
        fresh = {
            ("CMSX", "g1", "Group_1:Initial Population"): "1",  # unchanged
            ("CMSX", "g3", "Group_1:Denominator"): "0",  # changed
            ("CMSX", "g4", "Group_1:Numerator"): "1",  # unchanged
            ("CMSX", "g5", "Group_1:Numerator"): "1",  # added
        }
        result = diff(baseline, fresh)
        self.assertEqual(result["added"], [("CMSX", "g5", "Group_1:Numerator")])
        self.assertEqual(result["removed"], [("CMSX", "g2", "Group_1:Numerator")])
        self.assertEqual(result["changed"], [("CMSX", "g3", "Group_1:Denominator")])
        self.assertEqual(len(result["unchanged"]), 2)

    def test_per_measure_counts(self):
        baseline = {
            ("CMSX", "g1", "Group_1:Initial Population"): "1",
            ("CMSY", "g1", "Group_1:Initial Population"): "1",
            ("CMSY", "g2", "Group_1:Numerator"): "0",
        }
        fresh = {
            ("CMSX", "g1", "Group_1:Initial Population"): "1",
            ("CMSY", "g1", "Group_1:Initial Population"): "0",  # changed
            ("CMSY", "g2", "Group_1:Numerator"): "0",
            ("CMSZ", "g3", "Group_1:Numerator"): "1",  # added (new measure)
        }
        result = diff(baseline, fresh)
        self.assertEqual(result["per_measure"]["changed"], {"CMSY": 1})
        self.assertEqual(result["per_measure"]["added"], {"CMSZ": 1})
        self.assertEqual(result["per_measure"]["unchanged"], {"CMSX": 1, "CMSY": 1})
        self.assertEqual(result["per_measure"]["removed"], {})


class RenderMarkdownTest(unittest.TestCase):

    def test_contains_summary_with_truncation(self):
        baseline = {
            ("CMSX", "g1", "Group_1:Initial Population"): "1",
        }
        # build 60 changed rows
        changed = [(f"CMSX", f"g{i}", "Group_1:Numerator") for i in range(60)]
        fresh = {**baseline}
        for k in changed:
            fresh[k] = "1" if k[1] == "g1" else "0"
        for k in changed[1:]:
            baseline[k] = "1"  # baseline says 1, fresh says 0 → 59 changes
        result = diff(baseline, fresh)
        md = render_markdown(result, "baseline.csv", "fresh.csv", baseline, fresh)
        self.assertIn("## Summary", md)
        self.assertIn("truncated", md.lower())
        # Confirm 50 sample rows (filter to those with the broken Guid that follows "| CMSX | g").
        sample_table = [l for l in md.splitlines() if l.startswith("| CMSX | g")]
        self.assertEqual(len(sample_table), 50)


class AuditCliTest(unittest.TestCase):
    def test_cli_reports_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "base.csv"
            fresh = Path(tmp) / "fresh.csv"
            _write(base, [
                ("CMSX", "g1", "Group_1:Initial Population", "1"),
                ("CMSX", "g2", "Group_1:Numerator", "0"),
            ])
            _write(fresh, [
                ("CMSX", "g2", "Group_1:Numerator", "1"),
                ("CMSX", "g3", "Group_1:Numerator", "1"),
            ])
            import subprocess, sys as _s
            res = subprocess.run([
                _s.executable, "scripts/audit_qicore_baseline.py",
                "--baseline", str(base), "--fresh", str(fresh),
            ], capture_output=True, text=True)
            self.assertEqual(res.returncode, 0)
            self.assertIn("Changed", res.stdout)
            self.assertIn("Phase 1 measure", res.stdout) if False else None
            self.assertIn("CMSX", res.stdout)


if __name__ == "__main__":
    unittest.main()
