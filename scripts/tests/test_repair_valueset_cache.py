import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


def _write(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2) + "\n")


class RepairValuesetCacheTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.valuesets = Path(self.tmp.name) / "valuesets"
        self.txcache = Path(self.tmp.name) / "txcache"
        self.valuesets.mkdir(parents=True)
        self.txcache.mkdir(parents=True)

    def tearDown(self):
        self.tmp.cleanup()

    def _vs(self, name: str, doc: dict):
        _write(self.valuesets / name, doc)

    def _cache(self, name: str, doc: dict):
        _write(self.txcache / name, doc)

    def _run(self, *extra):
        return subprocess.run([
            sys.executable, "scripts/comparison/repair_valueset_cache.py",
            "--valuesets", str(self.valuesets), "--txcache", str(self.txcache),
            *extra,
        ], capture_output=True, text=True)

    def test_repairs_truncated_from_cache(self):
        self._cache("vs-0000.json", {
            "resourceType": "ValueSet",
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
            "version": "20250101",
            "expansion": {
                "total": 2000,
                "contains": [{"code": f"C{i}"} for i in range(2000)],
            },
        })
        self._vs("ValueSet-2.16.840.1.1.1-20250101.json", {
            "resourceType": "ValueSet",
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
            "version": "20250101",
            "expansion": {
                "total": 2000,
                "contains": [{"code": f"C{i}"} for i in range(1000)],
                "parameter": [{"name": "count", "valueInteger": 1000},
                              {"name": "offset", "valueInteger": 0}],
            },
        })
        res = self._run()
        self.assertEqual(res.returncode, 0)
        self.assertIn("repaired: 1", res.stdout)
        doc = json.loads((self.valuesets / "ValueSet-2.16.840.1.1.1-20250101.json").read_text())
        self.assertEqual(len(doc["expansion"]["contains"]), 2000)
        self.assertIsNone(doc["expansion"].get("parameter"))
        self.assertTrue((self.valuesets / "ValueSet-2.16.840.1.1.1-20250101.json.bak").exists())

    def test_leaves_complete_and_unmatched_untouched(self):
        self._vs("ValueSet-9.9.9.9-20250101.json", {
            "resourceType": "ValueSet",
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/9.9.9.9",
            "version": "20250101",
            "expansion": {"total": 5, "contains": [{"code": f"C{i}"} for i in range(5)]},
        })
        res = self._run()
        self.assertEqual(res.returncode, 0)
        self.assertIn("ok_untouched: 1", res.stdout)
        doc = json.loads((self.valuesets / "ValueSet-9.9.9.9-20250101.json").read_text())
        self.assertEqual(len(doc["expansion"]["contains"]), 5)

    def test_truncated_without_cache_reports_and_exits_nonzero(self):
        self._vs("ValueSet-9.9.9.9-20250101.json", {
            "resourceType": "ValueSet",
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/9.9.9.9",
            "version": "20250101",
            "expansion": {
                "total": 2000,
                "contains": [{"code": f"C{i}"} for i in range(1000)],
                "parameter": [{"name": "count", "valueInteger": 1000},
                              {"name": "offset", "valueInteger": 0}],
            },
        })
        res = self._run()
        self.assertEqual(res.returncode, 1)
        self.assertIn("truncated_skipped_no_cache: 1", res.stdout)

    def test_truncated_version_drift_reports_no_backup(self):
        self._cache("vs-0000.json", {
            "resourceType": "ValueSet",
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
            "version": "20260101",
            "expansion": {"total": 10, "contains": [{"code": f"C{i}"} for i in range(10)]},
        })
        self._vs("ValueSet-2.16.840.1.1.1-20250101.json", {
            "resourceType": "ValueSet",
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
            "version": "20250101",
            "expansion": {
                "total": 2000,
                "contains": [{"code": f"C{i}"} for i in range(1000)],
                "parameter": [{"name": "count", "valueInteger": 1000},
                              {"name": "offset", "valueInteger": 0}],
            },
        })
        res = self._run()
        self.assertEqual(res.returncode, 1)
        self.assertIn("truncated_skipped_drift: 1", res.stdout)
        self.assertFalse((self.valuesets / "ValueSet-2.16.840.1.1.1-20250101.json.bak").exists())


if __name__ == "__main__":
    unittest.main()