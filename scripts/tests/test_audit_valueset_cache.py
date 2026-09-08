import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.comparison.audit_valueset_cache import classify, load_txcache


def _vs(valuesets_dir: Path, name: str, doc: dict) -> Path:
    path = valuesets_dir / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc))
    return path


def _vses(valuesets_dir: Path):
    valuesets_dir.mkdir(parents=True, exist_ok=True)
    return valuesets_dir


class ClassifyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.valuesets = Path(self.tmp.name) / "valuesets"
        self.txcache = Path(self.tmp.name) / "txcache"
        self.txcache.mkdir(parents=True, exist_ok=True)
        # Full expansion in cache: 2000 codes, no paging param.
        cache_doc = {
            "resourceType": "ValueSet",
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
            "version": "20250101",
            "expansion": {
                "total": 2000,
                "contains": [{"code": f"C{i}"} for i in range(2000)],
            },
        }
        (self.txcache / "vs-0000.json").write_text(json.dumps(cache_doc))

    def tearDown(self):
        self.tmp.cleanup()

    def test_ok(self):
        doc = {
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
            "version": "20250101",
            "expansion": {"total": 2000, "contains": [{"code": f"C{i}"} for i in range(2000)]},
        }
        r = classify(doc, load_txcache(self.txcache))
        self.assertEqual(r["status"], "OK")

    def test_leftover_complete_but_paging_param(self):
        doc = {
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
            "version": "20250101",
            "expansion": {
                "total": 2000,
                "contains": [{"code": f"C{i}"} for i in range(2000)],
                "parameter": [{"name": "count", "valueInteger": 1000},
                              {"name": "offset", "valueInteger": 0}],
            },
        }
        r = classify(doc, load_txcache(self.txcache))
        self.assertEqual(r["status"], "LEFTOVER")

    def test_truncated(self):
        doc = {
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
            "version": "20250101",
            "expansion": {
                "total": 2000,
                "contains": [{"code": f"C{i}"} for i in range(1000)],
                "parameter": [{"name": "count", "valueInteger": 1000},
                              {"name": "offset", "valueInteger": 0}],
            },
        }
        r = classify(doc, load_txcache(self.txcache))
        self.assertEqual(r["status"], "TRUNCATED")
        self.assertEqual(r["detail"], "1000 of 2000 codes present")

    def test_version_drift(self):
        doc = {
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
            "version": "20240101",  # older than cache's 20250101
            "expansion": {"total": 10, "contains": [{"code": f"C{i}"} for i in range(10)]},
        }
        r = classify(doc, load_txcache(self.txcache))
        self.assertEqual(r["status"], "VERSION_DRIFT")

    def test_txcache_missing(self):
        doc = {
            "url": "http://cts.nlm.nih.gov/fhir/ValueSet/9.9.9.9",
            "version": "20250101",
            "expansion": {"total": 5, "contains": [{"code": f"C{i}"} for i in range(5)]},
        }
        r = classify(doc, load_txcache(self.txcache))
        self.assertEqual(r["status"], "TXCACHE_MISSING")

    def test_no_expansion(self):
        doc = {"url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
               "version": "20250101"}
        r = classify(doc, load_txcache(self.txcache))
        self.assertEqual(r["status"], "NO_EXPANSION")

    def test_parse_error_missing_url(self):
        doc = {"version": "20250101"}
        r = classify(doc, load_txcache(self.txcache))
        self.assertEqual(r["status"], "PARSE_ERROR")


class AuditCliTest(unittest.TestCase):
    def test_cli_reports_truncated_and_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as tmp:
            valuesets = Path(tmp) / "valuesets"
            txcache = Path(tmp) / "txcache"
            _vses(valuesets)
            txcache.mkdir(parents=True, exist_ok=True)
            (txcache / "vs-0000.json").write_text(json.dumps({
                "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
                "version": "20250101",
                "expansion": {
                    "total": 2000,
                    "contains": [{"code": f"C{i}"} for i in range(2000)],
                },
            }))
            _vs(valuesets, "ValueSet-2.16.840.1.1.1-20250101.json", {
                "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
                "version": "20250101",
                "expansion": {
                    "total": 2000,
                    "contains": [{"code": f"C{i}"} for i in range(1000)],
                    "parameter": [{"name": "count", "valueInteger": 1000},
                                  {"name": "offset", "valueInteger": 0}],
                },
            })
            res = subprocess.run([
                sys.executable, "scripts/comparison/audit_valueset_cache.py",
                "--valuesets", str(valuesets), "--txcache", str(txcache),
            ], capture_output=True, text=True)
            self.assertEqual(res.returncode, 1)
            self.assertIn("TRUNCATED", res.stdout)
            self.assertIn("1", res.stdout)

    def test_cli_passes_when_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            valuesets = Path(tmp) / "valuesets"
            txcache = Path(tmp) / "txcache"
            _vses(valuesets)
            txcache.mkdir(parents=True, exist_ok=True)
            (txcache / "vs-0000.json").write_text(json.dumps({
                "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
                "version": "20250101",
                "expansion": {
                    "total": 2,
                    "contains": [{"code": "C1"}, {"code": "C2"}],
                },
            }))
            _vs(valuesets, "ValueSet-2.16.840.1.1.1-20250101.json", {
                "url": "http://cts.nlm.nih.gov/fhir/ValueSet/2.16.840.1.1.1",
                "version": "20250101",
                "expansion": {"total": 2, "contains": [{"code": "C1"}, {"code": "C2"}]},
            })
            res = subprocess.run([
                sys.executable, "scripts/comparison/audit_valueset_cache.py",
                "--valuesets", str(valuesets), "--txcache", str(txcache),
            ], capture_output=True, text=True)
            self.assertEqual(res.returncode, 0)


if __name__ == "__main__":
    unittest.main()