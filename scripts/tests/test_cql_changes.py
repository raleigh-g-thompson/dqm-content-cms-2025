"""Tests for the CQL-change log module (cql_changes.py).

The property under test: the append-only JSONL log round-trips through
read_all(), record() assigns monotonic next ids without clobbering, and
malformed entries fail loudly instead of producing a silently wrong
updated-cql.md.
"""
import json
import os
import tempfile
import unittest

from scripts.comparison.cql_changes import read_all, record, _validate_entry


def _entry(**overrides):
    entry = {
        "id": "CQL-001",
        "date": "2026-09-09",
        "measure": "CMS190FHIRVTEProphylaxisICU",
        "title": "Test entry",
        "changes": [{"location": "line 1", "before": "a", "after": "b"}],
        "issues": ["M-04"],
    }
    entry.update(overrides)
    return entry


class ReadAllTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.log = os.path.join(self.tmp, "cql-changes.jsonl")

    def test_missing_log_returns_empty_list(self):
        self.assertEqual(read_all(os.path.join(self.tmp, "nope.jsonl")), [])

    def test_reads_entries_in_file_order(self):
        with open(self.log, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(_entry(id="CQL-001")) + "\n")
            fh.write(json.dumps(_entry(id="CQL-002")) + "\n")
        entries = read_all(self.log)
        self.assertEqual([e["id"] for e in entries], ["CQL-001", "CQL-002"])

    def test_skips_blank_lines(self):
        with open(self.log, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(_entry(id="CQL-001")) + "\n\n")
        self.assertEqual(len(read_all(self.log)), 1)

    def test_bad_json_fails_loudly(self):
        with open(self.log, "w", encoding="utf-8") as fh:
            fh.write("{not json}\n")
        with self.assertRaises(Exception):
            read_all(self.log)

    def test_missing_required_key_fails_loudly(self):
        with open(self.log, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"id": "CQL-001", "date": "2026-09-09"}) + "\n")
        with self.assertRaises(ValueError):
            read_all(self.log)


class RecordTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.log = os.path.join(self.tmp, "cql-changes.jsonl")

    def test_record_assigns_next_id_in_order(self):
        record(_entry(id="CQL-005"), self.log)
        record({"date": "2026-09-09", "measure": "CMS190FHIRVTEProphylaxisICU",
                "title": "auto id"}, self.log)
        entries = read_all(self.log)
        self.assertEqual([e["id"] for e in entries], ["CQL-005", "CQL-006"])

    def test_record_appends_without_rewriting_prior_entries(self):
        record(_entry(id="CQL-001"), self.log)
        before = open(self.log, encoding="utf-8").read()
        record(_entry(), self.log)
        self.assertTrue(open(self.log, encoding="utf-8").read().startswith(before))

    def test_record_validates_before_writing(self):
        with self.assertRaises(ValueError):
            record({"date": "2026-09-09"}, self.log)
        self.assertFalse(os.path.exists(self.log))

    def test_record_creates_parent_dirs(self):
        nested = os.path.join(self.tmp, "a", "b", "cql-changes.jsonl")
        record(_entry(), nested)
        self.assertTrue(os.path.exists(nested))


class ValidateEntryTest(unittest.TestCase):

    def test_unknown_kind_rejected(self):
        with self.assertRaises(ValueError):
            _validate_entry(_entry(kind="bogus"), "test")

    def test_fix_kind_accepted(self):
        _validate_entry(_entry(kind="fix"), "test")  # no raise

    def test_change_without_location_rejected(self):
        with self.assertRaises(ValueError):
            _validate_entry(_entry(changes=[{"before": "a"}]), "test")

    def test_bad_changes_type_rejected(self):
        with self.assertRaises(ValueError):
            _validate_entry(_entry(changes="nope"), "test")

    def test_id_must_be_cql_prefixed(self):
        with self.assertRaises(ValueError):
            _validate_entry(_entry(id="X-01"), "test")


if __name__ == "__main__":
    unittest.main()