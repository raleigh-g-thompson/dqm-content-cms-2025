"""Tests for the append-only run-history log."""
import json
import os
import tempfile
import unittest

from scripts.comparison.run_history import latest, read_all, record


class RecordTest(unittest.TestCase):

    def setUp(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: None)  # tempdir cleanup below is explicit
        self.path = os.path.join(tmp, "run-history.jsonl")

    def test_missing_file_reads_as_empty(self):
        self.assertEqual(read_all(self.path), [])

    def test_record_appends_one_json_line(self):
        record({"total_cases": 10, "passing": 9, "failing": 1}, path=self.path)
        with open(self.path, encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["total_cases"], 10)

    def test_multiple_records_append_not_overwrite(self):
        record({"run": 1}, path=self.path)
        record({"run": 2}, path=self.path)
        entries = read_all(self.path)
        self.assertEqual([e["run"] for e in entries], [1, 2])

    def test_latest_returns_the_last_entry(self):
        record({"run": 1}, path=self.path)
        record({"run": 2}, path=self.path)
        self.assertEqual(latest(self.path)["run"], 2)

    def test_latest_is_none_when_no_history(self):
        self.assertIsNone(latest(self.path))

    def test_creates_parent_directory(self):
        nested = os.path.join(tempfile.mkdtemp(), "sub", "dir", "history.jsonl")
        record({"x": 1}, path=nested)
        self.assertTrue(os.path.exists(nested))

    def test_entry_is_json_serialisable_round_trip(self):
        entry = {"timestamp": "2026-09-08T12:00:00", "unattributed_failures": 0}
        record(entry, path=self.path)
        self.assertEqual(read_all(self.path), [entry])


if __name__ == "__main__":
    unittest.main()
