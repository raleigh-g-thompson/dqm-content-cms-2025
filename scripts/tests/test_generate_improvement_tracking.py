"""Tests for defect-tracking/improvement-tracking.md regeneration.

The property under test: render() is deterministic for a fixed log + fixed
clock (byte-identical output separated only by the "Generated:" line), renders
one table row per run in the log (oldest first), and degrades gracefully when
the log is empty or has a single entry.
"""
import os
import tempfile
import unittest
from datetime import datetime

from scripts.comparison.generate_improvement_tracking import (
    MARKER, render, write_improvement_tracking)


def _entry(timestamp, passing=100, failing=5, stale=10, **overrides):
    entry = {
        "timestamp": timestamp,
        "total_cases": 200,
        "passing": passing,
        "failing": failing,
        "attributed_failures": failing,
        "unattributed_failures": 0,
        "stale_attributions": stale,
        "phantom_attributions": 0,
        "unscored_cases": 0,
    }
    entry.update(overrides)
    return entry


FIXED_NOW = datetime(2026, 9, 8, 12, 0, 0)

TWO_RUNS = [
    _entry("2026-09-08T09:00:00", passing=3500, failing=400, stale=700),
    _entry("2026-09-08T10:00:00", passing=3585, failing=379, stale=683),
]


class RenderTest(unittest.TestCase):

    def test_starts_with_generated_marker_on_line_one(self):
        md = render(TWO_RUNS, now=FIXED_NOW)
        self.assertTrue(md.startswith(MARKER + "\n"))

    def test_one_table_row_per_run_oldest_first(self):
        md = render(TWO_RUNS, now=FIXED_NOW)
        rows = [line for line in md.splitlines()
                if line.startswith("| ") and line[2].isdigit()]
        self.assertEqual(len(rows), 2)
        self.assertIn("2026-09-08T09:00:00", rows[0])
        self.assertIn("2026-09-08T10:00:00", rows[1])

    def test_renders_logged_numbers_in_each_row(self):
        md = render(TWO_RUNS, now=FIXED_NOW)
        row = [line for line in md.splitlines()
               if line.startswith("| 2 ")].pop()
        self.assertIn("3585", row)
        self.assertIn("683", row)

    def test_single_run_notes_no_deltas_yet(self):
        md = render([TWO_RUNS[0]], now=FIXED_NOW)
        self.assertIn("Runs recorded: 1", md)
        self.assertIn("Only one run recorded", md)
        self.assertNotIn("Since the first recorded run\n\n| Metric", md)

    def test_empty_history_renders_with_no_rows(self):
        md = render([], now=FIXED_NOW)
        self.assertIn("Runs recorded: 0", md)
        self.assertIn("no runs recorded", md)

    def test_trend_table_reports_first_to_latest_deltas(self):
        md = render(TWO_RUNS, now=FIXED_NOW)
        lines = md.splitlines()
        passing_row = [l for l in lines if l.startswith("| Passing |")].pop()
        self.assertIn("| 3500 | 3585 | +85 |", passing_row)
        failing_row = [l for l in lines if l.startswith("| Failing |")].pop()
        self.assertIn("| 400 | 379 | -21 |", failing_row)

    def test_deterministic_given_same_log_and_clock(self):
        first = render(TWO_RUNS, now=FIXED_NOW)
        second = render(TWO_RUNS, now=FIXED_NOW)
        self.assertEqual(first, second)

    def test_differ_only_on_generated_line_when_clock_moves(self):
        earlier = render(TWO_RUNS, now=FIXED_NOW)
        later = render(TWO_RUNS, now=datetime(2026, 9, 9, 8, 0, 0))
        earlier_lines = [l for l in earlier.splitlines()
                         if not l.startswith("- Generated:")]
        later_lines = [l for l in later.splitlines()
                       if not l.startswith("- Generated:")]
        self.assertEqual(earlier_lines, later_lines)

    def test_missing_numeric_keys_default_to_zero(self):
        md = render([{"timestamp": "t-1"}], now=FIXED_NOW)
        self.assertIn("| 1 | t-1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |", md)


class WriteTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.log = os.path.join(self.tmp, "run-history.jsonl")
        self.out = os.path.join(self.tmp, "improvement-tracking.md")

    def test_writes_markdown_for_missing_log(self):
        write_improvement_tracking(self.log, self.out, now=FIXED_NOW)
        with open(self.out, encoding="utf-8") as fh:
            self.assertTrue(fh.read().startswith(MARKER))

    def test_round_trip_matches_render(self):
        import json
        with open(self.log, "w", encoding="utf-8") as fh:
            for entry in TWO_RUNS:
                fh.write(json.dumps(entry, sort_keys=True) + "\n")
        md = write_improvement_tracking(self.log, self.out, now=FIXED_NOW)
        self.assertEqual(md, render(TWO_RUNS, now=FIXED_NOW))
        with open(self.out, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), md)


if __name__ == "__main__":
    unittest.main()