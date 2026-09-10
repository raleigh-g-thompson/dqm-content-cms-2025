"""Tests for defect-tracking/updated-cql.md regeneration.

The property under test: render() is deterministic for a fixed log + fixed
clock (byte-identical output separated only by the "Generated:" line), renders
newest-first, keeps historical migration-origin entries first, emits one
before->after ```cql``` fence pair per change, and degrades gracefully on an
empty log.
"""
import json
import os
import tempfile
import unittest
from datetime import datetime

from scripts.comparison.generate_updated_cql import (
    MARKER, render, write_updated_cql)


def _entry(cql_id, date, measure="CMS190FHIRVTEProphylaxisICU", **overrides):
    entry = {
        "id": cql_id,
        "date": date,
        "measure": measure,
        "title": "Entry " + cql_id,
        "changes": [{"location": "line 1", "before": "old", "after": "new"}],
        "issues": ["M-04"],
        "engine_version": "5.3",
        "verified": "13/13 flip",
    }
    entry.update(overrides)
    return entry


THREE = [
    _entry("CQL-001", "historical", "CMS108FHIRVTEProphylaxis",
           kind="migration-origin"),
    _entry("CQL-002", "2026-09-08", "CMS108FHIRVTEProphylaxis", kind="fix"),
    _entry("CQL-003", "2026-09-09", kind="fix"),
]

FIXED_NOW = datetime(2026, 9, 10, 12, 0, 0)


class RenderTest(unittest.TestCase):

    def test_starts_with_generated_marker_on_line_one(self):
        md = render(THREE, now=FIXED_NOW)
        self.assertTrue(md.startswith(MARKER + "\n"))

    def test_newest_first_with_history_origin_at_bottom(self):
        md = render(THREE, now=FIXED_NOW)
        ids = [line for line in md.splitlines()
               if line.startswith("### CQL-")]
        expected = ["### CQL-003", "### CQL-002", "### CQL-001"]
        self.assertEqual(len(ids), 3)
        for actual, prefix in zip(ids, expected):
            self.assertTrue(actual.startswith(prefix),
                            f"{actual!r} does not start with {prefix!r}")

    def test_summary_table_one_row_per_entry(self):
        md = render(THREE, now=FIXED_NOW)
        rows = [line for line in md.splitlines()
                if line.startswith("| CQL-") and " | " in line]
        self.assertEqual(len(rows), 3)
        self.assertIn("CQL-003", rows[0])

    def test_before_and_after_fenced_with_cql_language(self):
        md = render([THREE[2]], now=FIXED_NOW)
        self.assertIn("```cql\nold\n```\n→\n```cql\nnew\n```", md)

    def test_after_only_change_renders_single_fence(self):
        md = render([_entry("CQL-030", "2026-09-09",
                            changes=[{"location": "line 9", "after": "new"}])],
                    now=FIXED_NOW)
        self.assertIn("```cql\nnew\n```", md)
        self.assertNotIn("→", md)

    def test_empty_log_renders_no_rows(self):
        md = render([], now=FIXED_NOW)
        self.assertIn("Entries recorded: 0", md)
        self.assertIn("No CQL changes recorded yet", md)
        self.assertNotIn("### CQL-", md)

    def test_deterministic_given_same_log_and_clock(self):
        self.assertEqual(render(THREE, now=FIXED_NOW),
                         render(THREE, now=FIXED_NOW))

    def test_differ_only_on_generated_line_when_clock_moves(self):
        earlier = render(THREE, now=FIXED_NOW)
        later = render(THREE, now=datetime(2026, 9, 11, 8, 0, 0))
        self.assertEqual(
            [l for l in earlier.splitlines() if not l.startswith("- Generated:")],
            [l for l in later.splitlines() if not l.startswith("- Generated:")])

    def test_library_defaults_to_measure_path(self):
        md = render([THREE[2]], now=FIXED_NOW)
        self.assertIn("`input/cql/CMS190FHIRVTEProphylaxisICU.cql`", md)

    def test_details_line_lists_issues_engine_and_verification(self):
        md = render([THREE[2]], now=FIXED_NOW)
        self.assertIn("**Issues**: M-04", md)
        self.assertIn("**Engine**: 5.3", md)
        self.assertIn("**Verified**: 13/13 flip", md)


class WriteTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.log = os.path.join(self.tmp, "cql-changes.jsonl")
        self.out = os.path.join(self.tmp, "updated-cql.md")

    def test_writes_markdown_for_missing_log(self):
        write_updated_cql(self.log, self.out, now=FIXED_NOW)
        with open(self.out, encoding="utf-8") as fh:
            self.assertTrue(fh.read().startswith(MARKER))

    def test_round_trip_matches_render(self):
        with open(self.log, "w", encoding="utf-8") as fh:
            for entry in THREE:
                fh.write(json.dumps(entry, sort_keys=True) + "\n")
        md = write_updated_cql(self.log, self.out, now=FIXED_NOW)
        self.assertEqual(md, render(THREE, now=FIXED_NOW))
        with open(self.out, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), md)


if __name__ == "__main__":
    unittest.main()