"""Tests for check_generated.py's drift detection.

The property under test: check_generated.run_checks() must pass when a
generated file matches a fresh regeneration from its source, and fail --
with the specific file named -- when it does not. The "Generated:" timestamp
line must be ignored either way, since it legitimately differs between the
committed file and any regeneration.
"""
import json
import os
import tempfile
import unittest

from scripts.check_generated import run_checks


def _write_catalog(path, issues):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({
            "schema_version": 1,
            "preamble_md": "# Engine / Translator Issues Tracker\n",
            "cross_cutting_lessons_md": "## Cross-Cutting Lessons\n",
            "issues": issues,
        }, fh)


class RunChecksTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _check(self, generated_text):
        """Build a single fake check: regenerated text is always
        `generated_text`; on-disk text is written by the test."""
        output_path = os.path.join(self.tmp, "fake.md")

        def fake_check():
            return output_path, generated_text

        return output_path, fake_check

    def test_passes_when_on_disk_matches_regeneration(self):
        output_path, fake_check = self._check("line one\nline two\n")
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write("line one\nline two\n")
        self.assertTrue(run_checks(checks=[("fake", fake_check)], verbose=False))

    def test_fails_when_on_disk_diverges(self):
        output_path, fake_check = self._check("line one\nline two\n")
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write("line one\nHAND EDITED\n")
        self.assertFalse(run_checks(checks=[("fake", fake_check)], verbose=False))

    def test_missing_output_file_is_skipped_not_failed(self):
        _, fake_check = self._check("anything\n")
        self.assertTrue(run_checks(checks=[("fake", fake_check)], verbose=False))

    def test_timestamp_bullet_line_is_ignored(self):
        output_path, fake_check = self._check(
            "# Doc\n- Generated: 2020-01-01T00:00:00\nbody\n")
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write("# Doc\n- Generated: 2026-09-08T13:00:00\nbody\n")
        self.assertTrue(run_checks(checks=[("fake", fake_check)], verbose=False))

    def test_timestamp_table_row_is_ignored(self):
        output_path, fake_check = self._check(
            "| Generated | 2020-01-01 |\n| Total | 5 |\n")
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write("| Generated | 2026-09-08 |\n| Total | 5 |\n")
        self.assertTrue(run_checks(checks=[("fake", fake_check)], verbose=False))

    def test_non_timestamp_drift_still_caught_alongside_timestamp(self):
        output_path, fake_check = self._check(
            "| Generated | 2020-01-01 |\n| Total | 5 |\n")
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write("| Generated | 2026-09-08 |\n| Total | 6 |\n")
        self.assertFalse(run_checks(checks=[("fake", fake_check)], verbose=False))

    def test_multiple_checks_one_failing_fails_overall(self):
        clean_path, clean_check = self._check("ok\n")
        with open(clean_path, "w", encoding="utf-8") as fh:
            fh.write("ok\n")
        dirty_path, dirty_check = self._check("expected\n")
        with open(dirty_path, "w", encoding="utf-8") as fh:
            fh.write("actual\n")
        self.assertFalse(run_checks(
            checks=[("clean", clean_check), ("dirty", dirty_check)],
            verbose=False))


class RealChecksIntegrationTest(unittest.TestCase):
    """Exercise the actual repo checks (engine-issues.md, catalog_issue_details.md)
    against whatever is currently committed. This is the real regression guard:
    if someone hand-edits engine-issues.md again, this test fails."""

    def test_repo_generated_files_match_their_source(self):
        self.assertTrue(run_checks(verbose=False),
                        "a generated file in the repo has drifted from its "
                        "source -- see check_generated.py's own diff output "
                        "(run `python3 scripts/check_generated.py` directly) "
                        "for which one and why")


if __name__ == "__main__":
    unittest.main()
