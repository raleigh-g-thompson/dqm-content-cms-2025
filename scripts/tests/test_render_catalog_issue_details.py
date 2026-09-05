"""Tests for render_catalog_issue_details.py."""
import tempfile
import unittest
from pathlib import Path

from scripts.comparison.render_catalog_issue_details import (
    count_citations,
    render_markdown,
    write_catalog_details,
)


def _issue(issue_id, category="engine", resolved=False, test_cases=None,
           body="### body\nprose", title="Some title", measures=None):
    return {
        "id": issue_id,
        "title": title,
        "category": category,
        "status": "**Confirmed**" if not resolved else "Resolved",
        "resolved": resolved,
        "workaround": "`workaround`",
        "affected_measures": measures or ["CMS1"],
        "affected_test_cases": test_cases or [],
        "body_md": body,
    }


def _catalog(issues):
    return {"schema_version": 1, "issues": issues}


def _cases(n, prefix="CMS1FHIRX"):
    return [{"measure": prefix, "guid": f"{i:04d}-guid"} for i in range(1, n + 1)]


class CountCitationsTest(unittest.TestCase):
    def test_counts_unique_ids_per_row_of_notes_column(self):
        lines = [
            "| Known Issues (resolution pending) | 28 issues / 1023 test cases |",
            "| C-03 | title | content | **Confirmed** | CMS1 | 120 |",
            "| [ tc ](link) | Group_1 | Denom | 1 | 0 | C-14 — resolution pending<br>B-01 — resolution pending | FAIL |",
        ]
        self.assertEqual(count_citations(lines), {"C-14": 1, "B-01": 1})

    def test_counts_citation_across_multiple_rows(self):
        lines = [
            f"| [ tc{i} ](link) | Group_1 | Denom | 1 | 0 | E-01 — resolution pending | FAIL |"
            for i in range(3)
        ]
        self.assertEqual(count_citations(lines), {"E-01": 3})

    def test_empty_returns_empty(self):
        self.assertEqual(count_citations([]), {})


class RenderMarkdownTest(unittest.TestCase):
    def test_renders_all_pending_in_all_categories(self):
        catalog = _catalog([
            _issue("C-01", category="content"),
            _issue("E-01"),
            _issue("F-01", category="fixture"),
        ])
        md = render_markdown(catalog)
        for issue_id in ("C-01", "E-01", "F-01"):
            self.assertIn(f"## {issue_id} — ", md)
            self.assertIn("### body\nprose", md)
            self.assertIn("`workaround`", md)

    def test_excludes_resolved_by_default_and_includes_with_flag(self):
        catalog = _catalog([
            _issue("E-01", resolved=False),
            _issue("E-02", resolved=True),
        ])
        md = render_markdown(catalog)
        self.assertIn("## E-01 — ", md)
        self.assertNotIn("## E-02 — ", md)
        md = render_markdown(catalog, include_resolved=True)
        self.assertIn("## E-02 — ", md)

    def test_filters_by_category(self):
        catalog = _catalog([
            _issue("C-01", category="content"),
            _issue("E-01"),
        ])
        md = render_markdown(catalog, categories=["content"])
        self.assertIn("## C-01 — ", md)
        self.assertNotIn("## E-01 — ", md)

    def test_only_cited_filters_uncited(self):
        catalog = _catalog([_issue("E-01"), _issue("E-02")])
        md = render_markdown(catalog, citations={"E-01": 3}, only_cited=True)
        self.assertIn("## E-01 — ", md)
        self.assertNotIn("## E-02 — ", md)

    def test_shows_citation_count_when_present(self):
        catalog = _catalog([_issue("E-01")])
        md = render_markdown(catalog, citations={"E-01": 5})
        self.assertIn("- **Cited in discrepancy_report.md**: 5", md)

    def test_sample_truncates_to_max_test_cases(self):
        catalog = _catalog([_issue("E-01", test_cases=_cases(12))])
        md = render_markdown(catalog, max_test_cases=5)
        self.assertIn("### Affected test cases (first 5 of 12)", md)
        shown = [g for g in (f"{i:04d}-guid" for i in range(1, 13))
                 if g in md]
        self.assertEqual(len(shown), 5)

    def test_empty_test_cases_rendered_as_none(self):
        catalog = _catalog([_issue("E-01")])
        md = render_markdown(catalog)
        self.assertIn("_None enumerated (0 test cases).", md)

    def test_no_body_and_no_workaround_fallbacks(self):
        issue = _issue("E-01")
        issue["body_md"] = "  "
        issue["workaround"] = ""
        catalog = _catalog([issue])
        md = render_markdown(catalog)
        self.assertIn("_No body authored._", md)
        self.assertIn("_No workaround authored._", md)

    def test_backtick_titles_render_verbatim(self):
        catalog = _catalog([_issue("E-01", title="`Min()` over DateTime throws")])
        md = render_markdown(catalog)
        self.assertIn("## E-01 — `Min()` over DateTime throws", md)

    def test_pending_present_in_citation_summary(self):
        catalog = _catalog([_issue("E-01"), _issue("F-01", resolved=True)])
        md = render_markdown(catalog, citations={"E-01": 1})
        self.assertIn("- Catalog size: 2 issues (1 pending, 1 resolved)", md)
        self.assertIn("- Issues cited in `discrepancy_report.md`: 1", md)


class WriteCatalogDetailsTest(unittest.TestCase):
    def _write_inputs(self, tmp):
        known = Path(tmp) / "known_issues.json"
        report = Path(tmp) / "discrepancy_report.md"
        import json
        known.write_text(json.dumps(_catalog([_issue("C-01", category="content", test_cases=_cases(2))])), encoding="utf-8")
        report.write_text(
            "| [ tc ](link) | Group_1 | Denom | 1 | 0 | C-01 — resolution pending | FAIL |\n",
            encoding="utf-8",
        )
        return known, report

    def test_write_catalog_details_writes_file_with_citation_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            known, report = self._write_inputs(tmp)
            out = Path(tmp) / "out.md"
            md = write_catalog_details(known, report, out)
            self.assertTrue(out.exists())
            self.assertIn("## C-01 — ", md)
            self.assertIn("- **Cited in discrepancy_report.md**: 1", md)

    def test_missing_discrepancy_report_does_not_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            known, _ = self._write_inputs(tmp)
            out = Path(tmp) / "out.md"
            md = write_catalog_details(known, Path(tmp) / "missing.md", out)
            self.assertTrue(out.exists())
            self.assertNotIn("Cited in discrepancy_report.md", md)


if __name__ == "__main__":
    unittest.main()