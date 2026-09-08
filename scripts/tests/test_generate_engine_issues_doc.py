import unittest

from scripts.comparison.generate_engine_issues_doc import (
    MARKER,
    doc_issues,
    render,
    summary_table,
)

MULTI_CATALOG = {
    "issues": [
        {
            "id": "E-13",
            "title": "Union of Conditions",
            "category": "engine",
            "status": "Confirmed / Applied",
            "affected_measures": ["CMS117"],
            "body_md": "### E-13\ndetail",
            "summary_row": "| E-13 | Union of Conditions | Confirmed / Applied | yes | CMS117 |",
        },
        {
            "id": "F-01",
            "title": "Fixture issue",
            "category": "fixture",
            "status": "Resolved",
            "affected_measures": ["CMS156"],
            "body_md": "### F-01\ndetail",
        },
    ],
    "preamble_md": "# Engine Issues\npreamble",
    "cross_cutting_lessons_md": "## Cross-Cutting Lessons\nnotes",
}


class DocIssuesTest(unittest.TestCase):

    def test_filters_to_engine_when_multiple_categories(self):
        ids = {i["id"] for i in doc_issues(MULTI_CATALOG)}
        self.assertEqual(ids, {"E-13"})

    def test_returns_all_when_single_category(self):
        single = {"issues": [MULTI_CATALOG["issues"][0]]}
        self.assertEqual(len(doc_issues(single)), 1)


class SummaryTableTest(unittest.TestCase):

    def test_uses_summary_row_verbatim(self):
        table = summary_table(doc_issues(MULTI_CATALOG))
        self.assertIn("| E-13 | Union of Conditions | Confirmed / Applied | yes | CMS117 |", table)

    def test_falls_back_to_columns_without_summary_row(self):
        issue = {"id": "E-1", "title": "T", "status": "S", "workaround": "W",
                 "affected_measures": ["CMS1"]}
        table = summary_table([issue])
        self.assertIn("| E-1 | T | S | W | CMS1 |", table)


class RenderTest(unittest.TestCase):

    def test_engine_only_and_marker_present(self):
        doc = render(MULTI_CATALOG)
        self.assertIn("### E-13", doc)
        self.assertNotIn("### F-01", doc)
        self.assertIn("## Cross-Cutting Lessons", doc)

    def test_marker_is_the_first_line(self):
        """The marker used to be emitted last, ~1,150 lines down, where readers
        never saw it -- the document was hand-edited repeatedly and forked from
        the catalog as a result. Line 1 is load-bearing, not cosmetic."""
        doc = render(MULTI_CATALOG)
        self.assertEqual(doc.splitlines()[0], MARKER)

    def test_render_is_idempotent(self):
        """CI diffs a fresh render against the committed file to detect
        hand-edits; that check is only meaningful if render() is stable."""
        self.assertEqual(render(MULTI_CATALOG), render(MULTI_CATALOG))


if __name__ == "__main__":
    unittest.main()
