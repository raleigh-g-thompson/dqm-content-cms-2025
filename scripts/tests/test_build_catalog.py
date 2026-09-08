"""Byte-level round-trip tests for the known_issues.json compiler.

``build_catalog.py`` compiles the authored ``defect-tracking/issues/`` tree into
``known_issues.json``. 4a made the JSON a build artifact; these tests guard the
compiler itself:

  * A tiny fixture issues-tree round-trips through build_catalog.serialize() at
    the string level, covering the front-matter / TOML edge cases that broke
    byte-equality during the bootstrap (summary_row before the [references]
    table, escaped quotes in titles, body text with no trailing newline).

  * A live test recompiles the real ``defect-tracking/issues/`` tree and
    compares it byte-for-byte against the committed ``known_issues.json``. If
    someone hand-edits the JSON, this is what flags it -- `check_generated.py`
    also catches it, but this keeps the equality check resident in the test
    suite where a diff is expensive to print.
"""
import tempfile
import unittest
from pathlib import Path

from scripts.comparison import build_catalog
from scripts.comparison.known_issues import (
    DEFAULT_CATALOG_PATH as _REPO_CATALOG,
)


def _write_files(base: Path, files: dict):
    for rel, content in files.items():
        p = base / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")


class BuildCatalogRoundTripTest(unittest.TestCase):

    def _fixture(self):
        """Minimal authored tree covering every front-matter edge case that
        broke byte-equality during the 4a bootstrap."""
        tmp = Path(tempfile.mkdtemp())
        _write_files(tmp, {
            "_preamble.md": (
                "+++\n"
                "schema_version = 1\n"
                "generated_from = \"defect-tracking/issues/\"\n"
                "+++\n"
                "# Preamble\n"),
            "_cross_cutting_lessons.md": (
                "## Cross-Cutting Lessons\n"),
            "_manifest.txt": (
                "E-07\n"
                "E-0802\n"
                "C-03\n"),
            "cases.csv": (
                "issue_id,measure,guid\n"
                "E-07,CMS128,g1\n"
                "E-07,CMS128,g2\n"
                "C-03,CMS56,\n"),      # blank guid preserved, per F-10 pattern
            "E-07.md": (
                "+++\n"
                "id = \"E-07\"\n"
                "title = \"execute CQL with F/A \\\"escape\\\" check\"\n"
                "category = \"engine\"\n"
                "status = \"**Confirmed**\"\n"
                "resolved = true\n"
                "root_cause_status = \"open\"\n"
                "workaround = \"Hand-rolled ToDays() helper\"\n"
                "affected_measures = [\"CMS128\", \"CMS156\"]\n"
                "summary_row = \"| E-07 | dq | unresolved |\"\n"
                "\n"
                "[references]\n"
                "engine_entry = \"E-07\"\n"
                "+++\n"
                "### E-07: body"),     # deliberately NO trailing newline
            "E-0802.md": (
                "+++\n"
                "id = \"E-0802\"\n"
                "title = \"two-digit-ish numeric id\"\n"
                "category = \"engine\"\n"
                "status = \"**Suspected**\"\n"
                "resolved = false\n"
                "root_cause_status = \"investigating\"\n"
                "affected_measures = []\n"
                "+++\n"
                "### E-0802: body\n"),
            "C-03.md": (
                "+++\n"
                "id = \"C-03\"\n"
                "title = \"content issue, blank-guid case\"\n"
                "category = \"content\"\n"
                "status = \"**Confirmed**\"\n"
                "resolved = true\n"
                "root_cause_status = \"closed\"\n"
                "affected_measures = [\"CMS56\"]\n"
                "+++\n"
                "### C-03: body\n"),
        })
        return tmp

    def test_compiles_fixture_with_expected_shape(self):
        tmp = self._fixture()
        catalog = build_catalog.build_catalog(tmp)
        self.assertEqual(catalog["schema_version"], 1)
        self.assertEqual(catalog["generated_from"], "defect-tracking/issues/")
        # manifest order wins over file-glob sort in 4a
        self.assertEqual([i["id"] for i in catalog["issues"]],
                         ["E-07", "E-0802", "C-03"])
        e07 = catalog["issues"][0]
        self.assertEqual(e07["summary_row"], "| E-07 | dq | unresolved |")
        self.assertEqual(e07["references"]["engine_entry"], "E-07")
        self.assertEqual(e07["body_md"], "### E-07: body")
        self.assertEqual([c["guid"] for c in e07["affected_test_cases"]],
                         ["g1", "g2"])
        c03 = catalog["issues"][2]
        self.assertEqual(c03["affected_test_cases"],
                         [{"measure": "CMS56", "guid": ""}])
        # not in a numeric-sort world would this survive: 0802 > 07 lexically,
        # but manifest order explicitly pins it
        self.assertNotIn("enriched", catalog)

    def test_compile_serialize_is_stable(self):
        """Same tree in, same bytes out (no timestamp / no nondeterminism)."""
        tmp = self._fixture()
        first = build_catalog.serialize(build_catalog.build_catalog(tmp))
        second = build_catalog.serialize(build_catalog.build_catalog(tmp))
        self.assertEqual(first, second)

    def test_missing_manifest_falls_back_to_stable_sort(self):
        """4b retires _manifest.txt; the compiler must then order issues by a
        stable (category, numeric id) sort without erroring."""
        tmp = Path(tempfile.mkdtemp())
        _write_files(tmp, {
            "_preamble.md": "+++\nschema_version = 1\n+++\n# P\n",
            "_cross_cutting_lessons.md": "## C\n",
            "cases.csv": "issue_id,measure,guid\n",
            "E-10.md": "+++\nid = \"E-10\"\n+++\nbody\n",
            "E-02.md": "+++\nid = \"E-02\"\n+++\nbody\n",
            "C-03.md": "+++\nid = \"C-03\"\n+++\nbody\n",
        })
        catalog = build_catalog.build_catalog(tmp)  # no _manifest.txt
        self.assertEqual([i["id"] for i in catalog["issues"]],
                         ["C-03", "E-02", "E-10"])

    def test_missing_preamble_fails_loudly(self):
        tmp = Path(tempfile.mkdtemp())
        _write_files(tmp, {"_manifest.txt": "E-01\n"})
        with self.assertRaises(OSError):
            build_catalog.build_catalog(tmp)  # no _preamble.md


class BuildCatalogLiveEqualityTest(unittest.TestCase):
    """The permanent gate: the committed catalog is reproducible from the
    authored tree, byte-for-byte. Run after any edit to either side."""

    def test_committed_json_reproduces_from_authored_tree(self):
        authored = build_catalog.REPO_ROOT / "defect-tracking" / "issues"
        self.assertTrue(authored.is_dir(),
                        f"authored tree missing: {authored}")
        compiled = build_catalog.serialize(build_catalog.build_catalog(authored))
        committed = _REPO_CATALOG.read_text(encoding="utf-8")
        self.assertEqual(
            committed, compiled,
            "defect-tracking/issues/ does not recompile into the committed "
            "known_issues.json. Fix the authored files (then run "
            "scripts/comparison/build_catalog.py), or regenerate the JSON.")


if __name__ == "__main__":
    unittest.main()