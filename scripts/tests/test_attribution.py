"""Tests for the attribution ledger.

The ledger replaced a headline metric that could not be wrong: the old report
subtracted every case cited by an open issue, so the "excluded" score was
computed over a denominator that had the failures removed from it. These tests
pin the properties that make the replacement trustworthy -- chiefly that the
denominator never shrinks and that an unexplained failure is always visible.
"""
import unittest

from scripts.comparison.attribution import (
    build_ledger,
    is_scaffold_measure,
    render_headline,
    render_health_section,
)

# Two open issues and one resolved. E-01 cites a genuinely failing case; C-01
# cites a case that now passes (stale) plus one that does not exist (phantom).
CATALOG = {
    "issues": [
        {
            "id": "E-01", "category": "engine", "resolved": False,
            "affected_test_cases": [{"measure": "M1", "guid": "g-fail"}],
        },
        {
            "id": "C-01", "category": "content", "resolved": False,
            "affected_test_cases": [
                {"measure": "M1", "guid": "g-pass"},
                {"measure": "M1", "guid": "g-nonexistent"},
                {"measure": "testE99Repro", "guid": "g-scaffold"},
            ],
        },
        {
            "id": "F-01", "category": "fixture", "resolved": True,
            "affected_test_cases": [{"measure": "M1", "guid": "g-orphan"}],
        },
    ]
}

OUTCOMES = {
    ("M1", "g-fail"): "FAIL",
    ("M1", "g-pass"): "PASS",
    ("M1", "g-untriaged"): "FAIL",
    ("M2", "g-ok"): "PASS",
}


def ledger(outcomes=None, catalog=None, unscored=()):
    return build_ledger({}, {}, catalog or CATALOG,
                        outcomes if outcomes is not None else OUTCOMES,
                        unscored)


class CoreCountsTest(unittest.TestCase):

    def test_denominator_is_every_case(self):
        """The whole point: one denominator, never reduced by exclusions."""
        self.assertEqual(ledger().total_cases, 4)

    def test_pass_fail_split(self):
        led = ledger()
        self.assertEqual((led.passing, led.failing), (2, 2))

    def test_counts_reconcile(self):
        led = ledger()
        self.assertEqual(led.passing + led.failing, led.total_cases)

    def test_attributed_plus_unattributed_equals_failing(self):
        led = ledger()
        self.assertEqual(
            led.attributed_failures + led.unattributed_count, led.failing)


class UnattributedTest(unittest.TestCase):

    def test_failure_with_no_open_issue_is_unattributed(self):
        self.assertEqual(ledger().unattributed_failures,
                         [("M1", "g-untriaged")])

    def test_failure_cited_by_an_open_issue_is_attributed(self):
        self.assertNotIn(("M1", "g-fail"), ledger().unattributed_failures)

    def test_resolved_issues_do_not_attribute(self):
        """A resolved issue must not absorb a live failure -- otherwise closing
        an issue would silently hide a regression."""
        catalog = {"issues": [{
            "id": "F-9", "resolved": True,
            "affected_test_cases": [{"measure": "M1", "guid": "g-untriaged"}],
        }]}
        led = ledger(catalog=catalog)
        self.assertIn(("M1", "g-untriaged"), led.unattributed_failures)

    def test_string_false_resolved_still_counts_as_open(self):
        """Some catalog entries were authored with the JSON string "false",
        which is truthy in Python; is_resolved() handles it and so must we.

        A raw `issue["resolved"]` read would treat this issue as resolved and
        leave g-untriaged unexplained.
        """
        catalog = {"issues": [{
            "id": "E-9", "resolved": "false",
            "affected_test_cases": [{"measure": "M1", "guid": "g-untriaged"}],
        }]}
        unattributed = ledger(catalog=catalog).unattributed_failures
        self.assertNotIn(("M1", "g-untriaged"), unattributed)
        # g-fail is genuinely uncited by this cut-down catalog, so it remains
        # unattributed -- confirming the ledger is not blanket-attributing.
        self.assertIn(("M1", "g-fail"), unattributed)


class DriftTest(unittest.TestCase):

    def test_cited_case_that_now_passes_is_stale(self):
        self.assertEqual(ledger().stale_attributions,
                         {"C-01": [("M1", "g-pass")]})

    def test_cited_case_absent_from_results_is_phantom(self):
        self.assertEqual(ledger().phantom_attributions,
                         {"C-01": [("M1", "g-nonexistent")]})

    def test_scaffold_reference_is_not_a_phantom(self):
        """testE* repro libraries are excluded from scoring on purpose."""
        led = ledger()
        self.assertEqual(led.scaffold_attributions,
                         {"C-01": [("testE99Repro", "g-scaffold")]})
        self.assertNotIn(("testE99Repro", "g-scaffold"),
                         led.phantom_attributions.get("C-01", []))

    def test_stale_cases_are_not_counted_as_attributed(self):
        """A passing case must not make its issue look like it explains a
        failure -- that is how the old exclusion score inflated itself."""
        self.assertEqual(ledger().attributed_failures, 1)

    def test_is_clean_requires_no_unattributed_and_no_phantoms(self):
        self.assertFalse(ledger().is_clean)
        clean = ledger(outcomes={("M1", "g-fail"): "FAIL"},
                       catalog={"issues": [CATALOG["issues"][0]]})
        self.assertTrue(clean.is_clean)


class ScaffoldTest(unittest.TestCase):

    def test_recognises_test_prefix(self):
        self.assertTrue(is_scaffold_measure("testE11MedicationReference"))
        self.assertTrue(is_scaffold_measure("TESTFoo"))

    def test_real_measures_are_not_scaffold(self):
        self.assertFalse(is_scaffold_measure("CMS986FHIRMalnutritionScore"))


class RenderTest(unittest.TestCase):

    def test_headline_uses_full_denominator_for_every_percentage(self):
        rows = dict(render_headline(ledger()))
        self.assertIn("50.00%", rows["Passing Test Cases"])
        self.assertIn("50.00%", rows["Failing Test Cases"])

    def test_headline_reports_unattributed_as_a_count_not_a_rate(self):
        rows = render_headline(ledger())
        unattributed = [v for k, v in rows if "UNATTRIBUTED" in k][0]
        self.assertNotIn("%", unattributed)

    def test_headline_is_denominated_only_in_test_cases(self):
        """The audience reads test cases. Population-level units ("cells") are
        an internal detail and must never reach the report."""
        text = " ".join(k for k, _ in render_headline(ledger(
            unscored=[("M1", "g-pass", "Group_1:Odd")])))
        self.assertNotIn("cell", text.lower())

    def test_health_section_never_says_cell(self):
        text = "\n".join(render_health_section(
            ledger(unscored=[("M1", "g-pass", "Group_1:Odd")])))
        self.assertNotIn("cell", text.lower())

    def test_health_section_lists_each_drift_class(self):
        text = "\n".join(render_health_section(ledger()))
        self.assertIn("Unattributed failures (1)", text)
        self.assertIn("Stale attributions (1)", text)
        self.assertIn("Phantom attributions (1)", text)
        self.assertIn("Repro-scaffold references (1)", text)

    def test_unscored_populations_are_reported_as_affected_test_cases(self):
        """Two skipped populations on one patient is one affected test case,
        not two -- the report counts patients, never populations."""
        led = ledger(unscored=[("M1", "g-pass", "Group_1:Odd"),
                               ("M1", "g-pass", "Group_2:Odd")])
        self.assertEqual(led.unscored_cases, [("M1", "g-pass")])
        self.assertIn("Test cases not fully measured (1)",
                      "\n".join(render_health_section(led)))

    def test_clean_ledger_says_so(self):
        clean = ledger(outcomes={("M1", "g-fail"): "FAIL"},
                       catalog={"issues": [CATALOG["issues"][0]]})
        text = "\n".join(render_health_section(clean))
        self.assertIn("No unattributed failures", text)


if __name__ == "__main__":
    unittest.main()
