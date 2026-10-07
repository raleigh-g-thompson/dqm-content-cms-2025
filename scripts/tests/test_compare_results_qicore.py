"""Tests for the optional QI-Core parity columns in the discrepancy report.

The two-input report says only that a test case fails. That is not enough to act
on, because there are two very different reasons: the measure is genuinely wrong
(fails on QI-Core too -- a parity gap), or the QI-Core -> USQualityCore migration
broke it (passes on QI-Core -- a regression). These tests pin the logic that
distinguishes them, and pin the backward-compatibility guarantee that a run
without the QI-Core file renders exactly as it always has.

Scored against the *same* fixture expectations as the UQC columns, never against
the UQC engine's output, so `PASS` means "QI-Core reproduced what the fixture
asks for".
"""
import os
import re
import shutil
import tempfile
import unittest

from scripts.compare_results import (
    DEFAULT_QICORE_ACTUAL_FILE,
    ResultKey,
    capture_results,
    generate_comparison_report,
    generate_output,
    qi_core_parity,
    qicore_status,
    parse_args,
    render_no_discrepancy_section,
    render_qi_core_parity_section,
)

MEASURE = "CMS999FHIRTestMeasure"
GUID_A = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
GUID_B = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
GUID_C = "cccccccc-3333-4333-8333-cccccccccccc"
GUID_D = "dddddddd-4444-4444-8444-dddddddddddd"

CSV_HEADER = "measure_name,guid,population,count\n"


def _csv(*rows):
    return CSV_HEADER + "".join(f"{r}\n" for r in rows)


def _write(path, text):
    with open(path, "w", newline="") as f:
        f.write(text)
    return path


class QicoreStatusTest(unittest.TestCase):
    """`qicore_status` decides PASS / FAIL / MISSING for one test case."""

    def test_passes_when_every_expected_population_matches(self):
        expected = {"Initial Population": "1", "Denominator": "1", "Numerator": "1"}
        self.assertEqual(qicore_status(expected, dict(expected)), "PASS")

    def test_passes_on_extra_populations_qicore_reports(self):
        # QI-Core emitting a population the fixture does not list is not a
        # mismatch: the report only ever checks what is expected.
        expected = {"Denominator": "1"}
        qicore = {"Initial Population": "1", "Denominator": "1", "Numerator": "0"}
        self.assertEqual(qicore_status(expected, qicore), "PASS")

    def test_fails_when_a_value_differs(self):
        self.assertEqual(
            qicore_status({"Denominator": "1"}, {"Denominator": "0"}), "FAIL")

    def test_fails_when_qicore_omits_an_expected_population(self):
        # Absence is not agreement: a measure that never emitted a row has not
        # reproduced the expectation.
        self.assertEqual(
            qicore_status(
                {"Denominator": "1", "Numerator": "1"},
                {"Denominator": "1"}),
            "FAIL")

    def test_missing_when_qicore_has_no_result(self):
        self.assertEqual(qicore_status({"Denominator": "1"}, None), "MISSING")

    def test_missing_when_qicore_preserves_the_key_but_empties_it(self):
        self.assertEqual(qicore_status({}, {}), "PASS")

    def test_compares_stringified_counts_like_row_outcome(self):
        # Counts arrive as CSV strings, but an int must not read as a mismatch.
        self.assertEqual(
            qicore_status({"Denominator": "1"}, {"Denominator": 1}), "PASS")


class ReportWithQicoreTest(unittest.TestCase):
    """The QI-Core columns render correctly when a QI-Core file is supplied."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def _paths(self, qicore_text):
        # GUID_A: UQC wrong, QI-Core right         -> regression (PASS)
        # GUID_B: UQC wrong, QI-Core wrong same way -> parity (FAIL)
        # GUID_C: absent from the UQC actuals      -> Missing Result, QI-Core PASS
        # GUID_D: UQC wrong, absent from QI-Core   -> MISSING
        # The QI-Core file written by each test omits GUID_D.
        expected = _write(os.path.join(self.tmp, "expected.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Numerator,1",
            f"{MEASURE},{GUID_B},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Numerator,1",
            f"{MEASURE},{GUID_C},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_C},Group_1:Denominator,1",
            f"{MEASURE},{GUID_D},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_D},Group_1:Denominator,1",
            f"{MEASURE},{GUID_D},Group_1:Numerator,1",
        ))
        actual = _write(os.path.join(self.tmp, "actual.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Numerator,0",
            f"{MEASURE},{GUID_B},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Numerator,0",
            f"{MEASURE},{GUID_D},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_D},Group_1:Denominator,1",
            f"{MEASURE},{GUID_D},Group_1:Numerator,0",
        ))
        qicore = _write(os.path.join(self.tmp, "qicore.csv"), qicore_text)
        output = os.path.join(self.tmp, "output.csv")
        report = os.path.join(self.tmp, "report.md")
        return expected, actual, qicore, output, report

    def _render(self, qicore_text):
        expected, actual, qicore, output, report = self._paths(qicore_text)
        expected_r = capture_results(expected)
        actual_r = capture_results(actual)
        qicore_r = capture_results(qicore)
        pass_count, fail_count = generate_output(output, expected_r.rows,
                                                actual_r.rows)
        generate_comparison_report(report, expected_r.groups, actual_r.groups,
                                   pass_count, fail_count,
                                   qicore_results=qicore_r, qicore_file=qicore)
        with open(report) as f:
            return f.read()

    def test_summary_column_reports_n_of_m(self):
        # Four failing cases: A is a QI-Core regression (PASS), B is parity
        # (FAIL), C is a missing UQC result that QI-Core passes, D is MISSING.
        # With MISSING counted as non-PASS: 2 of 4.
        report = self._render(_csv(
            f"{MEASURE},{GUID_A},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Numerator,1",
            f"{MEASURE},{GUID_B},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Numerator,0",
            f"{MEASURE},{GUID_C},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_C},Group_1:Denominator,1",
        ))
        self.assertIn("| Measure | Total Test Cases | Missing Results | "
                      "Missing Populations | Mismatched Test Cases | "
                      "QI-Core Also Failing |", report)
        self.assertIn("| 2 of 4 |", report)

    def test_mismatched_rows_carry_status_and_aligned_actual(self):
        report = self._render(_csv(
            f"{MEASURE},{GUID_A},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Numerator,1",
            f"{MEASURE},{GUID_B},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Numerator,0",
            f"{MEASURE},{GUID_C},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_C},Group_1:Denominator,1",
        ))
        # Separator must gain two columns, right-aligned like their values.
        self.assertIn("|---|---|---|:---:|:---:|:---:|:---:|", report)
        row_a = next(l for l in report.splitlines()
                     if GUID_A in l and l.startswith("|"))
        self.assertIn("Numerator | 1 | 0 | PASS | 1", row_a)
        row_b = next(l for l in report.splitlines()
                     if GUID_B in l and l.startswith("|"))
        self.assertIn("Numerator | 1 | 0 | FAIL | 0", row_b)

    def test_missing_results_rows_carry_status(self):
        report = self._render(_csv(
            f"{MEASURE},{GUID_A},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Numerator,1",
            f"{MEASURE},{GUID_B},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Numerator,0",
            f"{MEASURE},{GUID_C},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_C},Group_1:Denominator,1",
        ))
        self.assertIn("| Test Case | Group | QI-Core |", report)
        row_c = next(l for l in report.splitlines()
                     if GUID_C in l and l.startswith("|"))
        self.assertTrue(row_c.rstrip().endswith("PASS |"), row_c)

    def test_qicore_actual_is_missing_where_qicore_has_no_result(self):
        report = self._render(_csv(
            f"{MEASURE},{GUID_A},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Numerator,1",
            f"{MEASURE},{GUID_B},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Numerator,0",
            f"{MEASURE},{GUID_C},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_C},Group_1:Denominator,1",
        ))
        # GUID_D is in the QI-Core file's absence: its row shows MISSING rather
        # than a blank, so the column is never mistaken for a value.
        row_d = next(l for l in report.splitlines()
                     if GUID_D in l and l.startswith("|"))
        self.assertIn("| MISSING |", row_d)

    def test_note_names_the_source_file(self):
        expected, actual, qicore, output, report = self._paths(_csv(
            f"{MEASURE},{GUID_A},Group_1:Numerator,1"))
        expected_r, actual_r = capture_results(expected), capture_results(actual)
        qicore_r = capture_results(qicore)
        pass_count, fail_count = generate_output(output, expected_r.rows,
                                                actual_r.rows)
        generate_comparison_report(report, expected_r.groups, actual_r.groups,
                                   pass_count, fail_count,
                                   qicore_results=qicore_r, qicore_file=qicore)
        with open(report) as f:
            text = f.read()
        self.assertIn(f"`{qicore}`", text)
        self.assertIn("MISSING", text)


class ReportWithoutQicoreTest(unittest.TestCase):
    """Backward compatibility: no QI-Core file, no QI-Core anything."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def test_report_has_no_qicore_content_and_none_parameters_render_no_columns(self):
        expected = _write(os.path.join(self.tmp, "expected.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Numerator,1",
        ))
        actual = _write(os.path.join(self.tmp, "actual.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Initial Population,1",
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Numerator,0",
        ))
        output = os.path.join(self.tmp, "output.csv")
        report = os.path.join(self.tmp, "report.md")
        expected_r, actual_r = capture_results(expected), capture_results(actual)
        pass_count, fail_count = generate_output(output, expected_r.rows,
                                                actual_r.rows)
        generate_comparison_report(report, expected_r.groups, actual_r.groups,
                                   pass_count, fail_count, qicore_results=None)
        with open(report) as f:
            text = f.read()
        self.assertNotIn("QI-Core", text)
        self.assertIn("| Measure | Total Test Cases | Missing Results | "
                      "Missing Populations | Mismatched Test Cases |", text)
        self.assertIn("|---|---|---|:---:|:---:|", text)
        self.assertIn("| Test Case | Group |", text)
        self.assertIn("| Test Case | Group | Population | Expected | Actual |", text)

    def test_empty_qicore_results_object_is_still_rendered(self):
        # `qicore_results=None` is the off switch; an empty Results is not, and
        # must still produce the columns (all MISSING) rather than being
        # silently treated as "no QI-Core input".
        from scripts.compare_results import Results
        expected = _write(os.path.join(self.tmp, "e.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1"))
        actual = _write(os.path.join(self.tmp, "a.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,0"))
        report = os.path.join(self.tmp, "r.md")
        expected_r, actual_r = capture_results(expected), capture_results(actual)
        generate_comparison_report(report, expected_r.groups, actual_r.groups,
                                   0, 1, qicore_results=Results({}, {}, []))
        with open(report) as f:
            text = f.read()
        self.assertIn("QI-Core Also Failing", text)
        self.assertIn("MISSING", text)


class CqfmExclusionsApplyToQicoreTest(unittest.TestCase):
    """CQFM-excluded cells are unscored on the QI-Core side too."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def test_excluded_cell_is_not_scored_as_a_qicore_mismatch(self):
        exclusions = {MEASURE: frozenset({"Group_1:Measure Observation"})}
        qicore = _write(os.path.join(self.tmp, "q.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Measure Observation,1",
        ))
        expected = _write(os.path.join(self.tmp, "e.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_1:Measure Observation,1",
        ))
        results = capture_results(qicore, exclusions)
        key = ResultKey(MEASURE, GUID_A, "Group_1")
        # The excluded population never enters `groups`, so it cannot be
        # compared -- and its absence cannot read as a QI-Core mismatch.
        self.assertEqual(results.groups[key], {"Denominator": "1"})
        self.assertEqual(qicore_status(results.groups[key], results.groups[key]),
                         "PASS")
        self.assertIn(
            ResultKey(MEASURE, GUID_A, "Group_1:Measure Observation"),
            {ResultKey(c.measure_name, c.patient_guid, c.population.split(':')[0]
                       + ':' + c.population.split(':')[1])
             for c in results.unscored})

    def test_excluded_cell_absent_from_qicore_is_still_not_a_mismatch(self):
        exclusions = {MEASURE: frozenset({"Group_1:Measure Observation"})}
        qicore = _write(os.path.join(self.tmp, "q.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1"))
        expected_r = capture_results(
            _write(os.path.join(self.tmp, "e.csv"), _csv(
                f"{MEASURE},{GUID_A},Group_1:Denominator,1",
                f"{MEASURE},{GUID_A},Group_1:Measure Observation,1")),
            exclusions)
        qicore_r = capture_results(qicore, exclusions)
        key = ResultKey(MEASURE, GUID_A, "Group_1")
        self.assertEqual(qicore_status(expected_r.groups[key],
                                       qicore_r.groups[key]), "PASS")


class ParseArgsTest(unittest.TestCase):
    """Flags and positionals can appear in any order; paths pass through as-is."""

    def test_defaults_when_no_arguments(self):
        (expected, actual, output, report,
         measure_dir, qicore) = parse_args([])
        self.assertEqual(expected, "./scripts/comparison/expected_results.csv")
        self.assertEqual(actual, "./scripts/comparison/actual_results.csv")
        self.assertEqual(output, "./scripts/comparison/output_results.csv")
        self.assertEqual(report, "./scripts/comparison/discrepancy_report.md")
        self.assertIsNone(measure_dir)
        self.assertEqual(qicore, DEFAULT_QICORE_ACTUAL_FILE)

    def test_positional_arguments(self):
        (expected, actual, output, report,
         measure_dir, qicore) = parse_args(["e.csv", "a.csv", "o.csv", "r.md"])
        self.assertEqual(
            [expected, actual, output, report],
            ["e.csv", "a.csv", "o.csv", "r.md"])
        self.assertEqual(qicore, DEFAULT_QICORE_ACTUAL_FILE)

    def test_positional_defaults_fill_in_the_tail(self):
        (expected, actual, _, report, _, qicore) = parse_args(["e.csv"])
        self.assertEqual(expected, "e.csv")
        self.assertEqual(actual, "./scripts/comparison/actual_results.csv")
        self.assertEqual(report, "./scripts/comparison/discrepancy_report.md")
        self.assertEqual(qicore, DEFAULT_QICORE_ACTUAL_FILE)

    def test_qicore_actual_flag_overrides_the_default(self):
        *_, qicore = parse_args(["--qicore-actual", "/tmp/q.csv"])
        self.assertEqual(qicore, "/tmp/q.csv")

    def test_paths_pass_through_without_normalisation(self):
        for path in ("/tmp/q.csv", r"C:\data\q.csv", "C:/data/q.csv",
                     "./a/b/../q.csv", "~/q.csv"):
            with self.subTest(path=path):
                *_, qicore = parse_args(["--qicore-actual", path])
                self.assertEqual(qicore, path)

    def test_flag_combined_with_positionals_and_measure_resource_dir(self):
        (expected, actual, output, report,
         measure_dir, qicore) = parse_args([
            "e.csv", "a.csv", "o.csv", "r.md",
            "--qicore-actual", "/tmp/q.csv",
            "--measure-resource-dir", "/tmp/measures",
        ])
        self.assertEqual(
            [expected, actual, output, report],
            ["e.csv", "a.csv", "o.csv", "r.md"])
        self.assertEqual(measure_dir, "/tmp/measures")
        self.assertEqual(qicore, "/tmp/q.csv")

    def test_flags_before_positionals(self):
        (expected, _, _, _, measure_dir, qicore) = parse_args([
            "--measure-resource-dir", "/tmp/measures",
            "--qicore-actual", "/tmp/q.csv",
            "e.csv",
        ])
        self.assertEqual(expected, "e.csv")
        self.assertEqual(measure_dir, "/tmp/measures")
        self.assertEqual(qicore, "/tmp/q.csv")

    def test_measure_resource_dir_still_works_alone(self):
        *_, measure_dir, qicore = parse_args(
            ["--measure-resource-dir", "/tmp/measures"])
        self.assertEqual(measure_dir, "/tmp/measures")
        self.assertEqual(qicore, DEFAULT_QICORE_ACTUAL_FILE)


class QiCoreParityTest(unittest.TestCase):
    """`qi_core_parity` scores every test case both ways, not just failing ones."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def _parity(self, expected_rows, actual_rows, qicore_rows):
        e = capture_results(_write(os.path.join(self.tmp, "e.csv"), _csv(*expected_rows)))
        a = capture_results(_write(os.path.join(self.tmp, "a.csv"), _csv(*actual_rows)))
        q = capture_results(_write(os.path.join(self.tmp, "q.csv"), _csv(*qicore_rows)))
        return qi_core_parity(e.groups, a.groups, q.groups)

    def test_cross_tab_sums_to_every_expected_test_case(self):
        parity = self._parity(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,0"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"])
        self.assertEqual(sum(parity.cross_tab.values()), 1)
        self.assertEqual(parity.cross_tab[("FAIL", "PASS")], 1)

    def test_case_the_two_engines_get_wrong_the_same_way_is_not_an_asymmetric_bucket(self):
        # Both wrong, identically: agreement on the value, but not a
        # regression, so it belongs in FAIL/FAIL and in the disagreement set.
        parity = self._parity(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,0"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,0"])
        self.assertEqual(parity.cross_tab[("FAIL", "FAIL")], 1)
        self.assertEqual(parity.cms_only_failure_case_count, 0)
        self.assertEqual(parity.qicore_only_failure_case_count, 0)
        self.assertEqual(parity.disagreeing_case_count, 0)

    def test_case_both_engines_pass_is_in_no_bucket(self):
        parity = self._parity(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"])
        self.assertEqual(parity.cross_tab[("PASS", "PASS")], 1)
        self.assertEqual(parity.cms_only_failure_case_count, 0)
        self.assertEqual(parity.qicore_only_failure_case_count, 0)
        self.assertEqual(parity.disagreeing_case_count, 0)
        self.assertEqual(parity.disagreeing_cells, 0)

    def test_regression_and_staleness_buckets_are_mutually_exclusive(self):
        parity = self._parity(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_B},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,0",
             f"{MEASURE},{GUID_B},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_B},Group_1:Denominator,0"])
        self.assertEqual(parity.cms_only_failure_case_count, 1)
        self.assertEqual(parity.qicore_only_failure_case_count, 1)
        self.assertEqual(parity.disagreeing_case_count, 2)
        self.assertEqual(parity.disagreeing_cells, 2)
        self.assertEqual(parity.total_cells, 2)

    def test_missing_counts_as_non_pass_in_the_staleness_direction(self):
        # CMS reproduces the fixture, QI-Core emitted nothing: not a CMS
        # defect, and consistent with `QI-Core Also Failing`'s MISSING rule.
        parity = self._parity(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [])
        self.assertEqual(parity.cross_tab[("PASS", "MISSING")], 1)
        self.assertEqual(parity.qicore_only_failure_case_count, 1)
        self.assertEqual(parity.cross_tab.get(("PASS", "FAIL")), None)

    def test_extra_populations_on_one_side_are_not_disagreements(self):
        # QI-Core emitting a population CMS does not is not a difference on an
        # authored cell. Dict-equality would have counted this.
        parity = self._parity(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_A},Group_1:Measure Observation,3"])
        self.assertEqual(parity.disagreeing_case_count, 0)
        self.assertEqual(parity.disagreeing_cells, 0)
        self.assertEqual(parity.cross_tab[("PASS", "PASS")], 1)

    def test_per_measure_buckets_are_keyed_by_measure(self):
        other = "CMS888FHIROtherMeasure"
        parity = self._parity(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{other},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,0",
             f"{other},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{other},{GUID_A},Group_1:Denominator,0"])
        self.assertEqual(list(parity.cms_only_failures), [MEASURE])
        self.assertEqual(list(parity.qicore_only_failures), [other])
        self.assertEqual(parity.cms_only_failure_measure_count, 1)
        self.assertEqual(parity.qicore_only_failure_measure_count, 1)

    def test_cell_counts_sum_across_measures(self):
        other = "CMS888FHIROtherMeasure"
        parity = self._parity(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_A},Group_1:Numerator,1",
             f"{other},{GUID_A},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,0",
             f"{MEASURE},{GUID_A},Group_1:Numerator,0",
             f"{other},{GUID_A},Group_1:Denominator,0"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_A},Group_1:Numerator,1",
             f"{other},{GUID_A},Group_1:Denominator,1"])
        self.assertEqual(parity.cms_only_failure_case_count, 2)
        self.assertEqual(sum(parity.cms_only_failure_cells.values()), 3)
        self.assertEqual(parity.total_cells, 3)


class ParitySectionRenderingTest(unittest.TestCase):
    """The `## QI-Core Parity` section and its headline counts."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def _render(self, expected_rows, actual_rows, qicore_rows,
                pass_count=1, fail_count=1, **kwargs):
        e = capture_results(_write(os.path.join(self.tmp, "e.csv"), _csv(*expected_rows)))
        a = capture_results(_write(os.path.join(self.tmp, "a.csv"), _csv(*actual_rows)))
        q = capture_results(_write(os.path.join(self.tmp, "q.csv"), _csv(*qicore_rows)))
        report = os.path.join(self.tmp, "r.md")
        generate_comparison_report(report, e.groups, a.groups, pass_count, fail_count,
                                   qicore_results=q, qicore_file="q.csv", **kwargs)
        with open(report) as f:
            return f.read()

    def _three_way_rows(self):
        return (
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_B},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,0",
             f"{MEASURE},{GUID_B},Group_1:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_B},Group_1:Denominator,0"])

    def test_section_renders_cross_tab_and_lists_only_the_regression_bucket(self):
        text = self._render(*self._three_way_rows())
        self.assertIn("## QI-Core Parity", text)
        self.assertIn("| CMS verdict \\ QI-Core verdict | PASS | FAIL | MISSING |",
                      text)
        self.assertIn("| PASS | 0 | 1 | 0 |", text)
        self.assertIn("| FAIL | 1 | 0 | 0 |", text)
        self.assertIn("### CMS Fails, QI-Core Reproduces (1 test case, 1 measure)",
                      text)

    def test_reverse_bucket_is_counted_but_not_listed(self):
        # GUID_B is CMS-passes / QI-Core-does-not. It stays in the cross-tab but
        # must not get a section, a GUID list, or a header row: it is
        # QI-Core-side staleness, not CMS work.
        text = self._render(*self._three_way_rows())
        self.assertNotIn("CMS Passes, QI-Core Does Not", text)
        self.assertNotIn(GUID_B, text)
        self.assertIn("| PASS | 0 | 1 | 0 |", text)

    def test_only_the_regression_count_is_in_the_details_table(self):
        # The total disagreement and the reverse bucket are reported inside
        # `## QI-Core Parity`; the header carries the one actionable count.
        text = self._render(*self._three_way_rows())
        self.assertIn("| CMS Fail / QI-Core OK | 1 test case (1 measure) |", text)
        self.assertNotIn("CMS vs QI-Core Disagreements", text)
        self.assertNotIn("CMS OK / QI-Core Not OK", text)
        # ...and neither of those numbers is actually lost from the report.
        self.assertIn("CMS and QI-Core disagree on 2 test cases "
                      "(2 of 2 population cells)", text)
        self.assertIn("| PASS | 0 | 1 | 0 |", text)

    def test_only_anchorable_measures_get_links(self):
        # A measure listed in the regression bucket always has a `####` section
        # in practice, because a CMS failure is what creates one. So the
        # dead-anchor guard is exercised directly rather than through a report:
        # a caller that passes no anchorable measures must get plain names.
        e = capture_results(_write(os.path.join(self.tmp, "e.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1")))
        a = capture_results(_write(os.path.join(self.tmp, "a.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,0")))
        q = capture_results(_write(os.path.join(self.tmp, "q.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1")))
        parity = qi_core_parity(e.groups, a.groups, q.groups)

        linked = "".join(render_qi_core_parity_section(
            parity, "q.csv", 1, {MEASURE}))
        self.assertIn(f"| [{MEASURE}](#{MEASURE.lower()}) | 1 | 1 |", linked)

        plain = "".join(render_qi_core_parity_section(parity, "q.csv", 1))
        self.assertIn(f"| {MEASURE} | 1 | 1 |", plain)
        self.assertNotIn(f"[{MEASURE}](#", plain)

    def test_regression_cases_render_as_one_nested_bullet_per_guid(self):
        # A comma-joined line of `guid` (Group_N) entries was unreadable on a
        # multi-group measure (CMS347: 79 entries for 24 GUIDs). Each GUID is its
        # own bullet under the measure, with its failing groups in order.
        other = "CMS998FHIRSingleGroup"
        e = capture_results(_write(os.path.join(self.tmp, "e.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_2:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{other},{GUID_C},Group_1:Denominator,1")))
        a = capture_results(_write(os.path.join(self.tmp, "a.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,0",
            f"{MEASURE},{GUID_A},Group_2:Denominator,0",
            f"{MEASURE},{GUID_B},Group_1:Denominator,0",
            f"{other},{GUID_C},Group_1:Denominator,0")))
        q = capture_results(_write(os.path.join(self.tmp, "q.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{MEASURE},{GUID_A},Group_2:Denominator,1",
            f"{MEASURE},{GUID_B},Group_1:Denominator,1",
            f"{other},{GUID_C},Group_1:Denominator,1")))
        text = "".join(render_qi_core_parity_section(
            qi_core_parity(e.groups, a.groups, q.groups), "q.csv", 3))

        self.assertIn(f"- **{MEASURE}** (3 test cases across 2 GUIDs)\n"
                      f"  - {GUID_A} (Group_1, Group_2)\n"
                      f"  - {GUID_B} (Group_1)\n", text)
        # Test cases equal GUIDs: the short count form.
        self.assertIn(f"- **{other}** (1)\n  - {GUID_C} (Group_1)\n", text)
        self.assertNotIn("), `", text)

    def test_section_states_its_unit_of_account_against_the_header_count(self):
        # The cross-tab counts (measure, guid, group) triples; the header's
        # Total Test Cases counts (measure, guid). On a measure with two groups
        # per patient these differ, so the section has to say so rather than
        # leave the reader to compare 3 against 2 and call it a bug.
        text = self._render(
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_A},Group_2:Denominator,1"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,0",
             f"{MEASURE},{GUID_A},Group_2:Denominator,0"],
            [f"{MEASURE},{GUID_A},Group_1:Denominator,1",
             f"{MEASURE},{GUID_A},Group_2:Denominator,1"],
            pass_count=0, fail_count=1)
        self.assertIn("| Total Test Cases | 1 |", text)   # header, per patient
        self.assertIn("these counts total 2", text)       # section, per group
        self.assertIn("rather than the header's 1 test case", text)

    def test_empty_expected_results_renders_without_dividing_by_zero(self):
        text = self._render([], [], [])
        self.assertIn("## QI-Core Parity", text)
        self.assertIn("CMS and QI-Core disagree on 0 test cases", text)
        self.assertIn("_No test case falls in this bucket._", text)

    def test_absent_qicore_renders_no_parity_content_at_all(self):
        e = capture_results(_write(os.path.join(self.tmp, "e.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1")))
        a = capture_results(_write(os.path.join(self.tmp, "a.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,0")))
        report = os.path.join(self.tmp, "r.md")
        generate_comparison_report(report, e.groups, a.groups, 0, 1,
                                   qicore_results=None)
        with open(report) as f:
            text = f.read()
        self.assertNotIn("QI-Core Parity", text)
        self.assertNotIn("CMS vs QI-Core Disagreements", text)
        self.assertNotIn("CMS Fail / QI-Core OK", text)
        self.assertNotIn("CMS OK / QI-Core Not OK", text)


class DiscrepancyHeadingTest(unittest.TestCase):
    """`## Measures with Discrepancies` states its share of the corpus."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)

    def test_heading_is_discrepant_of_total_measures(self):
        other = "CMS888FHIROtherMeasure"
        e = capture_results(_write(os.path.join(self.tmp, "e.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,1",
            f"{other},{GUID_B},Group_1:Denominator,1")))
        a = capture_results(_write(os.path.join(self.tmp, "a.csv"), _csv(
            f"{MEASURE},{GUID_A},Group_1:Denominator,0",
            f"{other},{GUID_B},Group_1:Denominator,1")))
        report = os.path.join(self.tmp, "r.md")
        generate_comparison_report(report, e.groups, a.groups, 1, 0)
        with open(report) as f:
            text = f.read()
        self.assertIn("## Measures with Discrepancies (1 of 2)", text)
        self.assertIn("## Measures with No Discrepancies (1 of 2)", text)


class NoDiscrepancyGridTest(unittest.TestCase):
    """The clean-measure grid: several measures per row, order preserved."""

    def _grid(self, measures):
        text = "".join(render_no_discrepancy_section(measures, len(measures) + 1))
        rows = [r for r in text.splitlines() if r.startswith('| [')]
        return text, [[c.strip() for c in r.strip().strip('|').split('|')]
                      for r in rows]

    def test_measures_are_laid_out_column_major_in_given_order(self):
        measures = [f"CMS{100 + i}FHIRTest{i}" for i in range(8)]
        text, grid = self._grid(measures)
        self.assertEqual(len(grid[0]), 3)
        self.assertIn("## Measures with No Discrepancies (8 of 9)", text)
        # Column-major: reading down column 1 then column 2 reproduces the
        # caller's order. A row-major grid would put CMS102 in row 1.
        read_down = [re.match(r"\[ (\S+) \]", c).group(1)
                     for column in zip(*grid) for c in column if c]
        self.assertEqual(read_down, measures)

    def test_padding_falls_only_in_the_last_column(self):
        # 7 measures over 3 columns needs 3 rows: 3, 3, 1. Column-major fills
        # whole columns before starting the next, so the two short rows are
        # both in the last column.
        _, grid = self._grid([f"CMS{200 + i}FHIRTest{i}" for i in range(7)])
        self.assertEqual([len(row) for row in grid], [3, 3, 3])
        empties = [(r, c) for r, row in enumerate(grid)
                   for c, cell in enumerate(row) if not cell]
        self.assertEqual(empties, [(1, 2), (2, 2)])

    def test_every_cell_carries_both_file_links(self):
        _, grid = self._grid(["CMS300FHIRTest"])
        cell = grid[0][0]
        self.assertIn("[ CMS300FHIRTest ](../../input/cql/CMS300FHIRTest.cql)", cell)
        self.assertIn(
            "[ test ](../../input/tests/results/CMS300FHIRTest.txt)", cell)

    def test_no_measures_renders_no_section(self):
        self.assertEqual(render_no_discrepancy_section([], 74), [])


if __name__ == "__main__":
    unittest.main()