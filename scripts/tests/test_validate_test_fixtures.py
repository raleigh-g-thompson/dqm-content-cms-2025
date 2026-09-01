import json
import os
import tempfile
import unittest

from scripts.validate_test_fixtures import (
    apply_misnamed_patient_fix,
    collect_anomaly_fixes,
    collect_test_cases,
    collect_fixable,
    discover_patient_fields,
    expected_patient_for,
    misnamed_patient_file,
    patient_resource_files,
    validate,
    _patient_ref_guid,
)


def write_json(directory, filename, data):
    path = os.path.join(directory, filename)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)
    return path


def make_patient(case_dir, guid):
    write_json(case_dir, f"Patient-{guid}.json",
               {"resourceType": "Patient", "id": guid})


class PatientRefHelpersTest(unittest.TestCase):

    def test_patient_ref_guid_extracts_guid(self):
        self.assertEqual(_patient_ref_guid({"reference": "Patient/abc-123"}), "abc-123")

    def test_patient_ref_guid_non_patient_reference(self):
        self.assertIsNone(_patient_ref_guid({"reference": "Encounter/x"}))

    def test_patient_ref_guid_ignores_non_dict(self):
        self.assertIsNone(_patient_ref_guid("Patient/abc"))
        self.assertIsNone(_patient_ref_guid(None))


class ExpectedPatientTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self.case = os.path.join(self._tmp, "CMS104X", "11111111-1111-1111-1111-111111111111")
        os.makedirs(self.case)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_single_patient_returns_id(self):
        make_patient(self.case, "11111111-1111-1111-1111-111111111111")
        files = [f for f in os.listdir(self.case) if f.startswith("Patient-")]
        self.assertEqual(expected_patient_for([os.path.join(self.case, f) for f in files]),
                         "11111111-1111-1111-1111-111111111111")

    def test_no_patient_returns_none(self):
        files = [f for f in os.listdir(self.case) if f.startswith("Patient-")]
        self.assertIsNone(expected_patient_for([os.path.join(self.case, f) for f in files]))

    def test_two_patients_returns_none(self):
        make_patient(self.case, "11111111-1111-1111-1111-111111111111")
        make_patient(self.case, "22222222-2222-2222-2222-222222222222")
        files = [f for f in os.listdir(self.case) if f.startswith("Patient-")]
        self.assertIsNone(expected_patient_for([os.path.join(self.case, f) for f in files]))


class DiscoverPatientFieldsTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self.case = os.path.join(self._tmp, "CMS104X", "11111111-1111-1111-1111-111111111111")
        os.makedirs(self.case)
        make_patient(self.case, "11111111-1111-1111-1111-111111111111")

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_discovers_all_patient_ref_fields(self):
        write_json(self.case, "Encounter-e.json", {
            "resourceType": "Encounter",
            "subject": {"reference": "Patient/11111111-1111-1111-1111-111111111111"},
        })
        write_json(self.case, "Task-t.json", {
            "resourceType": "Task",
            "for": {"reference": "Patient/11111111-1111-1111-1111-111111111111"},
            "focus": {"reference": "Patient/11111111-1111-1111-1111-111111111111"},
        })
        files = [os.path.join(self.case, f) for f in os.listdir(self.case)
                 if not f.startswith("Patient-")]
        self.assertEqual(discover_patient_fields(files), {"subject", "for", "focus"})

    def test_ignores_measure_report(self):
        write_json(self.case, "MeasureReport-m.json", {
            "resourceType": "MeasureReport", "subject": {"reference": "Patient/11111111-1111-1111-1111-111111111111"},
        })
        files = [os.path.join(self.case, f) for f in os.listdir(self.case)
                 if not f.startswith("Patient-")]
        self.assertEqual(discover_patient_fields(files), set())


class ValidateTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self.tests_root = self._tmp
        self.good = "11111111-1111-1111-1111-111111111111"
        self.other = "99999999-9999-9999-9999-999999999999"
        self.placeholder = "d170a0a8-b5ad-4303-b6df-e304dd5f92ad"

        # measure CMS104X with two test cases
        self.m1 = os.path.join(self.tests_root, "CMS104X", self.good)
        os.makedirs(self.m1)
        make_patient(self.m1, self.good)

        self.m2 = os.path.join(self.tests_root, "CMS104X", self.other)
        os.makedirs(self.m2)
        make_patient(self.m2, self.other)

        # a second measure to establish that `other`/`placeholder` are cross-measure
        self.m3 = os.path.join(self.tests_root, "CMS72X", self.other)
        os.makedirs(self.m3)
        make_patient(self.m3, self.other)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _cases(self):
        return collect_test_cases(self.tests_root)

    def test_correct_reference_not_flagged(self):
        write_json(self.m1, "Encounter-e.json", {
            "resourceType": "Encounter",
            "subject": {"reference": f"Patient/{self.good}"},
        })
        findings, _ = validate(self._cases())
        self.assertEqual(findings, [])

    def test_core_wrong_patient_flagged_fixable(self):
        write_json(self.m1, "Claim-c.json", {
            "resourceType": "Claim",
            "patient": {"reference": f"Patient/{self.other}"},
        })
        findings, _ = validate(self._cases())
        self.assertEqual(len(findings), 1)
        f = findings[0]
        self.assertEqual(f[3], "patient")
        self.assertEqual(f[4], self.other)
        self.assertEqual(f[5], self.good)
        self.assertTrue(f[6].startswith("CORE") and "FIXABLE" in f[6])

    def test_core_placeholder_flagged(self):
        write_json(self.m1, "Claim-c.json", {
            "resourceType": "Claim",
            "patient": {"reference": f"Patient/{self.placeholder}"},
        })
        findings, _ = validate(self._cases())
        self.assertEqual(len(findings), 1)
        f = findings[0]
        self.assertTrue(f[6].startswith("CORE") and "PLACEHOLDER" in f[6])

    def test_core_subject_mismatch_flagged(self):
        write_json(self.m1, "Encounter-e.json", {
            "resourceType": "Encounter",
            "subject": {"reference": f"Patient/{self.other}"},
        })
        findings, _ = validate(self._cases())
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0][3], "subject")

    def test_core_beneficiary_mismatch_flagged(self):
        write_json(self.m1, "Coverage-c.json", {
            "resourceType": "Coverage",
            "beneficiary": {"reference": f"Patient/{self.other}"},
        })
        findings, _ = validate(self._cases())
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0][3], "beneficiary")

    def test_broad_field_mismatch_reported_not_fixable(self):
        write_json(self.m1, "Task-t.json", {
            "resourceType": "Task",
            "for": {"reference": f"Patient/{self.other}"},
        })
        findings, _ = validate(self._cases())
        self.assertEqual(len(findings), 1)
        f = findings[0]
        self.assertEqual(f[3], "for")
        self.assertIn("NOT-FIXED", f[6])
        self.assertFalse(collect_fixable(findings))

    def test_missing_patient_field_not_flagged(self):
        write_json(self.m1, "Encounter-e.json", {"resourceType": "Encounter"})
        findings, _ = validate(self._cases())
        self.assertEqual(findings, [])

    def test_structural_anomaly_no_patient(self):
        bad = os.path.join(self.tests_root, "CMS104X", "0000bad")
        os.makedirs(bad)
        write_json(bad, "Encounter-e.json", {"resourceType": "Encounter"})
        findings, anomalies = validate(self._cases())
        self.assertEqual(findings, [])
        self.assertEqual(len(anomalies), 1)

    def test_misnamed_patient_flagged_but_references_validated(self):
        # A Patient resource named null-null.json (template artifact, no id):
        # reported as a fixable anomaly, yet references against the folder GUID
        # are still validated (a matching reference produces no finding).
        bad = os.path.join(self.tests_root, "CMS104X", "bad-guuid")
        os.makedirs(bad)
        write_json(bad, "null-null.json", {"resourceType": "Patient"})
        write_json(bad, "Encounter-e.json", {
            "resourceType": "Encounter",
            "subject": {"reference": "Patient/bad-guuid"},
        })
        findings, anomalies = validate(self._cases())
        self.assertEqual(findings, [])
        self.assertEqual(len([a for a in anomalies if "misnamed" in a]), 1)

    def test_misnamed_patient_reference_mismatch_still_flagged(self):
        bad = os.path.join(self.tests_root, "CMS104X", "bad-guuid")
        os.makedirs(bad)
        write_json(bad, "null-null.json", {"resourceType": "Patient"})
        write_json(bad, "Encounter-e.json", {
            "resourceType": "Encounter",
            "subject": {"reference": f"Patient/{self.other}"},
        })
        findings, anomalies = validate(self._cases())
        self.assertEqual(len([a for a in anomalies if "misnamed" in a]), 1)
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0][3], "subject")
        self.assertEqual(findings[0][5], "bad-guuid")

    def test_misnamed_patient_file_only_for_misnamed(self):
        files = [os.path.join(self.m1, f) for f in os.listdir(self.m1)]
        self.assertIsNone(misnamed_patient_file(files))
        self.assertEqual(patient_resource_files(files), [f for f in files
                                                         if f.endswith(f"Patient-{self.good}.json")])

    def test_collect_fixable_only_core(self):
        write_json(self.m1, "Claim-c.json", {
            "resourceType": "Claim", "patient": {"reference": f"Patient/{self.other}"},
        })
        write_json(self.m1, "Task-t.json", {
            "resourceType": "Task", "for": {"reference": f"Patient/{self.other}"},
        })
        findings, _ = validate(self._cases())
        fixable = collect_fixable(findings)
        self.assertEqual(len(fixable), 1)
        self.assertEqual(fixable[0][3], "patient")


class ApplyFixTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mkdtemp()
        self.tests_root = self._tmp
        self.good = "11111111-1111-1111-1111-111111111111"
        self.other = "99999999-9999-9999-9999-999999999999"
        self.case = os.path.join(self.tests_root, "CMS104X", self.good)
        os.makedirs(self.case)
        make_patient(self.case, self.good)

    def tearDown(self):
        import shutil
        shutil.rmtree(self._tmp, ignore_errors=True)

    def test_apply_fix_rewrites_core_field_and_preserves_json(self):
        write_json(self.case, "Claim-c.json", {
            "resourceType": "Claim",
            "patient": {"reference": f"Patient/{self.other}"},
        })
        finding = ("CMS104X", self.good, "Claim-c.json", "patient", self.other, self.good, "CORE-WRONG-FIXABLE")
        path = os.path.join(self.tests_root, "CMS104X", self.good, "Claim-c.json")
        from scripts.validate_test_fixtures import apply_fix_finding
        self.assertTrue(apply_fix_finding(finding, tests_root=self.tests_root))
        with open(path) as fh:
            data = json.load(fh)
        self.assertEqual(data["patient"]["reference"], f"Patient/{self.good}")

    def test_apply_misnamed_patient_fix_renames_and_injects_id(self):
        bad = os.path.join(self.tests_root, "CMS104X", "bad-guuid")
        os.makedirs(bad)
        write_json(bad, "null-null.json", {"resourceType": "Patient"})
        self.assertTrue(apply_misnamed_patient_fix("CMS104X", "bad-guuid", "null-null.json",
                                                   tests_root=self.tests_root))
        new_path = os.path.join(bad, "Patient-bad-guuid.json")
        self.assertTrue(os.path.exists(new_path))
        self.assertFalse(os.path.exists(os.path.join(bad, "null-null.json")))
        with open(new_path) as fh:
            data = json.load(fh)
        self.assertEqual(data["id"], "bad-guuid")
        self.assertFalse(apply_misnamed_patient_fix("CMS104X", "bad-guuid", "null-null.json",
                                                    tests_root=self.tests_root))

    def test_collect_anomaly_fixes_lists_misnamed_only(self):
        bad = os.path.join(self.tests_root, "CMS104X", "bad-guuid")
        os.makedirs(bad)
        write_json(bad, "null-null.json", {"resourceType": "Patient"})
        fixes = collect_anomaly_fixes(collect_test_cases(self.tests_root))
        self.assertEqual(fixes, [("CMS104X", "bad-guuid", "null-null.json")])
        self.assertTrue(apply_misnamed_patient_fix(*fixes[0], tests_root=self.tests_root))
        self.assertEqual(collect_anomaly_fixes(collect_test_cases(self.tests_root)), [])


if __name__ == "__main__":
    unittest.main()
