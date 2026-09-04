import json
import tempfile
import unittest
from pathlib import Path

from scripts.fix_cms347_fixture_data import (
    REFERENCE_FIXES,
    TEST_DIR,
    UCUM_TARGETS,
    fix_broken_subject_references,
    fix_ucum_quantity_systems,
)


def build_bad_ucum_case(root: Path, measure: str = UCUM_TARGETS[0][0],
                        case: str = None) -> Path:
    case = case or (UCUM_TARGETS[0][1] + "-synthetic")
    dst = root / "input" / "tests" / "measure" / measure / case
    dst.mkdir(parents=True)
    obs = {
        "resourceType": "Observation",
        "id": "obs-bad-ucum",
        "status": "final",
        "code": {"coding": [{"system": "http://loinc.org", "code": "13457-7"}]},
        "valueQuantity": {"value": 220, "unit": "mg/dL",
                          "system": "https://ucum.org", "code": "mg/dL"},
    }
    (dst / "Observation-obs-bad-ucum.json").write_text(
        json.dumps(obs, indent=2) + "\n", encoding="utf-8")
    return dst


def build_bad_reference_case(root: Path, measure: str = REFERENCE_FIXES[0][0]) -> Path:
    _, case_prefix, bad, _ = REFERENCE_FIXES[0]
    case = case_prefix + "-synthetic"
    dst = root / "input" / "tests" / "measure" / measure / case
    dst.mkdir(parents=True)
    enc = {
        "resourceType": "Encounter",
        "id": "enc-broken-ref",
        "status": "finished",
        "subject": {"reference": bad},
    }
    obs = {
        "resourceType": "Observation",
        "id": "obs-broken-ref",
        "status": "amended",
        "subject": {"reference": bad},
    }
    (dst / "Encounter-9d311cdd.json").write_text(
        json.dumps(enc, indent=2) + "\n", encoding="utf-8")
    (dst / "Observation-ea2c69a6.json").write_text(
        json.dumps(obs, indent=2) + "\n", encoding="utf-8")
    return dst


class UcumSystemTest(unittest.TestCase):
    def test_repairs_to_canonical(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            build_bad_ucum_case(root)
            changed, _ = fix_ucum_quantity_systems(root, [UCUM_TARGETS[0]])
            self.assertEqual(changed, 1)
            target = root / "input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-synthetic/Observation-obs-bad-ucum.json"
            data = json.loads(target.read_text(encoding="utf-8"))
            self.assertEqual(data["valueQuantity"]["system"], "http://unitsofmeasure.org")

    def test_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            build_bad_ucum_case(root)
            fix_ucum_quantity_systems(root, [UCUM_TARGETS[0]])
            changed, _ = fix_ucum_quantity_systems(root, [UCUM_TARGETS[0]])
            self.assertEqual(changed, 0)

    def test_ignores_canonical_observations(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            build_bad_ucum_case(root)
            obs_path = root / "input/tests/measure/CMS347FHIRStatinPreventionTxCVD/6da189af-synthetic/Observation-ok.json"
            obs_path.write_text(json.dumps({
                "resourceType": "Observation",
                "id": "obs-ok",
                "status": "final",
                "valueQuantity": {"value": 1, "system": "http://unitsofmeasure.org", "code": "kg/m2"},
            }) + "\n", encoding="utf-8")
            changed, skipped = fix_ucum_quantity_systems(root, [UCUM_TARGETS[0]])
            self.assertEqual(changed, 1)
            self.assertEqual(skipped, 1)


class EncounterReferenceTest(unittest.TestCase):
    def test_repairs_reference_in_all_resources(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            case_dir = build_bad_reference_case(root)
            changed, _ = fix_broken_subject_references(root, [REFERENCE_FIXES[0]])
            self.assertEqual(changed, 2)
            for name in ("Encounter-9d311cdd.json", "Observation-ea2c69a6.json"):
                data = json.loads((case_dir / name).read_text(encoding="utf-8"))
                self.assertEqual(
                    data["subject"]["reference"],
                    "Patient/1d3021bb-b593-4efc-af5b-320243bbe9b7")

    def test_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            build_bad_reference_case(root)
            fix_broken_subject_references(root, [REFERENCE_FIXES[0]])
            changed, _ = fix_broken_subject_references(root, [REFERENCE_FIXES[0]])
            self.assertEqual(changed, 0)

    def test_only_reference_value_rewritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            build_bad_reference_case(root)
            bad, good = REFERENCE_FIXES[0][2], REFERENCE_FIXES[0][3]
            enc_file = root / "input/tests/measure/CMS347FHIRStatinPreventionTxCVD/1d3021bb-synthetic/Encounter-9d311cdd.json"
            original = enc_file.read_text(encoding="utf-8")
            fix_broken_subject_references(root, [REFERENCE_FIXES[0]])
            repaired = enc_file.read_text(encoding="utf-8")
            self.assertEqual(len(original) + (len(good) - len(bad)), len(repaired))
            self.assertNotIn(bad, repaired)
            self.assertIn(good, repaired)


class ScriptAlignmentTest(unittest.TestCase):
    """The repair script's target pins must still match the repo layout."""

    def test_ucum_targets_exist(self):
        for measure, prefix in UCUM_TARGETS:
            case = next(d for d in (TEST_DIR / measure).iterdir()
                        if d.is_dir() and d.name.startswith(prefix))
            self.assertTrue(any(p.name.startswith("Observation-") for p in case.iterdir()))

    def test_reference_fix_target_repaired(self):
        measure, case_prefix, bad, good = REFERENCE_FIXES[0]
        case = next(d for d in (TEST_DIR / measure).iterdir()
                    if d.is_dir() and d.name.startswith(case_prefix))
        broken = [p for p in case.iterdir()
                  if p.name.endswith(".json") and bad in p.read_text(encoding="utf-8")]
        self.assertFalse(broken, f"{broken}: still carry the broken reference {bad!r}")
        enc = next(p for p in case.iterdir() if p.name.startswith(REFERENCE_FIXES[0][2] if False else "Encounter-9d311cdd"))
        self.assertEqual(json.loads(enc.read_text(encoding="utf-8"))["subject"]["reference"], good)


if __name__ == "__main__":
    unittest.main()