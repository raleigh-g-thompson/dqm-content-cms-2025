#!/usr/bin/env python3
"""Repair two fixture-data defects surfaced by the CMS347 classification
(2026-09-04).  These are pure resource-data fixes that make the fixture's
resources evaluable per the FHIR spec and the US Quality Core model; no CQL,
no MeasureReport (expected) values, and no test-case membership change.

F-11 — non-canonical UCUM quantity system.
``valueQuantity.system`` must be the canonical UCUM system URL
(``http://unitsofmeasure.org``).  The remaining suite instances use
``https://ucum.org``, which this engine's ``FHIRHelpers.ToQuantity`` rejects as
an invalid quantity code, throwing and turning the whole library into Missing
Results:
* CMS347 ``6da189af`` — LDL cholesterol Observation (LOINC ``13457-7``,
  ``valueQuantity`` ``mg/dL``) -> all 4 groups Missing Results (20 cells).
  Every other CMS347 LDL observation uses the canonical system.
* CMS69 ``7b34e64e`` / ``45b1ce40`` — BMI Observations (``kg/m2``).  Latent
  today (the engine's BMI profile retrieve is empty on this measure), but the
  resources are non-conformant and would crash once that retrieve works.

F-12 — malformed resource `subject.reference`(s).
CMS347 ``1d3021bb``'s Encounter links ``subject`` to
``Patient/1d3021bb-b593-4efc-af5b-3  20243bbe9b7`` (two stray spaces split the
patient GUID), so neither engine can resolve the encounter to the patient:
``Qualifying Encounter During Day of Measurement Period`` is empty and
Group_3 (the only group the patient belongs to — diabetes + age) evaluates
0 against the fixture's expected 1 (Initial Population / Denominator /
Denominator Exception). The case's palliative-care FACIT-Pal screening
Observation (LOINC ``71007-9``) carries the **same** broken reference, so the
third expected cell stays 0 (``Has Palliative Care in the Measurement Period``
never links the screen to the patient). Fixing the reference to the patient's
actual id in every resource makes the encounter qualify and the exception fire.
Ensures the reference is correct in all resources of the case (grep-verified
unique to this case).

The edits are targeted text substitutions preserving the rest of the file
byte-for-byte (the fixtures use a compact JSON style; a ``json.dumps``
round-trip would reformat the whole file).

Idempotent: re-running reports only already-correct files.

Run from the repo root (Python 3.12):

    python ./scripts/fix_cms347_fixture_data.py
"""
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEST_DIR = REPO_ROOT / "input" / "tests" / "measure"

UCUM_CANONICAL = "http://unitsofmeasure.org"
UCUM_BAD = "https://ucum.org"

# (measure, case-prefix) whose Observation(s) carry https://ucum.org quantities.
UCUM_TARGETS = [
    ("CMS347FHIRStatinPreventionTxCVD", "6da189af"),
    ("CMS69FHIRPCSBMIScreenAndFollowUp", "7b34e64e"),
    ("CMS69FHIRPCSBMIScreenAndFollowUp", "45b1ce40"),
]

# (measure, case-prefix, bad-reference, good-reference): every resource file in the
# case directory whose subject.reference contains the broken substring is repaired.
REFERENCE_FIXES = [
    (
        "CMS347FHIRStatinPreventionTxCVD",
        "1d3021bb",
        "Patient/1d3021bb-b593-4efc-af5b-3  20243bbe9b7",
        "Patient/1d3021bb-b593-4efc-af5b-320243bbe9b7",
    ),
]


def _cases(measure_dir: Path, case_prefix: str) -> list:
    cases = [d for d in measure_dir.iterdir() if d.is_dir() and d.name.startswith(case_prefix)]
    if not cases:
        raise RuntimeError(f"{measure_dir.name}/{case_prefix}: no matching case directory")
    return cases


def fix_ucum_quantity_systems(root: Path = REPO_ROOT, targets=UCUM_TARGETS) -> tuple:
    """Fix https://ucum.org quantity systems; return (changed, skipped)."""
    changed = 0
    skipped = 0
    test_dir = root / "input" / "tests" / "measure"
    for measure, prefix in sorted(targets):
        measure_dir = test_dir / measure
        if not measure_dir.exists():
            raise RuntimeError(f"{measure_dir}: measure directory missing")
        for case_dir in _cases(measure_dir, prefix):
            for obs_path in sorted(case_dir.glob("Observation-*.json")):
                original = obs_path.read_text(encoding="utf-8")
                if UCUM_BAD not in original:
                    skipped += 1
                    continue
                if UCUM_CANONICAL in original:
                    skipped += 1  # already correct, but still carries the bad string? conservative
                    continue
                new_text = original.replace(UCUM_BAD, UCUM_CANONICAL)
                json_valid = json.loads(new_text)  # sanity: still valid JSON
                q = (json_valid.get("valueQuantity") or {}).get("system")
                if q != UCUM_CANONICAL:
                    raise RuntimeError(f"{obs_path}: valueQuantity.system not fixed")
                obs_path.write_text(new_text, encoding="utf-8")
                changed += 1
    return changed, skipped


def fix_broken_subject_references(
    root: Path = REPO_ROOT, reference_fixes=REFERENCE_FIXES
) -> tuple:
    """Repair malformed subject.reference strings in every resource of the target
    cases; return (changed_files, skipped_files)."""
    changed = 0
    skipped = 0
    test_dir = root / "input" / "tests" / "measure"
    for measure, prefix, bad, good in reference_fixes:
        if len(bad) == len(good):
            raise RuntimeError(f"{bad!r} and {good!r} must differ in length")
        measure_dir = test_dir / measure
        if not measure_dir.exists():
            raise RuntimeError(f"{measure_dir}: measure directory missing")
        for case_dir in _cases(measure_dir, prefix):
            for res_path in sorted(case_dir.glob("*.json")):
                original = res_path.read_text(encoding="utf-8")
                if bad not in original:
                    skipped += 1
                    continue
                new_text = original.replace(bad, good)
                json_valid = json.loads(new_text)  # sanity: still valid JSON
                subject = json_valid.get("subject") or {}
                if subject.get("reference") == bad:
                    raise RuntimeError(f"{res_path}: subject.reference not repaired")
                res_path.write_text(new_text, encoding="utf-8")
                changed += 1
    return changed, skipped


def main() -> int:
    u, us = fix_ucum_quantity_systems()
    r, rs = fix_broken_subject_references()
    print(f"UCUM system: {u} fixed, {us} already correct")
    print(f"subject.reference: {r} fixed, {rs} already correct")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())