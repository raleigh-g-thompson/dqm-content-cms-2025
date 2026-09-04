#!/usr/bin/env python3
"""Re-attribute test-case Observations that were mis-profiled during the
QI-Core -> US Quality Core conversion.

Certain measures retrieve observations via a US Core profile
(``[USCore.BMIProfile]``, ``[USCore.BloodPressureProfile]``,
``[USCore.ObservationPregnancyStatusProfile]``, ...).  The engine's
profile-based retrieve matches **only** resources whose ``meta.profile``
declares that exact US Core profile.  During the USQC conversion the fixtures
were re-attributed to ``us-quality-core-observation-screening-assessment``,
making them invisible to those retrieves, which silently drops the intended
population membership:

* CMS69  — 16 Numerator cases carry a qualifying BMI observation (LOINC
  ``39156-5``) under the wrong profile -> Numerator evaluates 0 instead of 1;
  ``e25fc2f1`` carries a pregnancy-status observation (LOINC ``82810-3``, SCT
  ``77386006`` "Pregnant") -> Denominator Exclusion evaluates 0 instead of 1.
* CMS165 — 2 Numerator cases carry a blood-pressure panel observation (LOINC
  ``85354-9`` with ``8480-6`` systolic / ``8462-4`` diastolic components) under
  the wrong profile -> Numerator (SBP <140 & DBP <90) evaluates 0 instead of 1.
* CMS135 — ``6a86918d`` carries a pregnancy-status observation under the wrong
  profile -> Denominator Exception (pregnancy) evaluates 0 instead of 1.

Each target is pinned by (measure, case-prefix, LOINC code) so the script can
only touch the intended observations; it deliberately never re-attributes
unlisted cases (exclusions / exceptions / nominal-numerator cases stay at their
converted profile so their expected counts are preserved) nor the malformed
``...observationcancelled`` profile URLs (tracked separately — see F-08).

The edit is a targeted text substitution of the profile URI, preserving the
rest of the file byte-for-byte (the fixtures use a compact JSON style; a naive
``json.dumps`` round-trip would reformat the whole file and bloat the diff).

Idempotent: re-running reports only already-correct observations.

Run from the repo root (Python 3.12):

    python ./scripts/reattribute_uscore_profiles.py
"""
import json
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parents[1] / "input" / "tests" / "measure"

US_CORE_BMI = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-bmi"
US_CORE_BLOOD_PRESSURE = (
    "http://hl7.org/fhir/us/core/StructureDefinition/us-core-blood-pressure"
)
US_CORE_PREGNANCY = (
    "http://hl7.org/fhir/us/core/StructureDefinition/us-core-observation-pregnancystatus"
)
USQC_SCREENING = (
    "http://fhir.org/guides/astp/us-quality-core/StructureDefinition/"
    "us-quality-core-observation-screening-assessment"
)

LOINC_BMI = "39156-5"
LOINC_BP = "85354-9"
LOINC_PREGNANCY = "82810-3"

# (measure, case-prefix, LOINC code) -> target US Core profile.
# Each entry is a single observation whose expected population membership needs
# the US Core profile to be visible to the CQL's profile-based retrieve.
REATTRIBUTIONS = {
    # CMS69: 16 failing Numerator cases with a qualifying BMI observation.
    ("CMS69FHIRPCSBMIScreenAndFollowUp", p, LOINC_BMI): US_CORE_BMI
    for p in [
        "050201c2", "1102009b", "1e23fb8f", "27849d59", "42e6b4d6", "461fdfab",
        "463dd868", "7902e3dc", "8835a50b", "8e38b797", "9d92be1d", "c3caf126",
        "c84bf29f", "d4d064be", "e0821eec", "f5ae6269",
    ]
}
# CMS69: the single Denominator-Exclusion pregnancy case.
REATTRIBUTIONS[("CMS69FHIRPCSBMIScreenAndFollowUp", "e25fc2f1", LOINC_PREGNANCY)] = US_CORE_PREGNANCY
# CMS165: two Numerator cases with a blood-pressure panel observation.
for p in ["6f37e357", "f2d1fd7e"]:
    REATTRIBUTIONS[("CMS165FHIRControllingHighBP", p, LOINC_BP)] = US_CORE_BLOOD_PRESSURE
# CMS135: the single Denominator-Exception pregnancy case.
REATTRIBUTIONS[("CMS135FHIRACEIorARBorARNIforHF", "6a86918d", LOINC_PREGNANCY)] = US_CORE_PREGNANCY


def loinc_code(obs) -> str:
    coding = (obs.get("code") or {}).get("coding") or []
    return coding[0].get("code") if coding else None


def main() -> int:
    changed_total = 0
    skipped_total = 0
    reports = []

    for (measure, case_prefix, code), target_profile in sorted(REATTRIBUTIONS.items()):
        measure_dir = TEST_DIR / measure
        if not measure_dir.exists():
            raise RuntimeError(
                f"{measure_dir}: measure directory missing (check REATTRIBUTIONS table)"
            )
        case_dirs = [d for d in measure_dir.iterdir() if d.is_dir() and d.name.startswith(case_prefix)]
        if not case_dirs:
            raise RuntimeError(
                f"{measure}/ {case_prefix}: no matching case directory (check REATTRIBUTIONS table)"
            )
        for case_dir in case_dirs:
            matched_obs = None
            for obs_path in sorted(case_dir.glob("Observation-*.json")):
                original_text = obs_path.read_text(encoding="utf-8")
                obs = json.loads(original_text)
                if loinc_code(obs) != code:
                    continue
                matched_obs = (obs_path, original_text)
                break
            if matched_obs is None:
                raise RuntimeError(
                    f"{measure}/ {case_prefix}: no Observation with LOINC {code} "
                    "(check REATTRIBUTIONS table)"
                )
            obs_path, original_text = matched_obs
            if USQC_SCREENING in original_text:
                new_text = original_text.replace(USQC_SCREENING, target_profile)
                if new_text != original_text:
                    json.loads(new_text)  # sanity: still valid JSON
                    obs_path.write_text(new_text, encoding="utf-8")
                    changed_total += 1
                    reports.append(
                        f"CHANGED {measure} {case_prefix[:8]} {code} -> "
                        f"{target_profile.split('/')[-1]}"
                    )
                else:
                    skipped_total += 1
            elif target_profile in original_text:
                skipped_total += 1  # already correct
            else:
                raise RuntimeError(
                    f"{obs_path}: expected {USQC_SCREENING!r} or {target_profile!r}, "
                    "found neither"
                )

    for r in sorted(reports):
        print(r)
    print(f"\n{changed_total} observation(s) re-attributed; {skipped_total} already correct.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())