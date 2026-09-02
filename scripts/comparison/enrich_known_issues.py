#!/usr/bin/env python3
"""Enrich the migrated known_issues.json catalog with resolution metadata and
non-engine issues.

Extends the catalog produced by ``migrate_engine_issues.py``:
  * sets ``resolved`` / ``root_cause_status`` on each engine issue,
  * seeds ``affected_test_cases`` where the failing GUIDs are documented,
  * appends the fixture/migration/vendored/content issues (F-/M-/V-/C- series)
    distilled from ``change-classification.md`` / ``conversion-notes.md``.

These are historical/archival fields that the migration script does not know
about; run this only once against a freshly migrated catalog. The E- series
retain their verbatim ``body_md`` so regenerating engine-issues.md stays
byte-identical.
"""
import json
import sys
from pathlib import Path

DEFAULT_CATALOG = Path(__file__).resolve().parent / "known_issues.json"

# ---------------------------------------------------------------------------
# Resolution metadata for the E- series.
#   resolved          = no longer affects the score (its cases pass / are not
#                       excluded from reported scores).
#   root_cause_status = upstream status of the underlying defect: open | resolved | retired
# ---------------------------------------------------------------------------
ENGINE_META = {
    "E-01": {"resolved": False, "root_cause_status": "open"},
    "E-02": {"resolved": False, "root_cause_status": "open"},
    "E-03": {"resolved": True, "root_cause_status": "open"},   # .ext() bypass applied; cases pass (except CMS144, tracked separately)
    "E-04": {"resolved": True, "root_cause_status": "open"},
    "E-05": {"resolved": True, "root_cause_status": "open"},
    "E-06": {"resolved": True, "root_cause_status": "open"},
    "E-07": {"resolved": True, "root_cause_status": "open"},
    "E-08": {"resolved": True, "root_cause_status": "open"},
    "E-09": {"resolved": True, "root_cause_status": "open"},
    "E-10": {"resolved": True, "root_cause_status": "open"},
    "E-11": {"resolved": False, "root_cause_status": "open"},
    "E-12": {"resolved": False, "root_cause_status": "open"},
    "E-13": {"resolved": False, "root_cause_status": "open"},
    "E-14": {"resolved": False, "root_cause_status": "open"},
    "E-15": {"resolved": True, "root_cause_status": "retired"},
    "E-16": {"resolved": False, "root_cause_status": "open"},
    "E-17": {"resolved": False, "root_cause_status": "open"},
    "E-18": {"resolved": False, "root_cause_status": "open"},
}

# Documented failing (measure, guid) pairs per engine issue. Only fully-documented
# GUIDs are seeded — the report matches on the exact (measure, patient_guid) key, so
# partial/abbreviated GUIDs are omitted rather than fabricating full ones. Cases for
# E-12/E-16/E-17 are tracked via affected_measures + resolved=false until full GUIDs
# are recorded from their TestCaseResult files.
ENGINE_CASES = {
    "E-11": [
        ("CMS135FHIRACEIorARBorARNIforHF", "ec508dbb-76f6-4878-b8a2-114ea8e82297"),
        ("CMS135FHIRACEIorARBorARNIforHF", "cba5a449-1c45-4e11-ae0b-ba3974b410f7"),
        ("CMS135FHIRACEIorARBorARNIforHF", "c095195c-8893-4bf1-aa7d-ad2bfd9bafa5"),
        ("CMS165FHIRControllingHighBP", "45e01fed-56bb-483d-a860-af3d566bda11"),
    ],
}

# ---------------------------------------------------------------------------
# Non-engine issues distilled from change-classification.md / conversion-notes.md
# ---------------------------------------------------------------------------
def _case(measure, guid):
    return {"measure": measure, "guid": guid}


def additional_issues() -> list[dict]:
    return [
        {
            "id": "F-01",
            "title": "Stale `onc` → `astp` profile namespace on fixtures",
            "category": "fixture",
            "status": "Fixed",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "Bulk find/replace of meta.profile/extension URLs",
            "references": {"conversion_notes": ["#1"]},
            "affected_measures": ["CMS347", "CMS104", "CMS71", "CMS135", "CMS144",
                                  "CMS645", "CMS22", "CMS90", "CMS143", "CMS128",
                                  "CMS153", "CMS129"],
            "affected_test_cases": [],
            "body_md": (
                "### F-01: Stale `onc` → `astp` profile namespace\n\n"
                "- **Symptom**: IG renamed its profile namespace\n"
                "  (`.../guides/onc/us-quality-core/...` → `.../guides/astp/us-quality-core/...`), but\n"
                "  2,679 fixture files across 12 measures still carried `meta.profile`/extension URLs\n"
                "  on the old `onc` path, so profile-typed retrieves silently matched nothing.\n"
                "- **Resolution**: bulk find/replace (original #1; re-applied on the `astp-update` branch\n"
                "  off UQC main 0866d738, 2026-08-25). +2.28 pts suite-wide expected.\n"
                "- **Category**: resource / fixture data (bulk-fixed).\n"
                "- **References**: conversion-notes.md #1; change-classification.md §1.\n"
            ),
        },
        {
            "id": "F-02",
            "title": "Wrong-patient `subject.reference` in fixtures",
            "category": "fixture",
            "status": "Fixed",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "GUID correction",
            "references": {"conversion_notes": ["#7"]},
            "affected_measures": ["CMS347", "CMS72", "CMS108", "NHSNGlycemicControl"],
            "affected_test_cases": [],
            "body_md": (
                "### F-02: Wrong-patient `subject.reference` in fixtures\n\n"
                "- **Symptom**: 40 files — `Condition`/`Encounter`/`Observation`/`MedicationRequest`/\n"
                "  `Procedure`/`ServiceRequest` across CMS347 (34), CMS72 (3), CMS108 (1),\n"
                "  NHSNGlycemicControl (1) — pointed `subject.reference` at a typo'd Patient GUID\n"
                "  (stray character, embedded double-space).\n"
                "- **Resolution**: corrected.\n"
                "- **Category**: resource / fixture data.\n"
                "- **References**: conversion-notes.md #7; change-classification.md §2.\n"
            ),
        },
        {
            "id": "F-03",
            "title": "Wrong-patient `Claim.patient` / `Coverage.beneficiary` / `AllergyIntolerance.patient`",
            "category": "fixture",
            "status": "Fixed",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "GUID correction",
            "references": {"conversion_notes": ["#11"]},
            "affected_measures": ["CMS72", "CMS104", "CMS108", "CMS190", "CMS1028",
                                  "CMS1264", "CMS71"],
            "affected_test_cases": [],
            "body_md": (
                "### F-03: Wrong-patient `Claim`/`Coverage`/`AllergyIntolerance` references\n\n"
                "- **Symptom**: 188 files across 7 measures (CMS72 97, CMS104 70, CMS108/CMS190 2 each,\n"
                "  CMS1028/CMS1264/CMS71 1 each, plus 14 `Coverage` + 1 `AllergyIntolerance`) referenced\n"
                "  two fixed placeholder GUIDs from a generation template that never existed as real\n"
                "  test cases.\n"
                "- **Resolution**: corrected.\n"
                "- **Category**: resource / fixture data.\n"
                "- **References**: conversion-notes.md #11; change-classification.md §2.\n"
            ),
        },
        {
            "id": "F-04",
            "title": "Invalid UCUM `system` URI on Observation fixtures",
            "category": "fixture",
            "status": "Fixed",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "URI correction",
            "references": {"conversion_notes": ["#3"]},
            "affected_measures": ["CMS347", "CMS69"],
            "affected_test_cases": [],
            "body_md": (
                "### F-04: Invalid UCUM `system` URI\n\n"
                "- **Symptom**: 3 `Observation` fixtures (CMS347 ×1, CMS69 ×2) used\n"
                "  `https://ucum.org` instead of `http://unitsofmeasure.org`.\n"
                "- **Resolution**: fixed (#3).\n"
                "- **Category**: resource / fixture data.\n"
            ),
        },
        {
            "id": "F-05",
            "title": "Missing vocabulary source file (CMS871)",
            "category": "fixture",
            "status": "Fixed",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "Commit external valueset source",
            "references": {"conversion_notes": ["#4"]},
            "affected_measures": ["CMS871"],
            "affected_test_cases": [],
            "body_md": (
                "### F-05: Missing vocabulary source file\n\n"
                "- **Symptom**: `ValueSet-2.16.840.1.113762.1.4.1196.394` (“Hypoglycemics Treatment\n"
                "  Medications”) existed only in the IG Publisher's terminology cache, never committed\n"
                "  to `input/vocabulary/valueset/external/` (CMS871).\n"
                "- **Resolution**: fixed (#4).\n"
                "- **Category**: resource / fixture data.\n"
            ),
        },
        {
            "id": "F-06",
            "title": "Sparse `MedicationRequest` dosage fixtures trip `singleton from empty list`",
            "category": "fixture",
            "status": "Fixed in probes",
            "resolved": True,
            "root_cause_status": "open",   # engine E-10 still open upstream
            "workaround": "Fixture-side enrichment (doseAndRate/timing)",
            "references": {"conversion_notes": ["#25"]},
            "affected_measures": ["CMS156"],
            "affected_test_cases": [],
            "body_md": (
                "### F-06: Sparse `MedicationRequest` dosage fixtures\n\n"
                "- **Symptom**: fixtures with no `doseAndRate`/timing trip an engine bug (E-10,\n"
                "  `singleton from empty list`) rather than falling through to a valid fallback.\n"
                "  Found via CMS156 probe fixtures; real CMS156 fixtures `c409fbc9`/`07f11229` flagged\n"
                "  as likely needing the same enrichment.\n"
                "- **Resolution**: fixed in probes; watch real CMS156 fixtures.\n"
                "- **Category**: resource / fixture data (interacts with engine E-10).\n"
            ),
        },
        {
            "id": "F-07",
            "title": "Automated CORE-field patient-reference fix (validate_test_fixtures.py)",
            "category": "fixture",
            "status": "Fixed",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "Scripted fix (commit fe41c7bcc)",
            "references": {"conversion_notes": ["#27"]},
            "affected_measures": ["CMS347", "CMS104", "CMS72", "CMS108", "CMS190",
                                  "CMS1028", "CMS1264", "CMS71", "CMS69", "CMS871"],
            "affected_test_cases": [],
            "body_md": (
                "### F-07: Automated CORE-field patient-reference fix\n\n"
                "- **Symptom**: 229 files across 10 measures (`subject`/`patient`/`beneficiary`)\n"
                "  pointed at the wrong Patient GUID, plus 2 structural anomalies\n"
                "  (`null-null.json` rename CMS871, oddly-named Claim CMS1264). Same class as F-02/F-03.\n"
                "- **Resolution**: applied via `scripts/validate_test_fixtures.py` (commit `fe41c7bcc`,\n"
                "  2026-09-01).\n"
                "- **Category**: resource / fixture data.\n"
            ),
        },
        {
            "id": "M-01",
            "title": "`doNotPerform` not excluded from MedicationRequest/ServiceRequest retrieves",
            "category": "migration",
            "status": "Fixed",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "CQL rework",
            "references": {"conversion_notes": ["#6", "#13", "#17"]},
            "affected_measures": ["CMS347", "CMS104", "CMS71", "CMS135", "CMS144",
                                  "CMS645", "CMS22"],
            "affected_test_cases": [],
            "body_md": (
                "### M-01: `doNotPerform` double-counted\n\n"
                "- **Symptom**: a “do NOT perform” record was not excluded from `MedicationRequest`/\n"
                "  `ServiceRequest` retrieves and was double-counted as an actual order.\n"
                "- **Resolution**: fixed (conversion-notes #6, #13, #17).\n"
                "- **Category**: migration regression we introduced converting the CQL.\n"
            ),
        },
        {
            "id": "M-02",
            "title": "Choice-typed `.effective`/`.performed` compared without `.toInterval()`",
            "category": "migration",
            "status": "Fixed",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "CQL rework",
            "references": {"conversion_notes": ["#12", "#17"]},
            "affected_measures": ["CMS72", "CMS646"],
            "affected_test_cases": [],
            "body_md": (
                "### M-02: Choice-typed effective/performed compared directly\n\n"
                "- **Symptom**: `.effective`/`.performed` compared directly against a temporal operator\n"
                "  without `.toInterval()` first.\n"
                "- **Resolution**: fixed (CMS72 #12; CMS646 #17; CMS190 attempted then reverted — wrong\n"
                "  diagnosis).\n"
                "- **Category**: migration regression.\n"
            ),
        },
        {
            "id": "M-03",
            "title": "`.onset.toInterval()` used instead of `.prevalenceInterval()` for chronic conditions",
            "category": "migration",
            "status": "Fixed (E-13)",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "Base `[FHIR.Condition: ...]` retrieve (see E-13)",
            "references": {"conversion_notes": ["#10", "#15", "#16", "#19", "#20-final", "#22"]},
            "affected_measures": ["CMS347", "CMS90", "CMS133", "CMS142", "CMS143",
                                  "CMS951", "CMS157", "CMS129", "CMS159", "CMS155",
                                  "CMS128", "CMS646", "CMS69"],
            "affected_test_cases": [],
            "body_md": (
                "### M-03: `.onset.toInterval()` vs `.prevalenceInterval()`\n\n"
                "- **Symptom**: a zero-width onset point used for “chronic/still-active” checks can\n"
                "  never `overlaps` a measurement period years later.\n"
                "- **Resolution**: fixed (10 measures, ~16 call sites); required 4 attempts; final fix\n"
                "  evolved into the E-13 base-retrieve generalization.\n"
                "- **Category**: migration regression (interacts with engine E-03/E-05/E-06/E-13).\n"
            ),
        },
        {
            "id": "M-04",
            "title": "Field swapped `.recorded` → `.effective`/`.performed` to dodge a translator ambiguity",
            "category": "migration",
            "status": "Fixed (.ext() bypass)",
            "resolved": True,
            "root_cause_status": "open",   # engine E-03 ambiguity still open upstream
            "workaround": "`.ext()` bypass",
            "references": {"conversion_notes": ["#19", "#21"]},
            "affected_measures": ["CMS190", "CMS996", "CMS108", "CMS68"],
            "affected_test_cases": [],
            "body_md": (
                "### M-04: Field swapped to `.effective`/`.performed`\n\n"
                "- **Symptom**: fields were swapped from `.recorded` to `.effective`/`.performed` during\n"
                "  migration specifically to dodge a translator ambiguity — wrong field, chosen to avoid\n"
                "  a crash.\n"
                "- **Resolution**: fixed via `.ext()` bypass (engine E-03 ambiguity remains upstream).\n"
                "- **Category**: migration regression (interacts with engine E-03).\n"
            ),
        },
        {
            "id": "M-05",
            "title": "`AHAOverall.cql` Choice narrowing dropped `ConditionProblemsHealthConcerns` support (CMS144)",
            "category": "migration",
            "status": "Not fixed",
            "resolved": False,
            "root_cause_status": "open",
            "workaround": "None — blocked on engine E-05 constraint",
            "references": {"conversion_notes": ["#19", "#20"]},
            "affected_measures": ["CMS144"],
            "affected_test_cases": [],
            "body_md": (
                "### M-05: `AHAOverall.cql` Choice narrowing\n\n"
                "- **Symptom**: `overlapsHeartFailureOutpatientEncounter`/\n"
                "  `overlapsAfterHeartFailureOutpatientEncounter` narrowed from\n"
                "  `Choice<ConditionEncounterDiagnosis, ConditionProblemsHealthConcerns>` to\n"
                "  `ConditionEncounterDiagnosis` only (CMS144, 7 DenExcl defines lose\n"
                "  `ConditionProblemsHealthConcerns` support).\n"
                "- **Resolution**: not fixed — needs a genuinely new approach, not another\n"
                "  sibling-overload guess (blocked on engine E-03/E-05 runtime-class collision).\n"
                "- **Category**: migration regression.\n"
            ),
        },
        {
            "id": "V-01",
            "title": "Vendored `CMD.cql` `convert…to days` null / calendar-unit bug (medication dispense side)",
            "category": "vendored",
            "status": "Fixed locally",
            "resolved": True,
            "root_cause_status": "open",   # upstream CMD not patched
            "workaround": "CMS128-local workaround function replicating logic minus broken `convert`",
            "references": {"conversion_notes": ["#22"]},
            "affected_measures": ["CMS128"],
            "affected_test_cases": [],
            "body_md": (
                "### V-01: Vendored `CumulativeMedicationDuration` dispense bug\n\n"
                "- **Symptom**: `MedicationDispensePeriod`/`medicationDispensePeriod()` compute\n"
                "  `daysSupply` via `(convert D.daysSupply to days).value`, which returns null on this\n"
                "  engine (engine E-07); unlike the sibling MedicationRequest-side functions, the\n"
                "  dispense-side was never patched by the library's own authors (their inline TODO:\n"
                "  “this isn't working as expected, convert results in null”).\n"
                "- **Resolution**: CMS128-local workaround function replicating the vendored logic minus\n"
                "  the broken `convert`; `CMD.Quantity()` reused for the shared helper. Not patched\n"
                "  upstream.\n"
                "- **Category**: third-party / vendored CQL library.\n"
            ),
        },
        {
            "id": "V-02",
            "title": "Vendored `CumulativeMedicationDuration` 6.0.000 model adaptation (CMS156)",
            "category": "vendored",
            "status": "Fixed locally",
            "resolved": True,
            "root_cause_status": "open",
            "workaround": "Model-adapted local copy (deviation #2/#3/#4 in vendor header)",
            "references": {"conversion_notes": ["#25"]},
            "affected_measures": ["CMS156"],
            "affected_test_cases": [],
            "body_md": (
                "### V-02: Vendored `CumulativeMedicationDuration` 6.0.000 model adaptation\n\n"
                "- **Symptom**: full vendored `CumulativeMedicationDuration` 6.0.000 needed model\n"
                "  adaptation beyond a drop-in copy: `timing.repeat.bounds as Interval<DateTime>` became\n"
                "  `as FHIR.Period`; a net-new `ToDays(FHIR.Duration)` helper was needed; and\n"
                "  `averageDailyDose()` had to bypass unit-aware Quantity division entirely.\n"
                "- **Resolution**: fixed locally for CMS156 (deviations #2/#3/#4 in the vendor file\n"
                "  header). Interacts with engine E-07/E-08/E-09.\n"
                "- **Category**: third-party / vendored CQL library.\n"
            ),
        },
        {
            "id": "C-01",
            "title": "CMS145 / CMS149 — no CQL authored (content gap)",
            "category": "content",
            "status": "Fixed (ported)",
            "resolved": True,
            "root_cause_status": "resolved",
            "workaround": "Port QC→UQC CQL",
            "references": {"conversion_notes": ["#36"]},
            "affected_measures": ["CMS145", "CMS149"],
            "affected_test_cases": [],
            "body_md": (
                "### C-01: CMS145 / CMS149 — no CQL authored\n\n"
                "- **Symptom**: no CQL authored at all — a content-authoring gap, not a conversion bug.\n"
                "- **Resolution**: CMS149's CQL ported QC→UQC (now fully passing, #36); CMS145 ported on\n"
                "  the `cms145-cms149-port` branch, pending verification.\n"
                "- **Category**: content gap (out of scope for engine classification).\n"
            ),
        },
        {
            "id": "C-02",
            "title": "CMS157 — Cancer diagnosis coded in ICD-10-CM vs SNOMED-only valueset",
            "category": "content",
            "status": "Not fixed",
            "resolved": False,
            "root_cause_status": "open",
            "workaround": "None — fixture/valueset alignment needed",
            "references": {"conversion_notes": ["#33"]},
            "affected_measures": ["CMS157"],
            "affected_test_cases": [],
            "body_md": (
                "### C-02: CMS157 — terminology/content gap, not engine\n\n"
                "- **Symptom**: fixtures code the “Cancer” diagnosis in ICD-10-CM (`C00.0`, `C40.00`, …)\n"
                "  but the measure's “Cancer” valueset (`2.16.840.1.113883.3.526.3.1010`) is\n"
                "  SNOMED-only, so the (E-13-sound) base `[Condition: \"Cancer\"]` retrieve correctly\n"
                "  matches none of them → 19 residual mismatches.\n"
                "- **Resolution**: fixture/valueset alignment needed.\n"
                "- **Category**: terminology / content gap (not an engine issue).\n"
            ),
        },
    ]


def main():
    catalog_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CATALOG
    with open(catalog_path, encoding="utf-8") as fh:
        catalog = json.load(fh)

    # 1. Apply engine resolution metadata + seeded cases
    by_id = {i["id"]: i for i in catalog["issues"]}
    for eid, meta in ENGINE_META.items():
        issue = by_id[eid]
        issue["resolved"] = meta["resolved"]
        issue["root_cause_status"] = meta["root_cause_status"]
        issue["affected_test_cases"] = [
            _case(m, g) for m, g in ENGINE_CASES.get(eid, [])
        ]

    # 2. Append non-engine issues (skip if already present)
    existing = set(by_id.keys())
    for issue in additional_issues():
        if issue["id"] not in existing:
            catalog["issues"].append(issue)
            existing.add(issue["id"])

    catalog["enriched"] = True
    catalog_path.write_text(
        json.dumps(catalog, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"Enriched {catalog_path}: {len(catalog['issues'])} issues")


if __name__ == "__main__":
    main()
