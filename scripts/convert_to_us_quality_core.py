#!/usr/bin/env python3
"""
Convert QICore 2025 CQL libraries and test cases to US Quality Core.

Usage:
  python convert_to_us_quality_core.py --sanity-check   # validate against already-converted files
  python convert_to_us_quality_core.py --convert-cql    # convert new CQL files
  python convert_to_us_quality_core.py --convert-tests  # convert new test case directories
  python convert_to_us_quality_core.py --all            # convert-cql + convert-tests
"""

import argparse
import difflib
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_DIR = Path(__file__).resolve().parent.parent.parent
QICORE_CQL_DIR = REPO_DIR / "dqm-content-qicore-2025" / "input" / "cql"
QICORE_TESTS_DIR = REPO_DIR / "dqm-content-qicore-2025" / "input" / "tests" / "measure"
CMS_CQL_DIR = REPO_DIR / "dqm-content-cms-2025" / "input" / "cql"
CMS_TESTS_DIR = REPO_DIR / "dqm-content-cms-2025" / "input" / "tests" / "measure"

# Copyright header added to all converted CQL files
COPYRIGHT_HEADER = (
    "/*\n"
    "NOTE: For use by March 2026 US Realm Connectathon participants for internal use only."
    " Not for use or distribution in commercial products.\n"
    "*/\n"
)

# ---------------------------------------------------------------------------
# Files that should NOT be converted as local CQL libraries
# (they become external namespaced references)
# ---------------------------------------------------------------------------

SKIP_CQL_FILES = {
    "FHIRHelpers.cql",          # → hl7.fhir.uv.cql.FHIRHelpers
    "CumulativeMedicationDuration.cql",  # → hl7.fhir.us.cql.CumulativeMedicationDuration
    "QICoreCommon.cql",         # → replaced by FHIRCommon + USCoreCommon + USCoreElements + USQualityCoreCommon
}

# ---------------------------------------------------------------------------
# Include version bump mapping for local shared libraries
# ---------------------------------------------------------------------------

LOCAL_LIB_NEW_VERSION = {
    "SupplementalDataElements": "6.1.000",
    "Status": "2.1.000",
    "AdultOutpatientEncounters": "5.1.000",
    "AdvancedIllnessandFrailty": "2.1.000",
    "Hospice": "7.1.000",
    "PalliativeCare": "2.1.000",
    "CQMCommon": "5.1.000",
    "TJCOverall": "9.1.000",
    "VTE": "9.1.000",
    "NHSNHelpers": "1.1.000",
    "PCMaternal": "6.1.000",
    "AHAOverall": "5.1.000",
    "AlaraCommonFunctions": "2.1.000",
    "Antibiotic": "2.1.000",
    "USQualityCoreCommon": "0.1.0-cibuild",
}

# ---------------------------------------------------------------------------
# Resource types that get USQualityCore. prefix in retrieval expressions
# ---------------------------------------------------------------------------

USQUALITYCORE_RESOURCE_TYPES = [
    # Order matters: longer/more-specific names first to avoid partial matches
    "ObservationScreeningAssessment",
    "ObservationClinicalResult",
    "ObservationCancelled",
    "DiagnosticReportNote",
    "MedicationAdministration",
    "MedicationDispense",
    "MedicationRequest",
    "ConditionEncounterDiagnosis",
    "ConditionProblemsHealthConcerns",
    "FamilyMemberHistory",
    "AllergyIntolerance",
    "ServiceRequest",
    "DeviceRequest",
    "Immunization",
    "Observation",
    "Encounter",
    "Procedure",
    "Coverage",
    "Communication",
    "Specimen",
    "AdverseEvent",
    "Claim",
    "Task",
]

# ---------------------------------------------------------------------------
# CQL conversion
# ---------------------------------------------------------------------------

def _parse_include_line(line: str):
    """
    Parse an include line, returning (lib_name, version, alias) or None.
    Handles both plain names and namespaced names like hl7.fhir.uv.cql.FHIRHelpers.
    """
    m = re.match(
        r"\s*include\s+(\S+)\s+version\s+'([^']+)'\s+called\s+(\S+)",
        line,
    )
    if m:
        return m.group(1), m.group(2), m.group(3)
    return None


def _transform_using(content: str) -> str:
    """Replace 'using QICore version ...' with the three USQualityCore using statements.
    Also removes any extra 'using USCore version ...' lines that may already be present."""
    # Remove any pre-existing USCore using line (it will be re-added with the -derived suffix)
    content = re.sub(r"\nusing USCore version '[^']+'\n", "\n", content)
    # Replace the QICore using line
    content = re.sub(
        r"using QICore version '6\.0\.0'",
        (
            "using USQualityCore version '0.1.0-cibuild'\n"
            "using USCore version '6.1.0-derived'\n"
            "using FHIR version '4.0.1'"
        ),
        content,
    )
    return content


def _transform_version(content: str) -> str:
    """Bump the library version declaration from x.0.000 to x.1.000."""
    return re.sub(
        r"^(library\s+\S+\s+version\s+')(\d+)\.0\.000'",
        r"\g<1>\2.1.000'",
        content,
        count=1,
        flags=re.MULTILINE,
    )


def _transform_includes(content: str) -> str:
    """
    Rebuild the entire include block:
    1. Replace FHIRHelpers with hl7.fhir.uv.cql.FHIRHelpers
    2. Remove QICoreCommon (absorbed into the standard block)
    3. Replace CumulativeMedicationDuration with hl7.fhir.us.cql version
    4. Add FHIRCommon, USCoreCommon, USCoreElements, USQualityCoreCommon
    5. Bump versions of other local includes
    """
    lines = content.split("\n")

    # Find the contiguous include block
    include_start = None
    include_end = None
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("include "):
            if include_start is None:
                include_start = i
            include_end = i
        elif include_start is not None and stripped and not stripped.startswith("//"):
            # Non-empty, non-comment line after the include block ends it
            if not stripped.startswith("include "):
                break

    if include_start is None:
        return content  # no includes found, nothing to do

    original_include_lines = lines[include_start : include_end + 1]

    # Categorise the original includes
    cmd_alias = None
    other_includes = []  # list of (lib_name, alias) for local libs

    for line in original_include_lines:
        parsed = _parse_include_line(line)
        if not parsed:
            continue
        lib_name, _version, alias = parsed

        # Extract the short name from namespaced includes (e.g. hl7.fhir.uv.cql.FHIRHelpers → FHIRHelpers)
        short_name = lib_name.split(".")[-1]

        if short_name == "FHIRHelpers":
            pass  # replaced by standard block
        elif short_name == "QICoreCommon":
            pass  # removed; replaced by USQualityCoreCommon etc.
        elif short_name == "CumulativeMedicationDuration":
            cmd_alias = alias
        else:
            other_includes.append((short_name, alias))

    # Build new include block
    new_includes = []
    new_includes.append("include hl7.fhir.uv.cql.FHIRHelpers version '4.0.1' called FHIRHelpers")
    new_includes.append("include hl7.fhir.uv.cql.FHIRCommon version '2.0.0' called FHIRCommon")
    if cmd_alias:
        new_includes.append(
            f"include hl7.fhir.us.cql.CumulativeMedicationDuration version '2.0.0-ballot' called {cmd_alias}"
        )
    new_includes.append(
        "include hl7.fhir.us.cql.USCoreCommon version '2.0.0-ballot' called USCoreCommon"
    )
    new_includes.append(
        "include hl7.fhir.us.cql.USCoreElements version '2.0.0-ballot' called USCoreElements"
    )
    new_includes.append("")  # blank separator
    new_includes.append(
        "include USQualityCoreCommon version '0.1.0-cibuild' called USQualityCoreCommon"
    )

    for lib_name, alias in other_includes:
        if lib_name in LOCAL_LIB_NEW_VERSION:
            new_ver = LOCAL_LIB_NEW_VERSION[lib_name]
            new_includes.append(f"include {lib_name} version '{new_ver}' called {alias}")
        else:
            # Unknown library - keep with original version (shouldn't happen for known measures)
            # Find the original line to preserve it
            for orig_line in original_include_lines:
                parsed = _parse_include_line(orig_line)
                if parsed and parsed[0].split(".")[-1] == lib_name:
                    new_includes.append(orig_line.rstrip())
                    break

    # Replace the include block in the original lines
    new_lines = lines[:include_start] + new_includes + lines[include_end + 1 :]
    return "\n".join(new_lines)


def _consolidate_condition_unions(content: str) -> str:
    """
    Replace unions of ConditionEncounterDiagnosis + ConditionProblemsHealthConcerns
    (for the same valueset) with a single [FHIR.Condition: "VS"].

    Handles both:
      - Same-line: [CED: "X"] union [CPHC: "X"]
      - Multi-line: [CED: "X"]\n      union [CPHC: "X"]
    """
    # Same line patterns
    content = re.sub(
        r'\[ConditionEncounterDiagnosis:\s*("(?:[^"]+)")\]\s*union\s*\[ConditionProblemsHealthConcerns:\s*\1\]',
        r"[FHIR.Condition: \1]",
        content,
    )
    content = re.sub(
        r'\[ConditionProblemsHealthConcerns:\s*("(?:[^"]+)")\]\s*union\s*\[ConditionEncounterDiagnosis:\s*\1\]',
        r"[FHIR.Condition: \1]",
        content,
    )
    # Multi-line patterns
    content = re.sub(
        r'\[ConditionEncounterDiagnosis:\s*("(?:[^"]+)")\]\s*\n\s*union\s*\[ConditionProblemsHealthConcerns:\s*\1\]',
        r"[FHIR.Condition: \1]",
        content,
    )
    content = re.sub(
        r'\[ConditionProblemsHealthConcerns:\s*("(?:[^"]+)")\]\s*\n\s*union\s*\[ConditionEncounterDiagnosis:\s*\1\]',
        r"[FHIR.Condition: \1]",
        content,
    )
    return content


def _qualify_resource_types(content: str) -> str:
    """Add USQualityCore. prefix to bare resource type retrieval expressions."""
    for rtype in USQUALITYCORE_RESOURCE_TYPES:
        # Match [ResourceType: ...] or [ResourceType] but NOT [Something.ResourceType:...]
        # The lookbehind ensures we're matching right after `[`
        pattern = r"(?<=\[)(" + re.escape(rtype) + r")(?=\s*[\]:])"
        content = re.sub(pattern, r"USQualityCore.\1", content)

    # USCoreBloodPressureProfile → USCore.BloodPressureProfile
    content = content.replace("[USCoreBloodPressureProfile]", "[USCore.BloodPressureProfile]")

    return content


def _fix_patient_sex(content: str) -> str:
    """Replace Patient.sex property access with Patient.sex() function call."""
    # Match Patient.sex when not already followed by (
    return re.sub(r"Patient\.sex\b(?!\s*\()", "Patient.sex()", content)


def _fix_medication_request_period(content: str) -> str:
    """Remove redundant .toInterval() chaining after medicationRequestPeriod()."""
    return content.replace(
        ".medicationRequestPeriod ( ).toInterval ( )",
        ".medicationRequestPeriod ( )",
    )


def _fix_get_encounter(content: str) -> str:
    """Replace getId()-based reference matching with reference.references(E)."""
    return re.sub(
        r"(\w+)\.id\s*=\s*(\w+)\.reference\.getId\s*\(\s*\)",
        r"\2.references(\1)",
        content,
    )


# Symbols from QICoreCommon that moved to USQualityCoreCommon
_USQUALITYCORE_COMMON_CODES = {
    "Fulfill",
    "ambulatory",
    "emergency",
    "field",
    "home health",
    "inpatient encounter",
    "inpatient acute",
    "inpatient non-acute",
    "observation encounter",
    "pre-admission",
    "short stay",
    "virtual",
}


def _remap_qicorecommon_refs(content: str, qicorecommon_alias: str) -> str:
    """
    Replace references to QICoreCommon symbols (via the given alias) with their
    new library homes:
      - Encounter class codes and Fulfill → USQualityCoreCommon
      - All other codes/functions (condition status, etc.) → FHIRCommon
    """
    alias = re.escape(qicorecommon_alias)

    # Pass 1: symbols that moved to USQualityCoreCommon
    for symbol in _USQUALITYCORE_COMMON_CODES:
        pattern = alias + r'\.' + re.escape('"' + symbol + '"')
        content = re.sub(pattern, f'USQualityCoreCommon."{symbol}"', content)

    # Pass 2: everything else moves to FHIRCommon
    content = re.sub(alias + r'\."', 'FHIRCommon."', content)

    return content


def _fix_type_casts(content: str) -> str:
    """
    Qualify ambiguous CQL type casts for the FHIR multi-model context:
      - 'as Integer' → 'as FHIR.integer'
      - 'as Concept'  → 'as FHIR.CodeableConcept'
    Only applied when the pattern follows '.value' to avoid over-broad substitutions.
    """
    content = re.sub(r"(\.value\s+)as\s+Integer\b", r"\1as FHIR.integer", content)
    content = re.sub(r"(\.value\s+)as\s+Concept\b", r"\1as FHIR.CodeableConcept", content)
    return content


# Extension properties that became fluent functions in USQualityCore
# These were plain element accesses in QICore but are now fluent function calls.
_EXTENSION_TO_FLUENT_FUNCTION = [
    "recorded",
    "reasonRefused",
    "doNotPerformReason",
    "doNotPerform",
    "vaccineCode",
    "notDoneReason",
    "abatement",
    "severity",
]


def _fix_extension_to_fluent_functions(content: str) -> str:
    """
    Convert extension property accesses to fluent function calls.
    E.g. .recorded → .recorded()   (when not already a function call)
    Only applies to properties defined as fluent functions in USQualityCoreCommon.
    """
    for prop in _EXTENSION_TO_FLUENT_FUNCTION:
        # Match .propName when NOT already followed by (
        content = re.sub(
            r"(\." + re.escape(prop) + r")\b(?!\s*\()",
            r"\1()",
            content,
        )
    return content


def _extract_qicorecommon_alias(content: str) -> str | None:
    """Return the alias used for QICoreCommon in this file's include block, or None."""
    m = re.search(
        r"include\s+QICoreCommon\s+version\s+'[^']+'\s+called\s+(\S+)",
        content,
    )
    return m.group(1) if m else None


def convert_cql(content: str) -> str:
    """Apply all CQL conversion rules in order."""
    # Capture QICoreCommon alias BEFORE the include transform removes the line
    qicorecommon_alias = _extract_qicorecommon_alias(content)

    content = _transform_version(content)
    content = _transform_using(content)
    content = _transform_includes(content)
    content = _consolidate_condition_unions(content)
    content = _qualify_resource_types(content)
    content = _fix_patient_sex(content)
    content = _fix_medication_request_period(content)
    content = _fix_get_encounter(content)
    content = _fix_type_casts(content)
    content = _fix_extension_to_fluent_functions(content)

    # Remap QICoreCommon references in the body (alias was captured before include transform)
    if qicorecommon_alias:
        content = _remap_qicorecommon_refs(content, qicorecommon_alias)

    # Prepend copyright header if not already present
    if not content.startswith("/*"):
        content = COPYRIGHT_HEADER + content
    return content


# ---------------------------------------------------------------------------
# Test case conversion
# ---------------------------------------------------------------------------

QICORE_PROFILE_PREFIX = "http://hl7.org/fhir/us/qicore/StructureDefinition/qicore-"
USQUALITYCORE_PROFILE_PREFIX = (
    "http://fhir.org/guides/astp/us-quality-core/StructureDefinition/us-quality-core-"
)


def convert_test_json(content: str) -> str:
    """Replace QI Core profile URLs with US Quality Core profile URLs."""
    return content.replace(QICORE_PROFILE_PREFIX, USQUALITYCORE_PROFILE_PREFIX)


# ---------------------------------------------------------------------------
# Sanity check: convert already-converted files and diff against ground truth
# ---------------------------------------------------------------------------

def _is_pure_qicore_source(path: Path) -> bool:
    """Return True if the file is a pure QICore 6.0.0 source (not already partially converted)."""
    try:
        content = path.read_text(encoding="utf-8")
        # Must have using QICore version '6.0.0' and NOT already have using USQualityCore
        return (
            "using QICore version '6.0.0'" in content
            and "using USQualityCore" not in content
        )
    except Exception:
        return False


def run_sanity_check():
    """
    Convert the QICore source measure files that have already-validated counterparts
    in cms-2025, write to a temp dir, and diff against the validated versions.

    Restricted to CMS/NHSN measure files to avoid comparing shared library files
    (which were converted more extensively than what this script automates).
    """
    cms_cql_files = {f.name for f in CMS_CQL_DIR.glob("*.cql")}
    qicore_cql_files = {f.name for f in QICORE_CQL_DIR.glob("*.cql")}

    # Files present in both (candidates for sanity check, minus skip list)
    overlap = (cms_cql_files & qicore_cql_files) - SKIP_CQL_FILES

    # Further restrict to measure files (CMS* and NHSN*) with pure QICore 6.0.0 source
    candidates = {
        f
        for f in overlap
        if (f.startswith("CMS") or f.startswith("NHSN"))
        and _is_pure_qicore_source(QICORE_CQL_DIR / f)
    }

    if not candidates:
        print("No candidate files found for sanity check.")
        return

    print(f"Sanity-checking {len(candidates)} file(s)...\n")

    pass_count = 0
    fail_count = 0

    with tempfile.TemporaryDirectory(prefix="us_quality_core_sanity_") as tmpdir:
        tmp_path = Path(tmpdir)

        for fname in sorted(candidates):
            src = QICORE_CQL_DIR / fname
            expected = CMS_CQL_DIR / fname

            src_content = src.read_text(encoding="utf-8")
            converted = convert_cql(src_content)

            expected_content = expected.read_text(encoding="utf-8")

            if converted == expected_content:
                print(f"  PASS  {fname}")
                pass_count += 1
            else:
                print(f"  FAIL  {fname}")
                fail_count += 1
                # Write diff
                diff = list(
                    difflib.unified_diff(
                        expected_content.splitlines(keepends=True),
                        converted.splitlines(keepends=True),
                        fromfile=f"expected/{fname}",
                        tofile=f"script/{fname}",
                        n=3,
                    )
                )
                diff_file = tmp_path / f"{fname}.diff"
                diff_file.write_text("".join(diff), encoding="utf-8")
                print(f"        diff written to: {diff_file}")
                # Print first 40 lines of diff to console
                print("".join(diff[:40]))

    print(f"\nResults: {pass_count} passed, {fail_count} failed")
    if fail_count > 0:
        print(
            "\nReview diffs above. Known acceptable differences:\n"
            "  - CMS165, CMS122, CMS130: marked 'in progress' in process doc; "
            "may have incomplete type qualification\n"
            "  - AgeInYearsAt: CMS125 partially converted in Initial Population "
            "but not stratifications\n"
            "  - Alias renames in Condition defines (e.g. RightMastectomyProcedure → "
            "RightMastectomyCondition) are manual changes not automated\n"
            "  - ObservationCancelled comment-out pattern in CMS2 is not automated\n"
            "  - DiagnosticReportNote additions in CMS125 are measure-specific, not automated\n"
        )
    return fail_count == 0


# ---------------------------------------------------------------------------
# Convert new CQL files
# ---------------------------------------------------------------------------

def convert_cql_files():
    """Convert all QICore CQL files that don't yet exist in cms-2025."""
    cms_existing = {f.name for f in CMS_CQL_DIR.glob("*.cql")}
    converted_count = 0
    skipped_count = 0

    for src_file in sorted(QICORE_CQL_DIR.glob("*.cql")):
        fname = src_file.name

        if fname in SKIP_CQL_FILES:
            print(f"  SKIP  {fname}  (becomes external reference)")
            skipped_count += 1
            continue

        if fname in cms_existing:
            print(f"  SKIP  {fname}  (already in cms-2025)")
            skipped_count += 1
            continue

        if not _is_pure_qicore_source(src_file):
            print(f"  SKIP  {fname}  (not a pure QICore 6.0.0 source)")
            skipped_count += 1
            continue

        content = src_file.read_text(encoding="utf-8")
        converted = convert_cql(content)

        dst_file = CMS_CQL_DIR / fname
        dst_file.write_text(converted, encoding="utf-8")
        print(f"  DONE  {fname}")
        converted_count += 1

    print(f"\nConverted {converted_count} CQL file(s), skipped {skipped_count}.")


# ---------------------------------------------------------------------------
# Convert test case directories
# ---------------------------------------------------------------------------

def convert_test_cases():
    """
    Copy test case directories from qicore-2025 that don't yet exist in cms-2025,
    applying profile URL substitution to all .json files.
    """
    cms_existing = {d.name for d in CMS_TESTS_DIR.iterdir() if d.is_dir()}
    converted_measures = 0
    converted_files = 0
    skipped_count = 0

    for measure_dir in sorted(QICORE_TESTS_DIR.iterdir()):
        if not measure_dir.is_dir():
            continue

        measure_name = measure_dir.name

        if measure_name in cms_existing:
            print(f"  SKIP  {measure_name}  (already in cms-2025)")
            skipped_count += 1
            continue

        dst_measure_dir = CMS_TESTS_DIR / measure_name
        dst_measure_dir.mkdir(parents=True, exist_ok=True)

        # Walk the source tree
        for root, dirs, files in os.walk(measure_dir):
            root_path = Path(root)
            rel_root = root_path.relative_to(measure_dir)
            dst_root = dst_measure_dir / rel_root
            dst_root.mkdir(parents=True, exist_ok=True)

            for fname in files:
                src_file = root_path / fname
                dst_file = dst_root / fname

                if fname.endswith(".json"):
                    content = src_file.read_text(encoding="utf-8")
                    converted = convert_test_json(content)
                    dst_file.write_text(converted, encoding="utf-8")
                    converted_files += 1
                else:
                    # Copy as-is (.madie files, etc.)
                    shutil.copy2(src_file, dst_file)

        print(f"  DONE  {measure_name}")
        converted_measures += 1

    print(
        f"\nConverted {converted_measures} measure(s), "
        f"{converted_files} JSON file(s), skipped {skipped_count}."
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Convert QICore 2025 CQL libraries and test cases to US Quality Core."
    )
    parser.add_argument(
        "--sanity-check",
        action="store_true",
        help="Validate script output against already-converted files in cms-2025.",
    )
    parser.add_argument(
        "--convert-cql",
        action="store_true",
        help="Convert new CQL files to dqm-content-cms-2025/input/cql/.",
    )
    parser.add_argument(
        "--convert-tests",
        action="store_true",
        help="Convert new test case directories to dqm-content-cms-2025/input/tests/measure/.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run --convert-cql and --convert-tests.",
    )
    args = parser.parse_args()

    if not any([args.sanity_check, args.convert_cql, args.convert_tests, args.all]):
        parser.print_help()
        sys.exit(1)

    if args.sanity_check:
        success = run_sanity_check()
        if not success:
            sys.exit(1)

    if args.convert_cql or args.all:
        print("\n=== Converting CQL files ===")
        convert_cql_files()

    if args.convert_tests or args.all:
        print("\n=== Converting test cases ===")
        convert_test_cases()


if __name__ == "__main__":
    main()
