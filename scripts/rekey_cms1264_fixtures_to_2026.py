#!/usr/bin/env python3
"""Re-key CMS1264 test-case fixtures from measurement period 2027 to 2026.

CMS1264 (E-CAT Re-Hydration Calculator) fixtures were authored for the 2027
measurement period: every clinical date is 2027/2028, and each case's
``MeasureReport.period`` is 2027-01-01 .. 2027-12-31.  The repo convention is
the 2026 measurement period (global ``input/tests/config.json`` overrides the
MP to ``@2026``; 71/74 measure resources and 83/84 CQL defaults are 2026), so
all CMS1264 encounters fall outside the measurement period and every population
that should be 1 evaluates 0 (both engines agree - the authoritative QI-Core
actuals show the same exp=1/act=0 pattern on the identical upstream fixtures).

The upstream sibling measure CMS1244 was already re-keyed to 2026 by commit
``3c756c47`` ("updates CMS1244 test case dates to align with 2026 measurement
period") in the dqm-content-qicore-2025 repository - CMS1264 was missed.  That
commit performed a uniform -1-year shift of *every* date (encounters 2027->2026,
birthDates 2010->2009, prior-MP references 2026->2025 and 2024->2023) and left
the expected population counts untouched.  This script applies the identical
transformation to CMS1264.

Mechanism

* Formatting-preserving: a targeted regex substitutes only the year of each
  ``YYYY-MM-DD`` date token, decrementing it by exactly one year.  Everything
  else (spacing, key order, version strings like the SNOMED ``2023-09`` value
  set date, GUIDs) is untouched.
* Every ``YYYY-MM-DD`` token in these fixtures is a FHIR date value (verified:
  the token count equals the count of date-typed JSON string values), and
  ``2023-09`` (``YYYY-MM``) does not match the ``YYYY-MM-DD`` pattern.
* Guarded / idempotent: a case whose ``MeasureReport.period.start`` already
  reads ``2026-01-01`` is considered migrated and is skipped, so re-running is
  a no-op and cannot double-shift dates.
* The uniform -1 shift preserves every relative relationship (ages, prior-MP
  diagnoses, 240-min/6h/8h boarded + ED-LOS windows, end-of-MP boundary cases),
  so the fixture ``MeasureReport`` population counts - and therefore
  ``expected_results.csv`` - are unchanged.
* The shift spans all resource types (Encounter including ``location[].period``,
  Patient ``birthDate``, Condition ``recordedDate``, Claim ``created``,
  ServiceRequest ``authoredOn``, Coverage ``period``, MeasureReport ``period``).

Run from the repo root (Python 3.12):

    python ./scripts/rekey_cms1264_fixtures_to_2026.py
"""
import json
import re
import sys
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parents[1] / "input" / "tests" / "measure"
MEASURE = "CMS1264FHIRECATREHQR"
MEASURE_DIR = TEST_DIR / MEASURE

PRE_MIGRATED_MR_START = "2027-01-01"
MIGRATED_MR_START = "2026-01-01"

DATE_TOKEN = re.compile(r"(?<!\d)(\d{4})-(\d{2})-(\d{2})(?!\d)")


def shift_year(match):  # type: ignore[no-untyped-def]
    """Return the same date with the year decremented by one."""
    year = int(match.group(1))
    if year < 1000:
        raise ValueError(f"date year {year} cannot be decremented: {match.group(0)}")
    return f"{year - 1:04d}-{match.group(2)}-{match.group(3)}"


def case_needs_migration(case_dir: Path) -> bool:
    """A case is un-migrated iff its MeasureReport period still starts 2027."""
    mrs = sorted(case_dir.glob("MeasureReport-*.json"))
    if len(mrs) != 1:
        raise RuntimeError(f"{case_dir}: expected exactly one MeasureReport, found {len(mrs)}")
    mr = json.loads(mrs[0].read_text(encoding="utf-8"))
    start = (mr.get("period") or {}).get("start")
    if start == PRE_MIGRATED_MR_START:
        return True
    if start == MIGRATED_MR_START or start.startswith(MIGRATED_MR_START):
        return False
    raise RuntimeError(f"{case_dir}: unexpected MeasureReport period start {start!r}")


def rekey_file_text(text: str) -> str:
    """Decrement the year of every YYYY-MM-DD date token by one."""
    return DATE_TOKEN.sub(shift_year, text)


def _verify_json_values(path: Path, text_after: str) -> None:
    """Every YYYY-MM-DD token must be a JSON-string date value, and the
    result must still parse.  This guards against ever rewriting a GUID or
    version string disguise as a date."""
    before = path.read_text(encoding="utf-8")
    json.loads(text_after)  # must stay valid JSON
    # tokens in the JSON value tree == tokens in the raw text
    json_tokens = set()

    def collect(o):  # type: ignore[no-untyped-def]
        if isinstance(o, dict):
            for v in o.values():
                collect(v)
        elif isinstance(o, list):
            for v in o:
                collect(v)
        elif isinstance(o, str) and DATE_TOKEN.search(o):
            for date in DATE_TOKEN.findall(o):
                json_tokens.add("-".join(date))

    collect(json.loads(text_after))
    raw_tokens = {"-".join(m.groups()) for m in DATE_TOKEN.finditer(text_after)}
    if raw_tokens - json_tokens:
        raise RuntimeError(
            f"{path}: date tokens outside JSON string values: "
            f"{sorted(raw_tokens - json_tokens)} (refusing to rewrite)"
        )


def main() -> int:
    if not MEASURE_DIR.exists():
        raise RuntimeError(f"{MEASURE_DIR}: measure directory missing")
    case_dirs = sorted(d for d in MEASURE_DIR.iterdir() if d.is_dir())
    if not case_dirs:
        raise RuntimeError(f"{MEASURE_DIR}: no case directories")

    already_migrated = 0
    newly_migrated = 0
    changed_files = 0
    changed_tokens = 0
    for case_dir in case_dirs:
        if not case_needs_migration(case_dir):
            already_migrated += 1
            continue
        for fixture in sorted(case_dir.glob("*.json")):
            original = fixture.read_text(encoding="utf-8")
            rekeyed = rekey_file_text(original)
            if rekeyed != original:
                _verify_json_values(fixture, rekeyed)
                fixture.write_text(rekeyed, encoding="utf-8")
                changed_files += 1
                changed_tokens += len(DATE_TOKEN.findall(original))
        # after the shift the guard must flip
        if case_needs_migration(case_dir):
            raise RuntimeError(f"{case_dir}: not marked migrated by the re-key")
        newly_migrated += 1

    print(f"{len(case_dirs)} case(s) in {MEASURE}")
    print(f"re-keyed {changed_files} fixture file(s) ({changed_tokens} date token(s) shifted -1 year)")
    print(f"already migrated: {already_migrated}; re-keyed now: {newly_migrated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())