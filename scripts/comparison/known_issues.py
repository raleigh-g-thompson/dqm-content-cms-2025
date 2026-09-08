"""Shared helpers for reading the known-issues catalog.

The catalog (``known_issues.json``) is a single source of truth for every known
issue that affects test-case results — engine/translator issues, fixture/data
issues, migration regressions, vendored-library bugs, and content gaps. It
drives the discrepancy report (dual pass/fail scores + row marking) and the
measure-failure tracker, and ``defect-tracking/engine-issues.md`` is generated
from it.
"""
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple

DEFAULT_CATALOG_PATH = Path(__file__).resolve().parent / "known_issues.json"

TestCaseKey = Tuple[str, str]  # (measure_name, patient_guid)


def load_catalog(path=None) -> dict:
    """Load the known-issues catalog as a dict. Returns empty catalog if missing."""
    catalog_path = Path(path) if path else DEFAULT_CATALOG_PATH
    if not catalog_path.exists():
        return {"schema_version": 1, "issues": []}
    with open(catalog_path, encoding="utf-8") as fh:
        return json.load(fh)


# Authored defect states (Phase 4b two-field model). ``defect_status`` answers
# "is the underlying bug actually fixed?" and is authored in each issue's front
# matter. It deliberately distinguishes a worked-around-but-unfixed engine bug
# from a genuinely fixed one, so the engine handoff register stays populated.
DEFECT_STATUS_OPEN = {"confirmed", "workaround-applied", "suspected"}
DEFECT_STATUS_RESOLVED = {"fixed-upstream", "retired"}
DEFECT_STATUS_ALL = DEFECT_STATUS_OPEN | DEFECT_STATUS_RESOLVED


def defect_status_of(issue: dict) -> str:
    """The authored defect state, defaulting sensibly when absent/historical.

    Phase 4b replaced the single ``resolved`` boolean with the authored
    ``defect_status`` enum plus a strictly auto-derived ``failing_cases`` (the
    'does it cost us test failures right now' evidence side, computed live at
    the report layer -- never stored here). Consumers that classify "is this
    bug actually fixed" read ``defect_status`` through this helper.

    A few pre-4b/transitional entries may still carry the legacy ``resolved``
    boolean instead of a ``defect_status``; map those so callers never have to
    know about the migration.
    """
    val = issue.get("defect_status")
    if isinstance(val, str) and val in DEFECT_STATUS_ALL:
        return val
    legacy = issue.get("resolved", False)
    if isinstance(legacy, bool):
        return "fixed-upstream" if legacy else "confirmed"
    if isinstance(legacy, int):
        return "fixed-upstream" if legacy else "confirmed"
    if isinstance(legacy, str):  # historical JSON string "false"
        return "fixed-upstream" if legacy.strip().lower() == "true" else "confirmed"
    return "confirmed"


def is_resolved(issue: dict) -> bool:
    """True when the authored defect is actually fixed (or retired).

    Phase 4b: the underlying bug is "resolved" only for ``fixed-upstream`` and
    ``retired``. ``workaround-applied`` and ``confirmed`` stay open even though
    their cited cases may be passing -- a workaround hides a live engine bug and
    must remain visible in the handoff register. This is the seam that
    ``pending_issues`` / ``resolved_issues`` / ``pending_case_set`` / the
    discrepancy-report label and the catalog-details renderer all read through.
    """
    return defect_status_of(issue) in DEFECT_STATUS_RESOLVED


def pending_issues(catalog: dict) -> List[dict]:
    """Issues whose cited cases still affect the score (not resolved)."""
    return [i for i in catalog.get("issues", []) if not is_resolved(i)]


def resolved_issues(catalog: dict) -> List[dict]:
    """Issues that no longer affect the score (kept for history)."""
    return [i for i in catalog.get("issues", []) if is_resolved(i)]


def affected_measure_guid_pairs(issue: dict) -> List[TestCaseKey]:
    """Return [(measure, guid), ...] for an issue's documented test cases."""
    pairs: List[TestCaseKey] = []
    for case in issue.get("affected_test_cases", []):
        if isinstance(case, dict):
            pairs.append((case.get("measure", ""), case.get("guid", "")))
        elif isinstance(case, (list, tuple)) and len(case) == 2:
            pairs.append((case[0], case[1]))
    return pairs


def pending_case_set(catalog: dict) -> Set[TestCaseKey]:
    """Set of (measure, guid) flagged by any unresolved issue.

    Issues that are not ``is_resolved`` (i.e. ``defect_status`` in the open set)
    contribute their affected test cases to the set used for 'resolution
    pending' handling. Issue-level
    ``affected_measures`` alone (without GUIDs) is NOT sufficient to mark
    specific cases — only enumerated ``affected_test_cases`` pairs are.
    """
    pending: Set[TestCaseKey] = set()
    for issue in pending_issues(catalog):
        pending.update(affected_measure_guid_pairs(issue))
    return pending


def issues_for_case(catalog: dict, measure: str, guid: str) -> List[dict]:
    """All issues (any resolution) whose affected test cases include the case."""
    matches = []
    for issue in catalog.get("issues", []):
        for pair in affected_measure_guid_pairs(issue):
            if pair == (measure, guid):
                matches.append(issue)
                break
    return matches
