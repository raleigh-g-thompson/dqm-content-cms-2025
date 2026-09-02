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


def pending_issues(catalog: dict) -> List[dict]:
    """Issues whose cases still affect the score (``resolved`` false)."""
    return [i for i in catalog.get("issues", []) if not i.get("resolved", False)]


def resolved_issues(catalog: dict) -> List[dict]:
    """Issues that no longer affect the score (kept for history)."""
    return [i for i in catalog.get("issues", []) if i.get("resolved", False)]


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

    Issues with ``resolved == false`` contribute their affected test cases to
    the set used for 'resolution pending' handling. Issue-level
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
