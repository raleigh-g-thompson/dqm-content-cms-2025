"""Two classification vocabularies, kept deliberately disjoint.

Three scripts (`compare_results.py`, `engine_shared_issues.py`,
`all_measures_summary.py`) each reimplemented cross-engine bucket
classification over the same three CSVs, and two of them used the exact
strings "cms-only"/"qicore-only" to mean **opposite things**:

  * In `compare_results.diff_actual_results` (a two-way, expected-blind
    comparison of CMS actuals vs QI-Core actuals): "cms-only" meant *a row is
    present in CMS's data but absent from QI-Core's* -- a presence fact, with
    no judgement about which engine is correct.
  * In `engine_shared_issues.classify` / `all_measures_summary.classify` (a
    three-way comparison against expected): "cms-only" meant *CMS deviates
    from expected while QI-Core matches it* -- a correctness verdict.

Reading one script's output with the other's mental model silently reverses
the meaning. This module gives each axis its own vocabulary so the ambiguous
terms cannot recur.

Presence axis -- row-set algebra, expected not consulted
----------------------------------------------------------
    match | mismatch | missing-in-cms | missing-in-qicore

Agreement axis -- three-way comparison against expected
----------------------------------------------------------
    pass | shared | shared-direction | cms-wrong | qicore-wrong |
    conflicting | incomplete | no-expected

No token is shared between the two vocabularies.
"""
from typing import Dict, NamedTuple, Optional, Tuple

from populations import canonical_cell

# --------------------------------------------------------------------------
# Presence axis
# --------------------------------------------------------------------------

MATCH = "match"
MISMATCH = "mismatch"
MISSING_IN_CMS = "missing-in-cms"
MISSING_IN_QICORE = "missing-in-qicore"


class PresenceBucket(NamedTuple):
    mismatch: list
    missing_in_cms: list
    missing_in_qicore: list
    match: int


def classify_presence(cms_rows: Dict, qicore_rows: Dict) -> Dict[str, PresenceBucket]:
    """Two-way row-set comparison of CMS actuals vs QI-Core actuals.

    Keys are the ``Results.rows`` dicts from ``compare_results.capture_results``:
    ``(measure_name, patient_guid, "Group_N:Population")``. Expected results are
    not consulted -- this only reports where the two engines' raw output
    disagrees or one is missing a row the other has.

    Returns ``{measure: PresenceBucket}``. ``missing_in_qicore`` is a row CMS
    produced that QI-Core did not (formerly mislabelled "cms_only");
    ``missing_in_cms`` is a row QI-Core produced that CMS did not (formerly
    "qicore_only").
    """
    result: Dict[str, dict] = {}
    cms_keys = set(cms_rows)
    qi_keys = set(qicore_rows)

    def bucket_for(measure):
        return result.setdefault(measure, {
            "mismatch": [], "missing_in_cms": [], "missing_in_qicore": [],
            "match": 0,
        })

    for key in sorted(qi_keys):
        measure = key[0]
        bucket = bucket_for(measure)
        if key not in cms_keys:
            bucket["missing_in_cms"].append(key)
            continue
        if cms_rows[key] == qicore_rows[key]:
            bucket["match"] += 1
        else:
            bucket["mismatch"].append((key, cms_rows[key], qicore_rows[key]))

    for key in sorted(cms_keys - qi_keys):
        measure = key[0]
        bucket_for(measure)["missing_in_qicore"].append(key)

    return {m: PresenceBucket(**b) for m, b in result.items()}


# --------------------------------------------------------------------------
# Agreement axis
# --------------------------------------------------------------------------

PASS = "pass"
SHARED = "shared"
SHARED_DIRECTION = "shared-direction"
CMS_WRONG = "cms-wrong"
QICORE_WRONG = "qicore-wrong"
CONFLICTING = "conflicting"
INCOMPLETE = "incomplete"
NO_EXPECTED = "no-expected"


def _sign(x: Optional[int]) -> int:
    if x is None:
        return 0
    return (x > 0) - (x < 0)


def classify_cell(
    expected: Optional[int], cms: Optional[int], qicore: Optional[int]
) -> str:
    """Classify one population cell by three-way agreement with expected.

    ``pass``               cms == expected and qicore == expected
    ``shared``              both wrong, and wrong by the same value (strongest
                            signal of a defect shared by both engine versions)
    ``shared-direction``    both wrong, same direction, different magnitude
    ``cms-wrong``           only CMS deviates from expected
    ``qicore-wrong``        only QI-Core deviates from expected
    ``conflicting``         both wrong, in opposite directions
    ``incomplete``          expected exists but one engine produced no value
    ``no-expected``         no expected value for this cell (not scored)
    """
    if expected is None:
        return NO_EXPECTED
    if cms is None or qicore is None:
        return INCOMPLETE
    if cms == expected and qicore == expected:
        return PASS
    if cms != expected and qicore != expected:
        if cms == qicore:
            return SHARED
        if _sign(cms - expected) == _sign(qicore - expected):
            return SHARED_DIRECTION
        return CONFLICTING
    if cms != expected and qicore == expected:
        return CMS_WRONG
    if qicore != expected and cms == expected:
        return QICORE_WRONG
    return CONFLICTING  # unreachable given the branches above; kept as a safe default


def normalized_key(
    measure: str, guid: str, population: str
) -> Tuple[str, str, str]:
    """Build an (measure, guid, "Group_N:Population") key with the population
    name canonicalised, so keys from expected/CMS/QI-Core agree even where the
    raw population spelling differs (see populations.py)."""
    return (measure, guid, canonical_cell(population))
