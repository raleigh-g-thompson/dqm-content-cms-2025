"""Attribution ledger: which failures have a written, case-linked cause.

Replaces the old headline metric. Previously the report scored twice -- once on
all 3,964 cases, and once with every case cited by an unresolved known issue
removed, producing a "100.00%" that was unreachable by failure because the
exclusion set was exactly the failing set. Worse, the exclusion set had gone
stale: most excluded cases had started passing but were still being subtracted,
so the denominator collapsed far further than the real failures justified.

The ledger keeps one denominator and answers a sharper question:

    Does every failure have a written cause attached to it?

``unattributed_failures`` is the trust metric. Zero means every failing case is
explained by a catalog issue that names it. Anything above zero is either a
regression or untriaged work, and is the number to act on.

Two hygiene counts fall out of the same pass and are worth reporting because
they silently distort any exclusion-based score:

  * ``stale_attributions``  -- issue cites a case that now passes.
  * ``phantom_attributions`` -- issue cites a case absent from expected results.

Both are evidence that an issue's case list has drifted from reality. Under the
two-field issue model (evidence-derived ``failing_cases`` + authored
``defect_status``) the stale set is what auto-derivation purges.
"""

from typing import Dict, List, NamedTuple, Sequence, Set, Tuple

# compare_results.py puts scripts/comparison/ on sys.path and imports this as a
# bare module; the tests import it as scripts.comparison.attribution. Support
# both rather than forcing one caller to know about the other.
try:  # package-style (tests, any caller using the full dotted path)
    from scripts.comparison import known_issues as known_issues_lib
except ImportError:  # bare (compare_results.py, after its sys.path insert)
    import known_issues as known_issues_lib

Case = Tuple[str, str]  # (measure_name, patient_guid)


def is_scaffold_measure(measure: str) -> bool:
    """True for the `testE*` repro libraries.

    These exist to reproduce a defect in isolation and are deliberately excluded
    from scoring by ``capture_results`` (it skips any measure whose name starts
    with "test"). An issue citing one is therefore expected, not rot -- keeping
    them out of the phantom bucket stops the report sending people to chase
    scaffolding.
    """
    return measure.lower().startswith("test")


class AttributionLedger(NamedTuple):
    total_cases: int
    passing: int
    failing: int
    attributed_failures: int
    unattributed_failures: List[Case]
    stale_attributions: Dict[str, List[Case]]
    phantom_attributions: Dict[str, List[Case]]
    unscored_cells: List[tuple]
    cases_by_issue: Dict[str, Dict[str, List[Case]]]
    scaffold_attributions: Dict[str, List[Case]] = {}

    @property
    def unattributed_count(self) -> int:
        return len(self.unattributed_failures)

    @property
    def unscored_cases(self) -> List[Case]:
        """Distinct test cases that had at least one population skipped.

        The report speaks in test cases only -- population-level detail is an
        internal unit and means nothing to the audience. A case here is still
        scored on its other populations; it is listed because part of it was not
        measured, which is a data-quality warning rather than a result.
        """
        return sorted({(c[0], c[1]) for c in self.unscored_cells})

    @property
    def stale_count(self) -> int:
        return sum(len(v) for v in self.stale_attributions.values())

    @property
    def phantom_count(self) -> int:
        return sum(len(v) for v in self.phantom_attributions.values())

    @property
    def is_clean(self) -> bool:
        """No unexplained failures and no attributions pointing at nothing."""
        return not self.unattributed_failures and not self.phantom_attributions


def _open_issue_cases(catalog: dict) -> Dict[str, Set[Case]]:
    """(issue id -> cited cases) for issues that are still open.

    Uses ``known_issues_lib.is_resolved`` (the two-field ``defect_status``
    seam) rather than the raw ``resolved`` field: a worked-around-but-unfixed
    engine bug stays open, and historical string-valued ``resolved`` entries
    are handled by the shared helper, so a raw non-empty-string read never
    silently treats an issue as resolved.
    """
    out: Dict[str, Set[Case]] = {}
    for issue in known_issues_lib.pending_issues(catalog):
        pairs = known_issues_lib.affected_measure_guid_pairs(issue)
        out[issue.get("id", "?")] = {(m, g) for m, g in pairs if m and g}
    return out


def build_ledger(
    expected_rows: Dict,
    actual_rows: Dict,
    catalog: dict,
    outcomes: Dict[Case, str],
    unscored_cells: Sequence[tuple] = (),
) -> AttributionLedger:
    """Build the ledger from per-case outcomes and the issue catalog.

    ``outcomes`` is ``compare_results.test_case_outcomes`` output: the caller
    supplies it so the ledger can never disagree with the report's own scoring.
    """
    failing = {c for c, o in outcomes.items() if o == "FAIL"}
    passing = {c for c, o in outcomes.items() if o == "PASS"}
    known_cases = set(outcomes)

    open_cases = _open_issue_cases(catalog)

    attributed: Set[Case] = set()
    stale: Dict[str, List[Case]] = {}
    phantom: Dict[str, List[Case]] = {}
    scaffold: Dict[str, List[Case]] = {}
    cases_by_issue: Dict[str, Dict[str, List[Case]]] = {}

    for issue_id, cited in open_cases.items():
        still_failing = sorted(cited & failing)
        now_passing = sorted(cited & passing)
        absent = cited - known_cases
        # Split "absent" two ways: scaffolding is excluded from scoring on
        # purpose, so citing it is expected; anything else is genuine rot.
        scaffolded = sorted(c for c in absent if is_scaffold_measure(c[0]))
        not_present = sorted(c for c in absent if not is_scaffold_measure(c[0]))

        attributed |= set(still_failing)
        if now_passing:
            stale[issue_id] = now_passing
        if not_present:
            phantom[issue_id] = not_present
        if scaffolded:
            scaffold[issue_id] = scaffolded
        cases_by_issue[issue_id] = {
            "claimed": sorted(cited),
            "still_failing": still_failing,
            "now_passing": now_passing,
            "not_present": not_present,
            "scaffold": scaffolded,
        }

    unattributed = sorted(failing - attributed)

    return AttributionLedger(
        total_cases=len(outcomes),
        passing=len(passing),
        failing=len(failing),
        attributed_failures=len(attributed & failing),
        unattributed_failures=unattributed,
        stale_attributions=stale,
        phantom_attributions=phantom,
        unscored_cells=list(unscored_cells),
        cases_by_issue=cases_by_issue,
        scaffold_attributions=scaffold,
    )


def render_headline(ledger: AttributionLedger) -> List[str]:
    """Summary rows for the top of the discrepancy report.

    Every figure is denominated in **test cases**. Percentages always use the
    full case count, and the explained/unexplained split is reported as counts
    rather than a second percentage, so nothing in this block can be quoted as a
    pass rate against a shrunken denominator.
    """
    total = ledger.total_cases or 1
    return [
        ["Total Test Cases", f"{ledger.total_cases:,}"],
        ["Passing Test Cases",
         f"{ledger.passing:,} ({ledger.passing / total * 100:.2f}%)"],
        ["Failing Test Cases",
         f"{ledger.failing:,} ({ledger.failing / total * 100:.2f}%)"],
        ["&nbsp;&nbsp;— attributed to an open known issue",
         f"{ledger.attributed_failures:,}"],
        ["&nbsp;&nbsp;— **UNATTRIBUTED** (regression / untriaged)",
         f"**{ledger.unattributed_count:,}**"],
    ]


def render_health_section(ledger: AttributionLedger) -> List[str]:
    """`## Attribution Health` -- makes drift in the issue catalog visible."""
    out: List[str] = ["## Attribution Health", ""]
    out.append(
        "_Every percentage in the summary uses the full "
        f"{ledger.total_cases:,}-case denominator. **Unattributed** is the "
        "trust metric: 0 means every failing case has a written, case-linked "
        "cause._")
    out.append("")

    if ledger.unattributed_failures:
        out.append(
            f"### Unattributed failures ({ledger.unattributed_count})")
        out.append("")
        out.append("Failing with no open issue naming them. Triage or fix.")
        out.append("")
        out.append("| Measure | Test Case |")
        out.append("| --- | --- |")
        for measure, guid in ledger.unattributed_failures:
            out.append(f"| {measure} | {guid} |")
        out.append("")

    if ledger.stale_attributions:
        out.append(f"### Stale attributions ({ledger.stale_count})")
        out.append("")
        out.append(
            "Cases an open issue still claims that now pass. These inflate any "
            "known-issues-excluded score; each is a candidate for closing the "
            "issue or trimming its case list.")
        out.append("")
        out.append("| Issue | Cases now passing |")
        out.append("| --- | --- |")
        for issue_id in sorted(ledger.stale_attributions):
            out.append(
                f"| {issue_id} | {len(ledger.stale_attributions[issue_id])} |")
        out.append("")

    if ledger.phantom_attributions:
        out.append(f"### Phantom attributions ({ledger.phantom_count})")
        out.append("")
        out.append(
            "Cases cited by an issue that do not exist in the expected results "
            "at all -- a renamed measure, a deleted fixture, or a typo.")
        out.append("")
        out.append("| Issue | Measure | Test Case |")
        out.append("| --- | --- | --- |")
        for issue_id in sorted(ledger.phantom_attributions):
            for measure, guid in ledger.phantom_attributions[issue_id]:
                out.append(f"| {issue_id} | {measure} | {guid} |")
        out.append("")

    if ledger.unscored_cases:
        out.append(
            f"### Test cases not fully measured ({len(ledger.unscored_cases)})")
        out.append("")
        out.append(
            "These cases were scored, but at least one of their populations "
            "was not recognised and so was skipped. Decide whether that "
            "population should count -- see "
            "`scripts/comparison/populations.py`.")
        out.append("")
        out.append("| Measure | Test Case |")
        out.append("| --- | --- |")
        for measure, guid in ledger.unscored_cases:
            out.append(f"| {measure} | {guid} |")
        out.append("")

    if ledger.scaffold_attributions:
        n = sum(len(v) for v in ledger.scaffold_attributions.values())
        out.append(f"### Repro-scaffold references ({n})")
        out.append("")
        out.append(
            "Issues citing a `testE*` repro library. These are excluded from "
            "scoring by design, so they never appear as pass or fail -- listed "
            "for completeness, not as a problem to fix.")
        out.append("")
        out.append("| Issue | Measure |")
        out.append("| --- | --- |")
        for issue_id in sorted(ledger.scaffold_attributions):
            for measure, _guid in ledger.scaffold_attributions[issue_id]:
                out.append(f"| {issue_id} | {measure} |")
        out.append("")

    if ledger.is_clean and not ledger.stale_attributions:
        out.append("No unattributed failures, stale claims, or phantom cases.")
        out.append("")

    return out
