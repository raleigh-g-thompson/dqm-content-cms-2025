"""Render a standalone catalog-issue details report.

Reads ``known_issues.json`` (single source of truth) and, when present, the
current ``discrepancy_report.md`` to count how many test-case rows cite each
unresolved issue. Produces ``catalog_issue_details.md`` with the full per-issue
prose (``body_md`` / ``workaround``) that the discrepancy report only lists as
a one-line title.

Usage:
  python3 scripts/comparison/render_catalog_issue_details.py
  python3 scripts/comparison/render_catalog_issue_details.py --categories content
  python3 scripts/comparison/render_catalog_issue_details.py --only-cited
"""
import argparse
import re
import sys
from pathlib import Path

try:
    from scripts.comparison.known_issues import is_resolved, load_catalog
    from scripts.comparison.clock import resolve as _resolve_now
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from known_issues import is_resolved, load_catalog
    from clock import resolve as _resolve_now

ROOT = Path(__file__).resolve().parent

CITATION_RE = re.compile(r"\b([A-Z]-\d+) — resolution pending")


def count_citations(report_lines):
    """Count per-issue citations across discrepancy-report markdown lines.

    Matches ``<ID> — resolution pending`` tokens (the ``<br>``-joined notes
    column of per-test-case rows). Each issue ID is counted at most once per
    report line, so the value reads as "cited on N test-case rows".
    """
    counts = {}
    if not report_lines:
        return counts
    for line in report_lines:
        if not line.startswith("|"):
            continue
        for issue_id in set(CITATION_RE.findall(line)):
            counts[issue_id] = counts.get(issue_id, 0) + 1
    return counts


def _render_issue(issue, citation_count, max_test_cases):
    title = issue.get("title", "").strip()
    heading_title = title if len(title) <= 96 else title[:96].rstrip() + "…"
    lines = [
        f"## {issue['id']} — {heading_title}",
        "",
    ]
    meta = [
        ("ID", issue["id"]),
        ("Title", title),
        ("Category", issue.get("category", "")),
        ("Status", issue.get("status", "")),
        ("Resolution", "resolved" if is_resolved(issue) else "pending"),
    ]
    affected_measures = ", ".join(issue.get("affected_measures", [])) or "(none)"
    meta.append(("Affected measures", affected_measures))
    if citation_count is not None:
        meta.append(("Cited in discrepancy_report.md", str(citation_count)))
    for key, value in meta:
        lines.append(f"- **{key}**: {value}")
    lines.append("")

    test_cases = list(issue.get("affected_test_cases", []))
    if test_cases:
        shown = test_cases[:max_test_cases]
        total = len(test_cases)
        lines.append(f"### Affected test cases (first {len(shown)} of {total})")
        lines.append("")
        for case in shown:
            if isinstance(case, dict):
                measure = case.get("measure", "")
                guid = case.get("guid", "")
            else:
                measure, guid = "", str(case)
            lines.append(f"- `{measure}` / `{guid}`")
        lines.append("")
        lines.append(
            f"_Full enumeration ({total} case(s)): `scripts/comparison/known_issues.json` "
            "under the `affected_test_cases` field of this issue._"
        )
        lines.append("")
    else:
        lines.append("### Affected test cases")
        lines.append("")
        lines.append("_None enumerated (0 test cases). This issue is tracked "
                     "at the measure level only._")
        lines.append("")

    body = (issue.get("body_md") or "").strip()
    lines.append("### Body")
    lines.append("")
    lines.append(body if body else "_No body authored._")
    lines.append("")

    workaround = (issue.get("workaround") or "").strip()
    lines.append("### Workaround")
    lines.append("")
    lines.append(workaround if workaround else "_No workaround authored._")
    lines.append("")
    return "\n".join(lines)


def render_markdown(catalog, citations=None, categories=None, include_resolved=False,
                    only_cited=False, max_test_cases=5, now=None):
    """Render the catalog-issue details markdown.

    ``categories`` None means "all categories"; otherwise an iterable of
    category names to include. ``citations`` is the ``count_citations`` dict
    (or None to skip citation reporting).
    """
    issues = catalog.get("issues", [])
    if categories is not None:
        categories = set(categories)
        issues = [i for i in issues if i.get("category") in categories]
    if not include_resolved:
        issues = [i for i in issues if not is_resolved(i)]
    if only_cited:
        citations = citations or {}
        issues = [i for i in issues if i["id"] in citations]
    issues = sorted(issues, key=lambda i: (i.get("category", ""), i["id"]))

    citations = citations or {}
    total_catalog = len(catalog.get("issues", []))
    num_resolved = sum(1 for i in catalog.get("issues", [])
                       if is_resolved(i))
    num_pending = total_catalog - num_resolved
    citations_total = len({i["id"] for i in catalog.get("issues", [])
                           if i["id"] in citations})

    lines = [
        "# Known-Catalog Issue Details",
        "",
        f"- Generated: {_resolve_now(now).isoformat(timespec='seconds')}",
        "- Catalog: `scripts/comparison/known_issues.json`",
        f"- Catalog size: {total_catalog} issues ({num_pending} pending, {num_resolved} resolved)",
        f"- Issues rendered: {len(issues)}",
        f"- Issues cited in `discrepancy_report.md`: {citations_total}",
        "",
        "_This file is regenerated by `compare_results.py`; each section carries "
        "the full `body_md` of the catalog issue that the discrepancy report "
        "reduces to a one-line title + label._",
        "",
        "---",
        "",
    ]
    for issue in issues:
        lines.append(_render_issue(issue, citations.get(issue["id"]), max_test_cases))
        lines.append("---")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write_catalog_details(known_issues_path, discrepancy_report_path, output_path,
                          categories=None, include_resolved=False, only_cited=False,
                          max_test_cases=5, now=None):
    """Read inputs and write the details report; returns the markdown."""
    catalog = load_catalog(known_issues_path)
    report_path = Path(discrepancy_report_path)
    report_lines = []
    if report_path.exists():
        report_lines = report_path.read_text(encoding="utf-8").splitlines()
    md = render_markdown(
        catalog,
        citations=count_citations(report_lines),
        categories=categories,
        include_resolved=include_resolved,
        only_cited=only_cited,
        max_test_cases=max_test_cases,
        now=now,
    )
    Path(output_path).write_text(md, encoding="utf-8")
    return md


def main(argv=None):
    parser = argparse.ArgumentParser(description="Render catalog-issue details markdown.")
    parser.add_argument("--input", default=str(ROOT / "known_issues.json"),
                        help="Path to known_issues.json (default: scripts/comparison/known_issues.json)")
    parser.add_argument("--discrepancy-report", default=str(ROOT / "discrepancy_report.md"),
                        help="Path to discrepancy_report.md for citation counts (default: scripts/comparison/discrepancy_report.md)")
    parser.add_argument("--output", default=str(ROOT / "catalog_issue_details.md"),
                        help="Output path (default: scripts/comparison/catalog_issue_details.md)")
    parser.add_argument("--categories", nargs="*", default=None,
                        help="Restrict to these categories (default: all)")
    parser.add_argument("--include-resolved", action="store_true",
                        help="Include resolved issues (default: pending only)")
    parser.add_argument("--only-cited", action="store_true",
                        help="Only include issues cited in the discrepancy report")
    parser.add_argument("--max-test-cases", type=int, default=5,
                        help="Max test cases to show inline per issue (default: 5)")
    args = parser.parse_args(argv)

    md = write_catalog_details(
        known_issues_path=args.input,
        discrepancy_report_path=args.discrepancy_report,
        output_path=args.output,
        categories=args.categories,
        include_resolved=args.include_resolved,
        only_cited=args.only_cited,
        max_test_cases=args.max_test_cases,
    )
    num_issues = sum(1 for line in md.splitlines()
                     if line.startswith("## ") and line[3] != "-")
    print(f"wrote -> {args.output} ({num_issues} issues)")
    return md


if __name__ == "__main__":
    main()