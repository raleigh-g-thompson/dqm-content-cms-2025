#!/usr/bin/env python3
"""Regenerate defect-tracking/engine-issues.md from scripts/comparison/known_issues.json.

The catalog is the single source of truth; this script rebuilds the human-
readable tracker from it. Run from the repo root:

    python3 scripts/comparison/generate_engine_issues_doc.py [catalog.json] [output.md]

Regeneration is idempotent: running this twice, or running it against an
unmodified catalog, reproduces the file byte-for-byte. That property is what
makes `render()` safe to diff against the on-disk document in CI to detect
hand-edits.

History note: this docstring previously claimed the output was "byte-identical
to the previously hand-maintained document". That was not true -- the markdown
and the catalog forked (each holding facts the other lacked) and regenerating
would have silently reverted a classification decision. The fork was reconciled
2026-09-08; do not reintroduce the assumption that the two cannot drift.
"""
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]               # _repo/dqm-content-cms-2025
DEFAULT_CATALOG = Path(__file__).resolve().parent / "known_issues.json"
DEFAULT_OUTPUT = REPO_ROOT / "defect-tracking" / "engine-issues.md"

MARKER = "<!-- GENERATED from scripts/comparison/known_issues.json — do not edit by hand -->"

# engine-issues.md documents ONLY the engine/translator issues. Non-engine
# issues (fixture/migration/vendored/content) live in the same catalog but are
# surfaced by the discrepancy report and failure tracker, not this document.
EMIT_CATEGORIES = {"engine", "engine/translator"}


def doc_issues(catalog: dict) -> list[dict]:
    all_issues = catalog.get("issues", [])
    has_multiple_categories = len({i.get("category") for i in all_issues}) > 1
    if not has_multiple_categories:
        return all_issues
    return [i for i in all_issues if i.get("category") in EMIT_CATEGORIES]


def summary_table(issues: list[dict]) -> str:
    lines = [
        "## Summary",
        "",
        "| ID | Issue | Status | Workaround | Affected measures |",
        "|----|-------|--------|------------|-------------------|",
    ]
    for issue in issues:
        if issue.get("summary_row"):
            lines.append(issue["summary_row"])
            continue
        lines.append(
            f"| {issue.get('id','')} | {issue.get('title','')} | "
            f"{issue.get('status','')} | {issue.get('workaround','')} | "
            f"{', '.join(issue.get('affected_measures',[]))} |"
        )
    return "\n".join(lines)


def render(parts: dict) -> str:
    out = []
    w = out.append
    issues = doc_issues(parts)

    # Marker first: it was previously emitted as the very last line, 1,150 lines
    # down, where nobody opening the file ever saw it. The document was
    # hand-edited repeatedly as a result. Keep it on line 1.
    w(MARKER)
    w("")
    w((parts.get("preamble_md", "") or "").rstrip("\n"))
    w("")
    w(summary_table(issues))
    w("")
    w("---")
    w("")
    w("## Detailed Entries")
    w("")
    for idx, issue in enumerate(issues):
        body = (issue.get("body_md", "") or "").rstrip("\n")
        w(body)
        if idx != len(issues) - 1:
            w("")
    w("")

    trailing = (parts.get("cross_cutting_lessons_md", "") or "").rstrip("\n")
    w(trailing)

    return "\n".join(out)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    catalog_path = Path(argv[0]) if len(argv) > 0 else DEFAULT_CATALOG
    output_path = Path(argv[1]) if len(argv) > 1 else DEFAULT_OUTPUT

    with open(catalog_path, encoding="utf-8") as fh:
        catalog = json.load(fh)

    doc = render(catalog)
    output_path.write_text(doc + "\n", encoding="utf-8")
    print(f"Wrote {len(doc.splitlines())} lines to {output_path}")


if __name__ == "__main__":
    main()
