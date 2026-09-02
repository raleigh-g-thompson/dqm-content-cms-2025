#!/usr/bin/env python3
"""One-time migration of defect-tracking/engine-issues.md into known_issues.json.

Parses the (hand-maintained) engine-issues.md into the structured known-issues
catalog used as the single source of truth. Each issue's detailed-entry markdown
is preserved verbatim in ``body_md`` so that regenerating the document is
byte-identical (minus a generator marker).

Usage:
    python3 migrate_engine_issues.py            # read default path, write default catalog
    python3 migrate_engine_issues.py IN.md OUT.json
"""
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]          # _repo/dqm-content-cms-2025
DEFAULT_SRC = REPO_ROOT / "defect-tracking" / "engine-issues.md"
DEFAULT_DST = Path(__file__).resolve().parent / "known_issues.json"

MARKER = "<!-- GENERATED from scripts/comparison/known_issues.json — do not edit by hand -->"


def split_document(text: str) -> dict:
    """Split the markdown into preamble, summary rows, detail blocks, and trailing sections."""
    lines = text.splitlines(keepends=True)

    # 1. Preamble: everything up to and including the '---' immediately before '## Summary'
    summary_idx = next(i for i, l in enumerate(lines) if l.startswith("## Summary"))
    # find the '---' line before summary_idx
    hrule = next(i for i in range(summary_idx - 1, -1, -1)
                 if lines[i].strip() == "---")
    preamble = "".join(lines[: hrule + 1])  # includes the trailing '---' + newline

    # 2. Summary table rows: between '| ID | Issue |' header and the '---' after the table
    header_idx = next(i for i, l in enumerate(lines) if l.startswith("| ID | Issue |"))
    # table runs until a line that is '---' (hrule) after the header
    table_end = next(i for i in range(header_idx, len(lines))
                     if lines[i].strip() == "---" and i > header_idx)
    summary_rows = lines[header_idx + 2: table_end]  # skip header + separator

    # 3. Detailed entries: '## Detailed Entries' ... up to (but not including)
    #    the '---' immediately before '## Cross-Cutting Lessons'.
    detailed_hdr = next(i for i, l in enumerate(lines) if l.startswith("## Detailed Entries"))
    crosscut_hdr = next(i for i, l in enumerate(lines)
                        if l.startswith("## Cross-Cutting Lessons"))
    trailing_sep = next(i for i in range(crosscut_hdr - 1, -1, -1)
                        if lines[i].strip() == "---")
    detail_region = lines[detailed_hdr + 1: trailing_sep]

    # split detail_region into blocks starting at '### '
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in detail_region:
        if line.startswith("### ") and current:
            blocks.append(current)
            current = []
        current.append(line)
    if current:
        blocks.append(current)

    # 4. Trailing sections: everything from the '---' immediately before
    #    '## Cross-Cutting Lessons' to EOF (includes that separator).
    trailing = "".join(lines[trailing_sep:])

    return {
        "preamble": preamble,
        "summary_rows": summary_rows,
        "detail_blocks": blocks,
        "trailing": trailing,
    }


def parse_cell(text: str) -> str:
    return text.strip()


def parse_summary_row(line: str) -> dict:
    """Parse one summary-table data row into issue structured fields."""
    cells = [c.strip() for c in line.strip().strip("|").split("|")]
    # cells: [ID, Issue, Status, Workaround, Affected measures]
    id_ = cells[0]
    title = cells[1]
    status = cells[2]
    workaround = cells[3]
    affected = cells[4] if len(cells) > 4 else ""
    return {
        "id": id_,
        "title": title,
        "status": status,
        "workaround": workaround,
        "affected_measures": affected,
    }


def measure_names_from_affected(text: str) -> list[str]:
    """Best-effort extraction of a sorted, distinct list of CMS* measure names."""
    names = re.findall(r"CMS\d+", text)
    seen = []
    for n in names:
        if n not in seen:
            seen.append(n)
    return seen


# Maps a detail-block heading's leading token to the summary-row ID when they
# differ (e.g. the retired E-15 operational record is filed under the
# "E-13 applied workaround and confirmatory evidence (E-15, RETIRED ...)" block).
HEADING_ALIASES = {
    "E-13 applied workaround": "E-15",
}


def block_heading_id(heading: str) -> str:
    """Extract the leading ID token from a '### ' heading."""
    m = re.match(r"###\s+(E-\d+)(?::|\s|$)", heading.strip())
    if not m:
        return "?"
    token = m.group(1)
    # The retired E-15 operational record lives in a block whose heading begins
    # "E-13 applied workaround ... (E-15, RETIRED ...)" — file it under E-15.
    if token == "E-13" and not re.match(r"###\s+E-13\s*:", heading.strip()):
        return "E-15"
    return HEADING_ALIASES.get(token, token)


def build_catalog(parts: dict) -> dict:
    issues = []
    header = None
    block_buffer: list[str] = []

    for block in parts["detail_blocks"]:
        heading = next((l for l in block if l.startswith("### ")), None)
        if heading is None:
            continue
        issues.append({
            "id": block_heading_id(heading),
            "title": "",
            "category": "engine",
            "status": "",
            "resolved": False,
            "root_cause_status": "open",
            "workaround": "",
            "references": {"engine_entry": block_heading_id(heading)},
            "affected_measures": [],
            "affected_test_cases": [],
            "body_md": "".join(block),
            "summary_row": None,
        })

    # Attach the corresponding summary table row (matched by ID, preserving order)
    rows_by_id = {}
    for row_line in parts["summary_rows"]:
        row_line = row_line.strip()
        if not row_line.startswith("|"):
            continue
        cells = [c.strip() for c in row_line.strip().strip("|").split("|")]
        # IDs may be struck-through for retired entries, e.g. ~~E-15~~
        raw_id = cells[0]
        key = raw_id.strip("~")
        rows_by_id[key] = row_line

    for issue in issues:
        id_ = issue["id"]
        row_line = rows_by_id.get(id_)
        if row_line:
            summary = parse_summary_row(row_line)
            issue["title"] = summary["title"]
            issue["status"] = summary["status"]
            issue["workaround"] = summary["workaround"]
            issue["affected_measures"] = measure_names_from_affected(summary["affected_measures"])
            issue["summary_row"] = row_line

    return issues


def main():
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SRC
    dst = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_DST

    text = src.read_text(encoding="utf-8")
    parts = split_document(text)
    issues = build_catalog(parts)

    catalog = {
        "schema_version": 1,
        "generated_from": str(src),
        "preamble_md": parts["preamble"].rstrip("\n"),
        "cross_cutting_lessons_md": parts["trailing"].rstrip("\n"),
        "issues": issues,
    }
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(
        json.dumps(catalog, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(issues)} issues to {dst}")


if __name__ == "__main__":
    main()
