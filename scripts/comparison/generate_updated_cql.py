#!/usr/bin/env python3
"""Regenerate defect-tracking/updated-cql.md from defect-tracking/cql-changes.jsonl.

The append-only CQL-change log (``defect-tracking/cql-changes.jsonl``, one JSON
line per committed CQL change, written via ``cql_changes.record()`` or by
hand) is the single source of truth for "what did we change in the CQL?".
This script renders a human-readable, reverse-chronological document from it.

Run from the repo root:

    python3 scripts/comparison/generate_updated_cql.py [log.jsonl] [output.md]

Regeneration is idempotent apart from the ``Generated:`` timestamp line: same
log, same clock -> same bytes. ``check_generated.py`` masks that one line and
diffs the rest, so a hand-edit to updated-cql.md is a build failure rather
than a silently forking document.
"""
import sys
from pathlib import Path

try:
    from scripts.comparison.clock import resolve as _resolve_now
    from scripts.comparison.cql_changes import read_all
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from clock import resolve as _resolve_now
    from cql_changes import read_all

REPO_ROOT = Path(__file__).resolve().parents[2]               # dqm-content-cms-2025
DEFAULT_LOG = REPO_ROOT / "defect-tracking" / "cql-changes.jsonl"
DEFAULT_OUTPUT = REPO_ROOT / "defect-tracking" / "updated-cql.md"

MARKER = "<!-- GENERATED from defect-tracking/cql-changes.jsonl — do not edit by hand -->"


def _kind(entry: dict) -> str:
    return entry.get("kind") or "other"


def _field(entry: dict, key: str, default: str = "") -> str:
    value = entry.get(key)
    return default if value is None else str(value)


def _render_code_fence(change: dict, side: str) -> list:
    """Emit a before/after code block for one side of a change, or nothing when
    that side has no snippet (pure insertion/deletion)."""
    code = change.get(side)
    if not code:
        return []
    return ["```cql", str(code).rstrip("\n"), "```"]


def render(entries: list, now=None) -> str:
    """Render the updated-cql markdown from ``entries`` (append-order).

    The document is newest-first (by ``date``, then ``id``); migration-origin
    / historical entries sort first regardless of date. ``now`` is a
    ``datetime`` used only for the ``Generated:`` line; None means the real
    clock (or ``SOURCE_DATE_EPOCH`` override, via ``clock.now()``).
    """
    ts = _resolve_now(now).isoformat(timespec="seconds")
    lines = [
        MARKER,
        "",
        "# Updated CQL",
        "",
        f"- Generated: {ts}",
        "- Source: `defect-tracking/cql-changes.jsonl` (append-only, one JSON line per CQL change)",
        "- Entries recorded: " + str(len(entries)),
        "",
        "_This file is regenerated from the CQL-change log; do not hand-edit. "
        "Append new changes to `cql-changes.jsonl` (or use "
        "`cql_changes.record()`), then re-run `scripts/run_reports.py`._",
        "",
    ]

    if not entries:
        lines += [
            "_No CQL changes recorded yet — entries appear once a change is "
            "appended to the log._",
            "",
        ]
        return "\n".join(lines) + "\n"

    def _sort_key(entry):
        if _kind(entry) == "migration-origin":
            return (0, "", entry["id"])
        return (1, entry.get("date", ""), entry["id"])

    ordered = sorted(entries, key=_sort_key)
    newest_first = list(reversed(ordered))

    lines += [
        "| Ref | Date | Measure | Kind | Issues | Engine | Verified |",
        "|-----|------|---------|------|--------|--------|----------|",
    ]
    for entry in newest_first:
        issues = ", ".join(entry.get("issues") or ["—"])
        lines.append(
            f"| {entry['id']} | {entry.get('date', '—')} | "
            f"{entry.get('measure', '—')} | {_kind(entry)} | "
            f"{issues} | {entry.get('engine_version') or '—'} | "
            f"{entry.get('verified') or '—'} |")
    lines.append("")

    for entry in newest_first:
        lines += _render_entry(entry)

    return "\n".join(lines) + "\n"


def _render_entry(entry: dict) -> list:
    library = entry.get("library") or f"input/cql/{entry.get('measure', '')}.cql"
    lines = [
        f"### {entry['id']} — {entry.get('date', '?')} — "
        f"{entry.get('measure', '?')} ({_kind(entry)})",
        "",
        f"**{entry['title']}**",
        "",
        f"- **Library**: `{library}`",
    ]
    if entry.get("summary"):
        lines.append(f"- **Summary**: {entry['summary']}")
    changes = entry.get("changes") or []
    if changes:
        lines.append("")
    for i, change in enumerate(changes, start=1):
        lines.append(f"**{i}. {change['location']}**")
        lines.append("")
        before = _render_code_fence(change, "before")
        after = _render_code_fence(change, "after")
        if before and after:
            lines += before
            lines.append("→")
            lines += after
        elif before:
            lines += before
        elif after:
            lines += after
        lines.append("")
    details = []
    if entry.get("issues"):
        details.append("**Issues**: " + ", ".join(entry["issues"]))
    if entry.get("engine_version"):
        details.append("**Engine**: " + entry["engine_version"])
    if entry.get("verified"):
        details.append("**Verified**: " + entry["verified"])
    details.append("**Propagation**: " + (entry.get("propagation") or "n/a"))
    details.append("**Commit**: " + (entry.get("commit") or "pending"))
    lines.append(" · ".join(details))
    lines.append("")
    return lines


def write_updated_cql(log_path=None, output_path=None, now=None) -> str:
    """Read the log, render the doc, and write it; returns the markdown."""
    log = Path(log_path) if log_path else DEFAULT_LOG
    out = Path(output_path) if output_path else DEFAULT_OUTPUT
    entries = read_all(str(log)) if log.exists() else []
    md = render(entries, now=now)
    out.write_text(md, encoding="utf-8")
    return md


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    log_path = Path(argv[0]) if len(argv) > 0 and argv[0] else DEFAULT_LOG
    output_path = Path(argv[1]) if len(argv) > 1 and argv[1] else DEFAULT_OUTPUT

    md = write_updated_cql(log_path, output_path)
    print(f"Wrote {len(md.splitlines())} lines to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())