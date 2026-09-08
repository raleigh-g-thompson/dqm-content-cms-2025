#!/usr/bin/env python3
"""Regenerate defect-tracking/improvement-tracking.md from run-history.jsonl.

The append-only run-history log (``scripts/comparison/run-history.jsonl``, one
JSON line per full-suite run written by ``compare_results.py`` via
``run_history.record()``) is the single source of truth for "is this getting
better?". This script renders a human-readable trend table from it.

Run from the repo root:

    python3 scripts/comparison/generate_improvement_tracking.py [log.jsonl] [output.md]

Regeneration is idempotent apart from the ``Generated:`` timestamp line: same
log, same clock -> same bytes. ``check_generated.py`` masks that one line and
diffs the rest, so a hand-edit to the trend table is a build failure rather
than a silently forking document.
"""
import json
import sys
from pathlib import Path

try:
    from scripts.comparison.clock import resolve as _resolve_now
    from scripts.comparison.run_history import read_all
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from clock import resolve as _resolve_now
    from run_history import read_all

REPO_ROOT = Path(__file__).resolve().parents[2]               # dqm-content-cms-2025
DEFAULT_LOG = Path(__file__).resolve().parent / "run-history.jsonl"
DEFAULT_OUTPUT = REPO_ROOT / "defect-tracking" / "improvement-tracking.md"

MARKER = "<!-- GENERATED from scripts/comparison/run-history.jsonl — do not edit by hand -->"

COUNTER_COLUMNS = [
    ("total_cases", "Total"),
    ("passing", "Passing"),
    ("failing", "Failing"),
    ("attributed_failures", "Attributed"),
    ("unattributed_failures", "Unattributed"),
    ("stale_attributions", "Stale attributions"),
    ("phantom_attributions", "Phantom attributions"),
    ("unscored_cases", "Unscored"),
]


def _num(entry: dict, key: str) -> int:
    value = entry.get(key)
    return value if isinstance(value, (int, float)) else 0


def render(entries: list, now=None) -> str:
    """Render the improvement-tracking markdown from ``entries`` (oldest first).

    ``now`` is a ``datetime`` used only for the ``Generated:`` line; None means
    the real clock (or ``SOURCE_DATE_EPOCH`` override, via ``clock.now()``).
    """
    ts = _resolve_now(now).isoformat(timespec="seconds")
    lines = [
        MARKER,
        "",
        "# Improvement Tracking",
        "",
        f"- Generated: {ts}",
        "- Source: `scripts/comparison/run-history.jsonl` (append-only, one line per full-suite run)",
        "- Runs recorded: " + str(len(entries)),
        "",
        "_This file is regenerated from the run-history log; do not hand-edit. "
        "Identical numbers across adjacent rows mean a rerun of the same data, "
        "not a claim about a plateau._",
        "",
        "| Run | Timestamp | Total | Passing | Failing | Attributed | Unattributed | Stale attributions | Phantom attributions | Unscored |",
        "|-----|-----------|------:|--------:|--------:|-----------:|-------------:|--------------------:|---------------------:|---------:|",
    ]
    if not entries:
        lines.append("| —  | _no runs recorded yet_ | | | | | | | | |")
        lines.append("")
        lines.append("_No runs recorded — the trend section appears once the "
                     "log has at least two entries._")
        return "\n".join(lines) + "\n"

    for idx, entry in enumerate(entries, start=1):
        timestamp = entry.get("timestamp", "")
        cells = [str(idx), str(timestamp)]
        cells += [str(_num(entry, key)) for key, _ in COUNTER_COLUMNS]
        lines.append("| " + " | ".join(cells) + " |")

    lines.append("")
    lines.append("## Since the first recorded run")
    lines.append("")
    if len(entries) < 2:
        lines.append("_Only one run recorded — deltas appear once a second "
                     "run exists._")
        return "\n".join(lines) + "\n"

    first, last = entries[0], entries[-1]
    lines.append("| Metric | First run | Latest run | Delta |")
    lines.append("|--------|----------:|-----------:|------:|")
    for key, label in COUNTER_COLUMNS:
        first_val = _num(first, key)
        last_val = _num(last, key)
        delta = last_val - first_val
        delta_cell = f"+{delta}" if delta > 0 else str(delta)
        lines.append(
            f"| {label} | {first_val} | {last_val} | {delta_cell} |")
    lines.append("")
    lines.append(f"_Latest run: {last.get('timestamp', '')}._")
    return "\n".join(lines) + "\n"


def write_improvement_tracking(log_path=None, output_path=None, now=None) -> str:
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

    md = write_improvement_tracking(log_path, output_path)
    print(f"Wrote {len(md.splitlines())} lines to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())