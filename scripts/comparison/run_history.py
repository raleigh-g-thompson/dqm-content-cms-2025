"""Append-only run-history log.

`compare_results.py` used to embed its only durable record of "how did we do"
directly in `discrepancy_report.md` -- a file that gets overwritten every run
and only ever shows the current state. There was no way to answer "is this
getting better?" without diffing old `_archive/` copies by hand.

`record()` appends one JSON line per run here instead. It is intentionally
dumb: no aggregation, no rewriting of prior entries, just a fact log. Phase 5's
`improvement-tracking.md` regeneration reads this file rather than being
hand-maintained prose that drifts from the actual numbers (as it does today).

One line per run, newest last, so `tail -f` and naive line-based tooling both
work without parsing the whole file.
"""
import json
from pathlib import Path
from typing import Optional

DEFAULT_PATH = Path(__file__).resolve().parent / "run-history.jsonl"


def record(entry: dict, path: Optional[str] = None) -> str:
    """Append ``entry`` as one JSON line. Returns the path written to.

    ``entry`` must be JSON-serialisable and should include a ``timestamp`` key
    (callers pass the same resolved clock value used for the rest of that run,
    so this entry's timestamp matches the report it describes).
    """
    target = Path(path) if path else DEFAULT_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, sort_keys=True) + "\n")
    return str(target)


def read_all(path: Optional[str] = None) -> list:
    """Return every recorded entry, oldest first. Empty list if the file
    doesn't exist yet -- a fresh checkout with no run history is not an error."""
    target = Path(path) if path else DEFAULT_PATH
    if not target.exists():
        return []
    entries = []
    with open(target, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


def latest(path: Optional[str] = None) -> Optional[dict]:
    """Return the most recent entry, or None if there is no history yet."""
    entries = read_all(path)
    return entries[-1] if entries else None
