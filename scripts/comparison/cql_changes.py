"""Append-only log of CQL source changes.

``defect-tracking/updated-cql.md`` is a generated document; its source is this
hand-maintained, append-only JSONL log (one JSON object per CQL change), the
same source/log split ``improvement-tracking.md`` has with
``scripts/comparison/run-history.jsonl``. This module owns loading, appending,
validating, and next-id assignment.

Authors append one entry per committed CQL change; do not rewrite or remove
prior entries (correcting a typo in an entry already in the log is fine only if
you re-run ``generate_updated_cql.py`` afterwards -- the generated markdown
must never fork from this file).

Entry shape (all keys optional except ``id``, ``date``, ``measure``, ``title``):

    id            auto-assigned "CQL-NNN" by record(); required on read
    date          YYYY-MM-DD (or "historical" for pre-log entries)
    kind          fix | migration-origin | refactor | other
    measure       measure library name, e.g. "CMS190FHIRVTEProphylaxisICU"
    library       path to the edited .cql file (defaults to input/cql/<measure>.cql)
    title         one-line summary
    changes       [{"location", "before", "after"}, ...] -- before/after may be
                  omitted for deletions/insertions
    issues        ["M-04", "E-03"] defect-tracking/issues ids this change pairs with
    engine_version  engine the change was verified against (e.g. "5.3")
    verified      free-text verification summary (run counts, flip results)
    propagation   how the change reached dependent artifacts (e.g. measure JSON
                  resync, IG Publisher refresh) or "n/a"
    commit        commit sha when merged ("pending" while in the working tree)
"""
import json
from pathlib import Path
from typing import Optional

DEFAULT_PATH = Path(__file__).resolve().parent.parent.parent / "defect-tracking" / "cql-changes.jsonl"

_REQUIRED_READ = {"id", "date", "measure", "title"}
_KINDS = {"fix", "migration-origin", "refactor", "other"}


def read_all(path: Optional[str] = None) -> list:
    """Return every recorded entry, oldest first.

    Entries are validated with ``_validate_entry`` so a malformed line fails
    loudly (a corruption that would silently produce a wrong report).
    """
    target = Path(path) if path else DEFAULT_PATH
    if not target.exists():
        return []
    entries = []
    with open(target, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            _validate_entry(entry, origin=f"{target}:{lineno}")
            entries.append(entry)
    return entries


def record(entry: dict, path: Optional[str] = None) -> str:
    """Validate ``entry``, assign the next ``id``, append, and return the path.

    The next id is the max existing numeric suffix plus one, so hand-inserted
    entries keep numbering monotonic. Entry order in the file is preserved
    (append-only), while the rendered document sorts newest-first.
    """
    target = Path(path) if path else DEFAULT_PATH
    entries = read_all(str(target))
    existing = [e["id"] for e in entries]
    numeric = [int(i[4:]) for i in existing if i.startswith("CQL-") and i[4:].isdigit()]
    entry = dict(entry)
    entry.setdefault("id", f"CQL-{max(numeric, default=0) + 1:03d}")
    _validate_entry(entry, origin=str(target))
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, sort_keys=True) + "\n")
    return str(target)


def _validate_entry(entry: dict, origin: str) -> None:
    missing = _REQUIRED_READ - set(entry)
    if missing:
        raise ValueError(f"{origin}: entry missing required keys: {sorted(missing)}")
    if not entry["id"].startswith("CQL-"):
        raise ValueError(f"{origin}: id must start with 'CQL-': {entry['id']!r}")
    if entry.get("kind") not in (None, *tuple(_KINDS)):
        raise ValueError(
            f"{origin}: kind {entry.get('kind')!r} not in {sorted(_KINDS)}")
    changes = entry.get("changes", [])
    if not isinstance(changes, list):
        raise ValueError(f"{origin}: 'changes' must be a list")
    for i, change in enumerate(changes):
        if not isinstance(change, dict) or not change.get("location"):
            raise ValueError(f"{origin}: changes[{i}] needs a 'location'")