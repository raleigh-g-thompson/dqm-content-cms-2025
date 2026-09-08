#!/usr/bin/env python3
"""Repair truncated committed value set files from the IG Publisher cache.

``audit_valueset_cache.py`` identified 45 committed value sets whose
``expansion.contains`` holds only the first 1000-code page of a paged
``$expand`` (``contains < total``). The IG Publisher's terminology cache
(``input-cache/txcache/vs-*.json``) holds the complete expansion for the same
``(url, version)``.

This script overwrites, for each truncated committed file that has a matching
complete cache entry, the file's ``expansion`` block with the cache's full
expansion. It never touches non-truncated files, and it only repairs files
whose cache counterpart is itself complete (``len(contains) == total``).

Repair policy (matches audit script classes):

* ``TRUNCATED`` + complete cache (``url`` + ``version``)  -> repaired.
* ``VERSION_DRIFT`` (same URL, different version)         -> reported, skipped.
* Any file with no complete cache counterpart             -> reported, skipped.

Usage::

    python scripts/comparison/repair_valueset_cache.py
        [--valuesets input/vocabulary/valueset/external]
        [--txcache input-cache/txcache]
        [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VALUESETS = REPO_ROOT / "input" / "vocabulary" / "valueset" / "external"
DEFAULT_TXCACHE = REPO_ROOT / "input-cache" / "txcache"


def load_json_files(directory: Path, pattern: str) -> list[tuple[Path, dict]]:
    out = []
    if not directory.is_dir():
        return out
    for path in sorted(directory.glob(pattern)):
        try:
            out.append((path, json.loads(path.read_text())))
        except Exception as exc:  # noqa: BLE001 - surface any unreadable file
            print(f"  unreadable {path.name}: {exc}", file=sys.stderr)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--valuesets", type=Path, default=DEFAULT_VALUESETS,
                        help=f"Default: {DEFAULT_VALUESETS.relative_to(REPO_ROOT)}")
    parser.add_argument("--txcache", type=Path, default=DEFAULT_TXCACHE,
                        help=f"Default: {DEFAULT_TXCACHE.relative_to(REPO_ROOT)}")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would change without writing files.")
    parser.add_argument("--backup-suffix", default=".bak",
                        help="Backup suffix written before modifying a file. "
                             "Set to empty (--) to disable backups.")
    args = parser.parse_args()

    # Index cache by (url, version); require a complete expansion.
    cache_index: dict[tuple[str, str], dict] = {}
    for path, doc in load_json_files(args.txcache, "vs-*.json"):
        url, version = doc.get("url"), doc.get("version")
        if not url or not version:
            continue
        expansion = doc.get("expansion") or {}
        contains = expansion.get("contains") or []
        if expansion.get("total") is not None and len(contains) == expansion["total"]:
            cache_index[(url, version)] = expansion

    committed = load_json_files(args.valuesets, "ValueSet-*.json")

    outcomes = Counter()
    skipped = []
    for path, doc in committed:
        url, version = doc.get("url"), doc.get("version")
        expansion = doc.get("expansion") or {}
        contains = expansion.get("contains") or []
        total = expansion.get("total")
        if not url or not version:
            outcomes["skipped_parse_error"] += 1
            continue
        if not (total is not None and len(contains) < total):
            outcomes["ok_untouched"] += 1
            continue

        cache_expansion = cache_index.get((url, version))
        if cache_expansion is None:
            if any(u == url for (u, _v) in cache_index):
                outcomes["truncated_skipped_drift"] += 1
                skipped.append((path.name, url, version,
                                "same url in cache at different version"))
            else:
                outcomes["truncated_skipped_no_cache"] += 1
                skipped.append((path.name, url, version,
                                "no complete (url, version) in cache"))
            continue

        if args.dry_run:
            outcomes["would_repair"] += 1
            continue

        backup = path.with_name(path.name + (args.backup_suffix or ""))
        if args.backup_suffix:
            shutil.copyfile(path, backup)

        doc["expansion"] = json.loads(json.dumps(cache_expansion))  # deep copy
        path.write_text(json.dumps(doc, indent=2) + "\n")
        outcomes["repaired"] += 1

    print("Summary:")
    for k in sorted(outcomes):
        print(f"  {k}: {outcomes[k]}")
    if skipped:
        print("\nSkipped (no repair source):")
        for name, url, version, reason in skipped:
            print(f"  {name} ({url} v{version}) - {reason}")
    leftovers = outcomes.get("truncated_skipped_no_cache", 0) + outcomes.get(
        "truncated_skipped_drift", 0)
    if leftovers:
        print(f"\n{leftovers} truncated file(s) remain with no repair source.",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())