#!/usr/bin/env python3
"""Audit committed value set files against the IG Publisher terminology cache.

The IG Publisher's ``_refresh.sh`` downloads each externally-referenced value
set from the terminology server (tx.fhir.org) into ``input-cache/txcache/``
as ``vs-<hash>.json``. The committed copies live under
``input/vocabulary/valueset/external/ValueSet-<oid>-<version>.json``.

The two copies are supposed to be the same resource. In practice the committed
copies are sometimes only the *first page* of a paged ``$expand``: the file
carries ``expansion.parameter`` ``{count: 1000, offset: 0}``, ``expansion.total``
equal to the full count, but ``expansion.contains`` of only 1000 entries. The
terminology cache holds the complete expansion (``len(contains) == total`` with
no paging parameter). The engine evaluates ``InValueSet`` against the committed
files, so a truncated file silently drops codes -- e.g. the Cancer value set
(``2.16.840.1.113883.3.526.3.1010``) that shares CMS157's 19 failing test cases.

This script is read-only. It classifies each committed value set file and
reports the result:

* ``OK``              -- complete expansion, no paging artifact.
* ``LEFTOVER``        -- complete expansion but ``count``/``offset`` paging
                        parameters remain from the fetch (cosmetic).
* ``TRUNCATED``       -- ``len(contains) < total`` (real data loss).
* ``TXCACHE_MISSING`` -- no matching ``(url, version)`` in the cache at all.
* ``VERSION_DRIFT``   -- same URL present in the cache but at a different
                        version.
* ``NO_EXPANSION``    -- committed file has no ``expansion`` element.
* ``PARSE_ERROR``     -- file unreadable as JSON or missing ``url``.

Exit code is non-zero when any ``TRUNCATED`` value sets remain, so the script
can gate a refresh/re-publish step.

Usage::

    python scripts/comparison/audit_valueset_cache.py
        [--report /tmp/valueset-audit.md]
        [--csv /tmp/valueset-audit.csv]
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VALUESETS = REPO_ROOT / "input" / "vocabulary" / "valueset" / "external"
DEFAULT_TXCACHE = REPO_ROOT / "input-cache" / "txcache"


def load_json_files(directory: Path, pattern: str) -> list[dict]:
    """Load every JSON file matching ``pattern`` in ``directory``.

    Unparseable files are returned as ``{"__error__": "..."}`` so the caller
    can still surface them.
    """
    if not directory.is_dir():
        return []
    out = []
    for path in sorted(directory.glob(pattern)):
        try:
            out.append(json.loads(path.read_text()))
        except Exception as exc:  # noqa: BLE001 - report any JSON issue
            out.append({"__error__": str(exc), "__path__": str(path)})
    return out


def load_txcache(directory: Path) -> dict[tuple[str, str | None], dict]:
    """Index cache files by ``(url, version)``."""
    index: dict[tuple[str, str | None], dict] = {}
    for doc in load_json_files(directory, "vs-*.json"):
        if "__error__" in doc:
            continue
        url = doc.get("url")
        if not url:
            continue
        index[(url, doc.get("version"))] = doc
    return index


def parse_paging_parameters(expansion: dict) -> bool:
    """True if the expansion carries a ``count`` paging parameter."""
    for param in expansion.get("parameter") or []:
        if param.get("name") == "count":
            return True
    return False


def classify(doc: dict, txcache: dict[tuple[str, str | None], dict]) -> dict:
    """Classify a single committed value set file."""
    if "__error__" in doc:
        return {"status": "PARSE_ERROR", "detail": doc.get("__error__")}
    url = doc.get("url")
    version = doc.get("version")
    if not url:
        return {"status": "PARSE_ERROR", "detail": "missing url"}

    expansion = doc.get("expansion") or {}
    contains = expansion.get("contains") or []
    total = expansion.get("total")
    n = len(contains)
    cache = txcache.get((url, version))

    result: dict = {
        "url": url,
        "version": version,
        "contains": n,
        "total": total,
        "paged": parse_paging_parameters(expansion),
        "cache": bool(cache),
        "cache_total": (cache.get("expansion") or {}).get("total") if cache else None,
        "cache_contains": len((cache.get("expansion") or {}).get("contains") or []) if cache else None,
        "cache_versions": sorted(
            {v for (u, v) in txcache if u == url}
        ) or None,
        "status": None,
        "detail": None,
    }

    if "expansion" not in doc:
        result["status"] = "NO_EXPANSION"
        result["detail"] = "no expansion element"
    elif total and n < total:
        result["status"] = "TRUNCATED"
        result["detail"] = f"{n} of {total} codes present"
    elif not cache:
        if any(v == url for (v, _) in [(u, ver) for (u, ver) in txcache]):
            result["status"] = "VERSION_DRIFT"
            result["detail"] = f"cache has versions: {', '.join(map(str, result['cache_versions']))}"
        else:
            result["status"] = "TXCACHE_MISSING"
            result["detail"] = "no matching (url, version) in txcache"
    elif result["paged"]:
        result["status"] = "LEFTOVER"
        result["detail"] = "complete but retains count/offset paging parameters"
    else:
        result["status"] = "OK"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--valuesets", type=Path, default=DEFAULT_VALUESETS,
                        help="Directory of committed ValueSet resources. "
                             f"Default: {DEFAULT_VALUESETS.relative_to(REPO_ROOT)}")
    parser.add_argument("--txcache", type=Path, default=DEFAULT_TXCACHE,
                        help="IG Publisher terminology cache directory. "
                             f"Default: {DEFAULT_TXCACHE.relative_to(REPO_ROOT)}")
    parser.add_argument("--report", type=Path, default=None,
                        help="Optional Markdown report path")
    parser.add_argument("--csv", type=Path, default=None,
                        help="Optional CSV detail path")
    parser.add_argument("--status", action="append", default=None,
                        help="Restrict report to one or more statuses "
                             "(e.g. --status TRUNCATED). Repeatable.")
    args = parser.parse_args()

    txcache = load_txcache(args.txcache)
    docs = load_json_files(args.valuesets, "ValueSet-*.json")
    rows = [classify(d, txcache) for d in docs]

    from collections import Counter
    counts = Counter(r["status"] for r in rows)

    lines = [
        "# Value Set Cache Audit",
        "",
        f"- committed valuesets: `{args.valuesets}`",
        f"- terminology cache:   `{args.txcache}`",
        "",
        "## Summary",
        "",
        "| Status | Count |",
        "|---|---:|",
    ]
    for status in ["OK", "LEFTOVER", "TRUNCATED", "VERSION_DRIFT",
                   "TXCACHE_MISSING", "NO_EXPANSION", "PARSE_ERROR"]:
        lines.append(f"| {status} | {counts.get(status, 0)} |")
    if sum(counts.values()) != len(rows):
        lines.append(f"| (unclassified) | {len(rows) - sum(counts.values())} |")
    lines.append("| **Total** | **%d** |" % len(rows))
    lines.append("")

    if args.status:
        keep = set(args.status)
        shown = [r for r in rows if r["status"] in keep]
    else:
        shown = [r for r in rows if r["status"] not in ("OK",)]
    if shown:
        lines.append("## Detail")
        lines.append("")
        lines.append("| Status | Url | Version | Contains | Total | Cache total | Detail |")
        lines.append("|---|---|---|---|---:|---:|---|")
        for r in sorted(shown, key=lambda r: (r["status"] or "", r["url"] or "")):
            lines.append(
                f"| {r['status']} | {r['url']} | {r['version'] or ''} | "
                f"{r['contains'] or ''} | {r['total'] or ''} | "
                f"{r['cache_total'] or ''} | {r['detail'] or ''} |"
            )
        lines.append("")

    md = "\n".join(lines)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(md)
        print(f"wrote report -> {args.report}")
    else:
        print(md)

    if args.csv:
        with args.csv.open("w", newline="") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=["status", "url", "version", "contains",
                                "total", "paged", "cache", "cache_total",
                                "cache_contains", "cache_versions", "detail"])
            writer.writeheader()
            for r in rows:
                writer.writerow({k: ("" if r.get(k) is None else r.get(k))
                                 for k in writer.fieldnames})
        print(f"wrote csv -> {args.csv}")

    truncated = counts.get("TRUNCATED", 0)
    if truncated:
        print(f"\n{truncated} truncated value set(s) remain -- re-run the IG "
              f"publisher refresh to repair.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())