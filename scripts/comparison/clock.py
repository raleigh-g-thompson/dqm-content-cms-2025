"""Deterministic-clock helper for report generation.

Every report generator used to call ``datetime.now()`` directly in the render
path (``compare_results.py:506`` for the "Generated" summary row,
``compare_results.py:730`` for the archive-file timestamp,
``render_catalog_issue_details.py:141`` for its own "Generated" line). That
makes two runs against identical inputs produce different bytes, which in turn
makes ``check_generated.py``'s regenerate-and-diff drift check impossible for
anything that embeds a timestamp, and forces any end-to-end test asserting
full report text to regex the timestamp out rather than compare it directly.

``now()`` here is a single seam: by default it is the real wall clock (nothing
about interactive behaviour changes), but setting the ``SOURCE_DATE_EPOCH``
environment variable -- the same convention reproducible-builds tooling uses --
pins it to a fixed instant. Generators that want deterministic output take an
explicit ``now: datetime | None`` parameter and fall back to this function, so
tests can also just pass a fixed value directly without touching the
environment.
"""
import os
from datetime import datetime, timezone


def now() -> datetime:
    """Current time, or the pinned instant if SOURCE_DATE_EPOCH is set."""
    epoch = os.environ.get("SOURCE_DATE_EPOCH")
    if epoch:
        return datetime.fromtimestamp(int(epoch), tz=timezone.utc).replace(tzinfo=None)
    return datetime.now()


def resolve(explicit: "datetime | None") -> datetime:
    """Return ``explicit`` if given, else ``now()``.

    The small helper every call site uses: ``ts = clock.resolve(now)``.
    """
    return explicit if explicit is not None else now()
