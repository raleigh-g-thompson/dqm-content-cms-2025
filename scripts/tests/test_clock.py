"""Tests for the deterministic-clock seam.

The whole point of clock.py is that report generation can be made
byte-reproducible on demand -- either by passing an explicit `now` or by
pinning SOURCE_DATE_EPOCH -- while defaulting to the real wall clock so
interactive behaviour is unchanged.
"""
import os
import unittest
from datetime import datetime

from scripts.comparison.clock import now, resolve


class NowTest(unittest.TestCase):

    def test_defaults_to_real_time(self):
        before = datetime.now()
        result = now()
        after = datetime.now()
        self.assertLessEqual(before, result)
        self.assertLessEqual(result, after)

    def test_source_date_epoch_pins_the_clock(self):
        old = os.environ.get("SOURCE_DATE_EPOCH")
        try:
            os.environ["SOURCE_DATE_EPOCH"] = "1735689600"  # 2025-01-01 UTC
            self.assertEqual(now(), datetime(2025, 1, 1, 0, 0, 0))
        finally:
            if old is None:
                os.environ.pop("SOURCE_DATE_EPOCH", None)
            else:
                os.environ["SOURCE_DATE_EPOCH"] = old

    def test_pinned_clock_is_reproducible(self):
        """The property check_generated.py depends on: two calls under the
        same pinned epoch must be identical, not just close."""
        old = os.environ.get("SOURCE_DATE_EPOCH")
        try:
            os.environ["SOURCE_DATE_EPOCH"] = "1700000000"
            self.assertEqual(now(), now())
        finally:
            if old is None:
                os.environ.pop("SOURCE_DATE_EPOCH", None)
            else:
                os.environ["SOURCE_DATE_EPOCH"] = old


class ResolveTest(unittest.TestCase):

    def test_explicit_value_wins(self):
        fixed = datetime(2020, 1, 1)
        self.assertEqual(resolve(fixed), fixed)

    def test_none_falls_back_to_now(self):
        before = datetime.now()
        result = resolve(None)
        after = datetime.now()
        self.assertLessEqual(before, result)
        self.assertLessEqual(result, after)


if __name__ == "__main__":
    unittest.main()
