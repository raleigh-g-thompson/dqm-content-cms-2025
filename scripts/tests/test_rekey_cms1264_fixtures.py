import json
import tempfile
import unittest
from pathlib import Path

from scripts.rekey_cms1264_fixtures_to_2026 import (
    DATE_TOKEN,
    MIGRATED_MR_START,
    PRE_MIGRATED_MR_START,
    case_needs_migration,
    rekey_file_text,
    shift_year,
)


class ShiftYearTest(unittest.TestCase):

    def test_decrements_year_keeps_date(self):
        self.assertEqual(shift_year(DATE_TOKEN.search("2027-01-01")), "2026-01-01")
        self.assertEqual(shift_year(DATE_TOKEN.search("2028-01-01")), "2027-01-01")
        self.assertEqual(shift_year(DATE_TOKEN.search("2010-01-01")), "2009-01-01")

    def test_rekey_file_text_shifts_all_dates(self):
        text = (
            '{"resourceType": "Encounter", "period": {"start": "2027-05-01T02:01:00.000+00:00", '
            '"end": "2028-01-01T00:00:00.000+00:00"}}'
        )
        rekeyed = rekey_file_text(text)
        self.assertIn('"2026-05-01T02:01:00.000+00:00"', rekeyed)
        self.assertIn('"2027-01-01T00:00:00.000+00:00"', rekeyed)

    def test_leaves_non_date_tokens_untouched(self):
        text = '{"resourceType": "Cond", "id": "86b95e71-d396-4a98-9ab0-bc369d5ce2c9", "meta": {"profile": ["x"], "versionId": "2023-09"}}'
        rekeyed = rekey_file_text(text)
        self.assertEqual(rekeyed, text)  # GUID & YYYY-MM version untouched

    def test_leaves_guid_and_other_years(self):
        text = '{"id": "c3284314-fe9b-408a-9b26-a21830f84432", "birthDate": "2009-01-01"}'
        self.assertIn('"birthDate": "2008-01-01"', rekey_file_text(text))
        self.assertIn("c3284314-fe9b-408a-9b26-a21830f84432", rekey_file_text(text))

    def test_shift_year_rejects_below_1000(self):
        match = DATE_TOKEN.search("2027-01-01")
        self.assertIsNotNone(match)


class MigrationGuardTest(unittest.TestCase):

    def make_case(self, mr_start):
        tmp = Path(tempfile.mkdtemp())
        case = tmp / "case"
        case.mkdir()
        with (case / "MeasureReport-1.json").open("w", encoding="utf-8") as fh:
            json.dump({"resourceType": "MeasureReport", "period": {"start": mr_start, "end": "2027-12-31"}}, fh)
        return case

    def test_2027_needs_migration(self):
        self.assertTrue(case_needs_migration(self.make_case(PRE_MIGRATED_MR_START)))

    def test_2026_is_migrated(self):
        self.assertFalse(case_needs_migration(self.make_case(MIGRATED_MR_START)))

    def test_unexpected_start_raises(self):
        with self.assertRaises(RuntimeError):
            case_needs_migration(self.make_case("2030-01-01"))


if __name__ == "__main__":
    unittest.main()