"""Synthetic regression tests; not a replacement for real Hadoop validation."""

import contextlib
import csv
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "score_single_job.py"
SPEC = importlib.util.spec_from_file_location("single_job_score", MODULE_PATH)
SCORER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCORER)


class ScoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def write_csv(self, name, rows, header=None):
        path = self.root / name
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(header or SCORER.EXPECTED_COLUMNS)
            for time, value in rows:
                writer.writerow([time] + [value] * 20)
        return path

    def compare(self, standard, actual):
        with contextlib.redirect_stdout(io.StringIO()):
            return SCORER.score(standard, actual, 0.01)

    def test_equal_values_and_normalized_times_pass(self):
        expected = self.write_csv("expected.csv", [("093000", 1), ("150000", 2)])
        actual = self.write_csv("actual.csv", [("93000", 1), ("150000", 2)])
        self.assertTrue(self.compare(expected, actual))

    def test_missing_close_timestamp_fails(self):
        expected = self.write_csv("expected.csv", [("093000", 1), ("150000", 1)])
        actual = self.write_csv("actual.csv", [("093000", 1)])
        self.assertFalse(self.compare(expected, actual))

    def test_extra_in_window_timestamp_fails(self):
        expected = self.write_csv("expected.csv", [("093000", 1)])
        actual = self.write_csv("actual.csv", [("093000", 1), ("093003", 1)])
        self.assertFalse(self.compare(expected, actual))

    def test_duplicate_timestamp_rejected(self):
        actual = self.write_csv("actual.csv", [("093000", 1), ("93000", 1)])
        with self.assertRaises(SCORER.CsvValidationError):
            SCORER.load_csv(actual)

    def test_nonfinite_value_rejected(self):
        for value in (float("nan"), float("inf")):
            with self.subTest(value=value):
                actual = self.write_csv("actual.csv", [("093000", value)])
                with self.assertRaises(SCORER.CsvValidationError):
                    SCORER.load_csv(actual)

    def test_wrong_header_rejected(self):
        actual = self.write_csv("actual.csv", [("093000", 1)],
                                ["time"] + SCORER.EXPECTED_COLUMNS[1:])
        with self.assertRaises(SCORER.CsvValidationError):
            SCORER.load_csv(actual)

    def test_zero_reference_policy(self):
        expected = self.write_csv("expected.csv", [("093000", 0)])
        actual = self.write_csv("actual.csv", [("093000", 0)])
        self.assertTrue(self.compare(expected, actual))
        actual = self.write_csv("actual.csv", [("093000", 0.000001)])
        self.assertFalse(self.compare(expected, actual))

    def test_threshold_is_inclusive(self):
        expected = self.write_csv("expected.csv", [("093000", 100)])
        actual = self.write_csv("actual.csv", [("093000", 101)])
        self.assertTrue(self.compare(expected, actual))
        actual = self.write_csv("actual.csv", [("093000", 102)])
        self.assertFalse(self.compare(expected, actual))

    def test_out_of_window_rows_do_not_affect_score(self):
        expected = self.write_csv("expected.csv", [("093000", 1)])
        actual = self.write_csv("actual.csv", [("092957", 9), ("093000", 1),
                                                ("150003", 9)])
        self.assertTrue(self.compare(expected, actual))


if __name__ == "__main__":
    unittest.main()
