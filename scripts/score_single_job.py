#!/usr/bin/env python3
"""Compare one day's SingleJob CSV with the provided standard answer."""

from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path


FACTOR_COUNT = 20
MARKET_OPEN = "093000"
# project21.pdf requires the complete interval, including 15:00:00.
EVALUATION_END = "150000"
EXPECTED_COLUMNS = ["tradeTime"] + [f"alpha_{i}" for i in range(1, FACTOR_COUNT + 1)]


class CsvValidationError(ValueError):
    pass


def normalize_time(raw: str) -> str:
    value = raw.strip()
    if not value:
        raise CsvValidationError("empty tradeTime")
    try:
        # Handles values such as 93000 and 093000 without changing their meaning.
        numeric = int(value)
    except ValueError as exc:
        raise CsvValidationError(f"invalid tradeTime: {raw!r}") from exc
    return f"{numeric:06d}"


def load_csv(path: Path) -> dict[str, list[float]]:
    if not path.is_file():
        raise CsvValidationError(f"file not found: {path}")

    rows: dict[str, list[float]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise CsvValidationError(
                f"unexpected header in {path}:\n"
                f"  expected: {','.join(EXPECTED_COLUMNS)}\n"
                f"  actual:   {','.join(reader.fieldnames or [])}"
            )

        for line_number, row in enumerate(reader, start=2):
            try:
                trade_time = normalize_time(row["tradeTime"])
                if trade_time < MARKET_OPEN or trade_time > EVALUATION_END:
                    continue
                factors = [float(row[f"alpha_{i}"]) for i in range(1, FACTOR_COUNT + 1)]
            except (TypeError, ValueError, CsvValidationError) as exc:
                raise CsvValidationError(f"{path}:{line_number}: {exc}") from exc

            if trade_time in rows:
                raise CsvValidationError(
                    f"{path}:{line_number}: duplicate tradeTime {trade_time}"
                )
            if not all(math.isfinite(value) for value in factors):
                raise CsvValidationError(
                    f"{path}:{line_number}: NaN or Infinity found at {trade_time}"
                )
            rows[trade_time] = factors

    if not rows:
        raise CsvValidationError(f"no rows in evaluation window: {path}")
    return rows


def score(standard_path: Path, actual_path: Path, threshold: float) -> bool:
    standard = load_csv(standard_path)
    actual = load_csv(actual_path)

    standard_times = set(standard)
    actual_times = set(actual)
    missing_times = sorted(standard_times - actual_times)
    extra_times = sorted(actual_times - standard_times)
    common_times = sorted(standard_times & actual_times)

    print("=== File validation ===")
    print(f"evaluation_window={MARKET_OPEN}-{EVALUATION_END} (inclusive)")
    print(f"standard_rows={len(standard_times)}")
    print(f"actual_rows={len(actual_times)}")
    print(f"matched_rows={len(common_times)}")
    print(f"missing_rows={len(missing_times)}")
    print(f"extra_rows={len(extra_times)}")
    if missing_times:
        print("missing_examples=" + ",".join(missing_times[:10]))
    if extra_times:
        print("extra_examples=" + ",".join(extra_times[:10]))

    if not common_times:
        print("RESULT=FAIL (no matching timestamps)")
        return False

    print("\n=== Factor accuracy ===")
    all_factors_pass = True
    for factor_index in range(FACTOR_COUNT):
        relative_errors = []
        max_error = -1.0
        max_error_time = ""

        for trade_time in common_times:
            expected = standard[trade_time][factor_index]
            observed = actual[trade_time][factor_index]
            # The PDF uses abs(expected), without an epsilon in the scoring formula.
            # Its zero-reference case is unspecified: apply a conservative policy.
            if expected == 0.0:
                relative_error = 0.0 if observed == 0.0 else math.inf
            else:
                relative_error = abs(expected - observed) / abs(expected)
            relative_errors.append(relative_error)
            if relative_error > max_error:
                max_error = relative_error
                max_error_time = trade_time

        mean_error = sum(relative_errors) / len(relative_errors)
        passed = mean_error <= threshold
        all_factors_pass = all_factors_pass and passed
        print(
            f"alpha_{factor_index + 1:02d}: "
            f"mean_error={mean_error:.10g}, "
            f"max_error={max_error:.10g}@{max_error_time}, "
            f"pass={passed}"
        )

    structure_pass = not missing_times and not extra_times
    passed = structure_pass and all_factors_pass
    print("\n=== Final result ===")
    print(f"threshold={threshold:.6g}")
    print("error_formula=abs(standard-actual)/abs(standard)")
    print("zero_reference_policy=equal:0; unequal:Infinity (PDF unspecified)")
    print(f"structure_pass={structure_pass}")
    print(f"all_factors_pass={all_factors_pass}")
    print("RESULT=" + ("PASS" if passed else "FAIL"))
    return passed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare one SingleJob output CSV with one standard-answer CSV."
    )
    parser.add_argument("standard_csv", type=Path)
    parser.add_argument("actual_csv", type=Path)
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.01,
        help="mean relative-error threshold (default: 0.01)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 0.0 < args.threshold < 1.0:
        print("threshold must be between 0 and 1", file=sys.stderr)
        return 2
    try:
        return 0 if score(args.standard_csv, args.actual_csv, args.threshold) else 1
    except CsvValidationError as exc:
        print(f"CSV validation error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
