# Experiment Setup and Evidence

## Task

The dataset covers the CSI300 stock universe over five trading days (`0102`, `0103`, `0104`, `0105`, and `0108`), with approximately 300 stock snapshot files per day. The pipeline uses the first five order book levels to calculate 20 LOB factors and their cross-sectional averages at each timestamp. Difference-based factors use adjacent valid snapshots, and factor denominators use a smoothing constant of `1e-7`.

## From Two Jobs to One

1. **Two-job baseline:** Job1 groups snapshots by trading day and stock, collects and sorts them, and calculates factors. Job2 accumulates per-factor `sum/count` by trading timestamp and calculates cross-sectional averages.
2. **Log-based diagnosis:** Job1 for `0102` had 300 input splits and Map tasks, with approximately 1.47 million Map input records. The two stages produced approximately 460 MB of intermediate text. Task startup, intermediate materialization, and repeated parsing were optimization targets; their individual timing contributions were not measured separately.
3. **Configuration experiment:** Map memory was reduced from 2048 to 1024 MiB and Reduce memory from 4096 to 2048 MiB, with corresponding JVM heap adjustments. Combined execution time decreased from 195.036 to 151.078 seconds in the recorded comparison. This is an observed result from one comparison, not a claim of a repeatable improvement of the same size.
4. **Single-job pipeline:** Multiple complete stock files are combined into each Map task. Factors are calculated sequentially within each file, aggregated by timestamp within the Mapper, and merged through the Combiner and Reducer using per-factor `sum/count`.
5. **Allocation reduction:** Only required fields are parsed. Two snapshot objects alternate between the current and previous states, and the factor array is reused to avoid allocating full snapshot objects and string arrays for every row.
6. **Validation:** Each of the five days is checked for timestamp coverage and mean relative error across all 20 factors within 09:30:00–15:00:00.

The single-job implementation requires complete stock files ordered by timestamp. It reports an error for decreasing timestamps rather than sorting the input automatically. Different trading days must be submitted separately. The optimization targets this input organization; it is not an interchangeable replacement for arbitrary input layouts.

## Environment and Configuration

- Ubuntu 22.04, JDK 11, and Hadoop 3.3.6.
- Cloud server: 16 vCPUs and 32 GiB of memory; single-node HDFS/YARN.
- Single job: one Reducer, a default maximum combined split size of 64 MiB, Snappy Map-output compression, a 128 MiB sort buffer, a spill threshold of 0.90, and 10 parallel shuffle copies.
- Additional exploratory runs for `0102` used `32 / 64 / 128` MiB split sizes, with pipeline times of `19.753 / 20.745 / 16.777` seconds, respectively. These are not repeated measurements of the same configuration and do not establish 128 MiB as universally optimal.

## Recorded Timings

The entries in `results/benchmark_summary.csv` use the following definitions:

- **Two-job entries:** Taken from archived `timing.csv` records, from the start of the Job1 submission command until Job2 exits successfully. Data import, compilation, export, and validation are excluded.
- **Single-job entries:** Taken from `pipeline_elapsed_seconds` in recorded terminal output. They include the original script's prior-output cleanup, job submission and execution, export, and validation. Data import and compilation are excluded.
- **Release-script behavior:** The current script refuses to overwrite existing results instead of automatically clearing them. This is a release-safety change, not evidence of new server timing runs using the revised script.
- **Five-day total:** 84.012 seconds, averaging 16.8024 seconds per day. One recorded run per day does not replace repeated measurements, confidence intervals, or load-controlled experiments.

The two timing scopes are not identical, so no exact percentage reduction or speedup factor is claimed. These server measurements are not benchmarks for the course Docker environment, a multi-node cluster, or other GitHub projects.

## Validation Evidence

`results/accuracy/*.log` preserves the archived validation output for the five final CSV files, including row counts, timestamp checks, per-factor mean errors, maximum errors, and PASS/FAIL results. These are scoring logs for archived outputs, not complete Hadoop scheduling logs, and they do not contain the single-job timings above.

All 20 factors passed the mean-error check on each of the five days. Zero reference values follow a conservative rule: an actual value of zero produces zero error; any other value produces Infinity.

The archived two-job status was marked `ACCURACY_FAIL` because that testing tool additionally required the stricter pointwise-error check to pass. The full-window mean-error check passed, while the strict pointwise check did not. The summary retains this distinction rather than relabeling the result as a pointwise pass.
