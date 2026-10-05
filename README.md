# LOB Factor Calculation with Hadoop MapReduce

A limit order book factor calculation and performance optimization project built with Java, HDFS, and MapReduce.

The pipeline processes approximately 1,500 stock snapshot files across five trading days for the CSI300 stock universe. It calculates 20 limit order book (LOB) factors and produces cross-sectional averages at each trading timestamp. The repository includes the two-job baseline, the optimized single-job implementation, runnable code, timing scripts, and five-day validation records.

## Project Highlights

- Used Hadoop logs and counters to identify overhead associated with approximately 300 small Map tasks and intermediate results between the two jobs.
- Refactored the two-stage pipeline into a single job by combining complete stock files, calculating factors sequentially from adjacent snapshots, and aggregating within each Mapper before the final Reduce step.
- Reduced object allocation and intermediate output through selective field parsing, alternating reuse of two snapshot objects, numeric timestamp keys, and per-factor `sum/count` statistics.
- Passed the mean relative error threshold of 1% for all 20 factors on each of the five days: `0102 / 0103 / 0104 / 0105 / 0108`, covering 09:30:00–15:00:00.
- Recorded approximately 17 seconds per day for the single-job pipeline, compared with approximately 151 seconds for the memory-tuned two-job execution. The timing scopes differ, so these figures are not a strictly like-for-like speedup measurement.

## Pipeline

```text
Single-day snapshots in HDFS (one complete, ordered file per stock)
  → Combine complete files into Map inputs
  → Parse sequentially and calculate 20 factors using the current and previous valid snapshots
  → Accumulate per-factor sum/count by tradeTime within each Mapper
  → Merge additive statistics in the Combiner
  → Calculate cross-sectional averages in a single Reducer
  → Sorted CSV → Export, timing, and validation
```

The two-job baseline first groups snapshots by trading day and stock, sorts them, calculates stock-level factors, and writes the intermediate results to HDFS. The second job computes cross-sectional averages. Both implementations remain in the same source directory for comparison.

## Runtime Environment

Recorded benchmark environment: Ubuntu 22.04, JDK 11, Hadoop 3.3.6, single-node HDFS/YARN, 16 vCPUs, and 32 GiB of memory.

Java, Hadoop, HDFS/YARN, Python 3, and GNU `time` must be installed and configured beforehand. The single-job implementation enables Snappy compression for Map outputs, so the Hadoop environment must support Snappy.

## Quick Start

### 1. Build

From the repository root, run the following on Linux or WSL:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/path/to/hadoop-3.3.6
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
bash scripts/build.sh
```

This creates `build/lob-factor-mapreduce.jar`. The build does not download data, modify cluster configuration, or start Hadoop services.

### 2. Prepare Single-Day Input

The raw dataset and reference answers are not bundled with the repository. Prepare data with the same field layout:

```text
data/0102/
  000001/snapshot.csv
  000002/snapshot.csv
  ...
standard/0102.csv
```

```bash
hdfs dfs -mkdir -p /lob-factor/input
hdfs dfs -put data/0102 /lob-factor/input/0102
```

Snapshots within each stock file must be in nondecreasing timestamp order. The first valid snapshot initializes the previous-snapshot state and does not produce an output row. The optimized implementation processes one trading day per run: do not mix days or arbitrarily split a stock's time series across multiple files.

### 3. Run, Export, and Validate

```bash
run_id=$(date +%Y%m%dT%H%M%S)
mkdir -p results/local
set -o pipefail
bash scripts/benchmark_single_job.sh \
  build/lob-factor-mapreduce.jar \
  /lob-factor/input/0102 \
  "/lob-factor/runs/$run_id/0102" \
  "results/local/0102-$run_id.csv" \
  standard/0102.csv \
  64 2>&1 | tee "results/local/0102-$run_id.log"
```

The final argument specifies the maximum combined split size in MiB. Compare `32 / 64 / 128` as needed. Use a unique output path for every run: the release script refuses to overwrite existing results.

Repeat the command separately for the remaining four days. Reference answers are used for local validation and do not need to be imported into HDFS.

### 4. Validate Existing Results

```bash
python3 scripts/score_single_job.py standard/0102.csv results/local/0102.csv
python3 -m unittest discover -s tests -v
```

The validator checks the CSV header, timestamp sets, duplicate timestamps, and finite numeric values within the evaluation window. It aligns rows by timestamp and calculates:

```text
relative_error = abs(standard - actual) / abs(standard)
mean(relative_error) <= 0.01 for each factor
```

A zero reference value is handled conservatively: the error is zero if the actual value is also zero, and Infinity otherwise. Rows outside the evaluation window are excluded. Successful job execution alone does not establish factor accuracy without reference answers.

## Benchmark Results

| Implementation / Day | Time (seconds) | Timing Scope |
|---|---:|---|
| Two-job baseline / 0102 | 195.036 | Submission and execution of both jobs |
| Two-job, memory-tuned / 0102 | 151.078 | Submission and execution of both jobs |
| Single-job / 0102 | 16.553 | Job execution, CSV export, and validation, including prior-output cleanup |
| Single-job / 0103 | 16.714 | Same scope |
| Single-job / 0104 | 16.764 | Same scope |
| Single-job / 0105 | 17.222 | Same scope |
| Single-job / 0108 | 16.759 | Same scope |

The recorded single-job times total 84.012 seconds across five days, averaging approximately 16.802 seconds per day. Timing starts when the original benchmark script clears the previous output and includes Hadoop job submission and execution, export from HDFS to a server-local CSV, and comparison with the reference answer. It excludes raw-data import, compilation, and downloading files from the server to a personal computer. The two-job memory-tuning experiment was a single before-and-after comparison.

See [Experiment Notes](docs/experiments.md) for measurement definitions and evidence sources, and [results/accuracy](results/accuracy) for the five-day validation records. Single-job timings come from recorded terminal output. The validation logs contain rescoring of archived results, not complete Hadoop execution logs.

## Repository Structure

```text
src/main/java/org/example/     Single-job, two-job, and shared Writable classes
scripts/                      Build, single-job timing, and validation scripts
tests/                        Validator regression tests using synthetic data
results/accuracy/             Validation records for five days of archived results
results/benchmark_summary.csv Recorded server timings
docs/experiments.md            Experiment setup and measurement definitions
```
