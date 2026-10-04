#!/usr/bin/env bash
set -uo pipefail

if [[ $# -lt 3 || $# -gt 6 ]]; then
  echo "Usage: $0 <jar> <one-day-hdfs-input> <hdfs-output> [local-csv] [standard-csv] [max-split-mb]" >&2
  echo "Example: $0 target/Project111-1.0-SNAPSHOT.jar /factor/input/0104 /factor/output_single/0104 mine_single/0104.csv std/0104.csv 64" >&2
  exit 2
fi

JAR_PATH=$1
INPUT_PATH=$2
OUTPUT_PATH=$3
LOCAL_CSV=${4:-}
STANDARD_CSV=${5:-}
MAX_SPLIT_MB=${6:-64}
MAIN_CLASS=org.example.OptimizedSingleJobFactorDriver
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

if [[ ! -f "$JAR_PATH" ]]; then
  echo "JAR not found: $JAR_PATH" >&2
  exit 2
fi

if [[ -n "$STANDARD_CSV" && -z "$LOCAL_CSV" ]]; then
  echo "A local output CSV is required when a standard CSV is supplied." >&2
  exit 2
fi

if [[ -n "$STANDARD_CSV" && ! -f "$STANDARD_CSV" ]]; then
  echo "Standard CSV not found: $STANDARD_CSV" >&2
  exit 2
fi

if ! [[ "$MAX_SPLIT_MB" =~ ^[0-9]+$ ]] || [[ "$MAX_SPLIT_MB" -lt 1 || "$MAX_SPLIT_MB" -gt 2048 ]]; then
  echo "max-split-mb must be an integer in [1, 2048]: $MAX_SPLIT_MB" >&2
  exit 2
fi

# 发布版不自动删除旧结果；为每次测试传入唯一的输出路径。
if hdfs dfs -test -e "$OUTPUT_PATH"; then
  echo "Output already exists; choose a new path: $OUTPUT_PATH" >&2
  exit 2
fi
if [[ -n "$LOCAL_CSV" && -e "$LOCAL_CSV" ]]; then
  echo "Local CSV already exists; choose a new path: $LOCAL_CSV" >&2
  exit 2
fi

total_start_ns=$(date +%s%N)

job_start_ns=$(date +%s%N)
echo "Starting single MapReduce job at $(date '+%F %T')"

/usr/bin/time -f $'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kb=%M' \
  hadoop jar "$JAR_PATH" "$MAIN_CLASS" "$INPUT_PATH" "$OUTPUT_PATH" "$MAX_SPLIT_MB"
job_status=$?

job_end_ns=$(date +%s%N)

if [[ $job_status -ne 0 ]]; then
  echo "Job failed with exit code $job_status" >&2
  exit "$job_status"
fi

if [[ -n "$LOCAL_CSV" ]]; then
  mkdir -p "$(dirname "$LOCAL_CSV")" || exit 1
  if ! hdfs dfs -getmerge "$OUTPUT_PATH/part-r-*" "$LOCAL_CSV"; then
    echo "CSV export failed." >&2
    exit 1
  fi
  echo "Merged CSV: $LOCAL_CSV"
fi

total_end_ns=$(date +%s%N)

awk -v start="$job_start_ns" -v end="$job_end_ns" \
  'BEGIN { printf "job_elapsed_seconds=%.3f\n", (end-start)/1000000000 }'
awk -v start="$total_start_ns" -v end="$total_end_ns" \
  'BEGIN { printf "total_elapsed_seconds=%.3f\n", (end-start)/1000000000 }'

if [[ -n "$STANDARD_CSV" ]]; then
  echo
  echo "Comparing output with standard answer: $STANDARD_CSV"
  validation_start_ns=$(date +%s%N)
  python3 "$SCRIPT_DIR/score_single_job.py" "$STANDARD_CSV" "$LOCAL_CSV"
  score_status=$?
  validation_end_ns=$(date +%s%N)
  awk -v start="$validation_start_ns" -v end="$validation_end_ns" \
    'BEGIN { printf "validation_elapsed_seconds=%.3f\n", (end-start)/1000000000 }'
  pipeline_end_ns=$validation_end_ns
  awk -v start="$total_start_ns" -v end="$pipeline_end_ns" \
    'BEGIN { printf "pipeline_elapsed_seconds=%.3f\n", (end-start)/1000000000 }'
  if [[ $score_status -ne 0 ]]; then
    echo "Accuracy check failed with exit code $score_status" >&2
    exit "$score_status"
  fi
fi

echo "Finished successfully at $(date '+%F %T')"
