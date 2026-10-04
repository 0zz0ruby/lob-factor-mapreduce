#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
HADOOP_COMMAND="${HADOOP_HOME:+$HADOOP_HOME/bin/}hadoop"
JAVAC_COMMAND="${JAVA_HOME:+$JAVA_HOME/bin/}javac"
JAR_COMMAND="${JAVA_HOME:+$JAVA_HOME/bin/}jar"

for task_command in "$HADOOP_COMMAND" "$JAVAC_COMMAND" "$JAR_COMMAND"; do
  if ! command -v "$task_command" >/dev/null 2>&1; then
    echo "Required command not found: $task_command" >&2
    exit 2
  fi
done

task_classpath="$("$HADOOP_COMMAND" classpath)"
task_build_dir="$(mktemp -d "$REPO_DIR/.compile-XXXXXX")"
trap 'rmdir "$task_build_dir" 2>/dev/null || true' EXIT
mapfile -d '' task_sources < <(find "$REPO_DIR/src/main/java" -name '*.java' -print0)
if [[ ${#task_sources[@]} -eq 0 ]]; then
  echo "No Java sources found." >&2
  exit 2
fi

"$JAVAC_COMMAND" -encoding UTF-8 --release 11 \
  -cp "$task_classpath" -d "$task_build_dir" "${task_sources[@]}"
mkdir -p "$REPO_DIR/build"
"$JAR_COMMAND" --create --file "$REPO_DIR/build/lob-factor-mapreduce.jar" \
  --main-class org.example.OptimizedSingleJobFactorDriver \
  -C "$task_build_dir" .

# Remove only files generated inside this invocation's private compilation directory.
find "$task_build_dir" -type f -name '*.class' -delete
find "$task_build_dir" -depth -type d -empty -delete
echo "Built: $REPO_DIR/build/lob-factor-mapreduce.jar"

