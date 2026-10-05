# Local Release Checks

Check date: October 4, 2026.

- All 17 Java source files matched the pre-packaging local project in file-by-file SHA-256 comparisons. Calculation logic was not rewritten for presentation.
- `scripts/build.sh` compiled the sources and produced a JAR using the existing WSL Ubuntu, JDK 11, and Hadoop 3.3.6 installation.
- All nine synthetic validator regression tests passed, covering timestamp normalization, closing-time coverage, missing rows, extra timestamps, duplicate timestamps, nonfinite values, invalid headers, zero reference values, and the 1% threshold.
- The release validator was run against the five archived single-job CSV files and local reference answers. All five days returned `structure_pass=True`, `all_factors_pass=True`, and `RESULT=PASS`.
- The release files were inspected; no server public IP addresses, GitHub tokens, private keys, or password configurations were found.

These checks did not resubmit Hadoop jobs or produce new performance measurements. The raw dataset, reference answers, complete experiment logs, and full archive remain local and were not added to the repository.

Compared with the historical version, the release timing script refuses to overwrite existing results, propagates export failures, and enables `pipefail`. Historical timings retain their original measurement definitions.
