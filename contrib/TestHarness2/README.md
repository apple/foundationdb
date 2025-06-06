# FoundationDB TestHarness2

This directory contains TestHarness2, a Python-based test harness for FoundationDB, designed to be invoked by the Joshua testing framework via scripts like `correctnessTest.sh`. In typical FoundationDB testing setups orchestrated by Joshua, this harness and the tests it runs are executed within Kubernetes pods.

## Key Features
*   Parses FoundationDB trace event logs (`trace.*.xml` or `trace.*.json`).
*   Generates summary XML (`joshua.xml`) compatible with Joshua's expectations.
*   Supports configuration via command-line arguments and environment variables.
*   Includes an optional feature for preserving detailed logs on test failure to aid in debugging.

## TestHarness2 Operation and Outputs

Understanding how TestHarness2 operates and where it stores its output is essential for interpreting test results and debugging issues.

### Unified Output Directory

For each invocation, TestHarness2 (via its `correctnessTest.sh` wrapper) creates a single, consolidated output directory. This makes all artifacts from a single run easy to find.

*   **Location:** The base location defaults to `/tmp` but can be controlled by the `TH_OUTPUT_DIR` environment variable.
*   **Naming Convention:** The directory is named `th_run_<seed>`, where `<seed>` is the unique Joshua seed for the run (e.g., `/tmp/th_run_6709478271895344724`).

### Directory Structure

Inside each `th_run_<seed>` directory, you will find a standardized structure:

*   `joshua_output/`:
    *   **`joshua.xml`**: A comprehensive XML file containing detailed results and parsed events from all test parts. This is the most important file for a detailed analysis of the run.
    *   **`app_log.txt`**: The main log file for the Python test harness application itself. Check this file first to debug issues with the harness, such as configuration errors or crashes.
    *   Other summary files like `stats.json` or `run_times.json` if configured.

*   `run_files/`:
    *   This directory contains a subdirectory for each individual test part that was executed.
    *   Each per-test-part subdirectory contains:
        *   `logs/`: The raw FoundationDB trace event logs (`trace.*.json`).
        *   `command.txt`: The exact `fdbserver` command used for that test part.
        *   `stdout.txt` / `stderr.txt`: The raw standard output/error from the `fdbserver` process for that part.

### V1 Compatibility vs. Archival Mode

TestHarnessV2 has two primary modes of operation, controlled by the `TH_ARCHIVE_LOGS_ON_FAILURE` environment variable.

#### Default Behavior (`TH_ARCHIVE_LOGS_ON_FAILURE` is unset or `false`)

*   **V1 `stdout` Emulation:** For every test part (both success and failure), a single-line XML summary is printed to standard output. This is captured by Joshua and serves as the primary, persistent record of the test outcome.
*   **Cleanup:** The entire `th_run_<seed>` directory is **deleted** after the run completes, regardless of success or failure.

#### Archival Mode (`TH_ARCHIVE_LOGS_ON_FAILURE=true`)

This mode is designed to help debug failures by preserving all detailed logs and linking them directly from the summary.

*   **V1 `stdout` Emulation:** The harness continues to print the single-line XML summary to `stdout` for every test part, just like in the default mode.
*   **Log Referencing on Failure:** If a test part fails, special `<FDBClusterLogDir>`, `<HarnessLogFile>`, and other reference tags are injected into that test part's summary within the main `joshua_output/joshua.xml` file. These tags contain the absolute paths to the preserved log files and directories.
*   **Conditional Cleanup:**
    *   If the test run is **successful**, the `th_run_<seed>` directory is **deleted**.
    *   If the test run **fails**, the entire `th_run_<seed>` directory is **preserved**, allowing you to inspect all the artifacts and follow the paths referenced in the `joshua.xml`.

**Example of enabling archival mode:**
```bash
joshua start --env TH_ARCHIVE_LOGS_ON_FAILURE=true --tarball /path/to/your/test.tar.gz
```

### Summary of Outputs and Preservation:

*   **Joshua `stdout` (Always):**
    *   Contains the official single-line XML summaries for each test part. This is the "V1 compatible" output.
*   **`/tmp/th_run_<seed>/` (Or `$TH_OUTPUT_DIR/th_run_<seed>/`):**
    *   Contains all detailed artifacts: FDB traces, `joshua.xml`, `app_log.txt`, etc.
    *   **Default Mode:** Deleted after every run.
    *   **Archival Mode:** Preserved **only if** the run fails. Deleted on success.