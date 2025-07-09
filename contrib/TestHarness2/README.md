# FoundationDB TestHarness2

This directory contains TestHarness2, a Python-based test harness for FoundationDB (that supercedes `../TestHarness`), designed to be invoked by the Joshua testing framework via scripts like `../Joshua/scripts/correctnessTest.sh`. In typical FoundationDB testing setups orchestrated by Joshua, this harness and the tests it runs are executed within Kubernetes pods.

## Key Features
*   Parses FoundationDB trace event logs (`trace.*.xml` or `trace.*.json`).
*   Generates summary XML (`joshua.xml`) compatible with Joshua's expectations.
*   Supports configuration via command-line arguments and environment variables.
*   Optionally preserves logs on test failure to aid in debugging.

## TestHarness2 Operation and Outputs

### Unified Output Directory

For each invocation, TestHarness2 (via its `../Joshua/scripts/correctnessTest.sh` wrapper) creates a single, consolidated output directory. This makes all artifacts from a single run easy to find.

*   **Location:** The base location defaults to `/tmp` but can be controlled by the `TH_OUTPUT_DIR` environment variable.
*   **Naming Convention:** The directory is named `th_run_<ENSEMBLE_ID>_<TIMESTAMP>`, where `<ENSEMBLE_ID>` is the unique ensemble identifier and `<TIMESTAMP>` is an ISO8601 timestamp to prevent collisions when rerunning the same ensemble (e.g., `/tmp/th_run_20250706-234137-stack-b6708f02fcced157_20250706T235426Z`).

### Directory Structure

Inside each `th_run_<ENSEMBLE_ID>_<TIMESTAMP>` directory, you will find a standardized structure:

*   `joshua_output/`:
    *   **`joshua.xml`**: A comprehensive XML file containing detailed results and parsed events from all test parts. This is the most important file for a detailed analysis of the run.
    *   **`app_log.txt`**: The main log file for the Python test harness application itself. Check this file first to debug issues with the harness, such as configuration errors or crashes.

*   `run_files/`:
    *   This directory contains a subdirectory for each individual test part that was executed.
    *   Each per-test-part subdirectory contains:
        *   `logs/`: The raw FoundationDB trace event logs (`trace.*.json`).
        *   `command.txt`: The exact `fdbserver` command used for that test part.
        *   `stdout.txt` / `stderr.txt`: The raw standard output/error from the `fdbserver` process for that part.

Logs are generally cleared after a test run.

## Obtaining the Logs from Test Runs

TestHarness2 supports a log archiving mode designed to help debug failures by preserving all detailed logs and linking them directly from the summary. The harness continues to print the single-line XML summary to `stdout` for every test part.

**Enable log archiving with:**
```bash
joshua start --env TH_ARCHIVE_LOGS_ON_FAILURE=true --tarball /path/to/your/test.tar.gz
```

### Log Archiving Behavior

*   **Log Referencing on Failure:** If a test part fails, special `<FDBClusterLogDir>`, `<HarnessLogFile>`, and other reference tags are injected into that test part's summary within the main `joshua_output/joshua.xml` file. These tags contain the absolute paths to the preserved log files and directories.
*   **Conditional Cleanup:**
    *   If the test run is **successful**, the `th_run_<ENSEMBLE_ID>_<TIMESTAMP>` directory is **deleted**.
    *   If the test run **fails**, the entire `th_run_<ENSEMBLE_ID>_<TIMESTAMP>` directory is **preserved**, allowing you to inspect all the artifacts and follow the paths referenced in the `joshua.xml`. Copy down the logs if tests were run inside a pod: `kubectl cp POD_NAME:/tmp/ .`.

## Joshua LogTool Integration

TestHarness2 integrates with `../joshua_logtool.py` to automatically upload trace logs to a FoundationDB cluster for long-term storage and analysis when test failures occur.

### How joshua_logtool.py Works

The `joshua_logtool.py` script provides three main functions:
- **Upload**: Stores trace logs and metadata in a FoundationDB cluster using a structured key-value format
- **Download**: Retrieves previously uploaded logs for analysis
- **List**: Shows available log uploads with filtering options

When uploading, the tool:
1. Compresses trace log files using gzip
2. Stores them in the FDB cluster under keys like `/joshua_logs/<ensemble_id>/<filename>`
3. Creates metadata entries with timestamps, file sizes, and test information
4. Supports filtering modes (default uploads all logs, RocksDB mode uploads only RocksDB-related logs)

### Automatic Integration

TestHarness2 automatically invokes joshua_logtool.py to upload logs when **all** of the following conditions are met:
- A test fails (and it's not a negative test expected to fail)
- `TH_ARCHIVE_LOGS_ON_FAILURE=true` (log archiving is enabled)
- `TH_SKIP_JOSHUA_LOGTOOL=false` (joshua_logtool is not explicitly disabled)
- `TH_ENABLE_JOSHUA_LOGTOOL=true` (joshua_logtool is explicitly enabled)

The XML output has a JoshuaLogTool section with status on the logtool run.

### Usage Examples

Here is how to download any logs uploaded to the coordinating fdb instance:

**Manual joshua_logtool usage:**
```bash
# List available uploads in the coordinating fdb instance.
python3 contrib/joshua_logtool.py list --ensemble-id 6709478271895344724

# Download logs for analysis from the coordinating fdb instance.
python3 contrib/joshua_logtool.py download --ensemble-id 6709478271895344724 --output-dir ./downloaded_logs
```