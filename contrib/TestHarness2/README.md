# FoundationDB TestHarness2

This directory contains TestHarness2, a Python-based test harness for FoundationDB (that supercedes `../TestHarness`), designed to be invoked by the Joshua testing framework via scripts like `../Joshua/scripts/correctnessTest.sh`. In typical FoundationDB testing setups orchestrated by Joshua, this harness and the tests it runs are executed within Kubernetes pods.

## TestHarness2 Operation and Outputs

### Output Model
TestHarness2 outputs individual test results directly to stdout in XML format. Each test that runs produces a single XML summary that is immediately printed to stdout. Joshua consumes these XML outputs from stdout to track test results.

### Test Execution Directory Structure

TestHarness2 creates directories for test execution under `config.run_temp_dir` (defaults to `/tmp` but can be controlled by the `TH_RUN_TEMP_DIR` environment variable).

*   **Main Directory:** `config.run_temp_dir`
    *   **`app_log.txt`**: The main log file for the Python test harness application itself. Check this file first to debug issues with the harness, such as configuration errors or crashes.

*   **Individual Test Directories:** `config.run_temp_dir/<test_uid>/`
    *   Each individual test creates its own subdirectory named with the test's UUID.
    *   Each test subdirectory contains:
        *   Raw FoundationDB trace event logs (`trace.*.json`).
        *   `simfdb/`: Simulation database files (if running simulation tests).
        *   Other test artifacts generated during execution.

Logs are generally cleared after a test run (unless log archiving is enabled and the test fails -- see below).

## Obtaining the Logs from Test Runs

TestHarness2 supports a log archiving mode designed to help debug failures by preserving all detailed logs and linking them directly from the individual test result summaries. The harness continues to print the individual XML summary to `stdout` for every test part.

**Enable log archiving with:**
```bash
joshua start --env TH_ARCHIVE_LOGS_ON_FAILURE=true --env TH_ENABLE_JOSHUA_LOGTOOL=true --tarball /path/to/your/test.tar.gz
```

Both environment variables need to be set.

### Log Archiving Behavior

*   **Log Referencing on Failure:** If a test part fails, special `<FDBClusterLogDir>`, `<HarnessLogFile>`, and other reference tags are injected into that test part's individual XML summary output. These tags contain the absolute paths to the preserved log files and directories.
*   **Conditional Cleanup:**
    *   If the test run is **successful**, the test execution directories are **deleted**.
    *   If the test run **fails**, the test execution directories are **preserved**, allowing you to inspect all the artifacts and follow the paths referenced in the individual test XML outputs. Copy down the logs if tests were run inside a pod: `kubectl cp POD_NAME:/tmp/ .`.

## Joshua LogTool Integration

TestHarness2 integrates with `../joshua_logtool.py` to automatically upload trace logs to a FoundationDB cluster for long-term storage and analysis when test failures occur. You have to turn on this feature.

### How joshua_logtool.py Works

The `joshua_logtool.py` script provides three main functions:
- **Upload**: Stores trace logs and metadata in a FoundationDB cluster using a structured key-value format. This is usually done by the test runner script if appropriately configured after the test completes.
- **List**: Shows available log uploads for all tests in an ensemble. This is done by the user after the test completes to find the list of what logs are available.
- **Download**: Retrieves previously uploaded logs for analysis.

When uploading, the tool:
1. Compresses trace log files using tar.xz compression
2. Stores them in the FDB cluster under ensemble-specific subspaces with test UIDs
3. Excludes simfdb directories and core files to reduce upload size
4. Can be configured to upload all logs or only RocksDB-related logs

### Automatic Integration

TestHarness2 automatically invokes joshua_logtool.py to upload logs when **all** of the following conditions are met:
- A test fails (and it's not a negative test expected to fail) OR `TH_FORCE_JOSHUA_LOGTOOL=true`
- `TH_ARCHIVE_LOGS_ON_FAILURE=true` (log archiving is enabled)
- `TH_ENABLE_JOSHUA_LOGTOOL=true` (joshua_logtool is explicitly enabled)

The individual test XML output includes a JoshuaLogTool section with status on the logtool run.

### Usage Examples

Here is how to download any logs uploaded to the coordinating fdb instance:

**Manual joshua_logtool usage:**
```bash
# See the general usage (nothing is printed if you do not supply the '-h').
python3 contrib/joshua_logtool.py -h

# List available uploads for all tests in an ensemble.
python3 contrib/joshua_logtool.py list --ensemble-id 20250710-191937-stack-4b0b134a11ad9c5b

# Download logs for a specific test UID.
python3 contrib/joshua_logtool.py download --ensemble-id 20250710-191937-stack-4b0b134a11ad9c5b --test-uid 4d345bea-966a-48bf-9041-5031ecffce1d
```

## Environment Variables

### Core Configuration
- `JOSHUA_SEED`: Random seed for test execution (required)
- `TH_RUN_TEMP_DIR`: Base directory for test execution (required)
- `TH_ARCHIVE_LOGS_ON_FAILURE`: Enable log preservation on failure (`true`/`false`, default: `false`)

### Joshua LogTool Integration
- `TH_ENABLE_JOSHUA_LOGTOOL`: Enable automatic log uploads (`true`/`false`, default: `false`)
- `TH_FORCE_JOSHUA_LOGTOOL`: Force log uploads even for passing tests (`true`/`false`, default: `false`)
- `TH_DISABLE_ROCKSDB_CHECK`: Disable RocksDB filtering for uploads (`true`/`false`, default: `false`)

### Test Configuration
- `JOSHUA_TEST_FILES_DIR`: Directory containing test files (default: auto-detect)
- `JOSHUA_CLUSTER_FILE`: Path to FDB cluster file (optional)
- `JOSHUA_APP_DIR`: Where to write FDB data to (optional)
- `RARE_PRIORITY`: Priority for rare tests (default: 10)

### System Configuration
- `FDB_VALGRIND_DBGPATH`: Extra debug info path for valgrind (optional)
- `ASAN_OPTIONS`: AddressSanitizer options (typically set by test environment)
- `PYTHONPATH`: Python module search path (automatically set by correctnessTest.sh) 
