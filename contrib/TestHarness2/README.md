# FoundationDB TestHarness2

This directory contains TestHarness2, a Python-based test harness for FoundationDB (that supercedes [`../TestHarness`](../TestHarness)), designed to be invoked by the Joshua testing framework via scripts like [`../Joshua/scripts/correctnessTest.sh`](../Joshua/scripts/correctnessTest.sh). In typical FoundationDB testing setups orchestrated by Joshua, this harness and the tests it runs are executed within Kubernetes pods.

## TestHarness2 Operation and Outputs

TestHarness2 outputs individual test results directly to stdout in XML format. Each test that runs produces a single XML summary that is immediately printed to stdout. Joshua consumes these XML outputs from stdout to track test results.

Here is an example of how to run the test harness standalone:
```
cd ${FOUNDATIONDB_SRC_DIR}/contrib/TestHarness2
python3 -m test_harness.app --run-temp-dir /tmp/fdb-test-run --binary ~/build_output/bin/fdbserver --test-source-dir ../../tests/ -s 54321 --no-clean-up
```
The above will choose a test to run at random and then output result as XML. We've passed the `--no-clean-up` so take a look at what is leftover under `/tmp/fdb-test-run`.

Pass `-h` to see usage/options.

Set environment variables when you start joshua and [`correctnessTest.sh`](../Joshua/scripts/correctnessTest.sh)) 
will set options the corresponding `test_harness.app` options on invocation.

### Test Execution Directory Structure

TestHarness2 uses directories typically created by the calling script, [`correctnessTest.sh`](../Joshua/scripts/correctnessTest.sh).

*   **Main Directory:** `config.run_temp_dir` which defaults as `th_run_<ENSEMBLE_ID>`
    *   **`app_log.txt`**: The main log file for the Python test harness application itself. Check this file first to debug issues with the harness, such as configuration errors or crashes.

*   **Individual Test Directories:** `config.run_temp_dir/<test_uid>/`
    *   Each individual test creates its own subdirectory named with the test's UUID.
    *   Each test subdirectory contains:
        *   Raw FoundationDB trace event logs (`trace.*.json` or `trace.*.xml`).
        *   `simfdb/`: Simulation database files (if running simulation tests).
        *   `valgrind-<seed>.xml`: Valgrind output files (if running with valgrind).
        *   Other test artifacts generated during execution.

### Cleanup Behavior

By default, TestHarness2 cleans up individual test directories after each test run. The cleanup behavior is controlled by:

- **`--no-clean-up` flag**: Prevents cleanup of all test directories
- **`config.clean_up`**: When `True` (default), test directories are removed after completion

## Joshua LogTool Integration

TestHarness2 integrates with [`../joshua_logtool.py`](../joshua_logtool.py) to automatically upload trace logs to a FoundationDB cluster for long-term storage and analysis when test failures occur.

### How `joshua_logtool.py` Works

The [`joshua_logtool.py`](../joshua_logtool.py) script provides three main functions:
- **Upload**: Stores trace logs and metadata in a FoundationDB cluster using a structured key-value format
- **List**: Shows available log uploads for all tests in an ensemble
- **Download**: Retrieves previously uploaded logs for analysis

When uploading, the tool:
1. Compresses trace log files using tar.xz compression
2. Stores them in the FDB cluster under ensemble-specific subspaces with test UIDs
3. Excludes simfdb directories and core files to reduce upload size
4. Can be configured to upload all logs or only RocksDB-related logs

### Automatic Integration

TestHarness2 automatically invokes [`joshua_logtool.py`](../joshua_logtool.py) to upload logs when **all** of the following conditions are met:
- A test fails (and it's not a negative test expected to fail) OR `TH_FORCE_JOSHUA_LOGTOOL=true`
- `TH_ARCHIVE_LOGS_ON_FAILURE=true` (enables joshua_logtool integration)
- `TH_ENABLE_JOSHUA_LOGTOOL=true` (explicitly enables joshua_logtool)

The individual test XML output includes a `<JoshuaLogTool>` section with status on the logtool run.

### Usage Examples

**Manual joshua_logtool usage:**
```bash
# See the general usage
python3 contrib/joshua_logtool.py -h

# List available uploads for all tests in an ensemble
python3 contrib/joshua_logtool.py list --ensemble-id 20250710-191937-stack-4b0b134a11ad9c5b

# Download logs for a specific test UID
python3 contrib/joshua_logtool.py download --ensemble-id 20250710-191937-stack-4b0b134a11ad9c5b --test-uid 4d345bea-966a-48bf-9041-5031ecffce1d
```

## Environment Variables

TestHarness2 supports environment variables for configuration. There are two mapping systems:

### Environment Variable Mapping

1. **Via correctnessTest.sh**: Shell script maps specific env vars to command line args
2. **Via TestHarness2**: Most variables follow the pattern `TH_<VARIABLE_NAME>` and are read directly by TestHarness2

**Example mappings:**
- `JOSHUA_SEED` → `--joshua-seed` (via correctnessTest.sh)
- `TH_ARCHIVE_LOGS_ON_FAILURE=true` → `--archive-logs-on-failure` (via correctnessTest.sh)
- `TH_CLEAN_UP=false` → `config.clean_up = False` (read directly by TestHarness2)

### Environment Variables

**Core Configuration:**
- **`JOSHUA_SEED`**: Random seed for test execution (required)
- **`TH_RUN_TEMP_DIR`**: Base directory for test execution (required)

**Test Sources:**
- **`JOSHUA_TEST_FILES_DIR`**: Directory containing test files (default: [`tests/`](../../tests/))
- **`OLDBINDIR`**: Path to old FDB binaries directory (for restarting tests)

**Joshua LogTool Integration:**
- **`TH_ARCHIVE_LOGS_ON_FAILURE`**: Enable joshua_logtool integration (`true`/`false`, default: `false`)
- **`TH_ENABLE_JOSHUA_LOGTOOL`**: Enable automatic log uploads (`true`/`false`, default: `false`)
- **`TH_FORCE_JOSHUA_LOGTOOL`**: Force log uploads even for passing tests (`true`/`false`, default: `false`)

**Test Execution Control:**
- **`TH_KILL_SECONDS`**: Timeout for individual tests in seconds (default: `1800`)
- **`TH_BUGGIFY`**: Buggify mode (`on`, `off`, or `random`, default: `random`)
- **`TH_CLEAN_UP`**: Clean up test directories after completion (`true`/`false`, default: `true`)
- **`TH_USE_VALGRIND`**: Run tests under valgrind (`true`/`false`, default: `false`)
- **`TH_LONG_RUNNING`**: Enable long-running test mode (`true`/`false`, default: `false`)

**Optional Configuration:**
- **`JOSHUA_CLUSTER_FILE`**: Path to FDB cluster file (for stats and joshua_logtool)
- **`TH_RANDOM_SEED`**: Force specific random seed for debugging
- **`TH_OUTPUT_FORMAT`**: Output format (`xml` or `json`, default: `xml`)

For a complete list of all variables, run: `python3 -m test_harness.app --help`

## XML Output Format

TestHarness2 produces XML output with the following structure:

```xml
<Test TestUID="..." JoshuaSeed="..." Ok="1" Runtime="..." ...>
  <!-- Test-specific elements like errors, warnings, etc. -->
  <JoshuaLogTool ExitCode="0" Note="..." />
  <!-- Other test artifacts -->
</Test>
```

Key XML attributes:
- `TestUID`: Unique identifier for the test run
- `JoshuaSeed`: Random seed used for the test
- `Ok`: "1" for success, "0" for failure
- `Runtime`: Test execution time in seconds
- `RandomSeed`: Seed passed to fdbserver
- `TestFile`: Path to the test file
- `BuggifyEnabled`: Whether buggify was enabled

## Error Handling

When TestHarness2 encounters fatal errors, it outputs an error XML document to stdout and exits with code 1. The error XML includes:
- Error message and type
- Joshua seed (if available)
- Test UID (if available)