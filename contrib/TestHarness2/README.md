# FoundationDB TestHarness2

This directory contains TestHarness2, a Python-based test harness for FoundationDB (that supercedes [`../TestHarness`](../TestHarness)). It can be used standalone or invoked by the Joshua testing framework via scripts like [`../Joshua/scripts/correctnessTest.sh`](../Joshua/scripts/correctnessTest.sh).

## Quick Start

Here is an example of how to run the test harness standalone:
```bash
cd ${FOUNDATIONDB_SRC_DIR}/contrib/TestHarness2
python3 -m test_harness.app --run-temp-dir /tmp/fdb-test-run --binary ~/build_output/bin/fdbserver --test-source-dir ../../tests/ -s 54321 --no-clean-up
```

The above will choose a test to run at random and then output result as XML. We've passed the `--no-clean-up` so take a look at what is leftover under `/tmp/fdb-test-run`.

To run a specific test, use the `--include-test-files` option:
```bash
python3 -m test_harness.app --run-temp-dir /tmp/fdb-test-run --binary ~/build_output/bin/fdbserver --test-source-dir ../../tests/ --include-test-files "BulkDumping.toml" -s 54321 --no-clean-up
```

To output results in JSON format instead of XML, use the `--output-format json` option:
```bash
python3 -m test_harness.app --run-temp-dir /tmp/fdb-test-run --binary ~/build_output/bin/fdbserver --test-source-dir ../../tests/ --include-test-files "BulkDumping.toml" --output-format json -s 54321 --no-clean-up
```

To run a test with determinism check enabled (100% probability instead of default 5%), use the `--unseed-check-ratio 1.0` option:
```bash
python3 -m test_harness.app --run-temp-dir /tmp/fdb-test-run --binary ~/build_output/bin/fdbserver --test-source-dir ../../tests/ --include-test-files "BulkDumpingS3.toml" --unseed-check-ratio 1.0 -s 54321 --no-clean-up
```

Pass `-h` to see usage/options:
```bash
python3 -m test_harness.app -h
```

## TestHarness2 Operation and Outputs

TestHarness2 outputs individual test results directly to stdout in XML format (default) or JSON format. Each test that runs produces a single summary that is immediately printed to stdout.

### Test Execution Directory Structure

TestHarness2 uses directories created by a calling script such as [`correctnessTest.sh`](../Joshua/scripts/correctnessTest.sh).

*   **Main Directory:** `config.run_temp_dir` (the `--run-temp-dir` passed on the command line)
    *   **`app_log.txt`**: The main log file for the Python test harness application itself. Check this file first to debug issues with the harness, such as configuration errors or crashes.

*   **Individual Test Directories:** `config.run_temp_dir/<test_uid>/`
    *   Each individual test creates its own subdirectory named with the test's UUID.
    *   Each test subdirectory contains:
        *   Raw FoundationDB trace event logs (`trace.*.json` or `trace.*.xml`).
        *   `simfdb/`: Simulation database files (if running simulation tests).
        *   `valgrind-<seed>.xml`: Valgrind output files (if running with valgrind).
        *   Other test artifacts generated during execution.



## Environment Variables

TestHarness2 supports environment variables for configuration. Most variables follow the pattern `TH_<VARIABLE_NAME>` and are read directly by TestHarness2.

**Example mappings:**
- `TH_RUN_TEMP_DIR=/tmp/test` → `--run-temp-dir /tmp/test` → `config.run_temp_dir = /tmp/test`
- `TH_KILL_SECONDS=3600` → `--kill-seconds 3600` → `config.kill_seconds = 3600`

### Environment Variables

**Core Configuration:**
- **`TH_RUN_TEMP_DIR`**: Base directory for test execution (required)

**Test Sources:**
- **`OLDBINDIR`**: Path to old FDB binaries directory (for restarting tests)



**Test Execution Control:**
- **`TH_KILL_SECONDS`**: Timeout for individual tests in seconds (default: `1800`)
- **`TH_BUGGIFY`**: Buggify mode (`on`, `off`, or `random`, default: `random`)
- **`TH_USE_VALGRIND`**: Run tests under valgrind (`true`/`false`, default: `false`)
- **`TH_LONG_RUNNING`**: Enable long-running test mode (`true`/`false`, default: `false`)

**Optional Configuration:**
- **`TH_RANDOM_SEED`**: Force specific random seed for debugging
- **`TH_OUTPUT_FORMAT`**: Output format (`xml` or `json`, default: `xml`)

For a complete list of all variables, run: `python3 -m test_harness.app --help`

## Output Format

TestHarness2 produces output in XML format (default) or JSON format. The XML output has the following structure:

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

When TestHarness2 encounters fatal errors, it outputs an error document to stdout (in the same format as normal output) and exits with code 1. The error document includes:
- Error message and type
- Joshua seed (if available)
- Test UID (if available)

## Determinism Analysis

TestHarness2 includes built-in support for analyzing determinism check failures. When a determinism check fails, the system preserves trace files from both runs for comparison.

**Note:** Determinism checks are probabilistic (5% chance by default) and only occur for certain test types. Use `--unseed-check-ratio` to control the probability of determinism checks.

### How It Works

1. **Before determinism check**: Initial run trace files are automatically backed up
2. **During determinism check**: The second run proceeds normally, overwriting trace files
3. **After failed determinism check**: Both sets of traces are organized for analysis

### Analysis Files

When determinism checks fail, analysis files are created in:
```
<run_temp_dir>/determinism_analysis_{uid}/
├── initial_run/          # Trace files from first run
├── determinism_check/    # Trace files from second run  
└── README.txt           # Analysis instructions
```

### Usage

To analyze determinism failures:
```bash
python3 contrib/TestHarness2/analyze_determinism_failure.py determinism_analysis_{uid}/initial_run/ determinism_analysis_{uid}/determinism_check/
```

**Note:** The analysis script is generic but includes optimizations for S3 and bulk dumping operations. It can be used as a basis for debugging other determinism failures by modifying the `relevant_types` list in the script to include event types specific to your test.

**Example:** To run BulkDumpingS3.toml with determinism check enabled (100% probability instead of default 5%):
```bash
python3 -m test_harness.app --run-temp-dir /tmp/fdb-test-run --binary ~/build_output/bin/fdbserver --test-source-dir ../../tests/ --include-test-files "BulkDumpingS3.toml" --unseed-check-ratio 1.0 -s 54321 --no-clean-up
```

## Joshua Integration

> **Note**: The following sections are specific to Joshua testing framework integration. If you're using TestHarness2 standalone, you can skip these sections.

### Joshua Context

In typical FoundationDB testing setups orchestrated by Joshua, this harness and the tests it runs are executed within Kubernetes pods. Joshua consumes the XML outputs from stdout to track test results.

TestHarness2 is typically invoked by the [`../Joshua/scripts/correctnessTest.sh`](../Joshua/scripts/correctnessTest.sh) script, which handles test orchestration, cleanup, and integration with the Joshua framework.

Set environment variables when you start joshua, and [`correctnessTest.sh`](../Joshua/scripts/correctnessTest.sh) will map them to the corresponding `test_harness.app` options on invocation.

### Joshua-Specific Environment Variables

**For comprehensive documentation of all Joshua environment variables, including their TestHarness2 command-line option mappings, defaults, and usage examples, see the detailed documentation at the head of [`../Joshua/scripts/correctnessTest.sh`](../Joshua/scripts/correctnessTest.sh).**

### Joshua LogTool Integration

TestHarness2 integrates with [`../joshua_logtool.py`](../joshua_logtool.py) to automatically upload trace logs to a FoundationDB cluster for long-term storage and analysis when test failures occur.

TODO: Integerate joshua_logtool.py into TestHarness2

#### How joshua_logtool.py Works

The [`joshua_logtool.py`](../joshua_logtool.py) script provides three main functions:
- **Upload**: Stores trace logs and metadata in a FoundationDB cluster using a structured key-value format
- **List**: Shows available log uploads for all tests in an ensemble
- **Download**: Retrieves previously uploaded logs for analysis



#### Automatic Integration

TestHarness2 automatically invokes [`joshua_logtool.py`](../joshua_logtool.py) to upload logs when **all** of the following conditions are met:
- A test fails (and it's not a negative test expected to fail) OR `TH_FORCE_JOSHUA_LOGTOOL=true`
- `TH_ARCHIVE_LOGS_ON_FAILURE=true` (enables joshua_logtool integration AND preserves logs on failure)
- `TH_ENABLE_JOSHUA_LOGTOOL=true` (explicitly enables joshua_logtool)

**Note:** `TH_ARCHIVE_LOGS_ON_FAILURE` serves dual purposes - it both preserves logs when tests fail AND enables joshua_logtool integration. This ensures logs are available for upload before cleanup occurs.

**Log Upload Behavior:** When enabled, joshua_logtool uploads logs for all tests regardless of whether they contain RocksDB events or not.

**App Log Inclusion:** When `TH_INCLUDE_APP_LOGS=true` is set, joshua_logtool will also include diagnostic files from the top-level directory:
- `app_log.txt` - Main application log file
- `python_app_stdout.log` - Python stdout log (Joshua integration)
- `python_app_stderr.log` - Python stderr log (Joshua integration)

This is useful for debugging test runner crashes where the test produces minimal output instead of a proper XML report.

The individual test XML output includes a `<JoshuaLogTool>` section with status on the logtool run.

#### Environment Variables

**Required for joshua_logtool integration:**
- `TH_ARCHIVE_LOGS_ON_FAILURE=true` - Enables log archiving and joshua_logtool integration
- `TH_ENABLE_JOSHUA_LOGTOOL=true` - Explicitly enables joshua_logtool functionality

**Optional joshua_logtool settings:**
- `TH_FORCE_JOSHUA_LOGTOOL=true` - Force upload even if test passes (default: false)
- `TH_INCLUDE_APP_LOGS=true` - Include app_log.txt and python_std* files from parent directory (default: false)

#### Usage Examples

**Basic usage (upload RocksDB tests on failure):**
```bash
TH_ARCHIVE_LOGS_ON_FAILURE=true TH_ENABLE_JOSHUA_LOGTOOL=true python3 -m test_harness.app --include-test-files "BulkDumpingS3.toml"
```



**Force upload (even on success):**
```bash
TH_ARCHIVE_LOGS_ON_FAILURE=true TH_ENABLE_JOSHUA_LOGTOOL=true TH_FORCE_JOSHUA_LOGTOOL=true python3 -m test_harness.app --include-test-files "BulkDumpingS3.toml"
```

**Include app logs for debugging crashes:**
```bash
TH_ARCHIVE_LOGS_ON_FAILURE=true TH_ENABLE_JOSHUA_LOGTOOL=true TH_INCLUDE_APP_LOGS=true python3 -m test_harness.app --include-test-files "BulkDumpingS3.toml"
```

**Manual joshua_logtool usage:**
```bash
# See the general usage
python3 contrib/joshua_logtool.py -h

# List available uploads for all tests in an ensemble
python3 contrib/joshua_logtool.py list --ensemble-id 20250710-191937-stack-4b0b134a11ad9c5b

# Download logs for a specific test UID
python3 contrib/joshua_logtool.py download --ensemble-id 20250710-191937-stack-4b0b134a11ad9c5b --test-uid 4d345bea-966a-48bf-9041-5031ecffce1d
```
