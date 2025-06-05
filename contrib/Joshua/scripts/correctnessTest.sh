#!/bin/bash

# Entry point for running FoundationDB correctness tests
# using Python-based TestHarness2 (invoked as `python3 -m test_harness.app`).
# It is designed to be called by the Joshua testing framework.
# For detailed documentation on TestHarness2 features, including log archival,
# see contrib/TestHarness2/README.md.
#
# Key Responsibilities:
# 1. Sets up unique temporary directories for test outputs (`APP_JOSHUA_OUTPUT_DIR`)
#    and runtime artifacts (`APP_RUN_TEMP_DIR`) based on JOSHUA_SEED or a timestamp.
# 2. Gathers necessary environment variables and parameters (e.g., JOSHUA_SEED,
#    OLDBINDIR, JOSHUA_TEST_FILES_DIR) and translates them into command-line
#    arguments for the Python test harness application (`app.py`).
# 3. Executes the Python test harness application, capturing its stdout (expected to be
#    a single XML summary line for Joshua) and stderr.
# 4. Forwards relevant environment variables like `FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY`
#    and `TH_JOB_ID` to the Python application.
# 5. Provides default values for some TestHarness2 arguments if not explicitly passed.
# 6. Conditionally preserves or cleans up the top-level temporary directories
#    (`APP_JOSHUA_OUTPUT_DIR` and `APP_RUN_TEMP_DIR`) based on the Python
#    application's exit code and the `TH_ARCHIVE_LOGS_ON_FAILURE` environment
#    variable. If `TH_ARCHIVE_LOGS_ON_FAILURE` is set to a true-like value
#    (e.g., '1', 'true', 'yes'), these directories are NOT deleted if the Python
#    application exits with a non-zero status, thus preserving all generated
#    artifacts for debugging (copy them local quick using 'kubectl cp podname:/tmp .'
#    before the pod goes away). The Python harness
#    also internally uses this variable to control its own more specific log archival behavior.
# 7. Exits with the same exit code as the Python test harness application.

# Exit on error
set -e

# --- Determine Unique Suffix for all logs and directories for this run ---
DIAG_LOG_DIR="/tmp" # Base directory for logs and temp dirs
mkdir -p "${DIAG_LOG_DIR}" # Ensure it exists

UNIQUE_RUN_SUFFIX=""

# Priority 1: JOSHUA_SEED (if set by the calling environment, e.g., from 4th column of test output)
if [ -n "${JOSHUA_SEED}" ]; then
    # Sanitize common problematic characters for file/directory names, just in case.
    # Though JOSHUA_SEED is typically numeric, this adds a bit of safety.
    TEMP_SUFFIX="${JOSHUA_SEED}"
    TEMP_SUFFIX="${TEMP_SUFFIX//\//_}" # Replace forward slashes
    TEMP_SUFFIX="${TEMP_SUFFIX//:/_}"  # Replace colons
    UNIQUE_RUN_SUFFIX="${TEMP_SUFFIX}"
    echo "Using JOSHUA_SEED ('${JOSHUA_SEED}') directly as suffix: ${UNIQUE_RUN_SUFFIX}" >&2
# Priority 2: Timestamp
else
    RAW_TIMESTAMP_SUFFIX=$(date -Iseconds 2>/dev/null)
    if [ -n "$RAW_TIMESTAMP_SUFFIX" ]; then
        UNIQUE_RUN_SUFFIX="${RAW_TIMESTAMP_SUFFIX//:/_}" # Replace colons
        echo "JOSHUA_SEED not set. Using timestamp for suffix: ${UNIQUE_RUN_SUFFIX}" >&2
    # Priority 3: Fallback to mktemp
    else
        UNIQUE_RUN_SUFFIX="$(mktemp -u XXXXXX)"
        echo "WARNING: JOSHUA_SEED is not set, and 'date -Iseconds' failed. Using random suffix: ${UNIQUE_RUN_SUFFIX}" >&2
    fi
fi

# --- Diagnostic Logging for this script ---
DIAG_LOG_FILE="${DIAG_LOG_DIR}/correctness_test_sh_diag.${UNIQUE_RUN_SUFFIX}.log"

# Redirect all of this script's stderr to the diagnostic log file
# AND ensure the tee'd output also goes to stderr, not stdout.
exec 2> >(tee -a "${DIAG_LOG_FILE}" 1>&2)

# Now that stderr is redirected, log the definitive messages
echo "--- correctnessTest.sh execution started at $(date) --- " >&2
echo "Using UNIQUE_RUN_SUFFIX: ${UNIQUE_RUN_SUFFIX}" >&2
echo "Diagnostic log for this script: ${DIAG_LOG_FILE}" >&2
echo "Script PID: $$" >&2
echo "Running as user: $(whoami)" >&2
echo "Bash version: $BASH_VERSION" >&2
echo "Initial PWD: $(pwd)" >&2
echo "Initial environment variables relevant to TestHarness:" >&2
echo "  JOSHUA_SEED: ${JOSHUA_SEED}" >&2
echo "  OLDBINDIR: ${OLDBINDIR}" >&2
echo "  JOSHUA_TEST_FILES_DIR: ${JOSHUA_TEST_FILES_DIR}" >&2
echo "  FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY: ${FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY}" >&2
echo "  TH_ARCHIVE_LOGS_ON_FAILURE: ${TH_ARCHIVE_LOGS_ON_FAILURE}" >&2
echo "-----------------------------------------------------" >&2

# Simulation currently has memory leaks. We need to investigate before we can enable leak detection in joshua.
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}"
echo "ASAN_OPTIONS set to: ${ASAN_OPTIONS}" >&2

# --- Prepare directories for the Python application ---
# These need to be absolute paths for consistency.

APP_JOSHUA_OUTPUT_DIR="${DIAG_LOG_DIR}/th_joshua_output.${UNIQUE_RUN_SUFFIX}"
mkdir -p "${APP_JOSHUA_OUTPUT_DIR}"

if [ ! -d "${APP_JOSHUA_OUTPUT_DIR}" ]; then
    echo "FATAL: Failed to create APP_JOSHUA_OUTPUT_DIR (path: ${APP_JOSHUA_OUTPUT_DIR})" >&2
    exit 1
fi
chmod 777 "${APP_JOSHUA_OUTPUT_DIR}" # Ensure Python app can write
echo "Created APP_JOSHUA_OUTPUT_DIR: ${APP_JOSHUA_OUTPUT_DIR}" >&2

APP_RUN_TEMP_DIR="${DIAG_LOG_DIR}/th_run_temp.${UNIQUE_RUN_SUFFIX}"
mkdir -p "${APP_RUN_TEMP_DIR}"

if [ ! -d "${APP_RUN_TEMP_DIR}" ]; then
    echo "FATAL: Failed to create APP_RUN_TEMP_DIR (path: ${APP_RUN_TEMP_DIR})" >&2
    exit 1
fi
chmod 777 "${APP_RUN_TEMP_DIR}" # Ensure Python app can write
echo "Created APP_RUN_TEMP_DIR: ${APP_RUN_TEMP_DIR}" >&2


# --- Prepare arguments for the Python application ---
# Default values are mostly handled by the Python app's config.py,
# but we provide what Joshua gives us.

# JOSHUA_SEED is mandatory for the python app
if [ -z "${JOSHUA_SEED}" ]; then
    echo "FATAL: JOSHUA_SEED environment variable is not set." >&2
    # Output a TestHarnessV1-style error XML to stdout for Joshua
    echo '<Test Ok="0" Error="InternalError"><JoshuaMessage Severity="40" Message="FATAL: JOSHUA_SEED environment variable is not set in correctnessTest.sh." /></Test>'
    exit 1
fi

# OLDBINDIR: Default if not set by Joshua
# The Python app's config.py has its own default, but we prefer Joshua's if available.
APP_OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/oldBinaries}" # Default from original script if not set by env
echo "Using OLDBINDIR for Python app: ${APP_OLDBINDIR}" >&2

# JOSHUA_TEST_FILES_DIR: This is the directory containing test definitions (.toml files).
# The python app calls this --test-dir. If not set, Python app will use its default.
APP_TEST_DIR="${JOSHUA_TEST_FILES_DIR}"
if [ -z "${APP_TEST_DIR}" ]; then
    echo "WARNING: JOSHUA_TEST_FILES_DIR environment variable is not set. Python app will use its default test_source_dir (typically 'tests/' relative to CWD)." >&2
    # We allow this to proceed, Python app will handle default or fail if no tests found there.
else
    echo "Using JOSHUA_TEST_FILES_DIR for Python app (--test-source-dir): ${APP_TEST_DIR}" >&2
fi

# Job ID from Joshua, if provided.
APP_JOB_ID="${TH_JOB_ID-}"

PYTHON_EXE="${PYTHON_EXE:-python3}" # Allow overriding the python executable

# Construct Python command arguments
PYTHON_CMD_ARGS=()
PYTHON_CMD_ARGS+=("--joshua-seed" "${JOSHUA_SEED}")
PYTHON_CMD_ARGS+=("--joshua-output-dir" "${APP_JOSHUA_OUTPUT_DIR}")
PYTHON_CMD_ARGS+=("--run-temp-dir" "${APP_RUN_TEMP_DIR}")

# Only pass --test-source-dir if APP_TEST_DIR (from JOSHUA_TEST_FILES_DIR) is set.
if [ -n "${APP_TEST_DIR}" ]; then
    PYTHON_CMD_ARGS+=("--test-source-dir" "${APP_TEST_DIR}")
fi

if [ -n "${APP_OLDBINDIR}" ]; then
    PYTHON_CMD_ARGS+=("--old-binaries-path" "${APP_OLDBINDIR}")
fi

# Forward FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY if set
if [ -n "${FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY}" ]; then
    PYTHON_CMD_ARGS+=("--external-client-library" "${FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY}")
fi

# Forward TH_ARCHIVE_LOGS_ON_FAILURE if set (Python app reads this from env if not on CLI)
# No need to explicitly pass as CLI if app.py handles TH_ARCHIVE_LOGS_ON_FAILURE env var.
# If you wanted to override env with a script default, you could add:
# if [ -n "${TH_ARCHIVE_LOGS_ON_FAILURE}" ]; then
#    PYTHON_CMD_ARGS+=("--archive-logs-on-failure" "${TH_ARCHIVE_LOGS_ON_FAILURE}")
# fi

# Forward TH_JOB_ID if set (Python app reads this from env if not on CLI)
if [ -n "${APP_JOB_ID}" ]; then
    PYTHON_CMD_ARGS+=("--job-id" "${APP_JOB_ID}")
fi

echo "Python app executable: python3 -m test_harness.app" >&2
echo "Python app arguments:" >&2
printf "  %s\n" "${PYTHON_CMD_ARGS[@]}" >&2
echo "-----------------------------------------------------" >&2


# --- Execute the Python Test Harness Application ---
PYTHON_APP_STDOUT_FILE="${APP_RUN_TEMP_DIR}/python_app_stdout.log" # Temporary capture
PYTHON_APP_STDERR_FILE="${APP_RUN_TEMP_DIR}/python_app_stderr.log" # Temporary capture

# Execute python app.
# stdout is redirected to this script's stdout (which goes to Joshua).
# stderr is redirected to this script's diagnostic log file.
echo "Executing Python app..." >&2
set +e # Disable exit on error temporarily to capture exit code
python3 -u -m test_harness.app "${PYTHON_CMD_ARGS[@]}" 1>"${PYTHON_APP_STDOUT_FILE}" 2>"${PYTHON_APP_STDERR_FILE}"
PYTHON_EXIT_CODE=$?
set -e # Re-enable exit on error

echo "Python app execution finished. Exit code: ${PYTHON_EXIT_CODE}" >&2
echo "-----------------------------------------------------" >&2
echo "Python app STDOUT captured in ${PYTHON_APP_STDOUT_FILE}:" >&2 # Note: content of STDOUT file itself is still cat'd to script's stdout
cat "${PYTHON_APP_STDOUT_FILE}" >&2 # Log python stdout to diag log as well for easier debugging
echo "-----------------------------------------------------" >&2
echo "Python app STDERR captured in ${PYTHON_APP_STDERR_FILE} (also tee'd to this script's diag log):" >&2
cat "${PYTHON_APP_STDERR_FILE}" >> "${DIAG_LOG_FILE}" # Append to main diag log
cat "${PYTHON_APP_STDERR_FILE}" # And also print to script's redirected stderr for live view if attached
echo "-----------------------------------------------------" >&2

# Output the Python app's stdout (the single XML line) to this script's stdout
if [ -f "${PYTHON_APP_STDOUT_FILE}" ]; then
    cat "${PYTHON_APP_STDOUT_FILE}"
else
    echo "WARNING: Python app stdout file (${PYTHON_APP_STDOUT_FILE}) not found." >&2
    # Output a fallback XML if Python produced no stdout
    echo '<Test Ok="0" Error="PythonAppNoStdout"><JoshuaMessage Severity="40" Message="Python application produced no stdout file." /></Test>'
fi

# If Python app failed, dump joshua.xml from its output dir to our diag log
if [ "${PYTHON_EXIT_CODE}" -ne 0 ]; then
    echo "Python app exited with error (${PYTHON_EXIT_CODE})." >&2
    JOSHUA_XML_IN_APP_DIR="${APP_JOSHUA_OUTPUT_DIR}/joshua.xml"
    if [ -f "${JOSHUA_XML_IN_APP_DIR}" ]; then
        echo "Contents of ${JOSHUA_XML_IN_APP_DIR} from failed run:" >&2
        echo "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv" >&2
        cat "${JOSHUA_XML_IN_APP_DIR}" >&2 # Send the cat output to stderr to be logged
        echo "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" >&2
    else
        echo "INFO: ${JOSHUA_XML_IN_APP_DIR} not found after failed run." >&2
    fi
fi

# --- Conditional Cleanup ---
echo "--- Evaluating conditional cleanup --- " >&2

# Helper function to check if archiving is considered ON
# (Uses the TH_ARCHIVE_LOGS_ON_FAILURE environment variable)
is_archive_on() {
    if [ -n "$TH_ARCHIVE_LOGS_ON_FAILURE" ]; then
        local val=$(echo "$TH_ARCHIVE_LOGS_ON_FAILURE" | tr '[:upper:]' '[:lower:]')
        if [ "$val" = "1" ] || [ "$val" = "true" ] || [ "$val" = "yes" ]; then
            return 0 # Bash success (true)
        fi
    fi
    return 1 # Bash failure (false)
}

if is_archive_on; then
    echo "Archive mode is ON (TH_ARCHIVE_LOGS_ON_FAILURE is '${TH_ARCHIVE_LOGS_ON_FAILURE}')." >&2
    if [ "${PYTHON_EXIT_CODE}" -eq 0 ]; then
        echo "Python app exited successfully (code 0). Cleaning up directories." >&2
        echo "Cleaning up: ${APP_JOSHUA_OUTPUT_DIR}" >&2
        rm -rf "${APP_JOSHUA_OUTPUT_DIR}"
        echo "Cleaning up: ${APP_RUN_TEMP_DIR}" >&2
        rm -rf "${APP_RUN_TEMP_DIR}"
    else
        echo "Python app exited with error (code ${PYTHON_EXIT_CODE}). ARCHIVE ON: NOT cleaning up APP_JOSHUA_OUTPUT_DIR and APP_RUN_TEMP_DIR for inspection." >&2
        echo "  Logs and joshua.xml retained in: ${APP_JOSHUA_OUTPUT_DIR}" >&2
        echo "  Raw test part artifacts retained in: ${APP_RUN_TEMP_DIR}" >&2
    fi
else
    echo "Archive mode is OFF (TH_ARCHIVE_LOGS_ON_FAILURE is '${TH_ARCHIVE_LOGS_ON_FAILURE}' or unset). Cleaning up directories regardless of Python app exit code." >&2
    echo "Cleaning up: ${APP_JOSHUA_OUTPUT_DIR}" >&2
    rm -rf "${APP_JOSHUA_OUTPUT_DIR}"
    echo "Cleaning up: ${APP_RUN_TEMP_DIR}" >&2
    rm -rf "${APP_RUN_TEMP_DIR}"
fi

echo "--- correctnessTest.sh execution finished at $(date) --- " >&2
exit ${PYTHON_EXIT_CODE}
