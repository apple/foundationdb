# FoundationDB TestHarness2

This directory contains TestHarness2, a Python-based test harness for FoundationDB, designed to be invoked by the Joshua testing framework via scripts like `correctnessTest.sh`. In typical FoundationDB testing setups orchestrated by Joshua, this harness and the tests it runs are executed within Kubernetes pods.

## Key Features
*   Parses FoundationDB trace event logs (`trace.*.xml` or `trace.*.json`).
*   Generates summary XML (`joshua.xml`) compatible with Joshua's expectations.
*   Supports configuration via command-line arguments and environment variables.
*   Includes an optional feature for preserving detailed logs on test failure to aid in debugging.

## TestHarness2 Operation and Outputs

Understanding how TestHarness2 operates, what outputs it produces, and where they are stored is essential for interpreting test results and debugging issues.

### Standard Operation and Joshua Integration

1.  **Invocation:** TestHarness2 is typically invoked via a wrapper script, `correctnessTest.sh`, which is called by the Joshua testing framework.
2.  **Execution:** `correctnessTest.sh` sets up the environment and then executes the main Python application (`python3 -m test_harness.app`).
3.  **Core Task:** The `test_harness.app` application runs the specified test(s), monitors their execution, and parses any generated FoundationDB trace event logs.
4.  **Primary Output for Joshua:** Upon completion (or if an early fatal error occurs in `app.py`), the Python application generates a **single-line XML summary**. This XML line encapsulates the overall result (pass/fail) and key attributes of the test invocation.
5.  **Capture by Joshua:** This single XML line is printed to the standard output of `app.py`, which is then relayed by `correctnessTest.sh` to its own standard output. Joshua captures this line.
6.  **Persistent Storage in Joshua Ensembles:** The single-line XML summary captured by Joshua is the **primary, persistent record of the test outcome**. It is stored by Joshua within its standard ensemble directory structure, typically under `/var/joshua/ensembles/YOUR_TEST_RUN_UID/...` (the exact path depends on the Joshua configuration and the specific test run).

**The data in `/var/joshua/ensembles/...` is the source of truth for test results as tracked by the Joshua framework.**

### Detailed Temporary Artifacts (Normally Transient)

During its operation, TestHarness2 (through `correctnessTest.sh` and `app.py`) generates more detailed artifacts and logs than what is sent to Joshua. These are stored in temporary directories, typically created under `/tmp`:

*   **`APP_JOSHUA_OUTPUT_DIR`** (e.g., `/tmp/th_joshua_output.<JOSHUA_SEED_OR_TIMESTAMP>`):
    *   **Full `joshua.xml`:** A comprehensive XML file produced by `app.py`, containing detailed results and parsed events from *all* test parts run within that invocation. This is much more verbose than the single line sent to Joshua.
    *   **Harness Application Logs:** TestHarness2 introduces its own logs for the Python application itself: `python_app_stdout.log` and `python_app_stderr.log`. These capture the direct standard output and error streams of the `test_harness.app` process, which can be invaluable for debugging the harness itself.
    *   Other summary files like `stats.json` or `run_times.json` if configured.

*   **`APP_RUN_TEMP_DIR`** (e.g., `/tmp/th_run_temp.<JOSHUA_SEED_OR_TIMESTAMP>`):
    *   This directory is a parent for per-test-run-part subdirectories (e.g., `.../run.SOME_UUID/`).
    *   Each per-test-part subdirectory contains:
        *   `simfdb/logs/`: The raw FoundationDB trace event logs (`trace.*.xml` or `trace.*.json`).
        *   `command.txt`: The exact command used for that test part.
        *   `stdout.txt` / `stderr.txt`: Raw standard output/error from the `fdbserver` process or test utility for that part.
        *   Other files created by the test itself.

**Default Cleanup Behavior:**

By default, the `correctnessTest.sh` script is designed to **clean up these temporary directories (`APP_JOSHUA_OUTPUT_DIR` and `APP_RUN_TEMP_DIR`) and all their contents upon its completion.** This cleanup occurs **regardless of whether the test succeeded or failed.**

Therefore, while these detailed logs and artifacts (including the new Python application logs in `APP_JOSHUA_OUTPUT_DIR`) are generated during every run, they are normally transient and not available for inspection after the `correctnessTest.sh` script finishes.

### Optional: Preserving Temporary Artifacts for Debugging (`TH_ARCHIVE_LOGS_ON_FAILURE`)

To aid in debugging test failures, TestHarness2 offers an optional mechanism to prevent the default cleanup of the temporary directories if a test fails.

**Environment Variable:** `TH_ARCHIVE_LOGS_ON_FAILURE`

Set this environment variable to a true-like value (e.g., `1`, `true`, `yes`) in the environment where `correctnessTest.sh` is executed. 

For example, when starting a Joshua test run, you can pass it using the `--env` flag:
```bash
joshua start --env TH_ARCHIVE_LOGS_ON_FAILURE=true --tarball /path/to/your/test.tar.gz --max-runs 100 
```

**Behavior When Enabled and a Test Fails:**

*   If `TH_ARCHIVE_LOGS_ON_FAILURE` is set **and** the Python application (`app.py`) exits with a non-zero status (indicating a test failure or harness error), then `correctnessTest.sh` will **not delete** the `APP_JOSHUA_OUTPUT_DIR` and `APP_RUN_TEMP_DIR` directories.
*   This preserves all the detailed artifacts and logs within them (as listed in the "Detailed Temporary Artifacts" section above) for post-mortem analysis.
*   If the test succeeds, cleanup proceeds as normal even if the variable is set.

**Accessing Preserved Artifacts:**

When preserved, these directories can be found at their usual paths (e.g., under `/tmp/`). If running in a containerized environment (like a Kubernetes pod), you will need to copy these directories out of the container before it is terminated. For example:
`kubectl cp <pod-name>:/tmp/th_joshua_output.<SEED_OR_TS> ./th_joshua_output.<SEED_OR_TS> -c <container-name>`

### Summary of Outputs and Preservation:

*   **`/var/joshua/ensembles/...` (Always Persistent):**
    *   Contains the official single-line XML summary from Joshua's perspective.
*   **`/tmp/th_joshua_output...` and `/tmp/th_run_temp...` (Normally Transient):**
    *   Contain detailed FDB trace logs, the full multi-part `joshua.xml`, Python harness logs (`python_app_stdout.log`, `python_app_stderr.log`), and other per-test-part artifacts.
    *   **Default:** Deleted after every `correctnessTest.sh` run (success or failure).
    *   **With `TH_ARCHIVE_LOGS_ON_FAILURE=true`:** Preserved **only if** the test run fails. Deleted on success.

## Other Configuration
(Placeholder for other TestHarness2 specific configurations) 