#!/bin/sh

# Simulation currently has memory leaks. We need to investigate before we can enable leak detection in joshua.
export ASAN_OPTIONS="detect_leaks=0"

OLDBINDIR="${OLDBINDIR:-/app/deploy/global_data/oldBinaries}"
#mono bin/TestHarness.exe joshua-run "${OLDBINDIR}" false

# Capture output to check if TestHarness produces anything
OUTPUT_FILE="/tmp/th_output_${JOSHUA_SEED}.txt"
python3 -m test_harness.app -s ${JOSHUA_SEED} --old-binaries-path ${OLDBINDIR} --long-running 2>&1 | tee "${OUTPUT_FILE}"
PYTHON_EXIT_CODE=$?

# If no output was produced, generate fallback XML
if [ ! -s "${OUTPUT_FILE}" ]; then
    echo "WARNING: TestHarness2 produced no output - generating fallback XML" >&2
    echo "<Test TestFile=\"UNKNOWN\" RandomSeed=\"UNKNOWN\" BuggifyEnabled=\"UNKNOWN\" FaultInjectionEnabled=\"UNKNOWN\" JoshuaSeed=\"${JOSHUA_SEED}\" Ok=\"0\" CrashReason=\"TestHarnessProducedNoOutput\" PythonExitCode=\"${PYTHON_EXIT_CODE}\"><JoshuaMessage Severity=\"40\" Message=\"TestHarness2 crashed or timed out before producing any output.\"/></Test>"
fi

rm -f "${OUTPUT_FILE}"

exit ${PYTHON_EXIT_CODE}
