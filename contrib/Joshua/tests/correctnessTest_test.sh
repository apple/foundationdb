#!/usr/bin/env bash

set -euo pipefail

test_root=$(mktemp -d)
trap 'rm -rf "${test_root}"' EXIT

mkdir -p "${test_root}/bin"
cat > "${test_root}/bin/python3" <<'FAKE_PYTHON'
#!/usr/bin/env bash

case "${FAKE_HARNESS_MODE}" in
    pass_then_crash)
        echo '<Test Ok="1"/>'
        exit 23
        ;;
    no_output)
        exit 0
        ;;
    pass)
        echo '<Test Ok="1"/>'
        exit 0
        ;;
esac
FAKE_PYTHON
chmod +x "${test_root}/bin/python3"

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../scripts" && pwd)
wrapper="${script_dir}/correctnessTest.sh"

run_case() {
    local mode=$1
    local expected_exit=$2
    local expected_ok=$3
    local output_dir="${test_root}/${mode}"
    local ensemble_id="correctness-test-${mode}"
    local run_dir="${output_dir}/th_run_${ensemble_id}"
    local stdout_file="${output_dir}/stdout.log"
    local status

    mkdir -p "${output_dir}"
    set +e
    PATH="${test_root}/bin:${PATH}" \
        FAKE_HARNESS_MODE="${mode}" \
        JOSHUA_SEED=12345 \
        JOSHUA_ENSEMBLE_ID="${ensemble_id}" \
        TH_OUTPUT_DIR="${output_dir}" \
        TH_ARCHIVE_LOGS_ON_FAILURE=true \
        bash "${wrapper}" > "${stdout_file}" 2> "${output_dir}/stderr.log"
    status=$?
    set -e

    test "${status}" -eq "${expected_exit}"
    grep -q "Ok=\"${expected_ok}\"" "${stdout_file}"
    if [ "${expected_exit}" -ne 0 ]; then
        test -f "${run_dir}/python_app_stdout.log"
        grep -q "Ok=\"${expected_ok}\"" "${run_dir}/python_app_stdout.log"
    else
        test ! -e "${run_dir}"
    fi
}

run_case pass_then_crash 23 1
run_case no_output 1 0
run_case pass 0 1

grep -q 'CrashReason="TestHarnessProducedNoOutput"' "${test_root}/no_output/stdout.log"
test "$(grep -c 'CrashReason="TestHarnessProducedNoOutput"' "${test_root}/no_output/stdout.log")" -eq 1

echo 'correctnessTest wrapper regressions passed'
