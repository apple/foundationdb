# Concurrent S3Client Test Script

## Overview

The `concurrent_s3client_test.sh` script is designed to test the robustness of the s3client_test.sh script when multiple instances are run concurrently. It specifically tests:

1. **Port conflict handling** - Ensures that when multiple MockS3Server instances try to bind to the same port, the retry logic works correctly
2. **Process cleanup** - Verifies that all spawned processes are properly cleaned up, preventing orphaned MockS3Server processes
3. **Concurrent execution** - Tests that multiple s3client_test.sh instances can run simultaneously without interfering with each other
4. **Signal handling** - Ensures proper cleanup when tests are interrupted

## Usage

```bash
./concurrent_s3client_test.sh <build_dir> [max_concurrent] [test_duration]
```

### Parameters

- `build_dir` (required): Path to your FoundationDB build output directory (e.g., `~/build_output`)
- `max_concurrent` (optional): Maximum number of concurrent test instances to run (default: 5)
- `test_duration` (optional): Maximum time in seconds to wait for tests to complete (default: 60)

### Examples

```bash
# Basic usage with default settings (5 concurrent tests, 60s timeout)
./concurrent_s3client_test.sh ~/build_output

# Run 10 concurrent tests with 2-minute timeout
./concurrent_s3client_test.sh ~/build_output 10 120

# Run 3 concurrent tests with 30-second timeout
./concurrent_s3client_test.sh ~/build_output 3 30
```

## Test Scenarios

The script runs three main test scenarios:

### 1. Basic Concurrent Execution
- Starts multiple s3client_test.sh instances with staggered delays
- Tests normal concurrent operation
- Verifies all instances complete successfully

### 2. Port Occupation Simulation
- Pre-occupies some ports in the range that MockS3Server uses (8080-8090)
- Starts test instances that must find alternative ports
- Verifies the port retry logic works correctly

### 3. Signal Handling Test
- Starts test instances and then sends SIGTERM signals
- Verifies that cleanup happens properly when tests are interrupted
- Checks for orphaned processes

## What It Tests

### Port Conflict Resolution
The script verifies that the [`mocks3_fixture.sh`](mocks3_fixture.sh:75-141) port retry logic works correctly:
- When port 8080 is occupied, MockS3Server should try 8081, 8082, etc.
- Multiple instances should be able to find available ports
- Port conflicts should not cause test failures

### Process Cleanup
The script monitors for:
- All MockS3Server processes are terminated when tests complete
- No orphaned processes remain after test completion
- Proper cleanup happens even when tests are killed with signals

### Concurrent Safety
The script tests:
- Multiple s3client_test.sh instances can run simultaneously
- Tests don't interfere with each other's scratch directories
- Log files are properly isolated per instance

## Output and Reporting

The script provides:

### Real-time Logging
- Timestamped log messages showing test progress
- Port usage monitoring
- Process lifecycle tracking

### Final Report
- Summary of all test results
- Count of tests started, completed, failed, and killed
- Number of port conflicts detected
- Detection of any orphaned processes

### Detailed Report File
A detailed report is written to `/tmp/concurrent_s3client_test_report.txt` containing:
- Individual test instance details
- Port usage analysis
- Log file locations and sizes
- Complete test timeline

## Exit Codes

- `0`: All tests passed successfully with proper cleanup
- `1`: Some tests completed successfully but with issues
- `2`: Tests failed or cleanup problems detected

## Requirements

The script requires:
- Bash 4.0 or later
- Standard Unix utilities (`netstat` or `ss`, `pgrep`, `pkill`)
- The s3client_test.sh script must be executable
- A valid FoundationDB build directory with fdbserver binary

## Port Range

The script uses ports 8080-8090 by default (configurable via `BASE_PORT` and `MAX_PORT_RETRIES` constants).

## Troubleshooting

### "Port already in use" errors
This is expected behavior when testing port conflict resolution. The script should handle these automatically.

### Orphaned processes
If the script detects orphaned MockS3Server processes, it will:
1. Report them in the logs
2. Attempt to kill them automatically
3. Mark the test as failed

### Permission issues
Ensure the script has execute permissions:
```bash
chmod +x concurrent_s3client_test.sh
```

### Missing dependencies
The script will fail if required binaries are missing. Ensure:
- `fdbserver` exists in `<build_dir>/bin/fdbserver`
- Standard Unix utilities are available
- The system supports process monitoring commands

## Integration with CI/CD

This script can be integrated into continuous integration pipelines to:
- Verify port handling robustness under load
- Catch process cleanup regressions
- Test concurrent execution scenarios
- Validate signal handling behavior

Example CI usage:
```bash
# Run quick concurrent test
./concurrent_s3client_test.sh "$BUILD_DIR" 3 30

# Run stress test
./concurrent_s3client_test.sh "$BUILD_DIR" 10 180