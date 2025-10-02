# MockS3Server for FoundationDB Tests

This document describes MockS3Server as the default S3-compatible server for FoundationDB's S3-related tests.

## Overview

MockS3Server is a lightweight, in-process S3-compatible server built into FoundationDB that provides:
- Full S3 API compatibility (GET/PUT/DELETE/HEAD, multipart uploads, object tagging, ListObjects)
- Deterministic behavior for reliable testing
- No external dependencies or binary downloads
- Instant startup (no 25+ second SeaweedFS startup delay)
- In-memory storage with automatic cleanup

## Affected Tests

Three tests now use MockS3Server as the default fallback when real S3 is not available:

1. **s3client_test.sh** - S3 client functionality tests
2. **bulkload_test.sh** - Bulk dump and load operations
3. **s3_backup_test.sh** - Backup and restore operations

## Usage

### Default behavior

Tests now use MockS3Server by default when real S3 is not available:

```bash
# Uses real S3 if available, otherwise MockS3Server
./s3client_test.sh <build_dir>
./bulkload_test.sh <source_dir> <build_dir>
./s3_backup_test.sh <source_dir> <build_dir>

# Force real S3 usage
USE_S3=true ./s3client_test.sh <build_dir>
```

### Server Priority

The tests follow this priority order:
1. **Real S3** (when `USE_S3=true` or `OKTETO_NAMESPACE` is set)
2. **MockS3Server** (default fallback)

## Implementation Details

### MockS3Server Extensions

- **Header**: `fdbserver/include/fdbserver/MockS3Server.h`
  - Added `startMockS3ServerReal()` function for real HTTP mode
  
- **Implementation**: `fdbserver/MockS3Server.actor.cpp`  
  - Extended MockS3Server to work outside simulation context
  - Uses FoundationDB's existing HTTP server infrastructure

### Test Integration

- **Fixture**: `fdbclient/tests/mocks3_fixture.sh`
  - Provides SeaweedFS-compatible interface for MockS3Server
  - Handles server lifecycle, port allocation, and cleanup
  
- **URL Format**: `blobstore://mocks3:mocksecret@127.0.0.1:8080/container?bucket=test-bucket&region=us-east-1&secure_connection=0`

## Benefits Over SeaweedFS

- **No Download Required**: Built into fdbserver, no external binaries needed
- **Instant Startup**: Ready immediately vs 25+ seconds for SeaweedFS
- **Deterministic**: Consistent behavior across test runs
- **Memory Efficient**: In-process operation, no separate JVM
- **Maintenance Free**: No external dependency updates or compatibility issues

## Benefits

### Reliability Improvements
- ✅ Eliminates external dependency failures
- ✅ Deterministic in-memory behavior  
- ✅ No network timeouts or connection issues

### Performance Benefits  
- ✅ Faster test startup (no external process)
- ✅ Lower resource usage (in-memory storage)
- ✅ Parallel test execution without port conflicts

### Maintenance Benefits
- ✅ Single codebase ownership (no external binaries)
- ✅ Simplified debugging (in-process)
- ✅ No version management or downloads

## Troubleshooting

### Common Issues

**Port already in use**: MockS3Server automatically finds available ports
**Server startup failure**: Check logs in test scratch directory
**S3 compatibility**: MockS3Server implements full S3 API used by FoundationDB

### Debugging

Enable verbose logging:
```bash
USE_MOCKS3=1 HTTP_VERBOSE_LEVEL=10 ./s3client_test.sh <build_dir>
```

### Logs Location
Test logs are written to the scratch directory created by each test:
- S3 server logs: `<scratch_dir>/logs/`
- Test output: Console and test result files

## Migration Timeline

- **Completed**: Core MockS3Server implementation and test integration
- **Next**: CI/CD pipeline integration and validation
- **Future**: Optional SeaweedFS dependency removal

## Support

For issues or questions about MockS3Server integration:
1. Check test logs in scratch directory
2. Verify `USE_MOCKS3=1` environment variable is set
3. Ensure MockS3Server builds correctly with the project
4. Fall back to SeaweedFS if needed by unsetting `USE_MOCKS3`