# S3 Checksumming in FoundationDB

## Overview

FoundationDB implements comprehensive checksum verification for S3 operations to ensure data integrity during upload, download, and backup operations. This document describes the checksumming strategies and implementation details.

## Architecture

### Three-Layer S3 Implementation

FoundationDB has a layered S3 implementation:

1. **S3BlobStore** (Foundation): Low-level S3 REST API implementation
   - Handles basic S3 operations (PUT, GET, multipart upload/completion)
   - Implements upload integrity checking: MD5 (legacy) or SHA256 (when `enable_object_integrity_check` is enabled)
   - Sends appropriate headers (`Content-MD5` or `x-amz-checksum-sha256`) for S3 server-side verification

2. **AsyncFileS3BlobStore** (Backup Layer): `IAsyncFile` interface wrapper
   - **Uses S3BlobStore underneath** for all S3 operations
   - Provides streaming write interface for backup operations
   - Inherits S3BlobStore's upload integrity checking (MD5 or SHA256)
   - **No download verification** - AsyncFileS3BlobStoreRead has no checksum protection

3. **S3Client** (Utility Layer): Command-line and bulk operations
   - **Uses S3BlobStore underneath** via `S3BlobStoreEndpoint`
   - Adds file-level integrity verification using XXH64 checksums
   - Inherits S3BlobStore's part-level upload integrity (MD5 or SHA256)
   - Provides end-to-end file verification for large uploads/downloads

**Checksum Usage by Layer:**

- **S3BlobStore**: MD5 or SHA256 for upload integrity verification with S3
- **AsyncFileS3BlobStore**: Inherits S3BlobStore's upload integrity; no download verification
- **S3Client**: Inherits S3BlobStore's upload integrity + adds XXH64 file-level verification

### Checksum Flow

**SHA256 Flow** (when `enable_object_integrity_check` is enabled):
```
Upload Flow:
1. Calculate SHA256 checksum of data
2. Send x-amz-checksum-algorithm: SHA256 header
3. Send x-amz-checksum-sha256: <base64-encoded-checksum> header
4. AWS S3 verifies checksum server-side
5. For multipart uploads: Include ChecksumSHA256 in completion XML

Download Flow:
1. Small files: Use x-amz-checksum-mode: ENABLED for S3 verification
2. Large files: Use custom XXH64 checksums stored in tags/companion files
3. Range requests: Cannot use S3 checksums (AWS limitation)
```

**MD5 Flow** (default, when `enable_object_integrity_check` is disabled):
```
Upload Flow:
1. Calculate MD5 checksum of data
2. Send Content-MD5: <base64-encoded-checksum> header
3. AWS S3 verifies checksum server-side
4. For multipart uploads: Include MD5 in completion XML

Download Flow:
1. Small files: ETag comparison (limited protection)
2. Large files: Use custom XXH64 checksums stored in tags/companion files
3. Range requests: No checksum verification available
```

## Implementation Details

## Download Strategies and Limitations

### Three Download Approaches

1. **AsyncFileS3BlobStoreRead::read** (Range requests)
   - ❌ **No S3 transport-level checksum verification** - AWS limitation with range requests
   - ✅ **Application-level integrity checks** - backup file format validation catches corruption
   - Used by backup/restore operations

2. **S3BlobStoreEndpoint::readEntireFile** (Small files)
   - ✅ **Full S3 SHA256 verification** using `x-amz-checksum-mode: ENABLED`
   - Optimal for small files where full download is efficient

3. **S3Client::copyDownFile** (Large files)
   - ✅ **Custom XXH64 verification** after complete download
   - Uses parallel range-based downloads for performance
   - Verifies overall file integrity using stored checksums

### Why Range Requests Can't Use S3 Checksums

**AWS S3 Design**: The `x-amz-checksum-mode: ENABLED` header only works for full object downloads because:
- S3 checksums are calculated for entire objects, not arbitrary byte ranges
- AWS cannot verify partial content against full-object checksums
- Range requests (e.g., `Range: bytes=0-1023`) inherently cannot support this

**Integrity Protection**: While `AsyncFileS3BlobStoreRead::read()` cannot use S3 transport-level checksums, the backup/restore system provides integrity protection through:

1. **Application-level integrity checks** in backup file formats:
   - `decodeRangeFileBlock()` validates file headers and versions
   - Padding validation ensures remaining bytes are 0xFF
   - Throws specific errors: `restore_corrupted_data()`, `restore_corrupted_data_padding()`, `restore_unsupported_file_version()`

2. **Full-file checksum verification** for small files:
   - `S3BlobStoreEndpoint::readEntireFile()` uses `x-amz-checksum-mode: ENABLED`
   - Verifies SHA256 checksums when `enable_object_integrity_check` is enabled
   - Throws `checksum_failed()` if verification fails

3. **Protocol version validation** and encryption header validation for encrypted backups

4. **Error detection and retry logic** for different failure modes

### Multipart Completion Checksums

**Key Insight**: The `ChecksumSHA256` returned in multipart completion response is a **composite checksum**:
- It's a checksum calculated from the individual part checksums (not metadata)
- It **is** useful for content verification, though calculated differently than a full-object checksum
- It represents the actual object content through aggregated part-level checksums

## Configuration

### Default Settings

```cpp
// Enabled by default for stronger security
init(BLOBSTORE_ENABLE_OBJECT_INTEGRITY_CHECK, true);
```

### Performance Considerations

- **SHA256 vs MD5**: Slightly higher CPU cost, significantly better security
- **Network overhead**: Minimal (just additional headers)
- **Compatibility**: Fully backward compatible when disabled

## Error Handling

### Common Errors

1. **Missing Checksum in Completion**: 
   ```
   InvalidRequest: The complete request must include the checksum for each part
   ```
   **Solution**: Include `<ChecksumSHA256>` tags in completion XML

2. **Checksum Mismatch**:
   ```
   InvalidDigest: The Content-MD5 you specified was invalid
   ```
   **Solution**: Verify checksum calculation and encoding

3. **Heap Corruption**: 
   - **Cause**: Complex string manipulation in actor contexts
   - **Solution**: Use simpler approaches, avoid concatenation/parsing

## Testing

### Verification Steps

1. **Upload verification**: Check for `x-amz-checksum-sha256` in part upload responses
2. **Completion verification**: Ensure completion XML includes `<ChecksumSHA256>` tags
3. **Download verification**: Verify checksum headers in download responses
4. **End-to-end testing**: Upload with checksums, download and verify integrity

## Key Insights and Lessons Learned

### 1. AWS S3 Multipart Upload Requirements
- Must be all-or-nothing: if initiation includes checksum algorithm, completion must include all part checksums
- Cannot mix checksummed and non-checksummed parts in same upload

### 2. Performance vs Security Trade-offs
- Range requests more efficient for large files but cannot use S3 checksums
- Full downloads less efficient but enable S3 checksum verification
- Custom XXH64 checksums provide good compromise for large files

### 3. AWS S3 Checksum Limitations
- Multipart completion checksums are structural, not content-based
- Range requests fundamentally incompatible with S3 checksum verification
- These are AWS limitations, not implementation issues

## References

- [AWS S3 Object Integrity Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html)
- [AWS S3 Multipart Upload API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
- [AWS S3 GetObject Checksum Mode](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax) 