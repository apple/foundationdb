/*
 * BackupFileFormat.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "fdbclient/BackupContainer.h"

// Helper class for reading restore data from a buffer and throwing the right errors.
struct StringRefReader {
	explicit StringRefReader(StringRef s = StringRef(), Error e = Error())
	  : rptr(s.begin()), end(s.end()), failure_error(e) {}

	// Return remainder of data as a StringRef
	StringRef remainder() { return StringRef(rptr, end - rptr); }

	// Return a pointer to len bytes at the current read position and advance read pos
	const uint8_t* consume(unsigned int len) {
		if (rptr == end && len != 0)
			throw end_of_stream();
		const uint8_t* p = rptr;
		rptr += len;
		if (rptr > end)
			throw failure_error;
		return p;
	}

	// Return a T from the current read position and advance read pos
	template <typename T>
	const T consume() {
		return *(const T*)consume(sizeof(T));
	}

	// Functions for consuming big endian (network byte order) integers.
	// Consumes a big endian number, swaps it to little endian, and returns it.
	int32_t consumeNetworkInt32() { return (int32_t)bigEndian32((uint32_t)consume<int32_t>()); }
	uint32_t consumeNetworkUInt32() { return bigEndian32(consume<uint32_t>()); }

	// Convert big Endian value (e.g., encoded in log file) into a littleEndian uint64_t value.
	int64_t consumeNetworkInt64() { return (int64_t)bigEndian64((uint32_t)consume<int64_t>()); }
	uint64_t consumeNetworkUInt64() { return bigEndian64(consume<uint64_t>()); }

	bool eof() { return rptr == end; }

	const uint8_t *rptr, *end;
	Error failure_error;
};

namespace fileBackup {

Standalone<VectorRef<KeyValueRef>> decodeRangeFileBlock(const Standalone<StringRef>& buf);

Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file,
                                                                int64_t offset,
                                                                int len,
                                                                Database cx);

Standalone<VectorRef<KeyValueRef>> decodeMutationLogFileBlock(const Standalone<StringRef>& buf);

// Reads a mutation log block from file and parses into batch mutation blocks for further parsing.
Future<Standalone<VectorRef<KeyValueRef>>> decodeMutationLogFileBlock(Reference<IAsyncFile> file,
                                                                      int64_t offset,
                                                                      int len);

// Return a block of contiguous padding bytes "\0xff" for backup files, growing if needed.
Value makePadding(int size);

} // namespace fileBackup

void simulateBlobFailure();
