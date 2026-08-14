/*
 * FileBackupAgentFileFormat.h
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

#include "fdbclient/BackupFileFormat.h"

namespace fileBackup {

struct IRangeFileWriter {
public:
	virtual Future<Void> padEnd(bool final) = 0;

	virtual Future<Void> writeKV(Key k, Value v) = 0;

	virtual Future<Void> writeKey(Key k) = 0;

	virtual Future<Void> finish() = 0;

	virtual ~IRangeFileWriter() = default;
};

struct RangeFileWriter : public IRangeFileWriter {
	explicit RangeFileWriter(Reference<IBackupFile> file = Reference<IBackupFile>(), int blockSize = 0);

	// Handles the first block and internal blocks. Ends current block if needed.
	// The final flag is used in simulation to pad the file's final block to a whole block size.
	static Future<Void> newBlock(RangeFileWriter* self, int bytesNeeded, bool final = false);

	// Used in simulation only to create backup file sizes which are an integer multiple of the block size.
	Future<Void> padEnd(bool final) override;

	// Ends the current block if necessary based on bytesNeeded.
	Future<Void> newBlockIfNeeded(int bytesNeeded);

	// Start a new block if needed, then write the key and value.
	static Future<Void> writeKV_impl(RangeFileWriter* self, Key k, Value v);
	Future<Void> writeKV(Key k, Value v) override;

	// Write begin key or end key.
	static Future<Void> writeKey_impl(RangeFileWriter* self, Key k);
	Future<Void> writeKey(Key k) override;

	Future<Void> finish() override;

	Reference<IBackupFile> file;
	int blockSize;

private:
	int64_t blockEnd;
	uint32_t fileVersion;
	Key lastKey;
	Key lastValue;
};

struct LogFileWriter {
	explicit LogFileWriter(Reference<IBackupFile> file = Reference<IBackupFile>(), int blockSize = 0);

	// Start a new block if needed, then write the key and value.
	static Future<Void> writeKV_impl(LogFileWriter* self, Key k, Value v);
	Future<Void> writeKV(Key k, Value v);

	Reference<IBackupFile> file;
	int blockSize;

private:
	int64_t blockEnd;
};

} // namespace fileBackup
