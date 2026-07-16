/*
 * BackupFileFormat.cpp
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

#include "FileBackupAgentFileFormat.h"

#include <cstring>

#include "fdbrpc/simulator.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"
#include "flow/network.h"

namespace fileBackup {

// Return a block of contiguous padding bytes, growing if needed.
Value makePadding(int size) {
	static Value pad;
	if (pad.size() < size) {
		pad = makeString(size);
		memset(mutateString(pad), '\xff', pad.size());
	}

	return pad.substr(0, size);
}

// File Format handlers.
// Both Range and Log formats are designed to be readable starting at any BACKUP_RANGEFILE_BLOCK_SIZE boundary
// so they can be read in parallel.
//
// Writer instances must be kept alive while any member actors are in progress.
//
// RangeFileWriter must be used as follows:
//   1 - writeKey(key) the queried key range begin
//   2 - writeKV(k, v) each kv pair to restore
//   3 - writeKey(key) the queried key range end
//   4 - finish()
//
// RangeFileWriter will insert the required padding, header, and extra
// end/begin keys around the 1MB boundaries as needed.
//
// Example:
//   The range a-z is queries and returns c-j which covers 3 blocks.
//   The client code writes keys in this sequence:
//             a c d e f g h i j z
//
//   H = header   P = padding   a...z = keys  v = value | = block boundary
//
//   Encoded file:  H a cv dv ev P | H e ev fv gv hv P | H h hv iv jv z
//   Decoded in blocks yields:
//           Block 1: range [a, e) with kv pairs cv, dv
//           Block 2: range [e, h) with kv pairs ev, fv, gv
//           Block 3: range [h, z) with kv pairs hv, iv, jv
//
//   NOTE: All blocks except for the final block will have one last
//   value which will not be used. This isn't actually a waste since
//   if the next KV pair wouldn't fit within the block after the value
//   then the space after the final key to the next 1MB boundary would
//   just be padding anyway.
RangeFileWriter::RangeFileWriter(Reference<IBackupFile> file, int blockSize)
  : file(file), blockSize(blockSize), blockEnd(0), fileVersion(BACKUP_AGENT_SNAPSHOT_FILE_VERSION) {}

Future<Void> RangeFileWriter::newBlock(RangeFileWriter* self, int bytesNeeded, bool final) {
	// Write padding to finish current block if needed
	int bytesLeft = self->blockEnd - self->file->size();
	if (bytesLeft > 0) {
		Value paddingFFs = makePadding(bytesLeft);
		co_await self->file->append(paddingFFs.begin(), bytesLeft);
	}

	if (final) {
		ASSERT(g_network->isSimulated());
		co_return;
	}

	// Set new blockEnd
	self->blockEnd += self->blockSize;

	// write Header
	co_await self->file->append((uint8_t*)&self->fileVersion, sizeof(self->fileVersion));

	// If this is NOT the first block then write duplicate stuff needed from last block
	if (self->blockEnd > self->blockSize) {
		co_await self->file->appendStringRefWithLen(self->lastKey);
		co_await self->file->appendStringRefWithLen(self->lastKey);
		co_await self->file->appendStringRefWithLen(self->lastValue);
	}

	// There must now be room in the current block for bytesNeeded or the block size is too small
	if (self->file->size() + bytesNeeded > self->blockEnd)
		throw backup_bad_block_size();

	co_return;
}

Future<Void> RangeFileWriter::padEnd(bool final) {
	ASSERT(g_network->isSimulated());
	if (file->size() > 0) {
		return newBlock(this, 0, final);
	}
	return Void();
}

Future<Void> RangeFileWriter::newBlockIfNeeded(int bytesNeeded) {
	if (file->size() + bytesNeeded > blockEnd)
		return newBlock(this, bytesNeeded);
	return Void();
}

Future<Void> RangeFileWriter::writeKV_impl(RangeFileWriter* self, Key k, Value v) {
	int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
	co_await self->newBlockIfNeeded(toWrite);
	co_await self->file->appendStringRefWithLen(k);
	co_await self->file->appendStringRefWithLen(v);
	self->lastKey = k;
	self->lastValue = v;
	co_return;
}

Future<Void> RangeFileWriter::writeKV(Key k, Value v) {
	return writeKV_impl(this, k, v);
}

Future<Void> RangeFileWriter::writeKey_impl(RangeFileWriter* self, Key k) {
	int toWrite = sizeof(uint32_t) + k.size();
	co_await self->newBlockIfNeeded(toWrite);
	co_await self->file->appendStringRefWithLen(k);
	co_return;
}

Future<Void> RangeFileWriter::writeKey(Key k) {
	return writeKey_impl(this, k);
}

Future<Void> RangeFileWriter::finish() {
	return Void();
}

namespace {

void decodeKVPairs(StringRefReader* reader, Standalone<VectorRef<KeyValueRef>>* results) {
	// Read begin key, if this fails then block was invalid.
	uint32_t kLen = reader->consumeNetworkUInt32();
	const uint8_t* k = reader->consume(kLen);
	results->push_back(results->arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
	KeyRef prevKey = KeyRef(k, kLen);
	// Read kv pairs and end key
	while (1) {
		// Read a key.
		kLen = reader->consumeNetworkUInt32();
		k = reader->consume(kLen);

		// If eof reached or first value len byte is 0xFF then a valid block end was reached.
		if (reader->eof() || *reader->rptr == 0xFF) {
			results->push_back(results->arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
			break;
		}

		// Read a value, which must exist or the block is invalid
		uint32_t vLen = reader->consumeNetworkUInt32();
		const uint8_t* v = reader->consume(vLen);

		results->push_back(results->arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));

		// If eof reached or first byte of next key len is 0xFF then a valid block end was reached.
		if (reader->eof() || *reader->rptr == 0xFF)
			break;
	}

	// Make sure any remaining bytes in the block are 0xFF
	for (auto b : reader->remainder())
		if (b != 0xFF)
			throw restore_corrupted_data_padding();
}

} // namespace

Standalone<VectorRef<KeyValueRef>> decodeRangeFileBlock(const Standalone<StringRef>& buf) {
	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	StringRefReader reader(buf, restore_corrupted_data());

	// Read header, currently only decoding BACKUP_AGENT_SNAPSHOT_FILE_VERSION
	if (reader.consume<int32_t>() != BACKUP_AGENT_SNAPSHOT_FILE_VERSION)
		throw restore_unsupported_file_version();

	// Read begin key, if this fails then block was invalid.
	uint32_t beginKeyLen = reader.consumeNetworkUInt32();
	const uint8_t* beginKey = reader.consume(beginKeyLen);
	results.push_back(results.arena(), KeyValueRef(KeyRef(beginKey, beginKeyLen), ValueRef()));

	// Read kv pairs and end key
	while (1) {
		// If eof reached or first value len byte is 0xFF then a valid block end was reached.
		if (reader.eof() || *reader.rptr == 0xFF) {
			break;
		}

		// Read a key, which must exist or the block is invalid
		uint32_t kLen = reader.consumeNetworkUInt32();
		const uint8_t* k = reader.consume(kLen);

		// If eof reached or first value len byte is 0xFF then a valid block end was reached.
		if (reader.eof() || *reader.rptr == 0xFF) {
			// The last block in the file, will have Read End key.
			results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
			break;
		}

		// Read a value, which must exist or the block is invalid
		uint32_t vLen = reader.consumeNetworkUInt32();
		const uint8_t* v = reader.consume(vLen);
		results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
	}

	// Make sure any remaining bytes in the block are 0xFF
	for (auto b : reader.remainder())
		if (b != 0xFF)
			throw restore_corrupted_data_padding();

	return results;
}

Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file,
                                                                int64_t offset,
                                                                int len,
                                                                Database cx) {
	Standalone<StringRef> buf = makeString(len);
	int rLen = co_await uncancellable(holdWhile(buf, file->read(mutateString(buf), len, offset)));
	if (rLen != len)
		throw restore_bad_read();

	simulateBlobFailure();

	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	StringRefReader reader(buf, restore_corrupted_data());
	Arena arena;
	try {
		int32_t file_version = reader.consume<int32_t>();
		if (file_version != BACKUP_AGENT_SNAPSHOT_FILE_VERSION) {
			throw restore_unsupported_file_version();
		}
		decodeKVPairs(&reader, &results);
		co_return results;
	} catch (Error& e) {
		TraceEvent(SevWarn, "FileRestoreDecodeRangeFileBlockFailed")
		    .error(e)
		    .detail("Filename", file->getFilename())
		    .detail("BlockOffset", offset)
		    .detail("BlockLen", len)
		    .detail("ErrorRelativeOffset", reader.rptr - buf.begin())
		    .detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset);
		throw;
	}
}

// Very simple format compared to KeyRange files.
// Header, [Key, Value]... Key len
LogFileWriter::LogFileWriter(Reference<IBackupFile> file, int blockSize)
  : file(file), blockSize(blockSize), blockEnd(0) {}

Future<Void> LogFileWriter::writeKV_impl(LogFileWriter* self, Key k, Value v) {
	// If key and value do not fit in this block, end it and start a new one
	int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
	if (self->file->size() + toWrite > self->blockEnd) {
		// Write padding if needed
		int bytesLeft = self->blockEnd - self->file->size();
		if (bytesLeft > 0) {
			Value paddingFFs = makePadding(bytesLeft);
			co_await self->file->append(paddingFFs.begin(), bytesLeft);
		}

		// Set new blockEnd
		self->blockEnd += self->blockSize;

		// write the block header
		co_await self->file->append((uint8_t*)&BACKUP_AGENT_MLOG_VERSION, sizeof(BACKUP_AGENT_MLOG_VERSION));
	}

	co_await self->file->appendStringRefWithLen(k);
	co_await self->file->appendStringRefWithLen(v);

	// At this point we should be in whatever the current block is or the block size is too small
	if (self->file->size() > self->blockEnd)
		throw backup_bad_block_size();

	co_return;
}

Future<Void> LogFileWriter::writeKV(Key k, Value v) {
	return writeKV_impl(this, k, v);
}

Standalone<VectorRef<KeyValueRef>> decodeMutationLogFileBlock(const Standalone<StringRef>& buf) {
	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	StringRefReader reader(buf, restore_corrupted_data());

	// Read header, currently only decoding version BACKUP_AGENT_MLOG_VERSION
	if (reader.consume<int32_t>() != BACKUP_AGENT_MLOG_VERSION)
		throw restore_unsupported_file_version();

	// Read k/v pairs.  Block ends either at end of last value exactly or with 0xFF as first key len byte.
	while (1) {
		// If eof reached or first key len bytes is 0xFF then end of block was reached.
		if (reader.eof() || *reader.rptr == 0xFF)
			break;

		// Read key and value.  If anything throws then there is a problem.
		uint32_t kLen = reader.consumeNetworkUInt32();
		const uint8_t* k = reader.consume(kLen);
		uint32_t vLen = reader.consumeNetworkUInt32();
		const uint8_t* v = reader.consume(vLen);

		results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
	}

	// Make sure any remaining bytes in the block are 0xFF
	for (auto b : reader.remainder())
		if (b != 0xFF)
			throw restore_corrupted_data_padding();

	return results;
}

Future<Standalone<VectorRef<KeyValueRef>>> decodeMutationLogFileBlock(Reference<IAsyncFile> file,
                                                                      int64_t offset,
                                                                      int len) {
	Standalone<StringRef> buf = makeString(len);
	int rLen = co_await file->read(mutateString(buf), len, offset);
	if (rLen != len)
		throw restore_bad_read();

	try {
		co_return decodeMutationLogFileBlock(buf);
	} catch (Error& e) {
		TraceEvent(SevWarn, "FileRestoreCorruptLogFileBlock")
		    .error(e)
		    .detail("Filename", file->getFilename())
		    .detail("BlockOffset", offset)
		    .detail("BlockLen", len);
		throw;
	}
}

} // namespace fileBackup

void simulateBlobFailure() {
	if (buggify() && deterministicRandom()->random01() < 0.01) { // Simulate blob failures
		double i = deterministicRandom()->random01();
		if (i < 0.5) {
			throw http_request_failed();
		} else if (i < 0.7) {
			throw connection_failed();
		} else if (i < 0.8) {
			throw timed_out();
		} else if (i < 0.9) {
			throw lookup_failed();
		}
	}
}
