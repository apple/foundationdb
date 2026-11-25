/*
 * RocksDBRangeTransfer.h
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

// RocksDB helpers for shards moving into and out of a storage server.

#ifndef FDBSERVER_ROCKSDBRANGETRANSFER_H
#define FDBSERVER_ROCKSDBRANGETRANSFER_H
#pragma once

#ifdef WITH_ROCKSDB

#include "fdbclient/FDBTypes.h"

#include <rocksdb/db.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/sst_partitioner.h>

#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace RocksDBRangeTransfer {

// Deletes SST files that only have keys in [range.begin, range.end). Does not delete level-0 files, or files that also
// have keys outside the range.
rocksdb::Status deleteFilesInRange(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, KeyRangeRef range, UID id);

// RocksDB lets us plug in our own SstPartitioner. During every compaction that writes files to level 1, 2, ..., n (not
// level 0), RocksDB calls ShouldPartition() before it writes each key, to ask: close the current output file and start
// a new one here, or keep writing to the same file. Our partitioner says "start a new file" when one of
// `ingestBeginKeys` comes between the previous key and the next key.
//
// We use IngestSSTPartitioner so that our SST files can be ingested into a level directly. A file can be ingested into
// a level only if no file in that level covers its key range. So when a compaction rewrites a file that covers an
// ingest begin key, IngestSSTPartitioner splits it into two files at that key. Our files can then be ingested
// between the two, without overlapping either one.
class IngestSSTPartitioner : public rocksdb::SstPartitioner {
public:
	// `ingestBeginKeys` must be sorted.
	explicit IngestSSTPartitioner(std::vector<std::string> ingestBeginKeys)
	  : ingestBeginKeys(std::move(ingestBeginKeys)) {}

	const char* Name() const override { return "FdbIngestSSTPartitioner"; }

	rocksdb::PartitionerResult ShouldPartition(const rocksdb::PartitionerRequest& request) override;

	// RocksDB calls this before it moves a whole file to the next level without rewriting it (a "trivial move"). It
	// only asks for moves into level 1, 2, ..., n-1. We always say yes, so RocksDB moves files the same way it does
	// without our partitioner.
	bool CanDoTrivialMove(const rocksdb::Slice&, const rocksdb::Slice&) override { return true; }

private:
	const std::vector<std::string> ingestBeginKeys;
};

// We create one factory for each RocksDB store, and add or remove ingest begin keys in it. RocksDB calls
// CreatePartitioner() at the start of every compaction that writes files to level 1, 2, ..., n, and gets a new
// IngestSSTPartitioner with a copy of those keys.A
class IngestSSTPartitionerFactory : public rocksdb::SstPartitionerFactory {
public:
	const char* Name() const override { return "FdbIngestSSTPartitionerFactory"; }

	std::unique_ptr<rocksdb::SstPartitioner> CreatePartitioner(const rocksdb::SstPartitioner::Context&) const override;

	void addIngestBeginKey(const std::string& key);
	void removeIngestBeginKey(const std::string& key);

private:
	// RocksDB calls CreatePartitioner() on its compaction threads, while we add and remove keys on the main thread.
	mutable std::mutex mutex;
	// A multiset, so two ingests with the same begin key each keep it until they remove it.
	std::multiset<std::string> ingestBeginKeys;
};

// Builds one SST file from key-values given in increasing key order, using RocksDB's SstFileWriter. The file uses the
// column family's options, so it has the same format and bottom-level compression as the files RocksDB writes itself.
// The file is not part of the database until it is ingested. Writing it takes disk time, so use it on the RocksDB
// writer thread, not the main thread.
class IngestSSTFileBuilder {
public:
	IngestSSTFileBuilder(const rocksdb::Options& options, rocksdb::ColumnFamilyHandle* cf, std::string path)
	  : writer(rocksdb::EnvOptions(), options, cf), path(std::move(path)) {}

	// Adds a block of key-values. Keys must be strictly increasing, within the block and across calls.
	rocksdb::Status add(VectorRef<KeyValueRef> kvs);

	// Completes the file. If nothing was added, no file is created.
	rocksdb::Status finish();

	const std::string& getPath() const { return path; }
	const std::string& getFirstKey() const { return firstKey; }
	const std::string& getLastKey() const { return lastKey; }
	int64_t getKeyCount() const { return keyCount; }
	uint64_t getFileBytes() const { return fileBytes; } // Set by finish().

	uint64_t getCurrentFileBytes() { return writer.FileSize(); }

private:
	rocksdb::SstFileWriter writer;
	const std::string path;
	bool opened = false;
	std::string firstKey;
	std::string lastKey;
	int64_t keyCount = 0;
	uint64_t fileBytes = 0;
};

// Builds several SST files from key-values given in increasing key order. Before each block, it checks the current
// file: if it is already `targetFileBytes` or bigger, it finishes that file and starts a new one. So a file can be at
// most about one block bigger than `targetFileBytes`.
// Files are named "<basePath>.0", "<basePath>.1", and so on. Like IngestSSTFileBuilder, use it on the RocksDB writer
// thread.
class IngestSSTFileSetBuilder {
public:
	struct FileInfo {
		std::string path;
		std::string firstKey;
		std::string lastKey;
		int64_t keyCount = 0;
		uint64_t fileBytes = 0;
	};

	// A targetFileBytes of 0 means: put everything in one file.
	IngestSSTFileSetBuilder(const rocksdb::Options& options,
	                        rocksdb::ColumnFamilyHandle* cf,
	                        std::string basePath,
	                        uint64_t targetFileBytes)
	  : options(options), cf(cf), basePath(std::move(basePath)), targetFileBytes(targetFileBytes) {}

	// Adds a block of key-values. Keys must be strictly increasing, within the block and across calls.
	rocksdb::Status add(VectorRef<KeyValueRef> kvs);

	// Finishes the last file.
	rocksdb::Status finish();

	// The finished files, in key order.
	const std::vector<FileInfo>& getFiles() const { return files; }

	// Deletes every file built so far, finished or not. Don't use the builder after this. Returns the first error, if
	// a file could not be deleted; a file that is already gone is not an error.
	rocksdb::Status abort();

private:
	rocksdb::Status finishCurrentFile();

	const rocksdb::Options options;
	rocksdb::ColumnFamilyHandle* const cf;
	const std::string basePath;
	const uint64_t targetFileBytes;
	std::unique_ptr<IngestSSTFileBuilder> current; // The file being written now, if any.
	std::vector<FileInfo> files; // Finished files.
};

struct IngestSSTOptions {
	bool verifyChecksums = true;
	bool lastLevelOnly = false;
};

// Adds SST files to the database: either all of them are added, or none. On success, the files are moved into the
// database folder, so they are gone from their old paths. On failure, they stay where they were. The files should be
// sorted by key and their key ranges should not overlap; if they overlap, RocksDB can't put them all in the last level.
// If `lastLevelOnly` is set and a file can't go to the last level, nothing is added and the status is TryAgain. If
// `paths` is empty, does nothing. Ingesting takes disk time, so use it on the RocksDB writer thread.
rocksdb::Status ingestSSTFiles(rocksdb::DB* db,
                               rocksdb::ColumnFamilyHandle* cf,
                               const std::vector<std::string>& paths,
                               const IngestSSTOptions& options);

} // namespace RocksDBRangeTransfer

#endif // WITH_ROCKSDB
#endif // FDBSERVER_ROCKSDBRANGETRANSFER_H
