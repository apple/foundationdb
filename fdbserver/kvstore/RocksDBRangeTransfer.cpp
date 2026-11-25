/*
 * RocksDBRangeTransfer.cpp
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

#include "RocksDBRangeTransfer.h"

#ifdef WITH_ROCKSDB

#include "RocksDBCommon.h"
#include "flow/Platform.h"
#include "flow/Trace.h"

#include <rocksdb/convenience.h>

#include <algorithm>

namespace RocksDBRangeTransfer {

rocksdb::Status deleteFilesInRange(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf, KeyRangeRef range, UID id) {
	rocksdb::Slice begin = RocksDBCommon::toSlice(range.begin);
	rocksdb::Slice end = RocksDBCommon::toSlice(range.end);
	uint64_t sstBytesBefore = 0;
	uint64_t sstBytesAfter = 0;
	db->GetIntProperty(cf, rocksdb::DB::Properties::kTotalSstFilesSize, &sstBytesBefore);
	double start = timer_monotonic();
	// FDB ranges exclude their end key, which may belong to a range this server still owns.
	rocksdb::Status s = rocksdb::DeleteFilesInRange(db, cf, &begin, &end, /*include_end=*/false);
	if (s.ok()) {
		db->GetIntProperty(cf, rocksdb::DB::Properties::kTotalSstFilesSize, &sstBytesAfter);
		TraceEvent("RocksDBDeleteFilesInRange", id)
		    .detail("Range", range)
		    .detail("Duration", timer_monotonic() - start)
		    .detail("SstBytesBefore", sstBytesBefore)
		    .detail("SstBytesAfter", sstBytesAfter);
	}
	return s;
}

// Returns true to start a new file between two adjacent keys if any ingest begin key lies in (request.prev_user_key,
// request.current_user_key).
rocksdb::PartitionerResult IngestSSTPartitioner::ShouldPartition(const rocksdb::PartitionerRequest& request) {
	auto it = std::upper_bound(
	    ingestBeginKeys.begin(),
	    ingestBeginKeys.end(),
	    *request.prev_user_key,
	    [](const rocksdb::Slice& key, const std::string& ingestBeginKey) { return key.compare(ingestBeginKey) < 0; });
	return it != ingestBeginKeys.end() && request.current_user_key->compare(*it) >= 0 ? rocksdb::kRequired
	                                                                                  : rocksdb::kNotRequired;
}

std::unique_ptr<rocksdb::SstPartitioner> IngestSSTPartitionerFactory::CreatePartitioner(
    const rocksdb::SstPartitioner::Context&) const {
	std::lock_guard<std::mutex> lock(mutex);
	return std::make_unique<IngestSSTPartitioner>(
	    std::vector<std::string>(ingestBeginKeys.begin(), ingestBeginKeys.end()));
}

void IngestSSTPartitionerFactory::addIngestBeginKey(const std::string& key) {
	std::lock_guard<std::mutex> lock(mutex);
	ingestBeginKeys.insert(key);
}

void IngestSSTPartitionerFactory::removeIngestBeginKey(const std::string& key) {
	std::lock_guard<std::mutex> lock(mutex);
	auto it = ingestBeginKeys.find(key);
	if (it != ingestBeginKeys.end()) {
		ingestBeginKeys.erase(it);
	}
}

rocksdb::Status IngestSSTFileBuilder::add(VectorRef<KeyValueRef> kvs) {
	if (kvs.empty()) {
		return rocksdb::Status::OK();
	}
	// Open the file only when the first key arrives: RocksDB can't finish a file with no keys.
	if (!opened) {
		rocksdb::Status s = writer.Open(path);
		if (!s.ok()) {
			return s;
		}
		opened = true;
		firstKey = kvs.front().key.toString();
	}
	for (const KeyValueRef& kv : kvs) {
		rocksdb::Status s = writer.Put(RocksDBCommon::toSlice(kv.key), RocksDBCommon::toSlice(kv.value));
		if (!s.ok()) {
			return s;
		}
	}
	lastKey = kvs.back().key.toString();
	keyCount += kvs.size();
	return rocksdb::Status::OK();
}

rocksdb::Status IngestSSTFileBuilder::finish() {
	if (!opened) {
		return rocksdb::Status::OK();
	}
	rocksdb::ExternalSstFileInfo info;
	rocksdb::Status s = writer.Finish(&info);
	if (s.ok()) {
		fileBytes = info.file_size;
	}
	return s;
}

rocksdb::Status IngestSSTFileSetBuilder::add(VectorRef<KeyValueRef> kvs) {
	if (kvs.empty()) {
		return rocksdb::Status::OK();
	}
	// The current file is full: finish it, so this block goes into a new file.
	if (current && targetFileBytes > 0 && current->getCurrentFileBytes() >= targetFileBytes) {
		rocksdb::Status s = finishCurrentFile();
		if (!s.ok()) {
			return s;
		}
	}
	if (!current) {
		current = std::make_unique<IngestSSTFileBuilder>(options, cf, basePath + "." + std::to_string(files.size()));
	}
	return current->add(kvs);
}

rocksdb::Status IngestSSTFileSetBuilder::finish() {
	return current ? finishCurrentFile() : rocksdb::Status::OK();
}

// Finishes the current file and adds its info to `files`.
rocksdb::Status IngestSSTFileSetBuilder::finishCurrentFile() {
	rocksdb::Status s = current->finish();
	if (s.ok()) {
		files.push_back(FileInfo{ current->getPath(),
		                          current->getFirstKey(),
		                          current->getLastKey(),
		                          current->getKeyCount(),
		                          current->getFileBytes() });
		current.reset();
	}
	return s;
}

rocksdb::Status IngestSSTFileSetBuilder::abort() {
	std::vector<std::string> paths;
	for (const FileInfo& file : files) {
		paths.push_back(file.path);
	}
	if (current) {
		paths.push_back(current->getPath());
		current.reset(); // Close the unfinished file before deleting it.
	}
	files.clear();

	rocksdb::Status firstError;
	for (const std::string& path : paths) {
		rocksdb::Status s = options.env->DeleteFile(path);
		if (!s.ok() && !s.IsNotFound() && firstError.ok()) {
			firstError = s;
		}
	}
	return firstError;
}

} // namespace RocksDBRangeTransfer

#endif // WITH_ROCKSDB
