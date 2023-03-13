/*
 *RocksDBCheckpointUtils.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_ROCKSDB_CHECKPOINT_UTILS_ACTOR_G_H)
#define FDBSERVER_ROCKSDB_CHECKPOINT_UTILS_ACTOR_G_H
#include "fdbserver/RocksDBCheckpointUtils.actor.g.h"
#elif !defined(FDBSERVER_ROCKSDB_CHECKPOINT_UTILS_ACTOR_H)
#define FDBSERVER_ROCKSDB_CHECKPOINT_UTILS_ACTOR_H

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/ServerCheckpoint.actor.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

class IRocksDBSstFileWriter {
public:
	virtual void open(const std::string localFile) = 0;

	virtual void write(const KeyRef key, const ValueRef value) = 0;

	virtual bool finish() = 0;

	virtual ~IRocksDBSstFileWriter() {}
};

struct CheckpointFile {
	constexpr static FileIdentifier file_identifier = 13804348;
	std::string path;
	KeyRange range;
	int64_t size; // Logical bytes of the checkpoint.

	CheckpointFile() = default;
	CheckpointFile(std::string path, KeyRange range, int64_t size) : path(path), range(range), size(size) {}

	bool isValid() const { return !path.empty(); }

	std::string toString() const {
		return "CheckpointFile:\nFile Name: " + this->path + "\nRange: " + range.toString() +
		       "\nSize: " + std::to_string(size) + "\n";
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, path, range, size);
	}
};

// Copied from rocksdb/metadata.h, so that we can add serializer.
struct SstFileMetaData {
	constexpr static FileIdentifier file_identifier = 3804347;
	SstFileMetaData()
	  : size(0), file_number(0), smallest_seqno(0), largest_seqno(0), num_reads_sampled(0), being_compacted(false),
	    num_entries(0), num_deletions(0), temperature(0), oldest_blob_file_number(0), oldest_ancester_time(0),
	    file_creation_time(0) {}

	SstFileMetaData(const std::string& _file_name,
	                uint64_t _file_number,
	                const std::string& _path,
	                size_t _size,
	                uint64_t _smallest_seqno,
	                uint64_t _largest_seqno,
	                const std::string& _smallestkey,
	                const std::string& _largestkey,
	                uint64_t _num_reads_sampled,
	                bool _being_compacted,
	                int _temperature,
	                uint64_t _oldest_blob_file_number,
	                uint64_t _oldest_ancester_time,
	                uint64_t _file_creation_time,
	                std::string& _file_checksum,
	                std::string& _file_checksum_func_name)
	  : size(_size), name(_file_name), file_number(_file_number), db_path(_path), smallest_seqno(_smallest_seqno),
	    largest_seqno(_largest_seqno), smallestkey(_smallestkey), largestkey(_largestkey),
	    num_reads_sampled(_num_reads_sampled), being_compacted(_being_compacted), num_entries(0), num_deletions(0),
	    temperature(_temperature), oldest_blob_file_number(_oldest_blob_file_number),
	    oldest_ancester_time(_oldest_ancester_time), file_creation_time(_file_creation_time),
	    file_checksum(_file_checksum), file_checksum_func_name(_file_checksum_func_name) {}

	// File size in bytes.
	size_t size;
	// The name of the file.
	std::string name;
	// The id of the file.
	uint64_t file_number;
	// The full path where the file locates.
	std::string db_path;

	uint64_t smallest_seqno; // Smallest sequence number in file.
	uint64_t largest_seqno; // Largest sequence number in file.
	std::string smallestkey; // Smallest user defined key in the file.
	std::string largestkey; // Largest user defined key in the file.
	uint64_t num_reads_sampled; // How many times the file is read.
	bool being_compacted; // true if the file is currently being compacted.

	uint64_t num_entries;
	uint64_t num_deletions;

	// This feature is experimental and subject to change.
	int temperature;

	uint64_t oldest_blob_file_number; // The id of the oldest blob file
	                                  // referenced by the file.
	// An SST file may be generated by compactions whose input files may
	// in turn be generated by earlier compactions. The creation time of the
	// oldest SST file that is the compaction ancestor of this file.
	// The timestamp is provided SystemClock::GetCurrentTime().
	// 0 if the information is not available.
	//
	// Note: for TTL blob files, it contains the start of the expiration range.
	uint64_t oldest_ancester_time;
	// Timestamp when the SST file is created, provided by
	// SystemClock::GetCurrentTime(). 0 if the information is not available.
	uint64_t file_creation_time;

	// The checksum of a SST file, the value is decided by the file content and
	// the checksum algorithm used for this SST file. The checksum function is
	// identified by the file_checksum_func_name. If the checksum function is
	// not specified, file_checksum is "0" by default.
	std::string file_checksum;

	// The name of the checksum function used to generate the file checksum
	// value. If file checksum is not enabled (e.g., sst_file_checksum_func is
	// null), file_checksum_func_name is UnknownFileChecksumFuncName, which is
	// "Unknown".
	std::string file_checksum_func_name;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           size,
		           name,
		           file_number,
		           db_path,
		           smallest_seqno,
		           largest_seqno,
		           smallestkey,
		           largestkey,
		           num_reads_sampled,
		           being_compacted,
		           num_entries,
		           num_deletions,
		           temperature,
		           oldest_blob_file_number,
		           oldest_ancester_time,
		           file_creation_time,
		           file_checksum,
		           file_checksum_func_name);
	}
};

// Copied from rocksdb::LiveFileMetaData.
struct LiveFileMetaData : public SstFileMetaData {
	constexpr static FileIdentifier file_identifier = 3804346;
	std::string column_family_name; // Name of the column family
	int level; // Level at which this file resides.
	bool fetched;
	LiveFileMetaData() : column_family_name(), level(0), fetched(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           SstFileMetaData::size,
		           SstFileMetaData::name,
		           SstFileMetaData::file_number,
		           SstFileMetaData::db_path,
		           SstFileMetaData::smallest_seqno,
		           SstFileMetaData::largest_seqno,
		           SstFileMetaData::smallestkey,
		           SstFileMetaData::largestkey,
		           SstFileMetaData::num_reads_sampled,
		           SstFileMetaData::being_compacted,
		           SstFileMetaData::num_entries,
		           SstFileMetaData::num_deletions,
		           SstFileMetaData::temperature,
		           SstFileMetaData::oldest_blob_file_number,
		           SstFileMetaData::oldest_ancester_time,
		           SstFileMetaData::file_creation_time,
		           SstFileMetaData::file_checksum,
		           SstFileMetaData::file_checksum_func_name,
		           column_family_name,
		           level,
		           fetched);
	}
};

// Checkpoint metadata associated with RockDBColumnFamily format.
// Based on rocksdb::ExportImportFilesMetaData.
struct RocksDBColumnFamilyCheckpoint {
	constexpr static FileIdentifier file_identifier = 13804346;
	std::string dbComparatorName;

	std::vector<LiveFileMetaData> sstFiles;

	CheckpointFormat format() const { return DataMoveRocksCF; }

	std::string toString() const {
		std::string res = "RocksDBColumnFamilyCheckpoint:\nSST Files:\n";
		for (const auto& file : sstFiles) {
			res += file.db_path + file.name + "\n";
		}
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, dbComparatorName, sstFiles);
	}
};

// Checkpoint metadata associated with RocksDB format.
// The checkpoint is created via rocksdb::CreateCheckpoint().
struct RocksDBCheckpoint {
	constexpr static FileIdentifier file_identifier = 13804347;
	std::string checkpointDir; // Checkpoint directory on the storage server.
	std::vector<std::string> sstFiles; // All checkpoint files.
	std::vector<CheckpointFile> fetchedFiles; // Used for fetchCheckpoint, to record the progress.

	CheckpointFormat format() const { return RocksDB; }

	std::string toString() const {
		std::string res = "RocksDBCheckpoint:\nCheckpoint dir: " + checkpointDir + "\nFiles: ";
		for (const std::string& file : sstFiles) {
			res += (file + " ");
		}
		res += "\nFetched files:\n";
		for (const auto& file : fetchedFiles) {
			res += file.toString();
		}
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, checkpointDir, sstFiles, fetchedFiles);
	}
};

struct RocksDBCheckpointKeyValues {
	constexpr static FileIdentifier file_identifier = 13804349;
	std::vector<CheckpointFile> fetchedFiles; // Used for fetchCheckpoint, to record the progress.
	std::vector<KeyRange> ranges; // The ranges we want to fetch.

	RocksDBCheckpointKeyValues(std::vector<KeyRange> ranges) : ranges(ranges) {}
	RocksDBCheckpointKeyValues() = default;

	CheckpointFormat format() const { return FetchedRocksDBKeyValues; }

	std::string toString() const {
		std::string res = "FetchedRocksDBKeyValuesCheckpoint: [Target Ranges]: " + describe(ranges) + " [Fetched Files]: ";
		for (const auto& file : fetchedFiles) {
			res += file.toString();
		}
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, fetchedFiles, ranges);
	}
};

// Fetch the checkpoint file(s) to local dir, the checkpoint is specified by initialState.
// If cFun is provided, the fetch progress can be checkpointed, so that next time, the fetch process
// can be continued, in case of crash.
ACTOR Future<CheckpointMetaData> fetchRocksDBCheckpoint(Database cx,
                                                        CheckpointMetaData initialState,
                                                        std::string dir,
                                                        std::function<Future<Void>(const CheckpointMetaData&)> cFun);

// Returns the total logical bytes of all *fetched* checkpoints.
int64_t getTotalFetchedBytes(const std::vector<CheckpointMetaData>& checkpoints);

// Clean up on-disk files associated with checkpoint.
ACTOR Future<Void> deleteRocksCheckpoint(CheckpointMetaData checkpoint);

ICheckpointReader* newRocksDBCheckpointReader(const CheckpointMetaData& checkpoint,
                                              const CheckpointAsKeyValues checkpointAsKeyValues,
                                              UID logID);

std::unique_ptr<IRocksDBSstFileWriter> newRocksDBSstFileWriter();

RocksDBColumnFamilyCheckpoint getRocksCF(const CheckpointMetaData& checkpoint);

RocksDBCheckpoint getRocksCheckpoint(const CheckpointMetaData& checkpoint);

RocksDBCheckpointKeyValues getRocksKeyValuesCheckpoint(const CheckpointMetaData& checkpoint);

#include "flow/unactorcompiler.h"

#endif
