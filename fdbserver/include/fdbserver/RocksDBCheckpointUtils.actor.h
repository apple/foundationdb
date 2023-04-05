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
#include "rocksdb/types.h"
#include "rocksdb/options.h"

#include "flow/actorcompiler.h" // has to be last include

struct CheckpointFile {
	constexpr static FileIdentifier file_identifier = 13804348;
	std::string path;
	KeyRange range;
	int64_t size; // Logical bytes of the checkpoint.

	CheckpointFile() = default;
	CheckpointFile(std::string path, KeyRange range, int64_t size) : path(path), range(range), size(size) {}

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
//
// Basic identifiers and metadata for a file in a DB. This only includes
// information considered relevant for taking backups, checkpoints, or other
// services relating to DB file storage.
// This is only appropriate for immutable files, such as SST files or all
// files in a backup. See also LiveFileStorageInfo.
struct SstFileMetaData {
	constexpr static FileIdentifier file_identifier = 3804347;
	SstFileMetaData()
	  : file_number(0), size(0), temperature(rocksdb::Temperature::kUnknown),
       smallest_seqno(0), largest_seqno(0), num_reads_sampled(0),
       being_compacted(false), num_entries(0), num_deletions(0),
       oldest_blob_file_number(0), oldest_ancester_time(0),
       file_creation_time(0) {}

	SstFileMetaData(const std::string& _file_name,
	                uint64_t _file_number,
	                const std::string& _directory,
	                size_t _size,
	                rocksdb::SequenceNumber _smallest_seqno,
	                rocksdb::SequenceNumber _largest_seqno,
	                const std::string& _smallestkey,
	                const std::string& _largestkey,
	                uint64_t _num_reads_sampled,
	                bool _being_compacted,
	                rocksdb::Temperature _temperature,
	                uint64_t _oldest_blob_file_number,
	                uint64_t _oldest_ancester_time,
	                uint64_t _file_creation_time,
	                std::string& _file_checksum,
	                std::string& _file_checksum_func_name)
	  : smallest_seqno(_smallest_seqno), largest_seqno(_largest_seqno), smallestkey(_smallestkey),
	    largestkey(_largestkey), num_reads_sampled(_num_reads_sampled), being_compacted(_being_compacted),
	    num_entries(0), num_deletions(0), oldest_blob_file_number(_oldest_blob_file_number),
	    oldest_ancester_time(_oldest_ancester_time), file_creation_time(_file_creation_time) {
		if (!_file_name.empty()) {
			if (_file_name[0] == '/') {
				relative_filename = _file_name.substr(1);
				name = _file_name; // Deprecated field
			} else {
				relative_filename = _file_name;
				name = std::string("/") + _file_name; // Deprecated field
			}
			assert(relative_filename.size() + 1 == name.size());
			assert(relative_filename[0] != '/');
			assert(name[0] == '/');
		}
		directory = _directory;
		db_path = _directory; // Deprecated field
		file_number = _file_number;
		file_type = rocksdb::kTableFile;
		size = _size;
		temperature = _temperature;
		file_checksum = _file_checksum;
		file_checksum_func_name = _file_checksum_func_name;
	}

	// The name of the file within its directory (e.g. "123456.sst")
	std::string relative_filename;
	// The directory containing the file, without a trailing '/'. This could be
	// a DB path, wal_dir, etc.
	std::string directory;

	// The id of the file within a single DB. Set to 0 if the file does not have
	// a number (e.g. CURRENT)
	uint64_t file_number = 0;
	// The type of the file as part of a DB.
	rocksdb::FileType file_type = rocksdb::kTempFile;

	// File size in bytes. See also `trim_to_size`.
	uint64_t size = 0;

	// This feature is experimental and subject to change.
	rocksdb::Temperature temperature = rocksdb::Temperature::kUnknown;

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
	rocksdb::SequenceNumber smallest_seqno; // Smallest sequence number in file.
	rocksdb::SequenceNumber largest_seqno; // Largest sequence number in file.
	std::string smallestkey; // Smallest user defined key in the file.
	std::string largestkey; // Largest user defined key in the file.
	uint64_t num_reads_sampled; // How many times the file is read.
	bool being_compacted; // true if the file is currently being compacted.

	uint64_t num_entries;
	uint64_t num_deletions;

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

	// DEPRECATED: The name of the file within its directory with a
	// leading slash (e.g. "/123456.sst"). Use relative_filename from base struct
	// instead.
	std::string name;

	// DEPRECATED: replaced by `directory` in base struct
	std::string db_path;

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

	CheckpointFormat format() const { return RocksDBColumnFamily; }

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

ICheckpointReader* newRocksDBCheckpointReader(const CheckpointMetaData& checkpoint, UID logID);

RocksDBColumnFamilyCheckpoint getRocksCF(const CheckpointMetaData& checkpoint);

RocksDBCheckpoint getRocksCheckpoint(const CheckpointMetaData& checkpoint);

#include "flow/unactorcompiler.h"

#endif
