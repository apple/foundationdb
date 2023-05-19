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

// Copied from rocksdb/metadata.h, so that we can add serializer.
struct SstFileMetaData {
	constexpr static FileIdentifier file_identifier = 3804347;
	SstFileMetaData()
	  : file_number(0), file_type(2), size(0), temperature(0), smallest_seqno(0), largest_seqno(0),
	    num_reads_sampled(0), being_compacted(false), num_entries(0), num_deletions(0), oldest_blob_file_number(0),
	    oldest_ancester_time(0), file_creation_time(0), epoch_number(0) {}

	SstFileMetaData(const std::string& _relative_filename,
	                const std::string& _directory,
	                uint64_t _file_number,
	                int _file_type,
	                uint64_t _size,
	                int _temperature,
	                std::string& _file_checksum,
	                std::string& _file_checksum_func_name,
	                uint64_t _smallest_seqno,
	                uint64_t _largest_seqno,
	                const std::string& _smallestkey,
	                const std::string& _largestkey,
	                uint64_t _num_reads_sampled,
	                bool _being_compacted,
	                uint64_t _num_entries,
	                uint64_t _num_deletions,
	                uint64_t _oldest_blob_file_number,
	                uint64_t _oldest_ancester_time,
	                uint64_t _file_creation_time,
	                uint64_t _epoch_number,
	                const std::string& _name,
	                const std::string& _db_path)
	  : relative_filename(_relative_filename), directory(_directory), file_number(_file_number), file_type(_file_type),
	    size(_size), temperature(_temperature), file_checksum(_file_checksum),
	    file_checksum_func_name(_file_checksum_func_name), smallest_seqno(_smallest_seqno),
	    largest_seqno(_largest_seqno), smallestkey(_smallestkey), largestkey(_largestkey),
	    num_reads_sampled(_num_reads_sampled), being_compacted(_being_compacted), num_entries(_num_entries),
	    num_deletions(_num_deletions), oldest_blob_file_number(_oldest_blob_file_number),
	    oldest_ancester_time(_oldest_ancester_time), file_creation_time(_file_creation_time),
	    epoch_number(_epoch_number), name(_name), db_path(_db_path) {}

	// The name of the file within its directory (e.g. "123456.sst")
	std::string relative_filename;
	// The directory containing the file, without a trailing '/'. This could be
	// a DB path, wal_dir, etc.
	std::string directory;
	// The id of the file within a single DB. Set to 0 if the file does not have
	// a number (e.g. CURRENT)
	uint64_t file_number;
	// The type of the file as part of a DB.
	int file_type;
	// File size in bytes. See also `trim_to_size`.
	uint64_t size;
	// This feature is experimental and subject to change.
	int temperature;
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

	uint64_t smallest_seqno; // Smallest sequence number in file.
	uint64_t largest_seqno; // Largest sequence number in file.
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
	// The order of a file being flushed or ingested/imported.
	// Compaction output file will be assigned with the minimum `epoch_number`
	// among input files'.
	// For L0, larger `epoch_number` indicates newer L0 file.
	// 0 if the information is not available.
	uint64_t epoch_number;
	// DEPRECATED: The name of the file within its directory with a
	// leading slash (e.g. "/123456.sst"). Use relative_filename from base struct
	// instead.
	std::string name;
	// DEPRECATED: replaced by `directory` in base struct
	std::string db_path;

	// These bounds define the effective key range for range tombstones
	// in this file.
	// Currently only used by CreateColumnFamilyWithImport().
	std::string smallest{}; // Smallest internal key served by table
	std::string largest{}; // Largest internal key served by table

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           relative_filename,
		           directory,
		           file_number,
		           file_type,
		           size,
		           temperature,
		           file_checksum,
		           file_checksum_func_name,
		           smallest_seqno,
		           largest_seqno,
		           smallestkey,
		           largestkey,
		           num_reads_sampled,
		           being_compacted,
		           num_entries,
		           num_deletions,
		           oldest_blob_file_number,
		           oldest_ancester_time,
		           file_creation_time,
		           epoch_number,
		           name,
		           db_path,
		           smallest,
		           largest);
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
		           SstFileMetaData::relative_filename,
		           SstFileMetaData::directory,
		           SstFileMetaData::file_number,
		           SstFileMetaData::file_type,
		           SstFileMetaData::size,
		           SstFileMetaData::temperature,
		           SstFileMetaData::file_checksum,
		           SstFileMetaData::file_checksum_func_name,
		           SstFileMetaData::smallest_seqno,
		           SstFileMetaData::largest_seqno,
		           SstFileMetaData::smallestkey,
		           SstFileMetaData::largestkey,
		           SstFileMetaData::num_reads_sampled,
		           SstFileMetaData::being_compacted,
		           SstFileMetaData::num_entries,
		           SstFileMetaData::num_deletions,
		           SstFileMetaData::oldest_blob_file_number,
		           SstFileMetaData::oldest_ancester_time,
		           SstFileMetaData::file_creation_time,
		           SstFileMetaData::epoch_number,
		           SstFileMetaData::name,
		           SstFileMetaData::db_path,
		           column_family_name,
		           level,
		           fetched,
		           SstFileMetaData::smallest,
		           SstFileMetaData::largest);
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

// Fetch the checkpoint file(s) to local dir, the checkpoint is specified by initialState.
// If cFun is provided, the fetch progress can be checkpointed, so that next time, the fetch process
// can be continued, in case of crash.
ACTOR Future<CheckpointMetaData> fetchRocksDBCheckpoint(Database cx,
                                                        CheckpointMetaData initialState,
                                                        std::string dir,
                                                        std::function<Future<Void>(const CheckpointMetaData&)> cFun);

ACTOR Future<Void> deleteRocksCFCheckpoint(CheckpointMetaData checkpoint);

ICheckpointReader* newRocksDBCheckpointReader(const CheckpointMetaData& checkpoint, UID logID);

RocksDBColumnFamilyCheckpoint getRocksCF(const CheckpointMetaData& checkpoint);
#endif