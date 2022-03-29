/*
 * BackupContainer.h
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

#ifndef FDBCLIENT_BACKUP_CONTAINER_H
#define FDBCLIENT_BACKUP_CONTAINER_H
#pragma once

#include "flow/flow.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include <vector>

class ReadYourWritesTransaction;

Future<Optional<int64_t>> timeKeeperEpochsFromVersion(Version const& v, Reference<ReadYourWritesTransaction> const& tr);
Future<Version> timeKeeperVersionFromDatetime(std::string const& datetime, Database const& db);

// Append-only file interface for writing backup data
// Once finish() is called the file cannot be further written to.
// Backup containers should not attempt to use files for which finish was not called or did not complete.
// TODO: Move the log file and range file format encoding/decoding stuff to this file and behind interfaces.
class IBackupFile {
public:
	IBackupFile(const std::string& fileName) : m_fileName(fileName) {}
	virtual ~IBackupFile() {}
	// Backup files are append-only and cannot have more than 1 append outstanding at once.
	virtual Future<Void> append(const void* data, int len) = 0;
	virtual Future<Void> finish() = 0;
	inline std::string getFileName() const { return m_fileName; }
	virtual int64_t size() const = 0;
	virtual void addref() = 0;
	virtual void delref() = 0;

	Future<Void> appendStringRefWithLen(Standalone<StringRef> s);

protected:
	std::string m_fileName;
};

// Structures for various backup components

// Mutation log version written by old FileBackupAgent
static const uint32_t BACKUP_AGENT_MLOG_VERSION = 2001;

// Mutation log version written by BackupWorker
static const uint32_t PARTITIONED_MLOG_VERSION = 4110;

// Snapshot file version written by FileBackupAgent
static const uint32_t BACKUP_AGENT_SNAPSHOT_FILE_VERSION = 1001;

struct LogFile {
	Version beginVersion;
	Version endVersion;
	uint32_t blockSize;
	std::string fileName;
	int64_t fileSize;
	int tagId = -1; // Log router tag. Non-negative for new backup format.
	int totalTags = -1; // Total number of log router tags.

	// Order by beginVersion, break ties with endVersion
	bool operator<(const LogFile& rhs) const {
		return beginVersion == rhs.beginVersion ? endVersion < rhs.endVersion : beginVersion < rhs.beginVersion;
	}

	// Returns if this log file contains a subset of content of the given file
	// by comparing version range and tag ID.
	bool isSubset(const LogFile& rhs) const {
		return beginVersion >= rhs.beginVersion && endVersion <= rhs.endVersion && tagId == rhs.tagId;
	}

	bool isPartitionedLog() const { return tagId >= 0 && tagId < totalTags; }

	std::string toString() const {
		std::stringstream ss;
		ss << "beginVersion:" << std::to_string(beginVersion) << " endVersion:" << std::to_string(endVersion)
		   << " blockSize:" << std::to_string(blockSize) << " filename:" << fileName
		   << " fileSize:" << std::to_string(fileSize)
		   << " tagId: " << (tagId >= 0 ? std::to_string(tagId) : std::string("(None)"));
		return ss.str();
	}
};

struct RangeFile {
	Version version;
	uint32_t blockSize;
	std::string fileName;
	int64_t fileSize;

	// Order by version, break ties with name
	bool operator<(const RangeFile& rhs) const {
		return version == rhs.version ? fileName < rhs.fileName : version < rhs.version;
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "version:" << std::to_string(version) << " blockSize:" << std::to_string(blockSize)
		   << " fileName:" << fileName << " fileSize:" << std::to_string(fileSize);
		return ss.str();
	}
};

struct KeyspaceSnapshotFile {
	Version beginVersion;
	Version endVersion;
	std::string fileName;
	int64_t totalSize;
	Optional<bool> restorable; // Whether or not the snapshot can be used in a restore, if known
	bool isSingleVersion() const { return beginVersion == endVersion; }
	double expiredPct(Optional<Version> expiredEnd) const {
		double pctExpired = 0;
		if (expiredEnd.present() && expiredEnd.get() > beginVersion) {
			if (isSingleVersion()) {
				pctExpired = 1;
			} else {
				pctExpired =
				    double(std::min(endVersion, expiredEnd.get()) - beginVersion) / (endVersion - beginVersion);
			}
		}
		return pctExpired * 100;
	}

	// Order by beginVersion, break ties with endVersion
	bool operator<(const KeyspaceSnapshotFile& rhs) const {
		return beginVersion == rhs.beginVersion ? endVersion < rhs.endVersion : beginVersion < rhs.beginVersion;
	}
};

struct BackupFileList {
	std::vector<RangeFile> ranges;
	std::vector<LogFile> logs;
	std::vector<KeyspaceSnapshotFile> snapshots;

	void toStream(FILE* fout) const;
};

// The byte counts here only include usable log files and byte counts from kvrange manifests
struct BackupDescription {
	BackupDescription() : snapshotBytes(0) {}
	std::string url;
	Optional<std::string> proxy;
	std::vector<KeyspaceSnapshotFile> snapshots;
	int64_t snapshotBytes;
	// The version before which everything has been deleted by an expire
	Optional<Version> expiredEndVersion;
	// The latest version before which at least some data has been deleted by an expire
	Optional<Version> unreliableEndVersion;
	// The minimum log version in the backup
	Optional<Version> minLogBegin;
	// The maximum log version in the backup
	Optional<Version> maxLogEnd;
	// The maximum log version for which there is contiguous log version coverage extending back to minLogBegin
	Optional<Version> contiguousLogEnd;
	// The maximum version which this backup can be used to restore to
	Optional<Version> maxRestorableVersion;
	// The minimum version which this backup can be used to restore to
	Optional<Version> minRestorableVersion;
	std::string extendedDetail; // Freeform container-specific info.
	bool partitioned; // If this backup contains partitioned mutation logs.

	// Resolves the versions above to timestamps using a given database's TimeKeeper data.
	// toString will use this information if present.
	Future<Void> resolveVersionTimes(Database cx);
	std::map<Version, int64_t> versionTimeMap;

	std::string toString() const;
	std::string toJSON() const;
};

struct RestorableFileSet {
	Version targetVersion;
	std::vector<LogFile> logs;
	std::vector<RangeFile> ranges;

	// Range file's key ranges. Can be empty for backups generated before 6.3.
	std::map<std::string, KeyRange> keyRanges;

	// Mutation logs continuous range [begin, end). Both can be invalidVersion
	// when the entire key space snapshot is at the target version.
	Version continuousBeginVersion, continuousEndVersion;

	KeyspaceSnapshotFile snapshot; // Info. for debug purposes
};

/* IBackupContainer is an interface to a set of backup data, which contains
 *   - backup metadata
 *   - log files
 *   - range files
 *   - keyspace snapshot files defining a complete non overlapping key space snapshot
 *
 * Files in a container are identified by a name.  This can be any string, whatever
 * makes sense for the underlying storage system.
 *
 * Reading files is done by file name.  File names are discovered by getting a RestorableFileSet.
 *
 * For remote data stores that are filesystem-like, it's probably best to inherit BackupContainerFileSystem.
 */
class IBackupContainer {
public:
	virtual void addref() = 0;
	virtual void delref() = 0;

	IBackupContainer() {}
	virtual ~IBackupContainer() {}

	// Create the container
	virtual Future<Void> create() = 0;
	virtual Future<bool> exists() = 0;

	// Open a log file or range file for writing
	virtual Future<Reference<IBackupFile>> writeLogFile(Version beginVersion, Version endVersion, int blockSize) = 0;
	virtual Future<Reference<IBackupFile>> writeRangeFile(Version snapshotBeginVersion,
	                                                      int snapshotFileCount,
	                                                      Version fileVersion,
	                                                      int blockSize) = 0;

	// Open a tagged log file for writing, where tagId is the log router tag's id.
	virtual Future<Reference<IBackupFile>> writeTaggedLogFile(Version beginVersion,
	                                                          Version endVersion,
	                                                          int blockSize,
	                                                          uint16_t tagId,
	                                                          int totalTags) = 0;

	// Write a KeyspaceSnapshotFile of range file names representing a full non overlapping
	// snapshot of the key ranges this backup is targeting.
	virtual Future<Void> writeKeyspaceSnapshotFile(const std::vector<std::string>& fileNames,
	                                               const std::vector<std::pair<Key, Key>>& beginEndKeys,
	                                               int64_t totalBytes) = 0;

	// Open a file for read by name
	virtual Future<Reference<IAsyncFile>> readFile(const std::string& name) = 0;

	// Returns the key ranges in the snapshot file. This is an expensive function
	// and should only be used in simulation for sanity check.
	virtual Future<KeyRange> getSnapshotFileKeyRange(const RangeFile& file) = 0;

	struct ExpireProgress {
		std::string step;
		int total;
		int done;
		std::string toString() const;
	};
	// Delete backup files which do not contain any data at or after (more recent than) expireEndVersion.
	// If force is false, then nothing will be deleted unless there is a restorable snapshot which
	//   - begins at or after expireEndVersion
	//   - ends at or before restorableBeginVersion
	// If force is true, data is deleted unconditionally which could leave the backup in an unusable state.  This is not
	// recommended. Returns true if expiration was done.
	virtual Future<Void> expireData(Version expireEndVersion,
	                                bool force = false,
	                                ExpireProgress* progress = nullptr,
	                                Version restorableBeginVersion = std::numeric_limits<Version>::max()) = 0;

	// Delete entire container.  During the process, if pNumDeleted is not null it will be
	// updated with the count of deleted files so that progress can be seen.
	virtual Future<Void> deleteContainer(int* pNumDeleted = nullptr) = 0;

	// Return key details about a backup's contents.
	// Unless deepScan is true, use cached metadata, if present, as initial contiguous available log range.
	// If logStartVersionOverride is given, log data prior to that version will be ignored for the purposes
	// of this describe operation.  This can be used to calculate what the restorability of a backup would
	// be after deleting all data prior to logStartVersionOverride.
	virtual Future<BackupDescription> describeBackup(bool deepScan = false,
	                                                 Version logStartVersionOverride = invalidVersion) = 0;

	virtual Future<BackupFileList> dumpFileList(Version begin = 0,
	                                            Version end = std::numeric_limits<Version>::max()) = 0;

	// Get exactly the files necessary to restore the key space filtered by the specified key ranges to targetVersion.
	// If targetVersion is 'latestVersion', use the minimum restorable version in a snapshot.
	// If logsOnly is set, only use log files in [beginVersion, targetVervions) in restore set.
	// Returns non-present if restoring to the given version is not possible.
	virtual Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion,
	                                                          VectorRef<KeyRangeRef> keyRangesFilter = {},
	                                                          bool logsOnly = false,
	                                                          Version beginVersion = -1) = 0;

	// Get an IBackupContainer based on a container spec string
	static Reference<IBackupContainer> openContainer(const std::string& url,
	                                                 const Optional<std::string>& proxy,
	                                                 const Optional<std::string>& encryptionKeyFileName);
	static std::vector<std::string> getURLFormats();
	static Future<std::vector<std::string>> listContainers(const std::string& baseURL,
	                                                       const Optional<std::string>& proxy);

	std::string const& getURL() const { return URL; }
	Optional<std::string> const& getProxy() const { return proxy; }
	Optional<std::string> const& getEncryptionKeyFileName() const { return encryptionKeyFileName; }

	static std::string lastOpenError;

	// TODO: change the following back to `private` once blob obj access is refactored
protected:
	std::string URL;
	Optional<std::string> proxy;
	Optional<std::string> encryptionKeyFileName;
};

namespace fileBackup {
// Accumulates mutation log value chunks, as both a vector of chunks and as a combined chunk,
// in chunk order, and can check the chunk set for completion or intersection with a set
// of ranges.
struct AccumulatedMutations {
	AccumulatedMutations() : lastChunkNumber(-1) {}

	// Add a KV pair for this mutation chunk set
	// It will be accumulated onto serializedMutations if the chunk number is
	// the next expected value.
	void addChunk(int chunkNumber, const KeyValueRef& kv);

	// Returns true if both
	//   - 1 or more chunks were added to this set
	//   - The header of the first chunk contains a valid protocol version and a length
	//     that matches the bytes after the header in the combined value in serializedMutations
	bool isComplete() const;

	// Returns true if a complete chunk contains any MutationRefs which intersect with any
	// range in ranges.
	// It is undefined behavior to run this if isComplete() does not return true.
	bool matchesAnyRange(const std::vector<KeyRange>& ranges) const;

	std::vector<KeyValueRef> kvs;
	std::string serializedMutations;
	int lastChunkNumber;
};

// Decodes a mutation log key, which contains (hash, commitVersion, chunkNumber) and
// returns (commitVersion, chunkNumber)
std::pair<Version, int32_t> decodeMutationLogKey(const StringRef& key);

// Decodes an encoded list of mutations in the format of:
//   [includeVersion:uint64_t][val_length:uint32_t][mutation_1][mutation_2]...[mutation_k],
// where a mutation is encoded as:
//   [type:uint32_t][keyLength:uint32_t][valueLength:uint32_t][param1][param2]
std::vector<MutationRef> decodeMutationLogValue(const StringRef& value);
} // namespace fileBackup

#endif
