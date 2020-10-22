/*
 * BackupContainerFileSystem.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_BACKUP_CONTAINER_FILESYSTEM_H
#define FDBCLIENT_BACKUP_CONTAINER_FILESYSTEM_H
#pragma once

// FIXME: Trim this down
#include "flow/Platform.actor.h"
#include "fdbclient/AsyncTaskThread.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/JsonBuilder.h"
#include "flow/Arena.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/Hash3.h"
#include "fdbrpc/AsyncFileReadAhead.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/Platform.h"
#include "fdbclient/AsyncFileBlobStore.actor.h"
#include "fdbclient/Status.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/RunTransaction.actor.h"
#include <algorithm>
#include <cinttypes>
#include <time.h>

#include "fdbclient/BackupContainer.h"

/* BackupContainerFileSystem implements a backup container which stores files in a nested folder structure.
 * Inheritors must only defined methods for writing, reading, deleting, sizing, and listing files.
 *
 *   Snapshot manifests (a complete set of files constituting a database snapshot for the backup's target ranges)
 *   are stored as JSON files at paths like
 *       /snapshots/snapshot,minVersion,maxVersion,totalBytes
 *
 *   Key range files for snapshots are stored at paths like
 *       /kvranges/snapshot,startVersion/N/range,version,uid,blockSize
 *     where startVersion is the version at which the backup snapshot execution began and N is a number
 *     that is increased as key range files are generated over time (at varying rates) such that there
 *     are around 5,000 key range files in each folder.
 *
 *     Note that startVersion will NOT correspond to the minVersion of a snapshot manifest because
 *     snapshot manifest min/max versions are based on the actual contained data and the first data
 *     file written will be after the start version of the snapshot's execution.
 *
 *   Log files are at file paths like
 *       /plogs/.../log,startVersion,endVersion,UID,tagID-of-N,blocksize
 *       /logs/.../log,startVersion,endVersion,UID,blockSize
 *     where ... is a multi level path which sorts lexically into version order and results in approximately 1
 *     unique folder per day containing about 5,000 files. Logs after FDB 6.3 are stored in "plogs"
 *     directory and are partitioned according to tagIDs (0, 1, 2, ...) and the total number partitions is N.
 *     Old backup logs FDB 6.2 and earlier are stored in "logs" directory and are not partitioned.
 *     After FDB 6.3, users can choose to use the new partitioned logs or old logs.
 *
 *
 *   BACKWARD COMPATIBILITY
 *
 *   Prior to FDB version 6.0.16, key range files were stored using a different folder scheme.  Newer versions
 *   still support this scheme for all restore and backup management operations but key range files generated
 *   by backup using version 6.0.16 or later use the scheme describe above.
 *
 *   The old format stored key range files at paths like
 *       /ranges/.../range,version,uid,blockSize
 *     where ... is a multi level path with sorts lexically into version order and results in up to approximately
 *     900 unique folders per day.  The number of files per folder depends on the configured snapshot rate and
 *     database size and will vary from 1 to around 5,000.
 */
class BackupContainerFileSystem : public IBackupContainer {
public:
	void addref() override = 0;
	void delref() override = 0;

	BackupContainerFileSystem() {}
	virtual ~BackupContainerFileSystem() {}

	// Create the container
	Future<Void> create() override = 0;
	Future<bool> exists() override = 0;

	// Get a list of fileNames and their sizes in the container under the given path
	// Although not required, an implementation can avoid traversing unwanted subfolders
	// by calling folderPathFilter(absoluteFolderPath) and checking for a false return value.
	using FilesAndSizesT = std::vector<std::pair<std::string, int64_t>>;
	virtual Future<FilesAndSizesT> listFiles(const std::string& path = "",
	                                         std::function<bool(std::string const&)> folderPathFilter = nullptr) = 0;

	// Open a file for read by fileName
	Future<Reference<IAsyncFile>> readFile(const std::string& fileName) override = 0;

	// Open a file for write by fileName
	virtual Future<Reference<IBackupFile>> writeFile(const std::string& fileName) = 0;

	// Delete a file
	virtual Future<Void> deleteFile(const std::string& fileName) = 0;

	// Delete entire container.  During the process, if pNumDeleted is not null it will be
	// updated with the count of deleted files so that progress can be seen.
	Future<Void> deleteContainer(int* pNumDeleted) override = 0;

	// Creates a 2-level path (x/y) where v should go such that x/y/* contains (10^smallestBucket) possible versions
	static std::string versionFolderString(Version v, int smallestBucket);

	// This useful for comparing version folder strings regardless of where their "/" dividers are, as it is possible
	// that division points would change in the future.
	static std::string cleanFolderString(std::string f);

	// The innermost folder covers 100 seconds (1e8 versions) During a full speed backup it is possible though very
	// unlikely write about 10,000 snapshot range files during that time.
	static std::string old_rangeVersionFolderString(Version v);

	// Get the root folder for a snapshot's data based on its begin version
	static std::string snapshotFolderString(Version snapshotBeginVersion);

	// Extract the snapshot begin version from a path
	static Version extractSnapshotBeginVersion(const std::string& path);

	// The innermost folder covers 100,000 seconds (1e11 versions) which is 5,000 mutation log files at current
	// settings.
	static std::string logVersionFolderString(Version v, bool partitioned);

	Future<Reference<IBackupFile>> writeLogFile(Version beginVersion, Version endVersion, int blockSize) final;

	Future<Reference<IBackupFile>> writeTaggedLogFile(Version beginVersion, Version endVersion, int blockSize,
	                                                  uint16_t tagId, int totalTags) final;

	Future<Reference<IBackupFile>> writeRangeFile(Version snapshotBeginVersion, int snapshotFileCount,
	                                              Version fileVersion, int blockSize) override;

	// Find what should be the filename of a path by finding whatever is after the last forward or backward slash, or
	// failing to find those, the whole string.
	static std::string fileNameOnly(const std::string& path);

	static bool pathToRangeFile(RangeFile& out, const std::string& path, int64_t size);

	static bool pathToLogFile(LogFile& out, const std::string& path, int64_t size);

	static bool pathToKeyspaceSnapshotFile(KeyspaceSnapshotFile& out, const std::string& path);

	Future<std::pair<std::vector<RangeFile>, std::map<std::string, KeyRange>>> readKeyspaceSnapshot(
	    KeyspaceSnapshotFile snapshot);

	Future<Void> writeKeyspaceSnapshotFile(const std::vector<std::string>& fileNames,
	                                       const std::vector<std::pair<Key, Key>>& beginEndKeys,
	                                       int64_t totalBytes) final;

	// List log files, unsorted, which contain data at any version >= beginVersion and <= targetVersion.
	// "partitioned" flag indicates if new partitioned mutation logs or old logs should be listed.
	Future<std::vector<LogFile>> listLogFiles(Version beginVersion, Version targetVersion, bool partitioned);

	// List range files, unsorted, which contain data at or between beginVersion and endVersion
	// NOTE: This reads the range file folder schema from FDB 6.0.15 and earlier and is provided for backward
	// compatibility
	Future<std::vector<RangeFile>> old_listRangeFiles(Version beginVersion, Version endVersion);

	// List range files, unsorted, which contain data at or between beginVersion and endVersion
	// Note: The contents of each top level snapshot.N folder do not necessarily constitute a valid snapshot
	// and therefore listing files is not how RestoreSets are obtained.
	// Note: Snapshots partially written using FDB versions prior to 6.0.16 will have some range files stored
	// using the old folder scheme read by old_listRangeFiles
	Future<std::vector<RangeFile>> listRangeFiles(Version beginVersion, Version endVersion);

	// List snapshots which have been fully written, in sorted beginVersion order, which start before end and finish on
	// or after begin
	Future<std::vector<KeyspaceSnapshotFile>> listKeyspaceSnapshots(Version begin = 0,
	                                                                Version end = std::numeric_limits<Version>::max());

	Future<BackupFileList> dumpFileList(Version begin, Version end) override;

	static Version resolveRelativeVersion(Optional<Version> max, Version v, const char* name, Error e);

	// Computes the continuous end version for non-partitioned mutation logs up to
	// the "targetVersion". If "outLogs" is not nullptr, it will be updated with
	// continuous log files. "*end" is updated with the continuous end version.
	static void computeRestoreEndVersion(const std::vector<LogFile>& logs, std::vector<LogFile>* outLogs, Version* end,
	                                     Version targetVersion);

	// Uses the virtual methods to describe the backup contents
	Future<BackupDescription> describeBackup(bool deepScan, Version logStartVersionOverride) final;

	// Delete all data up to (but not including endVersion)
	Future<Void> expireData(Version expireEndVersion, bool force, ExpireProgress* progress,
	                        Version restorableBeginVersion) final;

	// For a list of log files specified by their indices (of the same tag),
	// returns if they are continous in the range [begin, end]. If "tags" is not
	// nullptr, then it will be populated with [begin, end] -> tags, where next
	// pair's begin <= previous pair's end + 1. On return, the last pair's end
	// version (inclusive) gives the continuous range from begin.
	static bool isContinuous(const std::vector<LogFile>& files, const std::vector<int>& indices, Version begin,
	                         Version end, std::map<std::pair<Version, Version>, int>* tags);

	// Returns true if logs are continuous in the range [begin, end].
	// "files" should be pre-sorted according to version order.
	static bool isPartitionedLogsContinuous(const std::vector<LogFile>& files, Version begin, Version end);

	// Returns log files that are not duplicated, or subset of another log.
	// If a log file's progress is not saved, a new log file will be generated
	// with the same begin version. So we can have a file that contains a subset
	// of contents in another log file.
	// PRE-CONDITION: logs are already sorted by (tagId, beginVersion, endVersion).
	static std::vector<LogFile> filterDuplicates(const std::vector<LogFile>& logs);

	// Analyze partitioned logs and set contiguousLogEnd for "desc" if larger
	// than the "scanBegin" version.
	static void updatePartitionedLogsContinuousEnd(BackupDescription* desc, const std::vector<LogFile>& logs,
	                                               const Version scanBegin, const Version scanEnd);

	// Returns the end version such that [begin, end] is continuous.
	// "logs" should be already sorted.
	static Version getPartitionedLogsContinuousEndVersion(const std::vector<LogFile>& logs, Version begin);

	Future<KeyRange> getSnapshotFileKeyRange(const RangeFile& file) final;

	static Optional<RestorableFileSet> getRestoreSetFromLogs(const std::vector<LogFile>& logs, Version targetVersion,
	                                                         RestorableFileSet restorable);

	Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion, VectorRef<KeyRangeRef> keyRangesFilter,
	                                                  bool logsOnly, Version beginVersion) final;

private:
	struct VersionProperty {
		VersionProperty(Reference<BackupContainerFileSystem> bc, const std::string& name)
		  : bc(bc), path("properties/" + name) {}
		Reference<BackupContainerFileSystem> bc;
		std::string path;
		Future<Optional<Version>> get();
		Future<Void> set(Version v);
		Future<Void> clear();
	};

public:
	// To avoid the need to scan the underyling filesystem in many cases, some important version boundaries are stored
	// in named files. These versions also indicate what version ranges are known to be deleted or partially deleted.
	//
	// The values below describe version ranges as follows:
	//                   0 - expiredEndVersion      All files in this range have been deleted
	//   expiredEndVersion - unreliableEndVersion   Some files in this range may have been deleted.
	//
	//   logBeginVersion   - logEnd                 Log files are contiguous in this range and have NOT been deleted by
	//   fdbbackup logEnd            - infinity               Files in this range may or may not exist yet
	//
	VersionProperty logBeginVersion();
	VersionProperty logEndVersion();
	VersionProperty expiredEndVersion();
	VersionProperty unreliableEndVersion();

	// Backup log types
	static constexpr Version NON_PARTITIONED_MUTATION_LOG = 0;
	static constexpr Version PARTITIONED_MUTATION_LOG = 1;
	VersionProperty logType();
};

#endif
