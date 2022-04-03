/*
 * BackupContainerFileSystem.h
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

#ifndef FDBCLIENT_BACKUP_CONTAINER_FILESYSTEM_H
#define FDBCLIENT_BACKUP_CONTAINER_FILESYSTEM_H
#pragma once

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/FDBTypes.h"
#include "flow/Trace.h"

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
	~BackupContainerFileSystem() override {}

	// Create the container
	Future<Void> create() override = 0;
	Future<bool> exists() override = 0;

	// TODO: refactor this to separate out the "deal with blob store" stuff from the backup business logic
	static Reference<BackupContainerFileSystem> openContainerFS(const std::string& url,
	                                                            const Optional<std::string>& proxy,
	                                                            const Optional<std::string>& encryptionKeyFileName);

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

	Future<Reference<IBackupFile>> writeLogFile(Version beginVersion, Version endVersion, int blockSize) final;

	Future<Reference<IBackupFile>> writeTaggedLogFile(Version beginVersion,
	                                                  Version endVersion,
	                                                  int blockSize,
	                                                  uint16_t tagId,
	                                                  int totalTags) final;

	Future<Reference<IBackupFile>> writeRangeFile(Version snapshotBeginVersion,
	                                              int snapshotFileCount,
	                                              Version fileVersion,
	                                              int blockSize) override;

	Future<std::pair<std::vector<RangeFile>, std::map<std::string, KeyRange>>> readKeyspaceSnapshot(
	    KeyspaceSnapshotFile snapshot);

	Future<Void> writeKeyspaceSnapshotFile(const std::vector<std::string>& fileNames,
	                                       const std::vector<std::pair<Key, Key>>& beginEndKeys,
	                                       int64_t totalBytes) final;

	// List log files, unsorted, which contain data at any version >= beginVersion and <= targetVersion.
	// "partitioned" flag indicates if new partitioned mutation logs or old logs should be listed.
	Future<std::vector<LogFile>> listLogFiles(Version beginVersion, Version targetVersion, bool partitioned);

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

	// Uses the virtual methods to describe the backup contents
	Future<BackupDescription> describeBackup(bool deepScan, Version logStartVersionOverride) final;

	// Delete all data up to (but not including endVersion)
	Future<Void> expireData(Version expireEndVersion,
	                        bool force,
	                        ExpireProgress* progress,
	                        Version restorableBeginVersion) final;

	Future<KeyRange> getSnapshotFileKeyRange(const RangeFile& file) final;

	Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion,
	                                                  VectorRef<KeyRangeRef> keyRangesFilter,
	                                                  bool logsOnly,
	                                                  Version beginVersion) final;
	static Future<Void> createTestEncryptionKeyFile(std::string const& filename);

protected:
	bool usesEncryption() const;
	void setEncryptionKey(Optional<std::string> const& encryptionKeyFileName);
	Future<Void> encryptionSetupComplete() const;

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
	VersionProperty logType();

	// List range files, unsorted, which contain data at or between beginVersion and endVersion
	// NOTE: This reads the range file folder schema from FDB 6.0.15 and earlier and is provided for backward
	// compatibility
	Future<std::vector<RangeFile>> old_listRangeFiles(Version beginVersion, Version endVersion);

	friend class BackupContainerFileSystemImpl;

	Future<Void> encryptionSetupFuture;
};

#endif
