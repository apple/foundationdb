/*
 * BackupContainer.h
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

#pragma once

#include "flow/flow.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include <vector>

Future<Optional<int64_t>> timeKeeperEpochsFromVersion(Version const &v, Reference<ReadYourWritesTransaction> const &tr);
Future<Version> timeKeeperVersionFromDatetime(std::string const &datetime, Database const &db);

// Append-only file interface for writing backup data
// Once finish() is called the file cannot be further written to.
// Backup containers should not attempt to use files for which finish was not called or did not complete.
// TODO: Move the log file and range file format encoding/decoding stuff to this file and behind interfaces.
class IBackupFile {
public:
	IBackupFile(std::string fileName) : m_fileName(fileName), m_offset(0) {}
	virtual ~IBackupFile() {}
	// Backup files are append-only and cannot have more than 1 append outstanding at once.
	virtual Future<Void> append(const void *data, int len) = 0;
	virtual Future<Void> finish() = 0;
	inline std::string getFileName() const {
		return m_fileName;
	}
	inline int64_t size() const {
		return m_offset;
	}
	virtual void addref() = 0;
	virtual void delref() = 0;

	Future<Void> appendStringRefWithLen(Standalone<StringRef> s);
protected:
	std::string m_fileName;
	int64_t m_offset;
};

// Structures for various backup components

struct LogFile {
	Version beginVersion;
	Version endVersion;
	uint32_t blockSize;
	std::string fileName;
	int64_t fileSize;

	// Order by beginVersion, break ties with endVersion
	bool operator< (const LogFile &rhs) const {
		return beginVersion == rhs.beginVersion ? endVersion < rhs.endVersion : beginVersion < rhs.beginVersion;
	}
};

struct RangeFile {
	Version version;
	uint32_t blockSize;
	std::string fileName;
	int64_t fileSize;

	// Order by version, break ties with name
	bool operator< (const RangeFile &rhs) const {
		return version == rhs.version ? fileName < rhs.fileName : version < rhs.version;
	}
};

struct KeyspaceSnapshotFile {
	Version beginVersion;
	Version endVersion;
	std::string fileName;
	int64_t totalSize;
	Optional<bool> restorable;  // Whether or not the snapshot can be used in a restore, if known

	// Order by beginVersion, break ties with endVersion
	bool operator< (const KeyspaceSnapshotFile &rhs) const {
		return beginVersion == rhs.beginVersion ? endVersion < rhs.endVersion : beginVersion < rhs.beginVersion;
	}
};

struct BackupFileList {
	std::vector<RangeFile> ranges;
	std::vector<LogFile> logs;
	std::vector<KeyspaceSnapshotFile> snapshots;

	void toStream(FILE *fout) const;
};

// The byte counts here only include usable log files and byte counts from kvrange manifests
struct BackupDescription {
	BackupDescription() : snapshotBytes(0) {}
	std::string url;
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
	std::string extendedDetail;  // Freeform container-specific info.

	// Resolves the versions above to timestamps using a given database's TimeKeeper data.
	// toString will use this information if present.
	Future<Void> resolveVersionTimes(Database cx);
	std::map<Version, int64_t> versionTimeMap;

	std::string toString() const;
};

struct RestorableFileSet {
	Version targetVersion;
	std::vector<LogFile> logs;
	std::vector<RangeFile> ranges;
	KeyspaceSnapshotFile snapshot;
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
	virtual Future<Reference<IBackupFile>> writeRangeFile(Version snapshotBeginVersion, int snapshotFileCount, Version fileVersion, int blockSize) = 0;

	// Write a KeyspaceSnapshotFile of range file names representing a full non overlapping
	// snapshot of the key ranges this backup is targeting.
	virtual Future<Void> writeKeyspaceSnapshotFile(std::vector<std::string> fileNames, int64_t totalBytes) = 0;

	// Open a file for read by name
	virtual Future<Reference<IAsyncFile>> readFile(std::string name) = 0;

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
	// If force is true, data is deleted unconditionally which could leave the backup in an unusable state.  This is not recommended.
	// Returns true if expiration was done.
	virtual Future<Void> expireData(Version expireEndVersion, bool force = false, ExpireProgress *progress = nullptr, Version restorableBeginVersion = std::numeric_limits<Version>::max()) = 0;

	// Delete entire container.  During the process, if pNumDeleted is not null it will be
	// updated with the count of deleted files so that progress can be seen.
	virtual Future<Void> deleteContainer(int *pNumDeleted = nullptr) = 0;

	// Return key details about a backup's contents.
	// Unless deepScan is true, use cached metadata, if present, as initial contiguous available log range.
	// If logStartVersionOverride is given, log data prior to that version will be ignored for the purposes
	// of this describe operation.  This can be used to calculate what the restorability of a backup would
	// be after deleting all data prior to logStartVersionOverride.
	virtual Future<BackupDescription> describeBackup(bool deepScan = false, Version logStartVersionOverride = invalidVersion) = 0;

	virtual Future<BackupFileList> dumpFileList(Version begin = 0, Version end = std::numeric_limits<Version>::max()) = 0;

	// Get exactly the files necessary to restore to targetVersion.  Returns non-present if
	// restore to given version is not possible.
	virtual Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion) = 0;

	// Get an IBackupContainer based on a container spec string
	static Reference<IBackupContainer> openContainer(std::string url);
	static std::vector<std::string> getURLFormats();
	static Future<std::vector<std::string>> listContainers(std::string baseURL);

	std::string getURL() const {
		return URL;
	}

	static std::string lastOpenError;

private:
	std::string URL;
};

