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
#include "fdbrpc/BlobStore.h"
#include "FDBTypes.h"
#include <vector>

// Append-only file interface for writing backup data
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

	Future<Void> appendString(Standalone<StringRef> s);
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

	// Order by beginVersion, break ties with endVersion
	bool operator< (const KeyspaceSnapshotFile &rhs) const {
		return beginVersion == rhs.beginVersion ? endVersion < rhs.endVersion : beginVersion < rhs.beginVersion;
	}
};

struct FullBackupListing {
	std::vector<RangeFile> ranges;
	std::vector<LogFile> logs;
	std::vector<KeyspaceSnapshotFile> snapshots;
};

struct BackupDescription {
	std::string url;
	std::vector<KeyspaceSnapshotFile> snapshots;
	Optional<Version> minLogBegin;
	Optional<Version> maxLogEnd;
	Optional<Version> contiguousLogEnd;
	Optional<Version> maxRestorableVersion;
	Optional<Version> minRestorableVersion;
	std::string extendedDetail;  // Freeform container-specific info.
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

	// Open a log file or range file for writing
	virtual Future<Reference<IBackupFile>> writeLogFile(Version beginVersion, Version endVersion, int blockSize) = 0;
	virtual Future<Reference<IBackupFile>> writeRangeFile(Version version, int blockSize) = 0;

	// Write a KeyspaceSnapshotFile of range file names representing a full non overlapping
	// snapshot of the key ranges this backup is targeting.
	virtual Future<Void> writeKeyspaceSnapshotFile(std::vector<std::string> fileNames, int64_t totalBytes) = 0;

	// Open a file for read by name
	virtual Future<Reference<IAsyncFile>> readFile(std::string name) = 0;

	// Delete all data up to (but not including endVersion)
	virtual Future<Void> expireData(Version endVersion) = 0;

	// Delete entire container.  During the process, if pNumDeleted is not null it will be
	// updated with the count of deleted files so that progress can be seen.
	virtual Future<Void> deleteContainer(int *pNumDeleted = nullptr) = 0;

	// Uses the virtual methods to describe the backup contents
	virtual Future<BackupDescription> describeBackup() = 0;

	virtual Future<FullBackupListing> listBackup() = 0;

	// Get exactly the files necessary to restore to targetVersion.  Returns non-present if
	// restore to given version is not possible.
	virtual Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion) = 0;

	// Get an IBackupContainer based on a container spec string
	static Reference<IBackupContainer> openContainer(std::string url);
	static std::vector<std::string> getURLFormats();
	static std::vector<std::string> listContainers(std::string baseURL);

	std::string getURL() const {
		return URL;
	}

	static std::string lastOpenError;

private:
	std::string URL;
};

