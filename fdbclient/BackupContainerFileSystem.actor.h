/*
 * BackupContainerFileSystem.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_BACKUP_CONTAINER_FILESYSTEM_ACTOR_G_H)
#define FDBCLIENT_BACKUP_CONTAINER_FILESYSTEM_ACTOR_G_H
#include "fdbclient/BackupContainerFileSystem.actor.g.h"
#elif !defined(FDBCLIENT_BACKUP_CONTAINER_FILESYSTEM_H)
#define FDBCLIENT_BACKUP_CONTAINER_FILESYSTEM_H

#include "storage_credential.h"
#include "storage_account.h"
#include "blob/blob_client.h"

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
#include "flow/actorcompiler.h" // has to be last include

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
	virtual Future<FilesAndSizesT> listFiles(std::string path = "",
	                                         std::function<bool(std::string const&)> folderPathFilter = nullptr) = 0;

	// Open a file for read by fileName
	Future<Reference<IAsyncFile>> readFile(std::string fileName) override = 0;

	// Open a file for write by fileName
	virtual Future<Reference<IBackupFile>> writeFile(const std::string& fileName) = 0;

	// Delete a file
	virtual Future<Void> deleteFile(std::string fileName) = 0;

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
	static Version extractSnapshotBeginVersion(std::string path);

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
	static std::string fileNameOnly(std::string path);

	static bool pathToRangeFile(RangeFile& out, std::string path, int64_t size);

	static bool pathToLogFile(LogFile& out, std::string path, int64_t size);

	static bool pathToKeyspaceSnapshotFile(KeyspaceSnapshotFile& out, std::string path);

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

	static Optional<RestorableFileSet> getRestoreSetFromLogs(std::vector<LogFile> logs, Version targetVersion,
	                                                         RestorableFileSet restorable);

	Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion, VectorRef<KeyRangeRef> keyRangesFilter,
	                                                  bool logsOnly, Version beginVersion) final;

private:
	struct VersionProperty {
		VersionProperty(Reference<BackupContainerFileSystem> bc, std::string name)
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

class BackupContainerBlobStore final : public BackupContainerFileSystem, ReferenceCounted<BackupContainerBlobStore> {
private:
	// Backup files to under a single folder prefix with subfolders for each named backup
	static const std::string DATAFOLDER;

	// Indexfolder contains keys for which user-named backups exist.  Backup names can contain an arbitrary
	// number of slashes so the backup names are kept in a separate folder tree from their actual data.
	static const std::string INDEXFOLDER;

	Reference<BlobStoreEndpoint> m_bstore;
	std::string m_name;

	// All backup data goes into a single bucket
	std::string m_bucket;

	std::string dataPath(const std::string path) { return DATAFOLDER + "/" + m_name + "/" + path; }

	// Get the path of the backups's index entry
	std::string indexEntry() { return INDEXFOLDER + "/" + m_name; }

public:
	BackupContainerBlobStore(Reference<BlobStoreEndpoint> bstore, std::string name,
	                         const BlobStoreEndpoint::ParametersT& params)
	  : m_bstore(bstore), m_name(name), m_bucket("FDB_BACKUPS_V2") {

		// Currently only one parameter is supported, "bucket"
		for (auto& kv : params) {
			if (kv.first == "bucket") {
				m_bucket = kv.second;
				continue;
			}
			TraceEvent(SevWarn, "BackupContainerBlobStoreInvalidParameter")
			    .detail("Name", kv.first)
			    .detail("Value", kv.second);
			IBackupContainer::lastOpenError = format("Unknown URL parameter: '%s'", kv.first.c_str());
			throw backup_invalid_url();
		}
	}

	void addref() override { return ReferenceCounted<BackupContainerBlobStore>::addref(); }
	void delref() override { return ReferenceCounted<BackupContainerBlobStore>::delref(); }

	static std::string getURLFormat() {
		return BlobStoreEndpoint::getURLFormat(true) + " (Note: The 'bucket' parameter is required.)";
	}

	Future<Reference<IAsyncFile>> readFile(std::string path) final {
		return Reference<IAsyncFile>(new AsyncFileReadAheadCache(
		    Reference<IAsyncFile>(new AsyncFileBlobStoreRead(m_bstore, m_bucket, dataPath(path))),
		    m_bstore->knobs.read_block_size, m_bstore->knobs.read_ahead_blocks,
		    m_bstore->knobs.concurrent_reads_per_file, m_bstore->knobs.read_cache_blocks_per_file));
	}

	ACTOR static Future<std::vector<std::string>> listURLs(Reference<BlobStoreEndpoint> bstore, std::string bucket) {
		state std::string basePath = INDEXFOLDER + '/';
		BlobStoreEndpoint::ListResult contents = wait(bstore->listObjects(bucket, basePath));
		std::vector<std::string> results;
		for (auto& f : contents.objects) {
			results.push_back(
			    bstore->getResourceURL(f.name.substr(basePath.size()), format("bucket=%s", bucket.c_str())));
		}
		return results;
	}

	class BackupFile : public IBackupFile, ReferenceCounted<BackupFile> {
	public:
		BackupFile(std::string fileName, Reference<IAsyncFile> file) : IBackupFile(fileName), m_file(file) {}

		Future<Void> append(const void* data, int len) {
			Future<Void> r = m_file->write(data, len, m_offset);
			m_offset += len;
			return r;
		}

		Future<Void> finish() {
			Reference<BackupFile> self = Reference<BackupFile>::addRef(this);
			return map(m_file->sync(), [=](Void _) {
				self->m_file.clear();
				return Void();
			});
		}

		void addref() final { return ReferenceCounted<BackupFile>::addref(); }
		void delref() final { return ReferenceCounted<BackupFile>::delref(); }

	private:
		Reference<IAsyncFile> m_file;
	};

	Future<Reference<IBackupFile>> writeFile(const std::string& path) final {
		return Reference<IBackupFile>(new BackupFile(
		    path, Reference<IAsyncFile>(new AsyncFileBlobStoreWrite(m_bstore, m_bucket, dataPath(path)))));
	}

	Future<Void> deleteFile(std::string path) final { return m_bstore->deleteObject(m_bucket, dataPath(path)); }

	ACTOR static Future<FilesAndSizesT> listFiles_impl(Reference<BackupContainerBlobStore> bc, std::string path,
	                                                   std::function<bool(std::string const&)> pathFilter) {
		// pathFilter expects container based paths, so create a wrapper which converts a raw path
		// to a container path by removing the known backup name prefix.
		state int prefixTrim = bc->dataPath("").size();
		std::function<bool(std::string const&)> rawPathFilter = [=](const std::string& folderPath) {
			ASSERT(folderPath.size() >= prefixTrim);
			return pathFilter(folderPath.substr(prefixTrim));
		};

		state BlobStoreEndpoint::ListResult result = wait(bc->m_bstore->listObjects(
		    bc->m_bucket, bc->dataPath(path), '/', std::numeric_limits<int>::max(), rawPathFilter));
		FilesAndSizesT files;
		for (auto& o : result.objects) {
			ASSERT(o.name.size() >= prefixTrim);
			files.push_back({ o.name.substr(prefixTrim), o.size });
		}
		return files;
	}

	Future<FilesAndSizesT> listFiles(std::string path, std::function<bool(std::string const&)> pathFilter) final {
		return listFiles_impl(Reference<BackupContainerBlobStore>::addRef(this), path, pathFilter);
	}

	ACTOR static Future<Void> create_impl(Reference<BackupContainerBlobStore> bc) {
		wait(bc->m_bstore->createBucket(bc->m_bucket));

		// Check/create the index entry
		bool exists = wait(bc->m_bstore->objectExists(bc->m_bucket, bc->indexEntry()));
		if (!exists) {
			wait(bc->m_bstore->writeEntireFile(bc->m_bucket, bc->indexEntry(), ""));
		}

		return Void();
	}

	Future<Void> create() final { return create_impl(Reference<BackupContainerBlobStore>::addRef(this)); }

	// The container exists if the index entry in the blob bucket exists
	Future<bool> exists() final { return m_bstore->objectExists(m_bucket, indexEntry()); }

	ACTOR static Future<Void> deleteContainer_impl(Reference<BackupContainerBlobStore> bc, int* pNumDeleted) {
		bool e = wait(bc->exists());
		if (!e) {
			TraceEvent(SevWarnAlways, "BackupContainerDoesNotExist").detail("URL", bc->getURL());
			throw backup_does_not_exist();
		}

		// First delete everything under the data prefix in the bucket
		wait(bc->m_bstore->deleteRecursively(bc->m_bucket, bc->dataPath(""), pNumDeleted));

		// Now that all files are deleted, delete the index entry
		wait(bc->m_bstore->deleteObject(bc->m_bucket, bc->indexEntry()));

		return Void();
	}

	Future<Void> deleteContainer(int* pNumDeleted) final {
		return deleteContainer_impl(Reference<BackupContainerBlobStore>::addRef(this), pNumDeleted);
	}

	std::string getBucket() const { return m_bucket; }
};

class BackupContainerAzureBlobStore final : public BackupContainerFileSystem,
                                            ReferenceCounted<BackupContainerAzureBlobStore> {

	using AzureClient = azure::storage_lite::blob_client;

	std::unique_ptr<AzureClient> client;
	std::string containerName;
	AsyncTaskThread asyncTaskThread;

	class ReadFile final : public IAsyncFile, ReferenceCounted<ReadFile> {
		AsyncTaskThread& asyncTaskThread;
		std::string containerName;
		std::string blobName;
		AzureClient* client;

	public:
		ReadFile(AsyncTaskThread& asyncTaskThread, const std::string& containerName, const std::string& blobName,
		         AzureClient* client)
		  : asyncTaskThread(asyncTaskThread), containerName(containerName), blobName(blobName), client(client) {}

		void addref() override { ReferenceCounted<ReadFile>::addref(); }
		void delref() override { ReferenceCounted<ReadFile>::delref(); }
		Future<int> read(void* data, int length, int64_t offset) {
			return asyncTaskThread.execAsync([client = this->client, containerName = this->containerName,
			                                  blobName = this->blobName, data, length, offset] {
				std::ostringstream oss(std::ios::out | std::ios::binary);
				client->download_blob_to_stream(containerName, blobName, offset, length, oss);
				auto str = oss.str();
				memcpy(data, str.c_str(), str.size());
				return static_cast<int>(str.size());
			});
		}
		Future<Void> zeroRange(int64_t offset, int64_t length) override { throw file_not_writable(); }
		Future<Void> write(void const* data, int length, int64_t offset) override { throw file_not_writable(); }
		Future<Void> truncate(int64_t size) override { throw file_not_writable(); }
		Future<Void> sync() override { throw file_not_writable(); }
		Future<int64_t> size() const override {
			return asyncTaskThread.execAsync([client = this->client, containerName = this->containerName,
			                                  blobName = this->blobName] {
				return static_cast<int64_t>(client->get_blob_properties(containerName, blobName).get().response().size);
			});
		}
		std::string getFilename() const override { return blobName; }
		int64_t debugFD() const override { return 0; }
	};

	class WriteFile final : public IAsyncFile, ReferenceCounted<WriteFile> {
		AsyncTaskThread& asyncTaskThread;
		AzureClient* client;
		std::string containerName;
		std::string blobName;
		int64_t m_cursor{ 0 };
		std::string buffer;

		static constexpr size_t bufferLimit = 1 << 20;

		// From https://tuttlem.github.io/2014/08/18/getting-istream-to-work-off-a-byte-array.html:
		class MemStream : public std::istream {
			class MemBuf : public std::basic_streambuf<char> {
			public:
				MemBuf(const uint8_t* p, size_t l) { setg((char*)p, (char*)p, (char*)p + l); }
			} buffer;

		public:
			MemStream(const uint8_t* p, size_t l) : std::istream(&buffer), buffer(p, l) { rdbuf(&buffer); }
		};

	public:
		WriteFile(AsyncTaskThread& asyncTaskThread, const std::string& containerName, const std::string& blobName,
		          AzureClient* client)
		  : asyncTaskThread(asyncTaskThread), containerName(containerName), blobName(blobName), client(client) {}

		void addref() override { ReferenceCounted<WriteFile>::addref(); }
		void delref() override { ReferenceCounted<WriteFile>::delref(); }
		Future<int> read(void* data, int length, int64_t offset) override { throw file_not_readable(); }
		Future<Void> write(void const* data, int length, int64_t offset) override {
			if (offset != m_cursor) {
				throw non_sequential_op();
			}
			m_cursor += length;
			auto p = static_cast<char const*>(data);
			buffer.insert(buffer.cend(), p, p + length);
			if (buffer.size() > bufferLimit) {
				return sync();
			} else {
				return Void();
			}
		}
		Future<Void> truncate(int64_t size) override {
			if (size != m_cursor) {
				throw non_sequential_op();
			}
			return Void();
		}
		Future<Void> sync() override {
			return asyncTaskThread.execAsync([client = this->client, containerName = this->containerName,
			                                  blobName = this->blobName, buffer = std::move(this->buffer)] {
				// MemStream memStream(buffer.data(), buffer.size());
				std::istringstream iss(buffer);
				auto resp = client->append_block_from_stream(containerName, blobName, iss).get();
				return Void();
			});
		}
		Future<int64_t> size() const override {
			return asyncTaskThread.execAsync(
			    [client = this->client, containerName = this->containerName, blobName = this->blobName] {
				    auto resp = client->get_blob_properties(containerName, blobName).get().response();
				    ASSERT(resp.valid()); // TODO: Should instead throw here
				    return static_cast<int64_t>(resp.size);
			    });
		}
		std::string getFilename() const override { return blobName; }
		int64_t debugFD() const override { return -1; }
	};

	class BackupFile final : public IBackupFile, ReferenceCounted<BackupFile> {
		Reference<IAsyncFile> m_file;

	public:
		BackupFile(const std::string& fileName, Reference<IAsyncFile> file) : IBackupFile(fileName), m_file(file) {}
		Future<Void> append(const void* data, int len) override {
			Future<Void> r = m_file->write(data, len, m_offset);
			m_offset += len;
			return r;
		}
		Future<Void> finish() override {
			Reference<BackupFile> self = Reference<BackupFile>::addRef(this);
			return map(m_file->sync(), [=](Void _) {
				self->m_file.clear();
				return Void();
			});
		}
		void addref() override { ReferenceCounted<BackupFile>::addref(); }
		void delref() override { ReferenceCounted<BackupFile>::delref(); }
	};

	Future<bool> blobExists(const std::string& fileName) {
		return asyncTaskThread.execAsync(
		    [client = this->client.get(), containerName = this->containerName, fileName = fileName] {
			    auto resp = client->get_blob_properties(containerName, fileName).get().response();
			    return resp.valid();
		    });
	}

	static bool isDirectory(const std::string& blobName) { return blobName.size() && blobName.back() == '/'; }

	ACTOR static Future<Reference<IAsyncFile>> readFile_impl(BackupContainerAzureBlobStore* self,
	                                                         std::string fileName) {
		bool exists = wait(self->blobExists(fileName));
		if (!exists) {
			throw file_not_found();
		}
		return Reference<IAsyncFile>(
		    new ReadFile(self->asyncTaskThread, self->containerName, fileName, self->client.get()));
	}

	ACTOR static Future<Reference<IBackupFile>> writeFile_impl(BackupContainerAzureBlobStore* self,
	                                                           std::string fileName) {
		wait(self->asyncTaskThread.execAsync(
		    [client = self->client.get(), containerName = self->containerName, fileName = fileName] {
			    auto outcome = client->create_append_blob(containerName, fileName).get();
			    return Void();
		    }));
		return Reference<IBackupFile>(
		    new BackupFile(fileName, Reference<IAsyncFile>(new WriteFile(self->asyncTaskThread, self->containerName,
		                                                                 fileName, self->client.get()))));
	}

	static void listFilesImpl(AzureClient* client, const std::string& containerName, const std::string& path,
	                          std::function<bool(std::string const&)> folderPathFilter, FilesAndSizesT& result) {
		auto resp = client->list_blobs_segmented(containerName, "/", "", path).get().response();
		for (const auto& blob : resp.blobs) {
			if (isDirectory(blob.name) && folderPathFilter(blob.name)) {
				listFilesImpl(client, containerName, blob.name, folderPathFilter, result);
			} else {
				result.emplace_back(blob.name, blob.content_length);
			}
		}
	}

	ACTOR static Future<Void> deleteContainerImpl(BackupContainerAzureBlobStore* self, int* pNumDeleted) {
		state int filesToDelete = 0;
		if (pNumDeleted) {
			FilesAndSizesT files = wait(self->listFiles());
			filesToDelete = files.size();
		}
		wait(self->asyncTaskThread.execAsync([containerName = self->containerName, client = self->client.get()] {
			client->delete_container(containerName).wait();
			return Void();
		}));
		if (pNumDeleted) {
			*pNumDeleted += filesToDelete;
		}
		return Void();
	}

public:
	BackupContainerAzureBlobStore() : containerName("test_container") {
		// std::string account_name = std::getenv("AZURE_TESTACCOUNT");
		// std::string account_key = std::getenv("AZURE_TESTKEY");
		// bool use_https = true;

		// auto credential = std::make_shared<azure::storage_lite::shared_key_credential>(account_name, account_key);
		// auto storage_account =
		//    std::make_shared<azure::storage_lite::storage_account>(account_name, credential, use_https);

		auto storage_account = azure::storage_lite::storage_account::development_storage_account();

		client = std::make_unique<AzureClient>(storage_account, 1);
	}

	void addref() override { return ReferenceCounted<BackupContainerAzureBlobStore>::addref(); }
	void delref() override { return ReferenceCounted<BackupContainerAzureBlobStore>::delref(); }

	Future<Void> create() override {
		return asyncTaskThread.execAsync([containerName = this->containerName, client = this->client.get()] {
			client->create_container(containerName).wait();
			return Void();
		});
	}
	Future<bool> exists() override {
		return asyncTaskThread.execAsync([containerName = this->containerName, client = this->client.get()] {
			auto resp = client->get_container_properties(containerName).get().response();
			return resp.valid();
		});
	}

	Future<Reference<IAsyncFile>> readFile(std::string fileName) override { return readFile_impl(this, fileName); }

	Future<Reference<IBackupFile>> writeFile(const std::string& fileName) override {
		return writeFile_impl(this, fileName);
	}

	Future<FilesAndSizesT> listFiles(std::string path = "",
	                                 std::function<bool(std::string const&)> folderPathFilter = nullptr) {
		return asyncTaskThread.execAsync([client = this->client.get(), containerName = this->containerName, path = path,
		                                  folderPathFilter = folderPathFilter] {
			FilesAndSizesT result;
			listFilesImpl(client, containerName, path, folderPathFilter, result);
			return result;
		});
	}

	Future<Void> deleteFile(std::string fileName) override {
		return asyncTaskThread.execAsync(
		    [containerName = this->containerName, fileName = fileName, client = client.get()]() {
			    client->delete_blob(containerName, fileName).wait();
			    return Void();
		    });
	}

	Future<Void> deleteContainer(int* pNumDeleted) override { return deleteContainerImpl(this, pNumDeleted); }
};

#include "flow/unactorcompiler.h"
#endif
