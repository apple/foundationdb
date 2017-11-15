/*
 * BackupContainer.actor.cpp
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

#include "BackupContainer.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/Hash3.h"
#include "fdbrpc/AsyncFileBlobStore.actor.h"
#include "fdbrpc/AsyncFileReadAhead.actor.h"
#include "fdbrpc/Platform.h"
#include "fdbclient/Status.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>

namespace IBackupFile_impl {

	ACTOR Future<Void> appendString(Reference<IBackupFile> file, Standalone<StringRef> s) {
		state uint32_t lenBuf = bigEndian32((uint32_t)s.size());
		Void _ = wait(file->append(&lenBuf, sizeof(lenBuf)));
		Void _ = wait(file->append(s.begin(), s.size()));
		return Void();
	}
}

Future<Void> IBackupFile::appendString(Standalone<StringRef> s) {
	return IBackupFile_impl::appendString(Reference<IBackupFile>::addRef(this), s);
}

std::string BackupDescription::toString() const {
	std::string info;

	info.append(format("URL: %s\n", url.c_str()));
	info.append(format("Restorable: %s\n", maxRestorableVersion.present() ? "true" : "false"));

	for(const KeyspaceSnapshotFile &m : snapshots)
		info.append(format("Snapshot:  startVersion=%lld  endVersion=%lld  totalBytes=%lld\n", m.beginVersion, m.endVersion, m.totalSize));

	if(minLogBegin.present())
		info.append(format("MinLogBeginVersion: %lld\n", minLogBegin.get()));
	if(contiguousLogEnd.present())
		info.append(format("MaxContiguousLogVersion: %lld\n", contiguousLogEnd.get()));
	if(maxLogEnd.present())
		info.append(format("MaxLogEndVersion: %lld\n", maxLogEnd.get()));
	if(minRestorableVersion.present())
		info.append(format("MinRestorableVersion: %lld\n", minRestorableVersion.get()));
	if(maxRestorableVersion.present())
		info.append(format("MaxRestorableVersion: %lld\n", maxRestorableVersion.get()));

	if(!extendedDetail.empty())
		info.append("ExtendedDetail: ").append(extendedDetail);

	return info;
}

/* BackupContainerFileSystem implements a backup container which stores files in a nested folder structure.
 * Inheritors must only defined methods for writing, reading, deleting, sizing, and listing files.
 *
 *   BackupInfo is stored as a JSON document at
 *     /info
 *   Snapshots are stored as JSON at file paths like
 *     /snapshots/snapshot,startVersion,endVersion,totalBytes
 *   Log and Range data files at file paths like
 *     /logs/.../log,startVersion,endVersion,blockSize
 *     /ranges/.../range,version,uid,blockSize
 *
 *   Where ... is a multi level path which causes each level to not have more than 10,000 entries.
 *     v - (v % 1e15) / v - (v % 1e11) / (v - (v ^ 1e7)
 */
class BackupContainerFileSystem : public IBackupContainer {
public:
	virtual void addref() = 0;
	virtual void delref() = 0;

	BackupContainerFileSystem() {}
	virtual ~BackupContainerFileSystem() {}

	// Create the container
	virtual Future<Void> create() = 0;

	// Get a list of fileNames and their sizes in the container under the given path
	typedef std::vector<std::pair<std::string, int64_t>> FilesAndSizesT;
	virtual Future<FilesAndSizesT> listFiles(std::string path = "") = 0;

	// Open a file for read by fileName
	virtual Future<Reference<IAsyncFile>> readFile(std::string fileName) = 0;

	// Open a file for write by fileName
	virtual Future<Reference<IBackupFile>> writeFile(std::string fileName) = 0;

	// Delete a file
	virtual Future<Void> deleteFile(std::string fileName) = 0;

	// Delete entire container.  During the process, if pNumDeleted is not null it will be
	// updated with the count of deleted files so that progress can be seen.
	virtual Future<Void> deleteContainer(int *pNumDeleted) = 0;


	static std::string versionFolderString(Version v) {
		return format("%lld/%lld/%lld", v - (v % (int64_t)1e15), v - (v % (int64_t)1e11), v - (v % (int64_t)1e7));
	}

	Future<Reference<IBackupFile>> writeLogFile(Version beginVersion, Version endVersion, int blockSize) {
		return writeFile(format("logs/%s/log,%lld,%lld,%d", versionFolderString(beginVersion).c_str(), beginVersion, endVersion, blockSize));
	}

	Future<Reference<IBackupFile>> writeRangeFile(Version version, int blockSize) {
		return writeFile(format("ranges/%s/range,%lld,%s,%d", versionFolderString(version).c_str(), version, g_random->randomUniqueID().toString().c_str(), blockSize));
	}

	static RangeFile pathToRangeFile(std::string path, int64_t size) {
		std::string name = basename(path);
		RangeFile f;
		f.fileName = path;
		f.fileSize = size;
		int len;
		if(sscanf(name.c_str(), "range,%lld,%*[^,],%u%n", &f.version, &f.blockSize, &len) == 2 && len == name.size())
			return f;
		throw restore_unknown_file_type();
	}

	static LogFile pathToLogFile(std::string path, int64_t size) {
		std::string name = basename(path);
		LogFile f;
		f.fileName = path;
		f.fileSize = size;
		int len;
		if(sscanf(name.c_str(), "log,%lld,%lld,%u%n", &f.beginVersion, &f.endVersion, &f.blockSize, &len) == 3 && len == name.size())
			return f;
		throw restore_unknown_file_type();
	}

	static KeyspaceSnapshotFile pathToKeyspaceSnapshotFile(std::string path) {
		std::string name = basename(path);
		KeyspaceSnapshotFile f;
		f.fileName = path;
		int len;
		if(sscanf(name.c_str(), "snapshot,%lld,%lld,%lld%n", &f.beginVersion, &f.endVersion, &f.totalSize, &len) == 3 && len == name.size())
			return f;
		throw restore_unknown_file_type();
	}

	// TODO:  Do this more efficiently, as the range file list for a snapshot could potentially be hundreds of megabytes.
	ACTOR static Future<std::vector<RangeFile>> readKeyspaceSnapshot_impl(Reference<BackupContainerFileSystem> bc, KeyspaceSnapshotFile snapshot) {
		// Read the range file list for the specified version range, and then index them by fileName.
		std::vector<RangeFile> files = wait(bc->listRangeFiles(snapshot.beginVersion, snapshot.endVersion));
		state std::map<std::string, RangeFile> rangeIndex;
		for(auto &f : files)
			rangeIndex[f.fileName] = std::move(f);

		// Read the snapshot file, verify the version range, then find each of the range files by name in the index and return them.
		state Reference<IAsyncFile> f = wait(bc->readFile(snapshot.fileName));
		int64_t size = wait(f->size());
		state Standalone<StringRef> buf = makeString(size);
		int _ = wait(f->read(mutateString(buf), buf.size(), 0));
		json_spirit::mValue json;
		json_spirit::read_string(buf.toString(), json);
		JSONDoc doc(json);

		Version v;
		if(!doc.tryGet("beginVersion", v) || v != snapshot.beginVersion)
			throw restore_corrupted_data();
		if(!doc.tryGet("endVersion", v) || v != snapshot.endVersion)
			throw restore_corrupted_data();

		json_spirit::mValue &filesArray = doc.create("files");
		if(filesArray.type() != json_spirit::array_type)
			throw restore_corrupted_data();

		std::vector<RangeFile> results;
		for(auto const &fileValue : filesArray.get_array()) {
			if(fileValue.type() != json_spirit::str_type)
				throw restore_corrupted_data();
			auto i = rangeIndex.find(fileValue.get_str());
			if(i == rangeIndex.end())
				throw restore_corrupted_data();

			results.push_back(i->second);
		}

		return results;
	}

	Future<std::vector<RangeFile>> readKeyspaceSnapshot(KeyspaceSnapshotFile snapshot) {
		return readKeyspaceSnapshot_impl(Reference<BackupContainerFileSystem>::addRef(this), snapshot);
	}

	ACTOR static Future<Void> writeKeyspaceSnapshotFile_impl(Reference<BackupContainerFileSystem> bc, std::vector<std::string> fileNames, int64_t totalBytes) {
		ASSERT(!fileNames.empty());

		json_spirit::mValue json;
		JSONDoc doc(json);

		json_spirit::mArray &array = (doc.create("files") = json_spirit::mArray()).get_array();

		Version minVer = std::numeric_limits<Version>::max();
		Version maxVer = std::numeric_limits<Version>::min();

		for(auto &f : fileNames) {
			array.push_back(f);
			RangeFile rf = pathToRangeFile(f, 0);
			if(rf.version < minVer)
				minVer = rf.version;
			if(rf.version > maxVer)
				maxVer = rf.version;
		}

		doc.create("totalBytes") = totalBytes;
		doc.create("beginVersion") = minVer;
		doc.create("endVersion") = maxVer;

		state std::string docString = json_spirit::write_string(json);

		state Reference<IBackupFile> f = wait(bc->writeFile(format("snapshots/snapshot,%lld,%lld,%lld", minVer, maxVer, totalBytes)));
		Void _ = wait(f->append(docString.data(), docString.size()));
		Void _ = wait(f->finish());

		return Void();
	}

	Future<Void> writeKeyspaceSnapshotFile(std::vector<std::string> fileNames, int64_t totalBytes) {
		return writeKeyspaceSnapshotFile_impl(Reference<BackupContainerFileSystem>::addRef(this), fileNames, totalBytes);
	};

	// List files in sorted order but without regard for overlaps in version ranges
	Future<std::vector<LogFile>> listLogFiles(Version beginVersion = std::numeric_limits<Version>::min(), Version endVersion = std::numeric_limits<Version>::max()) {
		return map(listFiles("logs/"), [=](const FilesAndSizesT &files) {
			std::vector<LogFile> results;
			for(auto &f : files) {
				LogFile lf = pathToLogFile(f.first, f.second);
				if(lf.endVersion > beginVersion && lf.beginVersion < endVersion)
					results.push_back(lf);
			}
			std::sort(results.begin(), results.end());
			return results;
		});
	}
	Future<std::vector<RangeFile>> listRangeFiles(Version beginVersion = std::numeric_limits<Version>::min(), Version endVersion = std::numeric_limits<Version>::max()) {
		return map(listFiles("ranges/"), [=](const FilesAndSizesT &files) {
			std::vector<RangeFile> results;
			for(auto &f : files) {
				RangeFile rf = pathToRangeFile(f.first, f.second);
				if(rf.version >= beginVersion && rf.version <= endVersion)
					results.push_back(rf);
			}
			std::sort(results.begin(), results.end());
			return results;
		});
	}
	Future<std::vector<KeyspaceSnapshotFile>> listKeyspaceSnapshots(Version beginVersion = std::numeric_limits<Version>::min(), Version endVersion = std::numeric_limits<Version>::max()) {
		return map(listFiles("snapshots/"), [=](const FilesAndSizesT &files) {
			std::vector<KeyspaceSnapshotFile> results;
			for(auto &f : files) {
				KeyspaceSnapshotFile sf = pathToKeyspaceSnapshotFile(f.first);
				if(sf.endVersion > beginVersion && sf.beginVersion < endVersion)
					results.push_back(sf);
			}
			std::sort(results.begin(), results.end());
			return results;
		});
	}

	ACTOR Future<BackupDescription> describeBackup_impl(Reference<BackupContainerFileSystem> bc) {
		state BackupDescription desc;
		desc.url = bc->getURL();

		state std::vector<KeyspaceSnapshotFile> snapshots = wait(bc->listKeyspaceSnapshots());
		desc.snapshots = snapshots;

		std::vector<LogFile> logs = wait(bc->listLogFiles(0, std::numeric_limits<Version>::max()));

		if(!logs.empty()) {
			desc.maxLogEnd = logs.rbegin()->endVersion;

			auto i = logs.begin();
			desc.minLogBegin = i->beginVersion;
			desc.contiguousLogEnd = i->endVersion;
			auto &end = desc.contiguousLogEnd.get();  // For convenience to make loop cleaner

			// Advance until continuity is broken
			while(++i != logs.end()) {
				if(i->beginVersion > end)
					break;
				// If the next link in the log chain is found, update the end
				if(i->beginVersion == end)
					end = i->endVersion;
			}

			for(auto &s : snapshots) {
				// If the snapshot is covered by the contiguous log chain then update min/max restorable.
				if(s.beginVersion >= desc.minLogBegin.get() && s.endVersion <= desc.contiguousLogEnd.get()) {
					// Update min restorable
					if(!desc.minRestorableVersion.present() || s.endVersion < desc.minRestorableVersion.get())
						desc.minRestorableVersion = s.endVersion;
					// Update max restorable, which is just the contigous log end as long as there is at least one
					// keyspace snapshot that is within the log chain range.
					desc.maxRestorableVersion = desc.contiguousLogEnd;
				}
			}
		}

		return desc;
	}

	// Uses the virtual methods to describe the backup contents
	Future<BackupDescription> describeBackup() {
		return describeBackup_impl(Reference<BackupContainerFileSystem>::addRef(this));
	}

	ACTOR Future<Void> expireData_impl(Reference<BackupContainerFileSystem> bc, Version endVersion) {
		state std::vector<Future<Void>> deletes;

		std::vector<LogFile> logs = wait(bc->listLogFiles(0, endVersion));
		for(auto const &f : logs)
			deletes.push_back(bc->deleteFile(f.fileName));

		std::vector<RangeFile> ranges = wait(bc->listRangeFiles(0, endVersion));
		for(auto const &f : ranges)
			deletes.push_back(bc->deleteFile(f.fileName));

		std::vector<KeyspaceSnapshotFile> snapshots = wait(bc->listKeyspaceSnapshots(0, endVersion));
		for(auto const &f : snapshots)
			deletes.push_back(bc->deleteFile(f.fileName));

		Void _ = wait(waitForAll(deletes));
		return Void();
	}

	// Delete all data up to (but not including endVersion)
	Future<Void> expireData(Version endVersion) {
		return expireData_impl(Reference<BackupContainerFileSystem>::addRef(this), endVersion);
	}

	ACTOR Future<Optional<RestorableFileSet>> getRestoreSet_impl(Reference<BackupContainerFileSystem> bc, Version targetVersion) {
		// Find the most recent keyrange snapshot to end at or before targetVersion
		state Optional<KeyspaceSnapshotFile> snapshot;
		std::vector<KeyspaceSnapshotFile> snapshots = wait(bc->listKeyspaceSnapshots());
		for(auto const &s : snapshots) {
			if(s.endVersion <= targetVersion)
				snapshot = s;
		}

		if(snapshot.present()) {
			state RestorableFileSet restorable;

			std::vector<RangeFile> ranges = wait(bc->readKeyspaceSnapshot(snapshot.get()));
			restorable.ranges = ranges;

			std::vector<LogFile> logs = wait(bc->listLogFiles(snapshot.get().beginVersion, targetVersion));

			// If there are logs and the first one starts before the snapshot begin version then proceed
			if(!logs.empty() && logs.front().beginVersion <= snapshot.get().beginVersion) {
				auto i = logs.begin();
				Version end = i->endVersion;
				restorable.logs.push_back(*i);

				// Add logs to restorable logs set until continuity is broken OR we reach targetVersion
				while(++i != logs.end()) {
					if(i->beginVersion > end || i->beginVersion >= targetVersion)
						break;
					// If the next link in the log chain is found, update the end
					if(i->beginVersion == end) {
						restorable.logs.push_back(*i);
						end = i->endVersion;
					}
				}

				if(end >= targetVersion) {
					restorable.targetVersion = targetVersion;
					return restorable;
				}
			}
		}

		return Optional<RestorableFileSet>();
	}

	Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion){
		return getRestoreSet_impl(Reference<BackupContainerFileSystem>::addRef(this), targetVersion);
	}

/*
struct BackupMetadata {
	BackupMetadata() : format_version(1) {}
	int64_t format_version;

	std::string toJSON() {
		json_spirit::mObject json;
		JSONDoc doc(json);

		doc.create("format_version") = formatVersion;

		return json_spirit::write_string(json_spirit::mValue(json));
	}

	static BackupInfo fromJSON(std::string docString) {
		BackupInfo info;
		json_spirit::mValue json;
		json_spirit::read_string(docString, json);
		JSONDoc doc(json);

		doc.tryGet("format_version", info.formatVersion);

		return info;
	}
};

ACTOR Future<Optional<BackupMetadata>> readInfo_impl(Reference<BackupContainerFileSystem> bc) {
	state Reference<IAsyncFile> f;
	try {
		Reference<IAsyncFile> _f = wait(bc->readFile(bc->backupInfoPath()));
		f = _f;
	} catch(Error &e) {
		if(e.code() == error_code_file_not_found)
			return Optional<BackupMetadata>();
}

	state int64_t size = wait(f->size());
	state Standalone<StringRef> buf = makeString(size);
	try {
		int read = wait(f->read(mutateString(buf), size, 0));
		if(read != size)
			throw io_error();

		return BackupMetadata::fromJSON(buf.toString());
	} catch(Error &e) {
		TraceEvent(SevWarn, "BackupContainerInvalidBackupInfo").error(e);
		throw backup_invalid_info();
	}
}


// Write backup metadata
ACTOR Future<Void> writeInfo_impl(Reference<BackupContainerFileSystem> bc, BackupMetadata info) {
	state Reference<IBackupFile> f = wait(bc->writeFile(bc->backupInfoPath()));
	Void _ = wait(f->append(info.toJSON()));
	Void _ = wait(f->finish());
	return Void();
}
*/

};



class BackupContainerLocalDirectory : public BackupContainerFileSystem, ReferenceCounted<BackupContainerLocalDirectory> {
public:
	void addref() { return ReferenceCounted<BackupContainerLocalDirectory>::addref(); }
	void delref() { return ReferenceCounted<BackupContainerLocalDirectory>::delref(); }

	static std::string getURLFormat() { return "file://</path/to/base/dir/>"; }

    BackupContainerLocalDirectory(std::string url) {
		std::string path;
		if(url.find("file://") != 0) {
			TraceEvent(SevWarn, "BackupContainerLocalDirectory").detail("Description", "Invalid URL for BackupContainerLocalDirectory").detail("URL", url);
		}

		path = url.substr(7);
		// Remove trailing slashes on path
		path.erase(path.find_last_not_of("\\/") + 1);

		if(!g_network->isSimulated() && path != abspath(path)) {
			TraceEvent(SevWarn, "BackupContainerLocalDirectory").detail("Description", "Backup path must be absolute (e.g. file:///some/path)").detail("URL", url).detail("Path", path);
			throw io_error();
		}

		// Finalized path written to will be will be <path>/backup-<uid>
		m_path = path;
	}

	Future<Void> create() {
		// Nothing should be done here because create() can be called by any process working with the container URL, such as fdbbackup.
		// Since "local directory" containers are by definition local to the machine they are accessed from,
		// the container's creation (in this case the creation of a directory) must be ensured prior to every file creation,
		// which is done in openFile().
		// Creating the directory here will result in unnecessary directories being created on machines that run fdbbackup but not agents.
		return Void();
	}

	Future<Reference<IAsyncFile>> readFile(std::string path) {
		int flags = IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED;
		// Simulation does not properly handle opening the same file from multiple machines using a shared filesystem,
		// so create a symbolic link to make each file opening appear to be unique.  This could also work in production
		// but only if the source directory is writeable which shouldn't be required for a restore.
		std::string fullPath = joinPath(m_path, path);
		#ifndef _WIN32
		if(g_network->isSimulated()) {
			if(!fileExists(fullPath))
				throw file_not_found();
			std::string uniquePath = fullPath + "." + g_random->randomUniqueID().toString() + ".lnk";
			unlink(uniquePath.c_str());
			ASSERT(symlink(basename(path).c_str(), uniquePath.c_str()) == 0);
			fullPath = uniquePath = uniquePath;
		}
		// Opening cached mode forces read/write mode at a lower level, overriding the readonly request.  So cached mode
		// can't be used because backup files are read-only.  Cached mode can only help during restore task retries handled
		// by the same process that failed the first task execution anyway, which is a very rare case.
		#endif
		return IAsyncFileSystem::filesystem()->open(fullPath, flags, 0644);
	}

	class BackupFile : public IBackupFile, ReferenceCounted<BackupFile> {
	public:
		BackupFile(std::string fileName, Reference<IAsyncFile> file, std::string finalFullPath) : IBackupFile(fileName), m_file(file), m_finalFullPath(finalFullPath) {}

		Future<Void> append(const void *data, int len) {
			Future<Void> r = m_file->write(data, len, m_offset);
			m_offset += len;
			return r;
		}

		ACTOR static Future<Void> finish_impl(Reference<BackupFile> f) {
			Void _ = wait(f->m_file->truncate(f->m_offset));
			Void _ = wait(f->m_file->sync());
			std::string name = f->m_file->getFilename();
			f->m_file.clear();
			renameFile(name, f->m_finalFullPath);
			return Void();
		}

		Future<Void> finish() {
			return finish_impl(Reference<BackupFile>::addRef(this));
		}

		void addref() { return ReferenceCounted<BackupFile>::addref(); }
		void delref() { return ReferenceCounted<BackupFile>::delref(); }

	private:
		Reference<IAsyncFile> m_file;
		std::string m_finalFullPath;
	};

	Future<Reference<IBackupFile>> writeFile(std::string path) {
		int flags = IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE;
		std::string fullPath = joinPath(m_path, path);
		platform::createDirectory(parentDirectory(fullPath));
		std::string temp = fullPath  + "." + g_random->randomUniqueID().toString() + ".temp";
		Future<Reference<IAsyncFile>> f = IAsyncFileSystem::filesystem()->open(temp, flags, 0644);
		return map(f, [=](Reference<IAsyncFile> f) {
			return Reference<IBackupFile>(new BackupFile(path, f, fullPath));
		});
	}

	Future<Void> deleteFile(std::string path) {
		::deleteFile(joinPath(m_path, path));
		return Void();
	}

	Future<FilesAndSizesT> listFiles(std::string path) {
		FilesAndSizesT results;

		std::vector<std::string> files;
		platform::findFilesRecursively(joinPath(m_path, path), files);

		// Remove .lnk files from resultst, they are a side effect of a backup that was *read* during simulation.  See openFile() above for more info on why they are created.
		if(g_network->isSimulated())
			files.erase(std::remove_if(files.begin(), files.end(), [](std::string const &f) { return StringRef(f).endsWith(LiteralStringRef(".lnk")); }), files.end());

		for(auto &f : files) {
			// Hide .part or .temp files.
			StringRef s(f);
			if(!s.endsWith(LiteralStringRef(".part")) && !s.endsWith(LiteralStringRef(".temp")))
				results.push_back({f.substr(m_path.size() + 1), ::fileSize(f)});
		}

		return results;
	}

	Future<Void> deleteContainer(int *pNumDeleted) {
		// TODO:  Providing a way to delete a backup folder doesn't seem very necessary since users generally
		// know how to manage files on disk.  However, if we want this to work then eraseDirectoryRecursive()
		// or something like it needs to work outside the pre-simulation phase of execution.
		/*
		// List files directly from the filesystem so we see .temp and .part files too.
		int count = platform::listFiles(m_path, "").size();
		platform::eraseDirectoryRecursive(m_path);
		if(pNumDeleted != nullptr)
			*pNumDeleted = count;
		*/
		return Void();
	}

private:
	std::string m_path;
};

class BackupContainerBlobStore : public BackupContainerFileSystem, ReferenceCounted<BackupContainerBlobStore> {
private:
	static const std::string BACKUP_BUCKET;

	Reference<BlobStoreEndpoint> m_bstore;
	std::string m_name;

public:
	BackupContainerBlobStore(Reference<BlobStoreEndpoint> bstore, std::string name)
	  : m_bstore(bstore), m_name(name) {
	}

	void addref() { return ReferenceCounted<BackupContainerBlobStore>::addref(); }
	void delref() { return ReferenceCounted<BackupContainerBlobStore>::delref(); }

	static std::string getURLFormat() { return BlobStoreEndpoint::getURLFormat(true); }

	virtual ~BackupContainerBlobStore() {}

	Future<Reference<IAsyncFile>> readFile(std::string path) {
			return Reference<IAsyncFile>(
				new AsyncFileReadAheadCache(
					Reference<IAsyncFile>(new AsyncFileBlobStoreRead(m_bstore, BACKUP_BUCKET, m_name + "/" + path)),
					m_bstore->knobs.read_block_size,
					m_bstore->knobs.read_ahead_blocks,
					m_bstore->knobs.concurrent_reads_per_file,
					m_bstore->knobs.read_cache_blocks_per_file
				)
			);
	}

	class BackupFile : public IBackupFile, ReferenceCounted<BackupFile> {
	public:
		BackupFile(std::string fileName, Reference<IAsyncFile> file) : IBackupFile(fileName), m_file(file) {}

		Future<Void> append(const void *data, int len) {
			Future<Void> r = m_file->write(data, len, m_offset);
			m_offset += len;
			return r;
		}

		Future<Void> finish() {
			Reference<BackupFile> self = Reference<BackupFile>::addRef(this);
			return map(m_file->sync(), [=](Void _) { self->m_file.clear(); return Void(); });
		}

		void addref() { return ReferenceCounted<BackupFile>::addref(); }
		void delref() { return ReferenceCounted<BackupFile>::delref(); }
	private:
		Reference<IAsyncFile> m_file;
	};

	Future<Reference<IBackupFile>> writeFile(std::string path) {
		return Reference<IBackupFile>(new BackupFile(path, Reference<IAsyncFile>(new AsyncFileBlobStoreWrite(m_bstore, BACKUP_BUCKET, m_name + "/" + path))));
	}

	Future<Void> deleteFile(std::string path) {
		return m_bstore->deleteObject(BACKUP_BUCKET, m_name + "/" + path);
	}

	ACTOR Future<FilesAndSizesT> listFiles_impl(Reference<BackupContainerBlobStore> bc, std::string path) {
		state BlobStoreEndpoint::ListResult result = wait(bc->m_bstore->listBucket(BACKUP_BUCKET, bc->m_name + "/" + path));
		FilesAndSizesT files;
		for(auto &o : result.objects)
			files.push_back({o.name.substr(bc->m_name.size() + 1), o.size});
		return files;
	}

	Future<FilesAndSizesT> listFiles(std::string path) {
		return listFiles_impl(Reference<BackupContainerBlobStore>::addRef(this), path);
	}

	ACTOR Future<Void> create_impl(Reference<BackupContainerBlobStore> bc) {
		Void _ = wait(bc->m_bstore->createBucket(BACKUP_BUCKET));

		/*
		Optional<BackupInfo> info = wait(bc->readInfo());

		if(info.present())
			throw backup_duplicate();

		BackupInfo newInfo;

		Void _ = wait(bc->writeInfo(newInfo));
		*/

		return Void();
	}

	Future<Void> create() {
		return create_impl(Reference<BackupContainerBlobStore>::addRef(this));
	}

	// TODO:  If there is a need, this can be made faster by discovering common prefixes and listing levels of the folder structure in parallel.
	ACTOR static Future<Void> deleteContainer_impl(Reference<BackupContainerBlobStore> bc, int *pNumDeleted) {
		state PromiseStream<BlobStoreEndpoint::ListResult> resultStream;
		state Future<Void> done = bc->m_bstore->listBucketStream(BACKUP_BUCKET, resultStream, bc->m_name + "/");
		state std::vector<Future<Void>> deleteFutures;
		loop {
			choose {
				when(Void _ = wait(done)) {
					break;
				}
				when(BlobStoreEndpoint::ListResult list = waitNext(resultStream.getFuture())) {
					for(auto &object : list.objects) {
						int *pNumDeletedCopy = pNumDeleted;   // avoid capture of this
						deleteFutures.push_back(map(bc->m_bstore->deleteObject(BACKUP_BUCKET, object.name), [pNumDeletedCopy](Void) {
							if(pNumDeletedCopy != nullptr)
								++*pNumDeletedCopy;
							return Void();
						}));
					}
				}
			}
		}

		Void _ = wait(waitForAll(deleteFutures));
		return Void();
	}

	Future<Void> deleteContainer(int *pNumDeleted) {
		return deleteContainer_impl(Reference<BackupContainerBlobStore>::addRef(this), pNumDeleted);
	}
};

const std::string BackupContainerBlobStore::BACKUP_BUCKET = "FDB_BACKUPS_V2";
std::string IBackupContainer::lastOpenError;

std::vector<std::string> IBackupContainer::getURLFormats() {
	std::vector<std::string> formats;
	formats.push_back(BackupContainerLocalDirectory::getURLFormat());
	formats.push_back(BackupContainerBlobStore::getURLFormat());
	return formats;
}

// Get an IBackupContainer based on a container URL string
Reference<IBackupContainer> IBackupContainer::openContainer(std::string url)
{
	static std::map<std::string, Reference<IBackupContainer>> m_cache;

	Reference<IBackupContainer> &r = m_cache[url];
	if(r)
		return r;

	try {
		StringRef u(url);
		if(u.startsWith(LiteralStringRef("file://")))
			r = Reference<IBackupContainer>(new BackupContainerLocalDirectory(url));
		else if(u.startsWith(LiteralStringRef("blobstore://"))) {
			std::string resource;
			Reference<BlobStoreEndpoint> bstore = BlobStoreEndpoint::fromString(url, &resource, &lastOpenError);
			if(resource.empty())
				throw backup_invalid_url();
			for(auto c : resource)
				if(!isalnum(c) && c != '_' && c != '-' && c != '.')
					throw backup_invalid_url();
			r = Reference<IBackupContainer>(new BackupContainerBlobStore(bstore, resource));
		}
		else {
			lastOpenError = "invalid URL prefix";
			throw backup_invalid_url();
		}

		r->URL = url;
		return r;
	} catch(Error &e) {
		if(e.code() == error_code_actor_cancelled)
			throw;

		TraceEvent m(SevWarn, "BackupContainer");
		m.detail("Description", "Invalid container specification.  See help.").detail("URL", url);

		if(e.code() == error_code_backup_invalid_url)
			m.detail("lastOpenError", lastOpenError);
		throw;
	}
}

ACTOR Future<Void> testBackupContainer(std::string url) {
	printf("BackupContainerTest URL %s\n", url.c_str());

	state Reference<IBackupContainer> c = IBackupContainer::openContainer(url);

	state Reference<IBackupFile> log1 = wait(c->writeLogFile(100, 150, 10));
	Void _ = wait(log1->append("asdf", 4));
	Void _ = wait(log1->finish());

	state Reference<IBackupFile> log2 = wait(c->writeLogFile(150, 300, 10));
	Void _ = wait(log2->append("asdf", 4));
	Void _ = wait(log2->finish());

	state Reference<IBackupFile> range1 = wait(c->writeRangeFile(110, 10));
	Void _ = wait(range1->append("asdf", 4));
	Void _ = wait(range1->finish());

	Void _ = wait(c->writeKeyspaceSnapshotFile({range1->getFileName()}, range1->size()));

	state BackupDescription desc = wait(c->describeBackup());
	printf("Backup Description\n%s", desc.toString().c_str());

	ASSERT(desc.maxRestorableVersion.present());
	Optional<RestorableFileSet> rest = wait(c->getRestoreSet(desc.maxRestorableVersion.get()));

	ASSERT(rest.present());
	ASSERT(rest.get().targetVersion == desc.maxRestorableVersion.get());
	ASSERT(rest.get().logs.size() == 2);
	ASSERT(rest.get().ranges.size() == 1);

	Void _ = wait(c->deleteContainer());

	// TODO:  Read the files back, verify contents.

	// TODO:  Expire data, very change to description

	printf("BackupContainerTest URL=%s PASSED.\n", url.c_str());

	return Void();
}

TEST_CASE("backup/containers/localdir") {
	if(g_network->isSimulated())
		Void _ = wait(testBackupContainer(format("file://simfdb/backups/%llx", timer_int())));
	else
		Void _ = wait(testBackupContainer(format("file:///private/tmp/fdb_backups/%llx", timer_int())));
	return Void();
};

TEST_CASE("backup/containers/blobstore") {
	Void _ = wait(testBackupContainer(format("blobstore://FDB_TEST:FDB_TEST_KEY@store-test.blobstore.apple.com/test_%llx", timer_int())));
	return Void();
};
