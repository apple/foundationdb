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
#include "fdbclient/SystemData.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/RunTransaction.actor.h"
#include <algorithm>
#include <time.h>

namespace IBackupFile_impl {

	ACTOR Future<Void> appendStringRefWithLen(Reference<IBackupFile> file, Standalone<StringRef> s) {
		state uint32_t lenBuf = bigEndian32((uint32_t)s.size());
		Void _ = wait(file->append(&lenBuf, sizeof(lenBuf)));
		Void _ = wait(file->append(s.begin(), s.size()));
		return Void();
	}
}

Future<Void> IBackupFile::appendStringRefWithLen(Standalone<StringRef> s) {
	return IBackupFile_impl::appendStringRefWithLen(Reference<IBackupFile>::addRef(this), s);
}

std::string formatTime(int64_t t) {
	time_t curTime = (time_t)t;
	char buffer[128];
	struct tm timeinfo;
	getLocalTime(&curTime, &timeinfo);
	strftime(buffer, 128, "%Y-%m-%d %H:%M:%S", &timeinfo);
	return buffer;
}

Future<Void> fetchTimes(Reference<ReadYourWritesTransaction> tr,  std::map<Version, int64_t> *pVersionTimeMap) {
	std::vector<Future<Void>> futures;

	// Resolve each version in the map,
	for(auto &p : *pVersionTimeMap) {
		futures.push_back(map(timeKeeperEpochsFromVersion(p.first, tr), [=](Optional<int64_t> t) {
			if(t.present())
				pVersionTimeMap->at(p.first) = t.get();
			else
				pVersionTimeMap->erase(p.first);
			return Void();
		}));
	}

	return waitForAll(futures);
}

Future<Void> BackupDescription::resolveVersionTimes(Database cx) {
	// Populate map with versions needed
	versionTimeMap.clear();

	for(const KeyspaceSnapshotFile &m : snapshots) {
		versionTimeMap[m.beginVersion];
		versionTimeMap[m.endVersion];
	}
	if(minLogBegin.present())
		versionTimeMap[minLogBegin.get()];
	if(maxLogEnd.present())
		versionTimeMap[maxLogEnd.get()];
	if(contiguousLogEnd.present())
		versionTimeMap[contiguousLogEnd.get()];
	if(minRestorableVersion.present())
		versionTimeMap[minRestorableVersion.get()];
	if(maxRestorableVersion.present())
		versionTimeMap[maxRestorableVersion.get()];

	return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return fetchTimes(tr, &versionTimeMap); });
};

std::string BackupDescription::toString() const {
	std::string info;

	info.append(format("URL: %s\n", url.c_str()));
	info.append(format("Restorable: %s\n", maxRestorableVersion.present() ? "true" : "false"));

	auto formatVersion = [&](Version v) {
		std::string s;
		if(!versionTimeMap.empty()) {
			auto i = versionTimeMap.find(v);
			if(i != versionTimeMap.end())
				s = format("%lld (%s)", v, formatTime(i->second).c_str());
			else
				s = format("%lld (unknown)", v);
		}
		else {
			s = format("%lld", v);
		}
		return s;
	};

	for(const KeyspaceSnapshotFile &m : snapshots) {
		info.append(format("Snapshot:  startVersion=%s  endVersion=%s  totalBytes=%lld  restorable=%s\n",
			formatVersion(m.beginVersion).c_str(), formatVersion(m.endVersion).c_str(), m.totalSize, m.restorable.orDefault(false) ? "true" : "false"));
	}

	info.append(format("SnapshotBytes: %lld\n", snapshotBytes));

	if(minLogBegin.present())
		info.append(format("MinLogBeginVersion:      %s\n", formatVersion(minLogBegin.get()).c_str()));
	if(contiguousLogEnd.present())
		info.append(format("ContiguousLogEndVersion: %s\n", formatVersion(contiguousLogEnd.get()).c_str()));
	if(maxLogEnd.present())
		info.append(format("MaxLogEndVersion:        %s\n", formatVersion(maxLogEnd.get()).c_str()));
	if(minRestorableVersion.present())
		info.append(format("MinRestorableVersion:    %s\n", formatVersion(minRestorableVersion.get()).c_str()));
	if(maxRestorableVersion.present())
		info.append(format("MaxRestorableVersion:    %s\n", formatVersion(maxRestorableVersion.get()).c_str()));

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
 *   Where ... is a multi level path which sorts lexically into version order and targets 10,000 or less
 *   entries in each folder (though a full speed snapshot could exceed this count at the innermost folder level)
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
	// The implementation can (but does not have to) use the folder path filter to avoid traversing
	// specific subpaths.
	typedef std::vector<std::pair<std::string, int64_t>> FilesAndSizesT;
	virtual Future<FilesAndSizesT> listFiles(std::string path = "", std::function<bool(std::string const &)> folderPathFilter = nullptr) = 0;

	// Open a file for read by fileName
	virtual Future<Reference<IAsyncFile>> readFile(std::string fileName) = 0;

	// Open a file for write by fileName
	virtual Future<Reference<IBackupFile>> writeFile(std::string fileName) = 0;

	// Delete a file
	virtual Future<Void> deleteFile(std::string fileName) = 0;

	// Delete entire container.  During the process, if pNumDeleted is not null it will be
	// updated with the count of deleted files so that progress can be seen.
	virtual Future<Void> deleteContainer(int *pNumDeleted) = 0;

	// Creates a 2-level path (x/y) where v should go such that x/y/* contains (10^smallestBucket) possible versions
	static std::string versionFolderString(Version v, int smallestBucket) {
		ASSERT(smallestBucket < 14);
		// Get a 0-padded fixed size representation of v
		std::string vFixedPrecision = format("%019lld", v);
		ASSERT(vFixedPrecision.size() == 19);
		// Truncate smallestBucket from the fixed length representation
		vFixedPrecision.resize(vFixedPrecision.size() - smallestBucket);

		// Split the remaining digits with a '/' 4 places from the right
		vFixedPrecision.insert(vFixedPrecision.size() - 4, 1, '/');

		return vFixedPrecision;
	}

	// This useful for comparing version folder strings regardless of where their "/" dividers are, as it is possible
	// that division points would change in the future.
	static std::string cleanFolderString(std::string f) {
		f.erase(std::remove(f.begin(), f.end(), '/'), f.end());
		return f;
	}

	// The innermost folder covers 100 seconds (1e8 versions) During a full speed backup it is possible though very unlikely write about 10,000 snapshot range files during that time.
	static std::string rangeVersionFolderString(Version v) {
		return format("ranges/%s/", versionFolderString(v, 8).c_str());
	}

	// The innermost folder covers 100,000 seconds (1e11 versions) which is 5,000 mutation log files at current settings.
	static std::string logVersionFolderString(Version v) {
		return format("logs/%s/", versionFolderString(v, 11).c_str());
	}

	Future<Reference<IBackupFile>> writeLogFile(Version beginVersion, Version endVersion, int blockSize) {
		return writeFile(logVersionFolderString(beginVersion) + format("log,%lld,%lld,%s,%d", beginVersion, endVersion, g_random->randomUniqueID().toString().c_str(), blockSize));
	}

	Future<Reference<IBackupFile>> writeRangeFile(Version version, int blockSize) {
		return writeFile(rangeVersionFolderString(version) + format("range,%lld,%s,%d", version, g_random->randomUniqueID().toString().c_str(), blockSize));
	}

	static bool pathToRangeFile(RangeFile &out, std::string path, int64_t size) {
		std::string name = basename(path);
		RangeFile f;
		f.fileName = path;
		f.fileSize = size;
		int len;
		if(sscanf(name.c_str(), "range,%lld,%*[^,],%u%n", &f.version, &f.blockSize, &len) == 2 && len == name.size()) {
			out = f;
			return true;
		}
		return false;
	}

	static bool pathToLogFile(LogFile &out, std::string path, int64_t size) {
		std::string name = basename(path);
		LogFile f;
		f.fileName = path;
		f.fileSize = size;
		int len;
		if(sscanf(name.c_str(), "log,%lld,%lld,%*[^,],%u%n", &f.beginVersion, &f.endVersion, &f.blockSize, &len) == 3 && len == name.size()) {
			out = f;
			return true;
		}
		return false;
	}

	static bool pathToKeyspaceSnapshotFile(KeyspaceSnapshotFile &out, std::string path) {
		std::string name = basename(path);
		KeyspaceSnapshotFile f;
		f.fileName = path;
		int len;
		if(sscanf(name.c_str(), "snapshot,%lld,%lld,%lld%n", &f.beginVersion, &f.endVersion, &f.totalSize, &len) == 3 && len == name.size()) {
			out = f;
			return true;
		}
		return false;
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


		state Version minVer = std::numeric_limits<Version>::max();
		state Version maxVer = 0;
		state RangeFile rf;
		state json_spirit::mArray fileArray;
		state int i;

		// Validate each filename, update version range
		for(i = 0; i < fileNames.size(); ++i) {
			auto const &f = fileNames[i];
			if(pathToRangeFile(rf, f, 0)) {
				fileArray.push_back(f);
				if(rf.version < minVer)
					minVer = rf.version;
				if(rf.version > maxVer)
					maxVer = rf.version;
			}
			else
				throw restore_unknown_file_type();
			Void _ = wait(yield());
		}

		state json_spirit::mValue json;
		state JSONDoc doc(json);

		doc.create("files") = std::move(fileArray);
		doc.create("totalBytes") = totalBytes;
		doc.create("beginVersion") = minVer;
		doc.create("endVersion") = maxVer;

		Void _ = wait(yield());
		state std::string docString = json_spirit::write_string(json);

		state Reference<IBackupFile> f = wait(bc->writeFile(format("snapshots/snapshot,%lld,%lld,%lld", minVer, maxVer, totalBytes)));
		Void _ = wait(f->append(docString.data(), docString.size()));
		Void _ = wait(f->finish());

		return Void();
	}

	Future<Void> writeKeyspaceSnapshotFile(std::vector<std::string> fileNames, int64_t totalBytes) {
		return writeKeyspaceSnapshotFile_impl(Reference<BackupContainerFileSystem>::addRef(this), fileNames, totalBytes);
	};

	// List log files which contain data at any version >= beginVersion and < endVersion
	// Lists files in sorted order by begin version. Does not check that results are non overlapping or contiguous.
	Future<std::vector<LogFile>> listLogFiles(Version beginVersion = 0, Version endVersion = std::numeric_limits<Version>::max()) {
		// The first relevant log file could have a begin version less than beginVersion based on the knobs which determine log file range size,
		// so start at an earlier version adjusted by how many versions a file could contain.
		//
		// Get the cleaned (without slashes) first and last folders that could contain relevant results.
		std::string firstPath = cleanFolderString(logVersionFolderString(
			std::max<Version>(0, beginVersion - CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE)
		));
		std::string lastPath =  cleanFolderString(logVersionFolderString(endVersion));

		std::function<bool(std::string const &)> pathFilter = [=](const std::string &folderPath) {
			// Remove slashes in the given folder path so that the '/' positions in the version folder string do not matter

			std::string cleaned = cleanFolderString(folderPath);
			return StringRef(firstPath).startsWith(cleaned) || StringRef(lastPath).startsWith(cleaned)
				|| (cleaned > firstPath && cleaned < lastPath);
		};

		return map(listFiles("logs/", pathFilter), [=](const FilesAndSizesT &files) {
			std::vector<LogFile> results;
			LogFile lf;
			for(auto &f : files) {
				if(pathToLogFile(lf, f.first, f.second) && lf.endVersion > beginVersion && lf.beginVersion < endVersion)
					results.push_back(lf);
			}
			std::sort(results.begin(), results.end());
			return results;
		});
	}

	// List range files, in sorted version order, which contain data at or between beginVersion and endVersion
	Future<std::vector<RangeFile>> listRangeFiles(Version beginVersion = 0, Version endVersion = std::numeric_limits<Version>::max()) {
		// Get the cleaned (without slashes) first and last folders that could contain relevant results.
		std::string firstPath = cleanFolderString(rangeVersionFolderString(beginVersion));
		std::string lastPath =  cleanFolderString(rangeVersionFolderString(endVersion));

		std::function<bool(std::string const &)> pathFilter = [=](const std::string &folderPath) {
			// Remove slashes in the given folder path so that the '/' positions in the version folder string do not matter
			std::string cleaned = cleanFolderString(folderPath);

			return StringRef(firstPath).startsWith(cleaned) || StringRef(lastPath).startsWith(cleaned)
				|| (cleaned > firstPath && cleaned < lastPath);
		};

		return map(listFiles("ranges/", pathFilter), [=](const FilesAndSizesT &files) {
			std::vector<RangeFile> results;
			RangeFile rf;
			for(auto &f : files) {
				if(pathToRangeFile(rf, f.first, f.second) && rf.version >= beginVersion && rf.version <= endVersion)
					results.push_back(rf);
			}
			std::sort(results.begin(), results.end());
			return results;
		});
	}

	// List snapshots which have been fully written, in sorted beginVersion order.
	Future<std::vector<KeyspaceSnapshotFile>> listKeyspaceSnapshots() {
		return map(listFiles("snapshots/"), [=](const FilesAndSizesT &files) {
			std::vector<KeyspaceSnapshotFile> results;
			KeyspaceSnapshotFile sf;
			for(auto &f : files) {
				if(pathToKeyspaceSnapshotFile(sf, f.first))
					results.push_back(sf);
			}
			std::sort(results.begin(), results.end());
			return results;
		});
	}

	ACTOR static Future<FullBackupListing> dumpFileList_impl(Reference<BackupContainerFileSystem> bc) {
		state Future<std::vector<RangeFile>> fRanges = bc->listRangeFiles(0, std::numeric_limits<Version>::max());
		state Future<std::vector<KeyspaceSnapshotFile>> fSnapshots = bc->listKeyspaceSnapshots();
		state Future<std::vector<LogFile>> fLogs = bc->listLogFiles(0, std::numeric_limits<Version>::max());
		Void _ = wait(success(fRanges) && success(fSnapshots) && success(fLogs));
		return FullBackupListing({fRanges.get(), fLogs.get(), fSnapshots.get()});
	}

	Future<FullBackupListing> dumpFileList() {
		return dumpFileList_impl(Reference<BackupContainerFileSystem>::addRef(this));
	}

	ACTOR static Future<BackupDescription> describeBackup_impl(Reference<BackupContainerFileSystem> bc, bool deepScan) {
		state BackupDescription desc;
		desc.url = bc->getURL();

		// This is the range of logs we'll have to list to determine log continuity
		state Version scanBegin = 0;
		state Version scanEnd = std::numeric_limits<Version>::max();

		// Get range for which we know there are logs, if available
		state Optional<Version> begin;
		state Optional<Version> end;

		if(!deepScan) {
			Void _ = wait(store(bc->logBeginVersion().get(), begin) && store(bc->logEndVersion().get(), end));
		}

		// Use the known log range if present
		if(begin.present() && end.present()) {
			// Logs are assumed to be contiguious between begin and max(begin, end), so initalize desc accordingly
			// The use of max() is to allow for a stale end version that has been exceeded by begin version
			desc.minLogBegin = begin.get();
			desc.maxLogEnd = std::max(begin.get(), end.get());
			desc.contiguousLogEnd = desc.maxLogEnd;

			// Begin file scan at the contiguous log end version
			scanBegin = desc.contiguousLogEnd.get();
		}

		std::vector<KeyspaceSnapshotFile> snapshots = wait(bc->listKeyspaceSnapshots());
		desc.snapshots = snapshots;

		std::vector<LogFile> logs = wait(bc->listLogFiles(scanBegin, scanEnd));

		if(!logs.empty()) {
			desc.maxLogEnd = logs.rbegin()->endVersion;

			auto i = logs.begin();
			// If we didn't get log versions above then seed them using the first log file
			if(!desc.contiguousLogEnd.present()) {
				desc.minLogBegin = i->beginVersion;
				desc.contiguousLogEnd = i->endVersion;
				++i;
			}
			auto &end = desc.contiguousLogEnd.get();  // For convenience to make loop cleaner

			// Advance until continuity is broken
			while(i != logs.end()) {
				if(i->beginVersion > end)
					break;
				// If the next link in the log chain is found, update the end
				if(i->beginVersion == end)
					end = i->endVersion;
				++i;
			}
		}

		// Try to update the saved log versions if they are not set and we have values for them,
		// but ignore errors in the update attempt in case the container is not writeable
		// Also update logEndVersion if it has a value but it is less than contiguousLogEnd
		try {
			state Future<Void> updates = Void();
			if(desc.minLogBegin.present() && !begin.present())
				updates = updates && bc->logBeginVersion().set(desc.minLogBegin.get());
			if(desc.contiguousLogEnd.present() && (!end.present() || end.get() < desc.contiguousLogEnd.get()) )
				updates = updates && bc->logEndVersion().set(desc.contiguousLogEnd.get());
			Void _ = wait(updates);
		} catch(Error &e) {
			if(e.code() == error_code_actor_cancelled)
				throw;
			TraceEvent(SevWarn, "BackupContainerSafeVersionUpdateFailure").detail("URL", bc->getURL());
		}

		for(auto &s : desc.snapshots) {
			// Calculate restorability of each snapshot.  Assume true, then try to prove false
			s.restorable = true;
			// If this is not a single-version snapshot then see if the available contiguous logs cover its range
			if(s.beginVersion != s.endVersion) {
				if(!desc.minLogBegin.present() || desc.minLogBegin.get() > s.beginVersion)
					s.restorable = false;
				if(!desc.contiguousLogEnd.present() || desc.contiguousLogEnd.get() < s.endVersion)
					s.restorable = false;
			}

			desc.snapshotBytes += s.totalSize;

			// If the snapshot is at a single version then it requires no logs.  Update min and max restorable.
			// TODO:  Somehow check / report if the restorable range is not or may not be contiguous.
			if(s.beginVersion == s.endVersion) {
				if(!desc.minRestorableVersion.present() || s.endVersion < desc.minRestorableVersion.get())
					desc.minRestorableVersion = s.endVersion;

				if(!desc.maxRestorableVersion.present() || s.endVersion > desc.maxRestorableVersion.get())
					desc.maxRestorableVersion = s.endVersion;
			}

			// If the snapshot is covered by the contiguous log chain then update min/max restorable.
			if(desc.minLogBegin.present() && s.beginVersion >= desc.minLogBegin.get() && s.endVersion <= desc.contiguousLogEnd.get()) {
				if(!desc.minRestorableVersion.present() || s.endVersion < desc.minRestorableVersion.get())
					desc.minRestorableVersion = s.endVersion;

				if(!desc.maxRestorableVersion.present() || desc.contiguousLogEnd.get() > desc.maxRestorableVersion.get())
					desc.maxRestorableVersion = desc.contiguousLogEnd;
			}
		}

		return desc;
	}

	// Uses the virtual methods to describe the backup contents
	Future<BackupDescription> describeBackup(bool deepScan = false) {
		return describeBackup_impl(Reference<BackupContainerFileSystem>::addRef(this), deepScan);
	}

	ACTOR static Future<Void> expireData_impl(Reference<BackupContainerFileSystem> bc, Version expireEndVersion, bool force, Version restorableBeginVersion) {
		if(restorableBeginVersion < expireEndVersion)
			throw backup_cannot_expire();

		state Version scanBegin = 0;

		// Get the backup description.
		state BackupDescription desc = wait(bc->describeBackup());

		// Assume force is needed, then try to prove otherwise.
		// Force is required if there is not a restorable snapshot which both
		//   - begins at or after expireEndVersion
		//   - ends at or before restorableBeginVersion
		state bool forceNeeded = true;
		for(KeyspaceSnapshotFile &s : desc.snapshots) {
			if(s.restorable.orDefault(false) && s.beginVersion >= expireEndVersion && s.endVersion <= restorableBeginVersion) {
				forceNeeded = false;
				break;
			}
		}

		// Get metadata
		state Optional<Version> expiredEnd;
		state Optional<Version> logBegin;
		state Optional<Version> logEnd;
		Void _ = wait(store(bc->expiredEndVersion().get(), expiredEnd) && store(bc->logBeginVersion().get(), logBegin) && store(bc->logEndVersion().get(), logEnd));

		// Update scan range if expiredEnd is present
		if(expiredEnd.present()) {
			if(expireEndVersion <= expiredEnd.get()) {
				// If the expire request is to the version already expired to then there is no work to do so return true
				return Void();
			}
			scanBegin = expiredEnd.get();
		}

		// Get log files that contain any data at or before expireEndVersion
		state std::vector<LogFile> logs = wait(bc->listLogFiles(scanBegin, expireEndVersion));
		// Get range files up to and including expireEndVersion
		state std::vector<RangeFile> ranges = wait(bc->listRangeFiles(scanBegin, expireEndVersion));

		// The new logBeginVersion will be taken from the last log file, if there is one
		state Optional<Version> newLogBeginVersion;
		if(!logs.empty()) {
			LogFile &last = logs.back();
			// If the last log ends at expireEndVersion then that will be the next log begin
			if(last.endVersion == expireEndVersion) {
				newLogBeginVersion = expireEndVersion;
			}
			else {
				// If the last log overlaps the expiredEnd then use the log's begin version and move the expiredEnd
				// back to match it.
				if(last.endVersion > expireEndVersion) {
					newLogBeginVersion = last.beginVersion;
					logs.pop_back();
					expireEndVersion = newLogBeginVersion.get();
				}
			}
		}

		// Make a list of files to delete
		state std::vector<std::string> toDelete;

		// Move filenames out of vector then destroy it to save memory
		for(auto const &f : logs) {
			toDelete.push_back(std::move(f.fileName));
		}
		logs.clear();

		// Move filenames out of vector then destroy it to save memory
		for(auto const &f : ranges) {
			// Must recheck version because list returns data up to and including the given endVersion
			if(f.version < expireEndVersion)
				toDelete.push_back(std::move(f.fileName));
		}
		ranges.clear();

		for(auto const &f : desc.snapshots) {
			if(f.endVersion < expireEndVersion)
				toDelete.push_back(std::move(f.fileName));
		}
		desc = BackupDescription();

		// If some files to delete were found AND force is needed AND the force option is NOT set, then fail
		if(!toDelete.empty() && forceNeeded && !force)
			throw backup_cannot_expire();

		// We are about to start deleting files, at which point no data prior to the expire end version can be 
		// safely assumed to exist. The [logBegin, logEnd) range from the container's metadata describes
		// a range of log versions which can be assumed to exist, so if the range of data being deleted overlaps
		// that range then the metadata range must be updated.

		// If we're expiring the entire log range described by the metadata then clear both metadata values
		if(logEnd.present() && logEnd.get() < expireEndVersion) {
			if(logBegin.present())
				Void _ = wait(bc->logBeginVersion().clear());
			if(logEnd.present())
				Void _ = wait(bc->logEndVersion().clear());
		}
		else {
			// If we are expiring to a point within the metadata range then update the begin if we have a new
			// log begin version (which we should!) or clear the metadata range if we do not (which would be
			// repairing the metadata from an incorrect state)
			if(logBegin.present() && logBegin.get() < expireEndVersion) {
				if(newLogBeginVersion.present()) {
					Void _ = wait(bc->logBeginVersion().set(newLogBeginVersion.get()));
				}
				else {
					if(logBegin.present())
						Void _ = wait(bc->logBeginVersion().clear());
					if(logEnd.present())
						Void _ = wait(bc->logEndVersion().clear());
				}
			}
		}

		// Delete files, but limit parallelism because the file list could use a lot of memory and the corresponding
		// delete actor states would use even more if they all existed at the same time.
		state std::list<Future<Void>> deleteFutures;

		while(!toDelete.empty() || !deleteFutures.empty()) {

			// While there are files to delete and budget in the deleteFutures list, start a delete
			while(!toDelete.empty() && deleteFutures.size() < CLIENT_KNOBS->BACKUP_CONCURRENT_DELETES) {
				deleteFutures.push_back(bc->deleteFile(toDelete.back()));
				toDelete.pop_back();
			}

			// Wait for deletes to finish until there are only targetDeletesInFlight remaining.
			// If there are no files left to start then this value is 0, otherwise it is one less
			// than the delete concurrency limit.
			state int targetFuturesSize = toDelete.empty() ? 0 : (CLIENT_KNOBS->BACKUP_CONCURRENT_DELETES - 1);

			while(deleteFutures.size() > targetFuturesSize) {
				Void _ = wait(deleteFutures.front());
				deleteFutures.pop_front();
			}
		}

		// Update the expiredEndVersion property.
		Void _ = wait(bc->expiredEndVersion().set(expireEndVersion));

		return Void();
	}

	// Delete all data up to (but not including endVersion)
	Future<Void> expireData(Version expireEndVersion, bool force, Version restorableBeginVersion) {
		return expireData_impl(Reference<BackupContainerFileSystem>::addRef(this), expireEndVersion, force, restorableBeginVersion);
	}

	ACTOR static Future<Optional<RestorableFileSet>> getRestoreSet_impl(Reference<BackupContainerFileSystem> bc, Version targetVersion) {
		// Find the most recent keyrange snapshot to end at or before targetVersion
		state Optional<KeyspaceSnapshotFile> snapshot;
		std::vector<KeyspaceSnapshotFile> snapshots = wait(bc->listKeyspaceSnapshots());
		for(auto const &s : snapshots) {
			if(s.endVersion <= targetVersion)
				snapshot = s;
		}

		if(snapshot.present()) {
			state RestorableFileSet restorable;
			restorable.snapshot = snapshot.get();
			restorable.targetVersion = targetVersion;

			std::vector<RangeFile> ranges = wait(bc->readKeyspaceSnapshot(snapshot.get()));
			restorable.ranges = ranges;

			// No logs needed if there is a complete key space snapshot at the target version.
			if(snapshot.get().beginVersion == snapshot.get().endVersion && snapshot.get().endVersion == targetVersion)
				return Optional<RestorableFileSet>(restorable);

			std::vector<LogFile> logs = wait(bc->listLogFiles(snapshot.get().beginVersion, targetVersion));

			// If there are logs and the first one starts at or before the snapshot begin version then proceed
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
					return Optional<RestorableFileSet>(restorable);
				}
			}
		}

		return Optional<RestorableFileSet>();
	}

	Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion){
		return getRestoreSet_impl(Reference<BackupContainerFileSystem>::addRef(this), targetVersion);
	}

private:
	struct VersionProperty {
		VersionProperty(Reference<BackupContainerFileSystem> bc, std::string name) : bc(bc), path("properties/" + name) {}
		Reference<BackupContainerFileSystem> bc;
		std::string path;
		Future<Optional<Version>> get() {
			return readVersionProperty(bc, path);
		}
		Future<Void> set(Version v) {
			return writeVersionProperty(bc, path, v);
		}
		Future<Void> clear() {
			return bc->deleteFile(path);
		}
	};

public:
	// To avoid the need to scan the underyling filesystem in many cases, some important version boundaries are stored in named files.
	// These files can be deleted from the filesystem if they appear to be wrong or corrupt, and full scans will done
	// when needed.
	//
	// The three versions below, when present, describe 4 version ranges which collectively cover the entire version timeline.
	//                   0 -   expiredEndVersion:  All files in this range have been deleted
	//   expiredEndVersion - presentBeginVersion:  Files in this range *may* have been deleted so their presence must not be assumed.
	// presentBeginVersion -   presentEndVersion:  Files in this range have NOT been deleted by any FDB backup operations.
	//   presentEndVersion -            infinity:  Files in this range may or may not exist yet.  Scan to find what is there.
	//
	VersionProperty logBeginVersion() { return {Reference<BackupContainerFileSystem>::addRef(this), "log_begin_version"}; }
	VersionProperty logEndVersion() {   return {Reference<BackupContainerFileSystem>::addRef(this), "log_end_version"}; }
	VersionProperty expiredEndVersion() {   return {Reference<BackupContainerFileSystem>::addRef(this), "expired_end_version"}; }

	ACTOR static Future<Void> writeVersionProperty(Reference<BackupContainerFileSystem> bc, std::string path, Version v) {
		try {
			state Reference<IBackupFile> f = wait(bc->writeFile(path));
			std::string s = format("%lld", v);
			Void _ = wait(f->append(s.data(), s.size()));
			Void _ = wait(f->finish());
			return Void();
		} catch(Error &e) {
			if(e.code() != error_code_actor_cancelled)
				TraceEvent(SevWarn, "BackupContainerWritePropertyFailed").detail("Path", path).error(e);
			throw;
		}
	}

	ACTOR static Future<Optional<Version>> readVersionProperty(Reference<BackupContainerFileSystem> bc, std::string path) {
		try {
			state Reference<IAsyncFile> f = wait(bc->readFile(path));
			state int64_t size = wait(f->size());
			state std::string s;
			s.resize(size);
			int rs = wait(f->read((uint8_t *)s.data(), size, 0));
			Version v;
			int len;
			if(rs == size && sscanf(s.c_str(), "%lld%n", &v, &len) == 1 && len == size)
				return v;

			TraceEvent(SevWarn, "BackupContainerInvalidProperty");
			throw backup_invalid_info();
		} catch(Error &e) {
			if(e.code() == error_code_file_not_found)
				return Optional<Version>();
			if(e.code() != error_code_actor_cancelled)
				TraceEvent(SevWarn, "BackupContainerReadPropertyFailed").detail("Path", path).error(e);
			throw;
		}
	}
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

	static Future<std::vector<std::string>> listURLs(std::string url) {
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
		std::vector<std::string> dirs = platform::listDirectories(path);
		std::vector<std::string> results;

		for(auto &r : dirs) {
			if(r == "." || r == "..")
				continue;
			results.push_back(std::string("file://") + joinPath(path, r));
		}

		return results;
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
			Void _ = wait(f->m_file->truncate(f->size()));  // Some IAsyncFile implementations extend in whole block sizes.
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

	Future<FilesAndSizesT> listFiles(std::string path, std::function<bool(std::string const &)>) {
		FilesAndSizesT results;

		std::vector<std::string> files;
		platform::findFilesRecursively(joinPath(m_path, path), files);

		// Remove .lnk files from results, they are a side effect of a backup that was *read* during simulation.  See openFile() above for more info on why they are created.
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
		// In order to avoid deleting some random directory due to user error, first describe the backup
		// and make sure it has something in it.
		return map(describeBackup(), [=](BackupDescription const &desc) {
			// If the backup has no snapshots and no logs then it's probably not a valid backup
			if(desc.snapshots.size() == 0 && !desc.minLogBegin.present())
				throw backup_invalid_url();

			int count = platform::eraseDirectoryRecursive(m_path);
			if(pNumDeleted != nullptr)
				*pNumDeleted = count;

			return Void();
		});
	}

private:
	std::string m_path;
};

class BackupContainerBlobStore : public BackupContainerFileSystem, ReferenceCounted<BackupContainerBlobStore> {
private:
	// All backup data goes into a single bucket
	static const std::string BUCKET;

	// Backup files to under a single folder prefix with subfolders for each named backup
	static const std::string DATAFOLDER;

	// Indexfolder contains keys for which user-named backups exist.  Backup names can contain an arbitrary
	// number of slashes so the backup names are kept in a separate folder tree from their actual data.
	static const std::string INDEXFOLDER;

	Reference<BlobStoreEndpoint> m_bstore;
	std::string m_name;

	std::string dataPath(const std::string path) {
		return DATAFOLDER + "/" + m_name + "/" + path;
	}

	// Get the path of the backups's index entry
	std::string indexEntry() {
		return INDEXFOLDER + "/" + m_name;
	}

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
					Reference<IAsyncFile>(new AsyncFileBlobStoreRead(m_bstore, BUCKET, dataPath(path))),
					m_bstore->knobs.read_block_size,
					m_bstore->knobs.read_ahead_blocks,
					m_bstore->knobs.concurrent_reads_per_file,
					m_bstore->knobs.read_cache_blocks_per_file
				)
			);
	}

	ACTOR static Future<std::vector<std::string>> listURLs(Reference<BlobStoreEndpoint> bstore) {
		state std::string basePath = INDEXFOLDER + '/';
		BlobStoreEndpoint::ListResult contents = wait(bstore->listBucket(BUCKET, basePath));
		std::vector<std::string> results;
		for(auto &f : contents.objects) {
			results.push_back(bstore->getResourceURL(f.name.substr(basePath.size())));
		}
		return results;
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
		return Reference<IBackupFile>(new BackupFile(path, Reference<IAsyncFile>(new AsyncFileBlobStoreWrite(m_bstore, BUCKET, dataPath(path)))));
	}

	Future<Void> deleteFile(std::string path) {
		return m_bstore->deleteObject(BUCKET, dataPath(path));
	}

	ACTOR static Future<FilesAndSizesT> listFiles_impl(Reference<BackupContainerBlobStore> bc, std::string path, std::function<bool(std::string const &)> pathFilter) {
		// pathFilter expects container based paths, so create a wrapper which converts a raw path
		// to a container path by removing the known backup name prefix.
		state int prefixTrim = bc->dataPath("").size();
		std::function<bool(std::string const &)> rawPathFilter = [=](const std::string &folderPath) {
			ASSERT(folderPath.size() >= prefixTrim);
			return pathFilter(folderPath.substr(prefixTrim));
		};

		state BlobStoreEndpoint::ListResult result = wait(bc->m_bstore->listBucket(BUCKET, bc->dataPath(path), '/', std::numeric_limits<int>::max(), rawPathFilter));
		FilesAndSizesT files;
		for(auto &o : result.objects) {
			ASSERT(o.name.size() >= prefixTrim);
			files.push_back({o.name.substr(prefixTrim), o.size});
		}
		return files;
	}

	Future<FilesAndSizesT> listFiles(std::string path, std::function<bool(std::string const &)> pathFilter) {
		return listFiles_impl(Reference<BackupContainerBlobStore>::addRef(this), path, pathFilter);
	}

	ACTOR static Future<Void> create_impl(Reference<BackupContainerBlobStore> bc) {
		Void _ = wait(bc->m_bstore->createBucket(BUCKET));

		// Check/create the index entry
		bool exists = wait(bc->m_bstore->objectExists(BUCKET, bc->indexEntry()));
		if(!exists) {
			Void _ = wait(bc->m_bstore->writeEntireFile(BUCKET, bc->indexEntry(), ""));
		}

		return Void();
	}

	Future<Void> create() {
		return create_impl(Reference<BackupContainerBlobStore>::addRef(this));
	}

	ACTOR static Future<Void> deleteContainer_impl(Reference<BackupContainerBlobStore> bc, int *pNumDeleted) {
		// First delete everything under the data prefix in the bucket
		Void _ = wait(bc->m_bstore->deleteRecursively(BUCKET, bc->dataPath(""), pNumDeleted));

		// Now that all files are deleted, delete the index entry
		Void _ = wait(bc->m_bstore->deleteObject(BUCKET, bc->indexEntry()));

		return Void();
	}

	Future<Void> deleteContainer(int *pNumDeleted) {
		return deleteContainer_impl(Reference<BackupContainerBlobStore>::addRef(this), pNumDeleted);
	}
};

const std::string BackupContainerBlobStore::BUCKET = "FDB_BACKUPS_V2";
const std::string BackupContainerBlobStore::DATAFOLDER = "data";
const std::string BackupContainerBlobStore::INDEXFOLDER = "backups";

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
				if(!isalnum(c) && c != '_' && c != '-' && c != '.' && c != '/')
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

// Get a list of URLS to backup containers based on some a shorter URL.  This function knows about some set of supported
// URL types which support this sort of backup discovery.
ACTOR Future<std::vector<std::string>> listContainers_impl(std::string baseURL) {
	try {
		StringRef u(baseURL);
		if(u.startsWith(LiteralStringRef("file://"))) {
			std::vector<std::string> results = wait(BackupContainerLocalDirectory::listURLs(baseURL));
			return results;
		}
		else if(u.startsWith(LiteralStringRef("blobstore://"))) {
			std::string resource;
			Reference<BlobStoreEndpoint> bstore = BlobStoreEndpoint::fromString(baseURL, &resource, &IBackupContainer::lastOpenError);
			if(!resource.empty()) {
				TraceEvent(SevWarn, "BackupContainer").detail("Description", "Invalid backup container base URL, resource aka path should be blank.").detail("URL", baseURL);
				throw backup_invalid_url();
			}

			std::vector<std::string> results = wait(BackupContainerBlobStore::listURLs(bstore));
			return results;
		}
		else {
			IBackupContainer::lastOpenError = "invalid URL prefix";
			throw backup_invalid_url();
		}

	} catch(Error &e) {
		if(e.code() == error_code_actor_cancelled)
			throw;

		TraceEvent m(SevWarn, "BackupContainer");
		m.detail("Description", "Invalid backup container URL prefix.  See help.").detail("URL", baseURL);

		if(e.code() == error_code_backup_invalid_url)
			m.detail("lastOpenError", IBackupContainer::lastOpenError);
		throw;
	}
}

Future<std::vector<std::string>> IBackupContainer::listContainers(std::string baseURL) {
	return listContainers_impl(baseURL);
}

ACTOR Future<Version> timeKeeperVersionFromDatetime(std::string datetime, Database db) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);
	state Reference<ReadYourWritesTransaction> tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(db));

	int year, month, day, hour, minute, second;
	if (sscanf(datetime.c_str(), "%d-%d-%d.%d:%d:%d", &year, &month, &day, &hour, &minute, &second) != 6) {
		fprintf(stderr, "ERROR: Incorrect date/time format.\n");
		throw backup_error();
	}
	struct tm expDateTime = {0};
	expDateTime.tm_year = year - 1900;
	expDateTime.tm_mon = month - 1;
	expDateTime.tm_mday = day;
	expDateTime.tm_hour = hour;
	expDateTime.tm_min = minute;
	expDateTime.tm_sec = second;
	expDateTime.tm_isdst = -1;
	state int64_t time = (int64_t) mktime(&expDateTime);

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			state std::vector<std::pair<int64_t, Version>> results = wait( versionMap.getRange(tr, 0, time, 1, false, true) );
			if (results.size() != 1) {
				// No key less than time was found in the database
				// Look for a key >= time.
				Void _ = wait( store( versionMap.getRange(tr, time, std::numeric_limits<int64_t>::max(), 1), results) );

				if(results.size() != 1) {
					fprintf(stderr, "ERROR: Unable to calculate a version for given date/time.\n");
					throw backup_error();
				}
			}

			// Adjust version found by the delta between time and the time found and min with 0.
			auto &result = results[0];
			return std::max<Version>(0, result.second + (time - result.first) * CLIENT_KNOBS->CORE_VERSIONSPERSECOND);

		} catch (Error& e) {
			Void _ = wait(tr->onError(e));
		}
	}
}

ACTOR Future<Optional<int64_t>> timeKeeperEpochsFromVersion(Version v, Reference<ReadYourWritesTransaction> tr) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);

	// Binary search to find the closest date with a version <= v
	state int64_t min = 0;
	state int64_t max = (int64_t)now();
	state int64_t mid;
	state std::pair<int64_t, Version> found;

	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	loop {
		mid = (min + max + 1) / 2;  // ceiling

		// Find the highest time < mid
		state std::vector<std::pair<int64_t, Version>> results = wait( versionMap.getRange(tr, min, mid, 1, false, true) );

		if (results.size() != 1) {
			if(mid == min) {
				// There aren't any records having a version < v, so just look for any record having a time < now
				// and base a result on it
				Void _ = wait(store(versionMap.getRange(tr, 0, (int64_t)now(), 1), results));

				if (results.size() != 1) {
					// There aren't any timekeeper records to base a result on so return nothing
					return Optional<int64_t>();
				}

				found = results[0];
				break;
			}

			min = mid;
			continue;
		}

		found = results[0];

		if(v < found.second) {
			max = found.first;
		}
		else {
			if(found.first == min) {
				break;
			}
			min = found.first;
		}
	}

	return found.first + (v - found.second) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
}

ACTOR Future<Void> writeAndVerifyFile(Reference<IBackupContainer> c, Reference<IBackupFile> f, int size) {
	state Standalone<StringRef> content;
	if(size > 0) {
		content = makeString(size);
		for(int i = 0; i < content.size(); ++i)
			mutateString(content)[i] = (uint8_t)g_random->randomInt(0, 256);

		Void _ = wait(f->append(content.begin(), content.size()));
	}
	Void _ = wait(f->finish());
	state Reference<IAsyncFile> inputFile = wait(c->readFile(f->getFileName()));
	int64_t fileSize = wait(inputFile->size());
	ASSERT(size == fileSize);
	if(size > 0) {
		state Standalone<StringRef> buf = makeString(size);
		int b = wait(inputFile->read(mutateString(buf), buf.size(), 0));
		ASSERT(b == buf.size());
		ASSERT(buf == content);
	}
	return Void();
}

ACTOR Future<Void> testBackupContainer(std::string url) {
	printf("BackupContainerTest URL %s\n", url.c_str());

	state Reference<IBackupContainer> c = IBackupContainer::openContainer(url);

	// Make sure container doesn't exist, then create it.
	try {
		Void _ = wait(c->deleteContainer());
	} catch(Error &e) {
		if(e.code() != error_code_backup_invalid_url)
			throw;
	}

	Void _ = wait(c->create());

	state int64_t versionShift = g_random->randomInt64(0, std::numeric_limits<Version>::max() - 500);

	state Reference<IBackupFile> log1 = wait(c->writeLogFile(100 + versionShift, 150 + versionShift, 10));
	state Reference<IBackupFile> log2 = wait(c->writeLogFile(150 + versionShift, 300 + versionShift, 10));
	state Reference<IBackupFile> range1 = wait(c->writeRangeFile(160 + versionShift, 10));
	state Reference<IBackupFile> range2 = wait(c->writeRangeFile(300 + versionShift, 10));
	state Reference<IBackupFile> range3 = wait(c->writeRangeFile(310 + versionShift, 10));

	Void _ = wait(
		   writeAndVerifyFile(c, log1, 0)
		&& writeAndVerifyFile(c, log2, g_random->randomInt(0, 10000000))
		&& writeAndVerifyFile(c, range1, g_random->randomInt(0, 1000))
		&& writeAndVerifyFile(c, range2, g_random->randomInt(0, 100000))
		&& writeAndVerifyFile(c, range3, g_random->randomInt(0, 3000000))
	);

	Void _ = wait(
		   c->writeKeyspaceSnapshotFile({range1->getFileName(), range2->getFileName()}, range1->size() + range2->size())
		&& c->writeKeyspaceSnapshotFile({range3->getFileName()}, range3->size())
	);

	printf("Checking file list dump\n");
	FullBackupListing listing = wait(c->dumpFileList());
	ASSERT(listing.logs.size() == 2);
	ASSERT(listing.ranges.size() == 3);
	ASSERT(listing.snapshots.size() == 2);

	state BackupDescription desc = wait(c->describeBackup());
	printf("Backup Description 1\n%s", desc.toString().c_str());

	ASSERT(desc.maxRestorableVersion.present());
	Optional<RestorableFileSet> rest = wait(c->getRestoreSet(desc.maxRestorableVersion.get()));
	ASSERT(rest.present());
	ASSERT(rest.get().logs.size() == 0);
	ASSERT(rest.get().ranges.size() == 1);

	Optional<RestorableFileSet> rest = wait(c->getRestoreSet(150 + versionShift));
	ASSERT(!rest.present());

	Optional<RestorableFileSet> rest = wait(c->getRestoreSet(300 + versionShift));
	ASSERT(rest.present());
	ASSERT(rest.get().logs.size() == 1);
	ASSERT(rest.get().ranges.size() == 2);

	printf("Expire 1\n");
	Void _ = wait(c->expireData(100 + versionShift));
	BackupDescription d = wait(c->describeBackup());
	printf("Backup Description 2\n%s", d.toString().c_str());
	ASSERT(d.minLogBegin == 100 + versionShift);
	ASSERT(d.maxRestorableVersion == desc.maxRestorableVersion);

	printf("Expire 2\n");
	Void _ = wait(c->expireData(101 + versionShift));
	BackupDescription d = wait(c->describeBackup());
	printf("Backup Description 3\n%s", d.toString().c_str());
	ASSERT(d.minLogBegin == 100 + versionShift);
	ASSERT(d.maxRestorableVersion == desc.maxRestorableVersion);

	printf("Expire 3\n");
	Void _ = wait(c->expireData(300 + versionShift));
	BackupDescription d = wait(c->describeBackup());
	printf("Backup Description 4\n%s", d.toString().c_str());
	ASSERT(d.minLogBegin.present());
	ASSERT(d.snapshots.size() == desc.snapshots.size());
	ASSERT(d.maxRestorableVersion == desc.maxRestorableVersion);

	printf("Expire 4\n");
	Void _ = wait(c->expireData(301 + versionShift, true));
	BackupDescription d = wait(c->describeBackup());
	printf("Backup Description 4\n%s", d.toString().c_str());
	ASSERT(d.snapshots.size() == 1);
	ASSERT(!d.minLogBegin.present());

	Void _ = wait(c->deleteContainer());

	BackupDescription d = wait(c->describeBackup());
	printf("Backup Description 5\n%s", d.toString().c_str());
	ASSERT(d.snapshots.size() == 0);
	ASSERT(!d.minLogBegin.present());

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

TEST_CASE("backup/containers/url") {
	if (!g_network->isSimulated()) {
		const char *url = getenv("FDB_TEST_BACKUP_URL");
		ASSERT(url != nullptr);
		Void _ = wait(testBackupContainer(url));
	}
	return Void();
};

TEST_CASE("backup/containers_list") {
	if (!g_network->isSimulated()) {
		state const char *url = getenv("FDB_TEST_BACKUP_URL");
		ASSERT(url != nullptr);
		printf("Listing %s\n", url);
		std::vector<std::string> urls = wait(IBackupContainer::listContainers(url));
		for(auto &u : urls) {
			printf("%s\n", u.c_str());
		}
	}
	return Void();
};
