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
#include "flow/actorcompiler.h" // has to be last include

namespace IBackupFile_impl {

ACTOR Future<Void> appendStringRefWithLen(Reference<IBackupFile> file, Standalone<StringRef> s) {
	state uint32_t lenBuf = bigEndian32((uint32_t)s.size());
	wait(file->append(&lenBuf, sizeof(lenBuf)));
	wait(file->append(s.begin(), s.size()));
	return Void();
}
} // namespace IBackupFile_impl

Future<Void> IBackupFile::appendStringRefWithLen(Standalone<StringRef> s) {
	return IBackupFile_impl::appendStringRefWithLen(Reference<IBackupFile>::addRef(this), s);
}

std::string IBackupContainer::ExpireProgress::toString() const {
	std::string s = step + "...";
	if (total > 0) {
		s += format("%d/%d (%.2f%%)", done, total, double(done) / total * 100);
	}
	return s;
}

void BackupFileList::toStream(FILE* fout) const {
	for (const RangeFile& f : ranges) {
		fprintf(fout, "range %" PRId64 " %s\n", f.fileSize, f.fileName.c_str());
	}
	for (const LogFile& f : logs) {
		fprintf(fout, "log %" PRId64 " %s\n", f.fileSize, f.fileName.c_str());
	}
	for (const KeyspaceSnapshotFile& f : snapshots) {
		fprintf(fout, "snapshotManifest %" PRId64 " %s\n", f.totalSize, f.fileName.c_str());
	}
}

Future<Void> fetchTimes(Reference<ReadYourWritesTransaction> tr, std::map<Version, int64_t>* pVersionTimeMap) {
	std::vector<Future<Void>> futures;

	// Resolve each version in the map,
	for (auto& p : *pVersionTimeMap) {
		futures.push_back(map(timeKeeperEpochsFromVersion(p.first, tr), [=](Optional<int64_t> t) {
			if (t.present())
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

	for (const KeyspaceSnapshotFile& m : snapshots) {
		versionTimeMap[m.beginVersion];
		versionTimeMap[m.endVersion];
	}
	if (minLogBegin.present())
		versionTimeMap[minLogBegin.get()];
	if (maxLogEnd.present())
		versionTimeMap[maxLogEnd.get()];
	if (contiguousLogEnd.present())
		versionTimeMap[contiguousLogEnd.get()];
	if (minRestorableVersion.present())
		versionTimeMap[minRestorableVersion.get()];
	if (maxRestorableVersion.present())
		versionTimeMap[maxRestorableVersion.get()];

	return runRYWTransaction(cx,
	                         [=](Reference<ReadYourWritesTransaction> tr) { return fetchTimes(tr, &versionTimeMap); });
};

std::string BackupDescription::toString() const {
	std::string info;

	info.append(format("URL: %s\n", url.c_str()));
	info.append(format("Restorable: %s\n", maxRestorableVersion.present() ? "true" : "false"));
	info.append(format("Partitioned logs: %s\n", partitioned ? "true" : "false"));

	auto formatVersion = [&](Version v) {
		std::string s;
		if (!versionTimeMap.empty()) {
			auto i = versionTimeMap.find(v);
			if (i != versionTimeMap.end())
				s = format("%lld (%s)", v, BackupAgentBase::formatTime(i->second).c_str());
			else
				s = format("%lld (unknown)", v);
		} else if (maxLogEnd.present()) {
			double days = double(maxLogEnd.get() - v) / (CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 24 * 60 * 60);
			s = format("%lld (maxLogEnd %s%.2f days)", v, days < 0 ? "+" : "-", days);
		} else {
			s = format("%lld", v);
		}
		return s;
	};

	for (const KeyspaceSnapshotFile& m : snapshots) {
		info.append(
		    format("Snapshot:  startVersion=%s  endVersion=%s  totalBytes=%lld  restorable=%s  expiredPct=%.2f\n",
		           formatVersion(m.beginVersion).c_str(),
		           formatVersion(m.endVersion).c_str(),
		           m.totalSize,
		           m.restorable.orDefault(false) ? "true" : "false",
		           m.expiredPct(expiredEndVersion)));
	}

	info.append(format("SnapshotBytes: %lld\n", snapshotBytes));

	if (expiredEndVersion.present())
		info.append(format("ExpiredEndVersion:       %s\n", formatVersion(expiredEndVersion.get()).c_str()));
	if (unreliableEndVersion.present())
		info.append(format("UnreliableEndVersion:    %s\n", formatVersion(unreliableEndVersion.get()).c_str()));
	if (minLogBegin.present())
		info.append(format("MinLogBeginVersion:      %s\n", formatVersion(minLogBegin.get()).c_str()));
	if (contiguousLogEnd.present())
		info.append(format("ContiguousLogEndVersion: %s\n", formatVersion(contiguousLogEnd.get()).c_str()));
	if (maxLogEnd.present())
		info.append(format("MaxLogEndVersion:        %s\n", formatVersion(maxLogEnd.get()).c_str()));
	if (minRestorableVersion.present())
		info.append(format("MinRestorableVersion:    %s\n", formatVersion(minRestorableVersion.get()).c_str()));
	if (maxRestorableVersion.present())
		info.append(format("MaxRestorableVersion:    %s\n", formatVersion(maxRestorableVersion.get()).c_str()));

	if (!extendedDetail.empty())
		info.append("ExtendedDetail: ").append(extendedDetail);

	return info;
}

std::string BackupDescription::toJSON() const {
	JsonBuilderObject doc;

	doc.setKey("SchemaVersion", "1.0.0");
	doc.setKey("URL", url.c_str());
	doc.setKey("Restorable", maxRestorableVersion.present());
	doc.setKey("Partitioned", partitioned);

	auto formatVersion = [&](Version v) {
		JsonBuilderObject doc;
		doc.setKey("Version", v);
		if (!versionTimeMap.empty()) {
			auto i = versionTimeMap.find(v);
			if (i != versionTimeMap.end()) {
				doc.setKey("Timestamp", BackupAgentBase::formatTime(i->second));
				doc.setKey("EpochSeconds", i->second);
			}
		} else if (maxLogEnd.present()) {
			double days = double(v - maxLogEnd.get()) / (CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 24 * 60 * 60);
			doc.setKey("RelativeDays", days);
		}
		return doc;
	};

	JsonBuilderArray snapshotsArray;
	for (const KeyspaceSnapshotFile& m : snapshots) {
		JsonBuilderObject snapshotDoc;
		snapshotDoc.setKey("Start", formatVersion(m.beginVersion));
		snapshotDoc.setKey("End", formatVersion(m.endVersion));
		snapshotDoc.setKey("Restorable", m.restorable.orDefault(false));
		snapshotDoc.setKey("TotalBytes", m.totalSize);
		snapshotDoc.setKey("PercentageExpired", m.expiredPct(expiredEndVersion));
		snapshotsArray.push_back(snapshotDoc);
	}
	doc.setKey("Snapshots", snapshotsArray);

	doc.setKey("TotalSnapshotBytes", snapshotBytes);

	if (expiredEndVersion.present())
		doc.setKey("ExpiredEnd", formatVersion(expiredEndVersion.get()));
	if (unreliableEndVersion.present())
		doc.setKey("UnreliableEnd", formatVersion(unreliableEndVersion.get()));
	if (minLogBegin.present())
		doc.setKey("MinLogBegin", formatVersion(minLogBegin.get()));
	if (contiguousLogEnd.present())
		doc.setKey("ContiguousLogEnd", formatVersion(contiguousLogEnd.get()));
	if (maxLogEnd.present())
		doc.setKey("MaxLogEnd", formatVersion(maxLogEnd.get()));
	if (minRestorableVersion.present())
		doc.setKey("MinRestorablePoint", formatVersion(minRestorableVersion.get()));
	if (maxRestorableVersion.present())
		doc.setKey("MaxRestorablePoint", formatVersion(maxRestorableVersion.get()));

	if (!extendedDetail.empty())
		doc.setKey("ExtendedDetail", extendedDetail);

	return doc.getJson();
}

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
	typedef std::vector<std::pair<std::string, int64_t>> FilesAndSizesT;
	virtual Future<FilesAndSizesT> listFiles(std::string path = "",
	                                         std::function<bool(std::string const&)> folderPathFilter = nullptr) = 0;

	// Open a file for read by fileName
	Future<Reference<IAsyncFile>> readFile(std::string fileName) override = 0;

	// Open a file for write by fileName
	virtual Future<Reference<IBackupFile>> writeFile(std::string fileName) = 0;

	// Delete a file
	virtual Future<Void> deleteFile(std::string fileName) = 0;

	// Delete entire container.  During the process, if pNumDeleted is not null it will be
	// updated with the count of deleted files so that progress can be seen.
	Future<Void> deleteContainer(int* pNumDeleted) override = 0;

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

	// The innermost folder covers 100 seconds (1e8 versions) During a full speed backup it is possible though very
	// unlikely write about 10,000 snapshot range files during that time.
	static std::string old_rangeVersionFolderString(Version v) {
		return format("ranges/%s/", versionFolderString(v, 8).c_str());
	}

	// Get the root folder for a snapshot's data based on its begin version
	static std::string snapshotFolderString(Version snapshotBeginVersion) {
		return format("kvranges/snapshot.%018" PRId64, snapshotBeginVersion);
	}

	// Extract the snapshot begin version from a path
	static Version extractSnapshotBeginVersion(std::string path) {
		Version snapshotBeginVersion;
		if (sscanf(path.c_str(), "kvranges/snapshot.%018" SCNd64, &snapshotBeginVersion) == 1) {
			return snapshotBeginVersion;
		}
		return invalidVersion;
	}

	// The innermost folder covers 100,000 seconds (1e11 versions) which is 5,000 mutation log files at current
	// settings.
	static std::string logVersionFolderString(Version v, bool partitioned) {
		return format("%s/%s/", (partitioned ? "plogs" : "logs"), versionFolderString(v, 11).c_str());
	}

	Future<Reference<IBackupFile>> writeLogFile(Version beginVersion, Version endVersion, int blockSize) final {
		return writeFile(logVersionFolderString(beginVersion, false) +
		                 format("log,%lld,%lld,%s,%d",
		                        beginVersion,
		                        endVersion,
		                        deterministicRandom()->randomUniqueID().toString().c_str(),
		                        blockSize));
	}

	Future<Reference<IBackupFile>> writeTaggedLogFile(Version beginVersion,
	                                                  Version endVersion,
	                                                  int blockSize,
	                                                  uint16_t tagId,
	                                                  int totalTags) final {
		return writeFile(logVersionFolderString(beginVersion, true) +
		                 format("log,%lld,%lld,%s,%d-of-%d,%d",
		                        beginVersion,
		                        endVersion,
		                        deterministicRandom()->randomUniqueID().toString().c_str(),
		                        tagId,
		                        totalTags,
		                        blockSize));
	}

	Future<Reference<IBackupFile>> writeRangeFile(Version snapshotBeginVersion,
	                                              int snapshotFileCount,
	                                              Version fileVersion,
	                                              int blockSize) override {
		std::string fileName = format("range,%" PRId64 ",%s,%d",
		                              fileVersion,
		                              deterministicRandom()->randomUniqueID().toString().c_str(),
		                              blockSize);

		// In order to test backward compatibility in simulation, sometimes write to the old path format
		if (g_network->isSimulated() && deterministicRandom()->coinflip()) {
			return writeFile(old_rangeVersionFolderString(fileVersion) + fileName);
		}

		return writeFile(snapshotFolderString(snapshotBeginVersion) +
		                 format("/%d/", snapshotFileCount / (BUGGIFY ? 1 : 5000)) + fileName);
	}

	// Find what should be the filename of a path by finding whatever is after the last forward or backward slash, or
	// failing to find those, the whole string.
	static std::string fileNameOnly(std::string path) {
		// Find the last forward slash position, defaulting to 0 if not found
		int pos = path.find_last_of('/');
		if (pos == std::string::npos) {
			pos = 0;
		}
		// Find the last backward slash position after pos, and update pos if found
		int b = path.find_last_of('\\', pos);
		if (b != std::string::npos) {
			pos = b;
		}
		return path.substr(pos + 1);
	}

	static bool pathToRangeFile(RangeFile& out, std::string path, int64_t size) {
		std::string name = fileNameOnly(path);
		RangeFile f;
		f.fileName = path;
		f.fileSize = size;
		int len;
		if (sscanf(name.c_str(), "range,%" SCNd64 ",%*[^,],%u%n", &f.version, &f.blockSize, &len) == 2 &&
		    len == name.size()) {
			out = f;
			return true;
		}
		return false;
	}

	static bool pathToLogFile(LogFile& out, std::string path, int64_t size) {
		std::string name = fileNameOnly(path);
		LogFile f;
		f.fileName = path;
		f.fileSize = size;
		int len;
		if (sscanf(name.c_str(),
		           "log,%" SCNd64 ",%" SCNd64 ",%*[^,],%u%n",
		           &f.beginVersion,
		           &f.endVersion,
		           &f.blockSize,
		           &len) == 3 &&
		    len == name.size()) {
			out = f;
			return true;
		} else if (sscanf(name.c_str(),
		                  "log,%" SCNd64 ",%" SCNd64 ",%*[^,],%d-of-%d,%u%n",
		                  &f.beginVersion,
		                  &f.endVersion,
		                  &f.tagId,
		                  &f.totalTags,
		                  &f.blockSize,
		                  &len) == 5 &&
		           len == name.size() && f.tagId >= 0) {
			out = f;
			return true;
		}
		return false;
	}

	static bool pathToKeyspaceSnapshotFile(KeyspaceSnapshotFile& out, std::string path) {
		std::string name = fileNameOnly(path);
		KeyspaceSnapshotFile f;
		f.fileName = path;
		int len;
		if (sscanf(name.c_str(),
		           "snapshot,%" SCNd64 ",%" SCNd64 ",%" SCNd64 "%n",
		           &f.beginVersion,
		           &f.endVersion,
		           &f.totalSize,
		           &len) == 3 &&
		    len == name.size()) {
			out = f;
			return true;
		}
		return false;
	}

	// TODO:  Do this more efficiently, as the range file list for a snapshot could potentially be hundreds of
	// megabytes.
	ACTOR static Future<std::pair<std::vector<RangeFile>, std::map<std::string, KeyRange>>> readKeyspaceSnapshot_impl(
	    Reference<BackupContainerFileSystem> bc,
	    KeyspaceSnapshotFile snapshot) {
		// Read the range file list for the specified version range, and then index them by fileName.
		// This is so we can verify that each of the files listed in the manifest file are also in the container at this
		// time.
		std::vector<RangeFile> files = wait(bc->listRangeFiles(snapshot.beginVersion, snapshot.endVersion));
		state std::map<std::string, RangeFile> rangeIndex;
		for (auto& f : files)
			rangeIndex[f.fileName] = std::move(f);

		// Read the snapshot file, verify the version range, then find each of the range files by name in the index and
		// return them.
		state Reference<IAsyncFile> f = wait(bc->readFile(snapshot.fileName));
		int64_t size = wait(f->size());
		state Standalone<StringRef> buf = makeString(size);
		wait(success(f->read(mutateString(buf), buf.size(), 0)));
		json_spirit::mValue json;
		json_spirit::read_string(buf.toString(), json);
		JSONDoc doc(json);

		Version v;
		if (!doc.tryGet("beginVersion", v) || v != snapshot.beginVersion)
			throw restore_corrupted_data();
		if (!doc.tryGet("endVersion", v) || v != snapshot.endVersion)
			throw restore_corrupted_data();

		json_spirit::mValue& filesArray = doc.create("files");
		if (filesArray.type() != json_spirit::array_type)
			throw restore_corrupted_data();

		std::vector<RangeFile> results;
		int missing = 0;

		for (auto const& fileValue : filesArray.get_array()) {
			if (fileValue.type() != json_spirit::str_type)
				throw restore_corrupted_data();

			// If the file is not in the index then log the error but don't throw yet, keep checking the whole list.
			auto i = rangeIndex.find(fileValue.get_str());
			if (i == rangeIndex.end()) {
				TraceEvent(SevError, "FileRestoreMissingRangeFile")
				    .detail("URL", bc->getURL())
				    .detail("File", fileValue.get_str());

				++missing;
			}

			// No point in using more memory once data is missing since an error will be thrown instead.
			if (missing == 0) {
				results.push_back(i->second);
			}
		}

		if (missing > 0) {
			TraceEvent(SevError, "FileRestoreMissingRangeFileSummary")
			    .detail("URL", bc->getURL())
			    .detail("Count", missing);

			throw restore_missing_data();
		}

		// Check key ranges for files
		std::map<std::string, KeyRange> fileKeyRanges;
		JSONDoc ranges = doc.subDoc("keyRanges"); // Create an empty doc if not existed
		for (auto i : ranges.obj()) {
			const std::string& filename = i.first;
			JSONDoc fields(i.second);
			std::string begin, end;
			if (fields.tryGet("beginKey", begin) && fields.tryGet("endKey", end)) {
				TraceEvent("ManifestFields")
				    .detail("File", filename)
				    .detail("Begin", printable(StringRef(begin)))
				    .detail("End", printable(StringRef(end)));
				fileKeyRanges.emplace(filename, KeyRange(KeyRangeRef(StringRef(begin), StringRef(end))));
			} else {
				TraceEvent("MalFormattedManifest").detail("Key", filename);
				throw restore_corrupted_data();
			}
		}

		return std::make_pair(results, fileKeyRanges);
	}

	Future<std::pair<std::vector<RangeFile>, std::map<std::string, KeyRange>>> readKeyspaceSnapshot(
	    KeyspaceSnapshotFile snapshot) {
		return readKeyspaceSnapshot_impl(Reference<BackupContainerFileSystem>::addRef(this), snapshot);
	}

	ACTOR static Future<Void> writeKeyspaceSnapshotFile_impl(Reference<BackupContainerFileSystem> bc,
	                                                         std::vector<std::string> fileNames,
	                                                         std::vector<std::pair<Key, Key>> beginEndKeys,
	                                                         int64_t totalBytes) {
		ASSERT(!fileNames.empty() && fileNames.size() == beginEndKeys.size());

		state Version minVer = std::numeric_limits<Version>::max();
		state Version maxVer = 0;
		state RangeFile rf;
		state json_spirit::mArray fileArray;
		state int i;

		// Validate each filename, update version range
		for (i = 0; i < fileNames.size(); ++i) {
			auto const& f = fileNames[i];
			if (pathToRangeFile(rf, f, 0)) {
				fileArray.push_back(f);
				if (rf.version < minVer)
					minVer = rf.version;
				if (rf.version > maxVer)
					maxVer = rf.version;
			} else
				throw restore_unknown_file_type();
			wait(yield());
		}

		state json_spirit::mValue json;
		state JSONDoc doc(json);

		doc.create("files") = std::move(fileArray);
		doc.create("totalBytes") = totalBytes;
		doc.create("beginVersion") = minVer;
		doc.create("endVersion") = maxVer;

		auto ranges = doc.subDoc("keyRanges");
		for (int i = 0; i < beginEndKeys.size(); i++) {
			auto fileDoc = ranges.subDoc(fileNames[i], /*split=*/false);
			fileDoc.create("beginKey") = beginEndKeys[i].first.toString();
			fileDoc.create("endKey") = beginEndKeys[i].second.toString();
		}

		wait(yield());
		state std::string docString = json_spirit::write_string(json);

		state Reference<IBackupFile> f =
		    wait(bc->writeFile(format("snapshots/snapshot,%lld,%lld,%lld", minVer, maxVer, totalBytes)));
		wait(f->append(docString.data(), docString.size()));
		wait(f->finish());

		return Void();
	}

	Future<Void> writeKeyspaceSnapshotFile(const std::vector<std::string>& fileNames,
	                                       const std::vector<std::pair<Key, Key>>& beginEndKeys,
	                                       int64_t totalBytes) final {
		return writeKeyspaceSnapshotFile_impl(
		    Reference<BackupContainerFileSystem>::addRef(this), fileNames, beginEndKeys, totalBytes);
	};

	// List log files, unsorted, which contain data at any version >= beginVersion and <= targetVersion.
	// "partitioned" flag indicates if new partitioned mutation logs or old logs should be listed.
	Future<std::vector<LogFile>> listLogFiles(Version beginVersion, Version targetVersion, bool partitioned) {
		// The first relevant log file could have a begin version less than beginVersion based on the knobs which
		// determine log file range size, so start at an earlier version adjusted by how many versions a file could
		// contain.
		//
		// Get the cleaned (without slashes) first and last folders that could contain relevant results.
		std::string firstPath = cleanFolderString(logVersionFolderString(
		    std::max<Version>(0,
		                      beginVersion - CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE),
		    partitioned));
		std::string lastPath = cleanFolderString(logVersionFolderString(targetVersion, partitioned));

		std::function<bool(std::string const&)> pathFilter = [=](const std::string& folderPath) {
			// Remove slashes in the given folder path so that the '/' positions in the version folder string do not
			// matter

			std::string cleaned = cleanFolderString(folderPath);
			return StringRef(firstPath).startsWith(cleaned) || StringRef(lastPath).startsWith(cleaned) ||
			       (cleaned > firstPath && cleaned < lastPath);
		};

		return map(listFiles((partitioned ? "plogs/" : "logs/"), pathFilter), [=](const FilesAndSizesT& files) {
			std::vector<LogFile> results;
			LogFile lf;
			for (auto& f : files) {
				if (pathToLogFile(lf, f.first, f.second) && lf.endVersion > beginVersion &&
				    lf.beginVersion <= targetVersion)
					results.push_back(lf);
			}
			return results;
		});
	}

	// List range files, unsorted, which contain data at or between beginVersion and endVersion
	// NOTE: This reads the range file folder schema from FDB 6.0.15 and earlier and is provided for backward
	// compatibility
	Future<std::vector<RangeFile>> old_listRangeFiles(Version beginVersion, Version endVersion) {
		// Get the cleaned (without slashes) first and last folders that could contain relevant results.
		std::string firstPath = cleanFolderString(old_rangeVersionFolderString(beginVersion));
		std::string lastPath = cleanFolderString(old_rangeVersionFolderString(endVersion));

		std::function<bool(std::string const&)> pathFilter = [=](const std::string& folderPath) {
			// Remove slashes in the given folder path so that the '/' positions in the version folder string do not
			// matter
			std::string cleaned = cleanFolderString(folderPath);

			return StringRef(firstPath).startsWith(cleaned) || StringRef(lastPath).startsWith(cleaned) ||
			       (cleaned > firstPath && cleaned < lastPath);
		};

		return map(listFiles("ranges/", pathFilter), [=](const FilesAndSizesT& files) {
			std::vector<RangeFile> results;
			RangeFile rf;
			for (auto& f : files) {
				if (pathToRangeFile(rf, f.first, f.second) && rf.version >= beginVersion && rf.version <= endVersion)
					results.push_back(rf);
			}
			return results;
		});
	}

	// List range files, unsorted, which contain data at or between beginVersion and endVersion
	// Note: The contents of each top level snapshot.N folder do not necessarily constitute a valid snapshot
	// and therefore listing files is not how RestoreSets are obtained.
	// Note: Snapshots partially written using FDB versions prior to 6.0.16 will have some range files stored
	// using the old folder scheme read by old_listRangeFiles
	Future<std::vector<RangeFile>> listRangeFiles(Version beginVersion, Version endVersion) {
		// Until the old folder scheme is no longer supported, read files stored using old folder scheme
		Future<std::vector<RangeFile>> oldFiles = old_listRangeFiles(beginVersion, endVersion);

		// Define filter function (for listFiles() implementations that use it) to reject any folder
		// starting after endVersion
		std::function<bool(std::string const&)> pathFilter = [=](std::string const& path) {
			return extractSnapshotBeginVersion(path) <= endVersion;
		};

		Future<std::vector<RangeFile>> newFiles =
		    map(listFiles("kvranges/", pathFilter), [=](const FilesAndSizesT& files) {
			    std::vector<RangeFile> results;
			    RangeFile rf;
			    for (auto& f : files) {
				    if (pathToRangeFile(rf, f.first, f.second) && rf.version >= beginVersion &&
				        rf.version <= endVersion)
					    results.push_back(rf);
			    }
			    return results;
		    });

		return map(success(oldFiles) && success(newFiles), [=](Void _) {
			std::vector<RangeFile> results = std::move(newFiles.get());
			std::vector<RangeFile> oldResults = std::move(oldFiles.get());
			results.insert(
			    results.end(), std::make_move_iterator(oldResults.begin()), std::make_move_iterator(oldResults.end()));
			return results;
		});
	}

	// List snapshots which have been fully written, in sorted beginVersion order, which start before end and finish on
	// or after begin
	Future<std::vector<KeyspaceSnapshotFile>> listKeyspaceSnapshots(Version begin = 0,
	                                                                Version end = std::numeric_limits<Version>::max()) {
		return map(listFiles("snapshots/"), [=](const FilesAndSizesT& files) {
			std::vector<KeyspaceSnapshotFile> results;
			KeyspaceSnapshotFile sf;
			for (auto& f : files) {
				if (pathToKeyspaceSnapshotFile(sf, f.first) && sf.beginVersion < end && sf.endVersion >= begin)
					results.push_back(sf);
			}
			std::sort(results.begin(), results.end());
			return results;
		});
	}

	ACTOR static Future<BackupFileList> dumpFileList_impl(Reference<BackupContainerFileSystem> bc,
	                                                      Version begin,
	                                                      Version end) {
		state Future<std::vector<RangeFile>> fRanges = bc->listRangeFiles(begin, end);
		state Future<std::vector<KeyspaceSnapshotFile>> fSnapshots = bc->listKeyspaceSnapshots(begin, end);
		state std::vector<LogFile> logs;
		state std::vector<LogFile> pLogs;

		wait(success(fRanges) && success(fSnapshots) && store(logs, bc->listLogFiles(begin, end, false)) &&
		     store(pLogs, bc->listLogFiles(begin, end, true)));
		logs.insert(logs.end(), std::make_move_iterator(pLogs.begin()), std::make_move_iterator(pLogs.end()));

		return BackupFileList({ fRanges.get(), std::move(logs), fSnapshots.get() });
	}

	Future<BackupFileList> dumpFileList(Version begin, Version end) override {
		return dumpFileList_impl(Reference<BackupContainerFileSystem>::addRef(this), begin, end);
	}

	static Version resolveRelativeVersion(Optional<Version> max, Version v, const char* name, Error e) {
		if (v == invalidVersion) {
			TraceEvent(SevError, "BackupExpireInvalidVersion").detail(name, v);
			throw e;
		}
		if (v < 0) {
			if (!max.present()) {
				TraceEvent(SevError, "BackupExpireCannotResolveRelativeVersion").detail(name, v);
				throw e;
			}
			v += max.get();
		}
		return v;
	}

	// Computes the continuous end version for non-partitioned mutation logs up to
	// the "targetVersion". If "outLogs" is not nullptr, it will be updated with
	// continuous log files. "*end" is updated with the continuous end version.
	static void computeRestoreEndVersion(const std::vector<LogFile>& logs,
	                                     std::vector<LogFile>* outLogs,
	                                     Version* end,
	                                     Version targetVersion) {
		auto i = logs.begin();
		if (outLogs != nullptr)
			outLogs->push_back(*i);

		// Add logs to restorable logs set until continuity is broken OR we reach targetVersion
		while (++i != logs.end()) {
			if (i->beginVersion > *end || i->beginVersion > targetVersion)
				break;

			// If the next link in the log chain is found, update the end
			if (i->beginVersion == *end) {
				if (outLogs != nullptr)
					outLogs->push_back(*i);
				*end = i->endVersion;
			}
		}
	}

	ACTOR static Future<BackupDescription> describeBackup_impl(Reference<BackupContainerFileSystem> bc,
	                                                           bool deepScan,
	                                                           Version logStartVersionOverride) {
		state BackupDescription desc;
		desc.url = bc->getURL();

		TraceEvent("BackupContainerDescribe1")
		    .detail("URL", bc->getURL())
		    .detail("LogStartVersionOverride", logStartVersionOverride);

		bool e = wait(bc->exists());
		if (!e) {
			TraceEvent(SevWarnAlways, "BackupContainerDoesNotExist").detail("URL", bc->getURL());
			throw backup_does_not_exist();
		}

		// If logStartVersion is relative, then first do a recursive call without it to find the max log version
		// from which to resolve the relative version.
		// This could be handled more efficiently without recursion but it's tricky, this will do for now.
		if (logStartVersionOverride != invalidVersion && logStartVersionOverride < 0) {
			BackupDescription tmp = wait(bc->describeBackup(false, invalidVersion));
			logStartVersionOverride = resolveRelativeVersion(
			    tmp.maxLogEnd, logStartVersionOverride, "LogStartVersionOverride", invalid_option_value());
		}

		// Get metadata versions
		state Optional<Version> metaLogBegin;
		state Optional<Version> metaLogEnd;
		state Optional<Version> metaExpiredEnd;
		state Optional<Version> metaUnreliableEnd;
		state Optional<Version> metaLogType;

		std::vector<Future<Void>> metaReads;
		metaReads.push_back(store(metaExpiredEnd, bc->expiredEndVersion().get()));
		metaReads.push_back(store(metaUnreliableEnd, bc->unreliableEndVersion().get()));
		metaReads.push_back(store(metaLogType, bc->logType().get()));

		// Only read log begin/end versions if not doing a deep scan, otherwise scan files and recalculate them.
		if (!deepScan) {
			metaReads.push_back(store(metaLogBegin, bc->logBeginVersion().get()));
			metaReads.push_back(store(metaLogEnd, bc->logEndVersion().get()));
		}

		wait(waitForAll(metaReads));

		TraceEvent("BackupContainerDescribe2")
		    .detail("URL", bc->getURL())
		    .detail("LogStartVersionOverride", logStartVersionOverride)
		    .detail("ExpiredEndVersion", metaExpiredEnd.orDefault(invalidVersion))
		    .detail("UnreliableEndVersion", metaUnreliableEnd.orDefault(invalidVersion))
		    .detail("LogBeginVersion", metaLogBegin.orDefault(invalidVersion))
		    .detail("LogEndVersion", metaLogEnd.orDefault(invalidVersion))
		    .detail("LogType", metaLogType.orDefault(-1));

		// If the logStartVersionOverride is positive (not relative) then ensure that unreliableEndVersion is equal or
		// greater
		if (logStartVersionOverride != invalidVersion &&
		    metaUnreliableEnd.orDefault(invalidVersion) < logStartVersionOverride) {
			metaUnreliableEnd = logStartVersionOverride;
		}

		// Don't use metaLogBegin or metaLogEnd if any of the following are true, the safest
		// thing to do is rescan to verify log continuity and get exact begin/end versions
		//   - either are missing
		//   - metaLogEnd <= metaLogBegin       (invalid range)
		//   - metaLogEnd < metaExpiredEnd      (log continuity exists in missing data range)
		//   - metaLogEnd < metaUnreliableEnd   (log continuity exists in incomplete data range)
		if (!metaLogBegin.present() || !metaLogEnd.present() || metaLogEnd.get() <= metaLogBegin.get() ||
		    metaLogEnd.get() < metaExpiredEnd.orDefault(invalidVersion) ||
		    metaLogEnd.get() < metaUnreliableEnd.orDefault(invalidVersion)) {
			TraceEvent(SevWarnAlways, "BackupContainerMetadataInvalid")
			    .detail("URL", bc->getURL())
			    .detail("ExpiredEndVersion", metaExpiredEnd.orDefault(invalidVersion))
			    .detail("UnreliableEndVersion", metaUnreliableEnd.orDefault(invalidVersion))
			    .detail("LogBeginVersion", metaLogBegin.orDefault(invalidVersion))
			    .detail("LogEndVersion", metaLogEnd.orDefault(invalidVersion));

			metaLogBegin = Optional<Version>();
			metaLogEnd = Optional<Version>();
		}

		// If the unreliable end version is not set or is < expiredEndVersion then increase it to expiredEndVersion.
		// Describe does not update unreliableEnd in the backup metadata for safety reasons as there is no
		// compare-and-set operation to atomically change it and an expire process could be advancing it simultaneously.
		if (!metaUnreliableEnd.present() || metaUnreliableEnd.get() < metaExpiredEnd.orDefault(0))
			metaUnreliableEnd = metaExpiredEnd;

		desc.unreliableEndVersion = metaUnreliableEnd;
		desc.expiredEndVersion = metaExpiredEnd;

		// Start scanning at the end of the unreliable version range, which is the version before which data is likely
		// missing because an expire process has operated on that range.
		state Version scanBegin = desc.unreliableEndVersion.orDefault(0);
		state Version scanEnd = std::numeric_limits<Version>::max();

		// Use the known log range if present
		// Logs are assumed to be contiguious between metaLogBegin and metaLogEnd, so initalize desc accordingly
		if (metaLogBegin.present() && metaLogEnd.present()) {
			// minLogBegin is the greater of the log begin metadata OR the unreliable end version since we can't count
			// on log file presence before that version.
			desc.minLogBegin = std::max(metaLogBegin.get(), desc.unreliableEndVersion.orDefault(0));

			// Set the maximum known end version of a log file, so far, which is also the assumed contiguous log file
			// end version
			desc.maxLogEnd = metaLogEnd.get();
			desc.contiguousLogEnd = desc.maxLogEnd;

			// Advance scanBegin to the contiguous log end version
			scanBegin = desc.contiguousLogEnd.get();
		}

		state std::vector<LogFile> logs;
		state std::vector<LogFile> plogs;
		TraceEvent("BackupContainerListFiles").detail("URL", bc->getURL());

		wait(store(logs, bc->listLogFiles(scanBegin, scanEnd, false)) &&
		     store(plogs, bc->listLogFiles(scanBegin, scanEnd, true)) &&
		     store(desc.snapshots, bc->listKeyspaceSnapshots()));

		TraceEvent("BackupContainerListFiles")
		    .detail("URL", bc->getURL())
		    .detail("LogFiles", logs.size())
		    .detail("PLogsFiles", plogs.size())
		    .detail("Snapshots", desc.snapshots.size());

		if (plogs.size() > 0) {
			desc.partitioned = true;
			logs.swap(plogs);
		} else {
			desc.partitioned = metaLogType.present() && metaLogType.get() == PARTITIONED_MUTATION_LOG;
		}

		// List logs in version order so log continuity can be analyzed
		std::sort(logs.begin(), logs.end());

		// Find out contiguous log end version
		if (!logs.empty()) {
			desc.maxLogEnd = logs.rbegin()->endVersion;
			// If we didn't get log versions above then seed them using the first log file
			if (!desc.contiguousLogEnd.present()) {
				desc.minLogBegin = logs.begin()->beginVersion;
				if (desc.partitioned) {
					// Cannot use the first file's end version, which may not be contiguous
					// for other partitions. Set to its beginVersion to be safe.
					desc.contiguousLogEnd = logs.begin()->beginVersion;
				} else {
					desc.contiguousLogEnd = logs.begin()->endVersion;
				}
			}

			if (desc.partitioned) {
				updatePartitionedLogsContinuousEnd(&desc, logs, scanBegin, scanEnd);
			} else {
				Version& end = desc.contiguousLogEnd.get();
				computeRestoreEndVersion(logs, nullptr, &end, std::numeric_limits<Version>::max());
			}
		}

		// Only update stored contiguous log begin and end versions if we did NOT use a log start override.
		// Otherwise, a series of describe operations can result in a version range which is actually missing data.
		if (logStartVersionOverride == invalidVersion) {
			// If the log metadata begin/end versions are missing (or treated as missing due to invalidity) or
			// differ from the newly calculated values for minLogBegin and contiguousLogEnd, respectively,
			// then attempt to update the metadata in the backup container but ignore errors in case the
			// container is not writeable.
			try {
				state Future<Void> updates = Void();

				if (desc.minLogBegin.present() && metaLogBegin != desc.minLogBegin) {
					updates = updates && bc->logBeginVersion().set(desc.minLogBegin.get());
				}

				if (desc.contiguousLogEnd.present() && metaLogEnd != desc.contiguousLogEnd) {
					updates = updates && bc->logEndVersion().set(desc.contiguousLogEnd.get());
				}

				if (!metaLogType.present()) {
					updates = updates && bc->logType().set(desc.partitioned ? PARTITIONED_MUTATION_LOG
					                                                        : NON_PARTITIONED_MUTATION_LOG);
				}

				wait(updates);
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				TraceEvent(SevWarn, "BackupContainerMetadataUpdateFailure").error(e).detail("URL", bc->getURL());
			}
		}

		for (auto& s : desc.snapshots) {
			// Calculate restorability of each snapshot.  Assume true, then try to prove false
			s.restorable = true;
			// If this is not a single-version snapshot then see if the available contiguous logs cover its range
			if (s.beginVersion != s.endVersion) {
				if (!desc.minLogBegin.present() || desc.minLogBegin.get() > s.beginVersion)
					s.restorable = false;
				if (!desc.contiguousLogEnd.present() || desc.contiguousLogEnd.get() <= s.endVersion)
					s.restorable = false;
			}

			desc.snapshotBytes += s.totalSize;

			// If the snapshot is at a single version then it requires no logs.  Update min and max restorable.
			// TODO:  Somehow check / report if the restorable range is not or may not be contiguous.
			if (s.beginVersion == s.endVersion) {
				if (!desc.minRestorableVersion.present() || s.endVersion < desc.minRestorableVersion.get())
					desc.minRestorableVersion = s.endVersion;

				if (!desc.maxRestorableVersion.present() || s.endVersion > desc.maxRestorableVersion.get())
					desc.maxRestorableVersion = s.endVersion;
			}

			// If the snapshot is covered by the contiguous log chain then update min/max restorable.
			if (desc.minLogBegin.present() && s.beginVersion >= desc.minLogBegin.get() &&
			    s.endVersion < desc.contiguousLogEnd.get()) {
				if (!desc.minRestorableVersion.present() || s.endVersion < desc.minRestorableVersion.get())
					desc.minRestorableVersion = s.endVersion;

				if (!desc.maxRestorableVersion.present() ||
				    (desc.contiguousLogEnd.get() - 1) > desc.maxRestorableVersion.get())
					desc.maxRestorableVersion = desc.contiguousLogEnd.get() - 1;
			}
		}

		return desc;
	}

	// Uses the virtual methods to describe the backup contents
	Future<BackupDescription> describeBackup(bool deepScan, Version logStartVersionOverride) final {
		return describeBackup_impl(
		    Reference<BackupContainerFileSystem>::addRef(this), deepScan, logStartVersionOverride);
	}

	ACTOR static Future<Void> expireData_impl(Reference<BackupContainerFileSystem> bc,
	                                          Version expireEndVersion,
	                                          bool force,
	                                          ExpireProgress* progress,
	                                          Version restorableBeginVersion) {
		if (progress != nullptr) {
			progress->step = "Describing backup";
			progress->total = 0;
		}

		TraceEvent("BackupContainerFileSystemExpire1")
		    .detail("URL", bc->getURL())
		    .detail("ExpireEndVersion", expireEndVersion)
		    .detail("RestorableBeginVersion", restorableBeginVersion);

		// Get the backup description.
		state BackupDescription desc = wait(bc->describeBackup(false, expireEndVersion));

		// Resolve relative versions using max log version
		expireEndVersion =
		    resolveRelativeVersion(desc.maxLogEnd, expireEndVersion, "ExpireEndVersion", invalid_option_value());
		restorableBeginVersion = resolveRelativeVersion(
		    desc.maxLogEnd, restorableBeginVersion, "RestorableBeginVersion", invalid_option_value());

		// It would be impossible to have restorability to any version < expireEndVersion after expiring to that version
		if (restorableBeginVersion < expireEndVersion)
			throw backup_cannot_expire();

		// If the expire request is to a version at or before the previous version to which data was already deleted
		// then do nothing and just return
		if (expireEndVersion <= desc.expiredEndVersion.orDefault(invalidVersion)) {
			return Void();
		}

		// Assume force is needed, then try to prove otherwise.
		// Force is required if there is not a restorable snapshot which both
		//   - begins at or after expireEndVersion
		//   - ends at or before restorableBeginVersion
		state bool forceNeeded = true;
		for (KeyspaceSnapshotFile& s : desc.snapshots) {
			if (s.restorable.orDefault(false) && s.beginVersion >= expireEndVersion &&
			    s.endVersion <= restorableBeginVersion) {
				forceNeeded = false;
				break;
			}
		}

		// If force is needed but not passed then refuse to expire anything.
		// Note that it is possible for there to be no actual files in the backup prior to expireEndVersion,
		// if they were externally deleted or an expire operation deleted them but was terminated before
		// updating expireEndVersion
		if (forceNeeded && !force)
			throw backup_cannot_expire();

		// Start scan for files to delete at the last completed expire operation's end or 0.
		state Version scanBegin = desc.expiredEndVersion.orDefault(0);

		TraceEvent("BackupContainerFileSystemExpire2")
		    .detail("URL", bc->getURL())
		    .detail("ExpireEndVersion", expireEndVersion)
		    .detail("RestorableBeginVersion", restorableBeginVersion)
		    .detail("ScanBeginVersion", scanBegin);

		state std::vector<LogFile> logs;
		state std::vector<LogFile> pLogs; // partitioned mutation logs
		state std::vector<RangeFile> ranges;

		if (progress != nullptr) {
			progress->step = "Listing files";
		}
		// Get log files or range files that contain any data at or before expireEndVersion
		wait(store(logs, bc->listLogFiles(scanBegin, expireEndVersion - 1, false)) &&
		     store(pLogs, bc->listLogFiles(scanBegin, expireEndVersion - 1, true)) &&
		     store(ranges, bc->listRangeFiles(scanBegin, expireEndVersion - 1)));
		logs.insert(logs.end(), std::make_move_iterator(pLogs.begin()), std::make_move_iterator(pLogs.end()));

		// The new logBeginVersion will be taken from the last log file, if there is one
		state Optional<Version> newLogBeginVersion;
		if (!logs.empty()) {
			// Linear scan the unsorted logs to find the latest one in sorted order
			LogFile& last = *std::max_element(logs.begin(), logs.end());

			// If the last log ends at expireEndVersion then that will be the next log begin
			if (last.endVersion == expireEndVersion) {
				newLogBeginVersion = expireEndVersion;
			} else {
				// If the last log overlaps the expiredEnd then use the log's begin version and move the expiredEnd
				// back to match it and keep the last log file
				if (last.endVersion > expireEndVersion) {
					newLogBeginVersion = last.beginVersion;

					// Instead of modifying this potentially very large vector, just clear LogFile
					last = LogFile();

					expireEndVersion = newLogBeginVersion.get();
				}
			}
		}

		// Make a list of files to delete
		state std::vector<std::string> toDelete;

		// Move filenames out of vector then destroy it to save memory
		for (auto const& f : logs) {
			// We may have cleared the last log file earlier so skip any empty filenames
			if (!f.fileName.empty()) {
				toDelete.push_back(std::move(f.fileName));
			}
		}
		logs.clear();

		// Move filenames out of vector then destroy it to save memory
		for (auto const& f : ranges) {
			// The file version must be checked here again because it is likely that expireEndVersion is in the middle
			// of a log file, in which case after the log and range file listings are done (using the original
			// expireEndVersion) the expireEndVersion will be moved back slightly to the begin version of the last log
			// file found (which is also the first log to not be deleted)
			if (f.version < expireEndVersion) {
				toDelete.push_back(std::move(f.fileName));
			}
		}
		ranges.clear();

		for (auto const& f : desc.snapshots) {
			if (f.endVersion < expireEndVersion)
				toDelete.push_back(std::move(f.fileName));
		}
		desc = BackupDescription();

		// We are about to start deleting files, at which point all data prior to expireEndVersion is considered
		// 'unreliable' as some or all of it will be missing.  So before deleting anything, read unreliableEndVersion
		// (don't use cached value in desc) and update its value if it is missing or < expireEndVersion
		if (progress != nullptr) {
			progress->step = "Initial metadata update";
		}
		Optional<Version> metaUnreliableEnd = wait(bc->unreliableEndVersion().get());
		if (metaUnreliableEnd.orDefault(0) < expireEndVersion) {
			wait(bc->unreliableEndVersion().set(expireEndVersion));
		}

		if (progress != nullptr) {
			progress->step = "Deleting files";
			progress->total = toDelete.size();
			progress->done = 0;
		}

		// Delete files, but limit parallelism because the file list could use a lot of memory and the corresponding
		// delete actor states would use even more if they all existed at the same time.
		state std::list<Future<Void>> deleteFutures;

		while (!toDelete.empty() || !deleteFutures.empty()) {

			// While there are files to delete and budget in the deleteFutures list, start a delete
			while (!toDelete.empty() && deleteFutures.size() < CLIENT_KNOBS->BACKUP_CONCURRENT_DELETES) {
				deleteFutures.push_back(bc->deleteFile(toDelete.back()));
				toDelete.pop_back();
			}

			// Wait for deletes to finish until there are only targetDeletesInFlight remaining.
			// If there are no files left to start then this value is 0, otherwise it is one less
			// than the delete concurrency limit.
			state int targetFuturesSize = toDelete.empty() ? 0 : (CLIENT_KNOBS->BACKUP_CONCURRENT_DELETES - 1);

			while (deleteFutures.size() > targetFuturesSize) {
				wait(deleteFutures.front());
				if (progress != nullptr) {
					++progress->done;
				}
				deleteFutures.pop_front();
			}
		}

		if (progress != nullptr) {
			progress->step = "Final metadata update";
			progress->total = 0;
		}
		// Update the expiredEndVersion metadata to indicate that everything prior to that version has been
		// successfully deleted if the current version is lower or missing
		Optional<Version> metaExpiredEnd = wait(bc->expiredEndVersion().get());
		if (metaExpiredEnd.orDefault(0) < expireEndVersion) {
			wait(bc->expiredEndVersion().set(expireEndVersion));
		}

		return Void();
	}

	// Delete all data up to (but not including endVersion)
	Future<Void> expireData(Version expireEndVersion,
	                        bool force,
	                        ExpireProgress* progress,
	                        Version restorableBeginVersion) final {
		return expireData_impl(Reference<BackupContainerFileSystem>::addRef(this),
		                       expireEndVersion,
		                       force,
		                       progress,
		                       restorableBeginVersion);
	}

	// For a list of log files specified by their indices (of the same tag),
	// returns if they are continous in the range [begin, end]. If "tags" is not
	// nullptr, then it will be populated with [begin, end] -> tags, where next
	// pair's begin <= previous pair's end + 1. On return, the last pair's end
	// version (inclusive) gives the continuous range from begin.
	static bool isContinuous(const std::vector<LogFile>& files,
	                         const std::vector<int>& indices,
	                         Version begin,
	                         Version end,
	                         std::map<std::pair<Version, Version>, int>* tags) {
		Version lastBegin = invalidVersion;
		Version lastEnd = invalidVersion;
		int lastTags = -1;

		ASSERT(tags == nullptr || tags->empty());
		for (int idx : indices) {
			const LogFile& file = files[idx];
			if (lastEnd == invalidVersion) {
				if (file.beginVersion > begin)
					return false;
				if (file.endVersion > begin) {
					lastBegin = begin;
					lastTags = file.totalTags;
				} else {
					continue;
				}
			} else if (lastEnd < file.beginVersion) {
				if (tags != nullptr) {
					tags->emplace(std::make_pair(lastBegin, lastEnd - 1), lastTags);
				}
				return false;
			}

			if (lastTags != file.totalTags) {
				if (tags != nullptr) {
					tags->emplace(std::make_pair(lastBegin, file.beginVersion - 1), lastTags);
				}
				lastBegin = file.beginVersion;
				lastTags = file.totalTags;
			}
			lastEnd = file.endVersion;
			if (lastEnd > end)
				break;
		}
		if (tags != nullptr && lastBegin != invalidVersion) {
			tags->emplace(std::make_pair(lastBegin, std::min(end, lastEnd - 1)), lastTags);
		}
		return lastBegin != invalidVersion && lastEnd > end;
	}

	// Returns true if logs are continuous in the range [begin, end].
	// "files" should be pre-sorted according to version order.
	static bool isPartitionedLogsContinuous(const std::vector<LogFile>& files, Version begin, Version end) {
		std::map<int, std::vector<int>> tagIndices; // tagId -> indices in files
		for (int i = 0; i < files.size(); i++) {
			ASSERT(files[i].tagId >= 0 && files[i].tagId < files[i].totalTags);
			auto& indices = tagIndices[files[i].tagId];
			indices.push_back(i);
		}

		// check partition 0 is continuous and create a map of ranges to tags
		std::map<std::pair<Version, Version>, int> tags; // range [begin, end] -> tags
		if (!isContinuous(files, tagIndices[0], begin, end, &tags)) {
			TraceEvent(SevWarn, "BackupFileNotContinuous")
			    .detail("Partition", 0)
			    .detail("RangeBegin", begin)
			    .detail("RangeEnd", end);
			return false;
		}

		// for each range in tags, check all tags from 1 are continouous
		for (const auto& [beginEnd, count] : tags) {
			for (int i = 1; i < count; i++) {
				if (!isContinuous(files, tagIndices[i], beginEnd.first, std::min(beginEnd.second - 1, end), nullptr)) {
					TraceEvent(SevWarn, "BackupFileNotContinuous")
					    .detail("Partition", i)
					    .detail("RangeBegin", beginEnd.first)
					    .detail("RangeEnd", beginEnd.second);
					return false;
				}
			}
		}
		return true;
	}

	// Returns log files that are not duplicated, or subset of another log.
	// If a log file's progress is not saved, a new log file will be generated
	// with the same begin version. So we can have a file that contains a subset
	// of contents in another log file.
	// PRE-CONDITION: logs are already sorted by (tagId, beginVersion, endVersion).
	static std::vector<LogFile> filterDuplicates(const std::vector<LogFile>& logs) {
		std::vector<LogFile> filtered;
		int i = 0;
		for (int j = 1; j < logs.size(); j++) {
			if (logs[j].isSubset(logs[i])) {
				ASSERT(logs[j].fileSize <= logs[i].fileSize);
				continue;
			}

			if (!logs[i].isSubset(logs[j])) {
				filtered.push_back(logs[i]);
			}
			i = j;
		}
		if (i < logs.size())
			filtered.push_back(logs[i]);
		return filtered;
	}

	// Analyze partitioned logs and set contiguousLogEnd for "desc" if larger
	// than the "scanBegin" version.
	static void updatePartitionedLogsContinuousEnd(BackupDescription* desc,
	                                               const std::vector<LogFile>& logs,
	                                               const Version scanBegin,
	                                               const Version scanEnd) {
		if (logs.empty())
			return;

		Version snapshotBeginVersion = desc->snapshots.size() > 0 ? desc->snapshots[0].beginVersion : invalidVersion;
		Version begin = std::max(scanBegin, desc->minLogBegin.get());
		TraceEvent("ContinuousLogEnd")
		    .detail("ScanBegin", scanBegin)
		    .detail("ScanEnd", scanEnd)
		    .detail("Begin", begin)
		    .detail("ContiguousLogEnd", desc->contiguousLogEnd.get());
		for (const auto& file : logs) {
			if (file.beginVersion > begin) {
				if (scanBegin > 0)
					return;

				// scanBegin is 0
				desc->minLogBegin = file.beginVersion;
				begin = file.beginVersion;
			}

			Version ver = getPartitionedLogsContinuousEndVersion(logs, begin);
			if (ver >= desc->contiguousLogEnd.get()) {
				// contiguousLogEnd is not inclusive, so +1 here.
				desc->contiguousLogEnd.get() = ver + 1;
				TraceEvent("UpdateContinuousLogEnd").detail("Version", ver + 1);
				if (ver > snapshotBeginVersion)
					return;
			}
		}
	}

	// Returns the end version such that [begin, end] is continuous.
	// "logs" should be already sorted.
	static Version getPartitionedLogsContinuousEndVersion(const std::vector<LogFile>& logs, Version begin) {
		Version end = 0;

		std::map<int, std::vector<int>> tagIndices; // tagId -> indices in files
		for (int i = 0; i < logs.size(); i++) {
			ASSERT(logs[i].tagId >= 0);
			ASSERT(logs[i].tagId < logs[i].totalTags);
			auto& indices = tagIndices[logs[i].tagId];
			// filter out if indices.back() is subset of files[i] or vice versa
			if (!indices.empty()) {
				if (logs[indices.back()].isSubset(logs[i])) {
					ASSERT(logs[indices.back()].fileSize <= logs[i].fileSize);
					indices.back() = i;
				} else if (!logs[i].isSubset(logs[indices.back()])) {
					indices.push_back(i);
				}
			} else {
				indices.push_back(i);
			}
			end = std::max(end, logs[i].endVersion - 1);
		}
		TraceEvent("ContinuousLogEnd").detail("Begin", begin).detail("InitVersion", end);

		// check partition 0 is continuous in [begin, end] and create a map of ranges to partitions
		std::map<std::pair<Version, Version>, int> tags; // range [start, end] -> partitions
		isContinuous(logs, tagIndices[0], begin, end, &tags);
		if (tags.empty() || end <= begin)
			return 0;
		end = std::min(end, tags.rbegin()->first.second);
		TraceEvent("ContinuousLogEnd").detail("Partition", 0).detail("EndVersion", end).detail("Begin", begin);

		// for each range in tags, check all partitions from 1 are continouous
		Version lastEnd = begin;
		for (const auto& [beginEnd, count] : tags) {
			Version tagEnd = beginEnd.second; // This range's minimum continous partition version
			for (int i = 1; i < count; i++) {
				std::map<std::pair<Version, Version>, int> rangeTags;
				isContinuous(logs, tagIndices[i], beginEnd.first, beginEnd.second, &rangeTags);
				tagEnd = rangeTags.empty() ? 0 : std::min(tagEnd, rangeTags.rbegin()->first.second);
				TraceEvent("ContinuousLogEnd")
				    .detail("Partition", i)
				    .detail("EndVersion", tagEnd)
				    .detail("RangeBegin", beginEnd.first)
				    .detail("RangeEnd", beginEnd.second);
				if (tagEnd == 0)
					return lastEnd == begin ? 0 : lastEnd;
			}
			if (tagEnd < beginEnd.second) {
				return tagEnd;
			}
			lastEnd = beginEnd.second;
		}

		return end;
	}

	ACTOR static Future<KeyRange> getSnapshotFileKeyRange_impl(Reference<BackupContainerFileSystem> bc,
	                                                           RangeFile file) {
		state int readFileRetries = 0;
		state bool beginKeySet = false;
		state Key beginKey;
		state Key endKey;
		loop {
			try {
				state Reference<IAsyncFile> inFile = wait(bc->readFile(file.fileName));
				beginKeySet = false;
				state int64_t j = 0;
				for (; j < file.fileSize; j += file.blockSize) {
					int64_t len = std::min<int64_t>(file.blockSize, file.fileSize - j);
					Standalone<VectorRef<KeyValueRef>> blockData =
					    wait(fileBackup::decodeRangeFileBlock(inFile, j, len));
					if (!beginKeySet) {
						beginKey = blockData.front().key;
						beginKeySet = true;
					}
					endKey = blockData.back().key;
				}
				break;
			} catch (Error& e) {
				if (e.code() == error_code_restore_bad_read ||
				    e.code() == error_code_restore_unsupported_file_version ||
				    e.code() == error_code_restore_corrupted_data_padding) { // no retriable error
					TraceEvent(SevError, "BackupContainerGetSnapshotFileKeyRange").error(e);
					throw;
				} else if (e.code() == error_code_http_request_failed || e.code() == error_code_connection_failed ||
				           e.code() == error_code_timed_out || e.code() == error_code_lookup_failed) {
					// blob http request failure, retry
					TraceEvent(SevWarnAlways, "BackupContainerGetSnapshotFileKeyRangeConnectionFailure")
					    .detail("Retries", ++readFileRetries)
					    .error(e);
					wait(delayJittered(0.1));
				} else {
					TraceEvent(SevError, "BackupContainerGetSnapshotFileKeyRangeUnexpectedError").error(e);
					throw;
				}
			}
		}
		return KeyRange(KeyRangeRef(beginKey, endKey));
	}

	Future<KeyRange> getSnapshotFileKeyRange(const RangeFile& file) final {
		ASSERT(g_network->isSimulated());
		return getSnapshotFileKeyRange_impl(Reference<BackupContainerFileSystem>::addRef(this), file);
	}

	ACTOR static Future<Optional<RestorableFileSet>> getRestoreSet_impl(Reference<BackupContainerFileSystem> bc,
	                                                                    Version targetVersion,
	                                                                    VectorRef<KeyRangeRef> keyRangesFilter) {
		// Find the most recent keyrange snapshot through which we can restore filtered key ranges into targetVersion.
		state std::vector<KeyspaceSnapshotFile> snapshots = wait(bc->listKeyspaceSnapshots());
		state int i = snapshots.size() - 1;

		for (const auto& range : keyRangesFilter) {
			TraceEvent("BackupContainerGetRestoreSet").detail("RangeFilter", printable(range));
		}

		for (; i >= 0; i--) {
			// The smallest version of filtered range files >= snapshot beginVersion > targetVersion
			if (targetVersion >= 0 && snapshots[i].beginVersion > targetVersion) {
				continue;
			}

			state RestorableFileSet restorable;
			state Version minKeyRangeVersion = MAX_VERSION;
			state Version maxKeyRangeVersion = -1;

			std::pair<std::vector<RangeFile>, std::map<std::string, KeyRange>> results =
			    wait(bc->readKeyspaceSnapshot(snapshots[i]));

			// Old backup does not have metadata about key ranges and can not be filtered with key ranges.
			if (keyRangesFilter.size() && results.second.empty() && !results.first.empty()) {
				throw backup_not_filterable_with_key_ranges();
			}

			// Filter by keyRangesFilter.
			if (keyRangesFilter.empty()) {
				restorable.ranges = std::move(results.first);
				restorable.keyRanges = std::move(results.second);
				minKeyRangeVersion = snapshots[i].beginVersion;
				maxKeyRangeVersion = snapshots[i].endVersion;
			} else {
				for (const auto& rangeFile : results.first) {
					const auto& keyRange = results.second.at(rangeFile.fileName);
					if (keyRange.intersects(keyRangesFilter)) {
						restorable.ranges.push_back(rangeFile);
						restorable.keyRanges[rangeFile.fileName] = keyRange;
						minKeyRangeVersion = std::min(minKeyRangeVersion, rangeFile.version);
						maxKeyRangeVersion = std::max(maxKeyRangeVersion, rangeFile.version);
					}
				}
				// No range file matches 'keyRangesFilter'.
				if (restorable.ranges.empty()) {
					throw backup_not_overlapped_with_keys_filter();
				}
			}
			// 'latestVersion' represents using the minimum restorable version in a snapshot.
			restorable.targetVersion = targetVersion == latestVersion ? maxKeyRangeVersion : targetVersion;
			// Any version < maxKeyRangeVersion is not restorable.
			if (restorable.targetVersion < maxKeyRangeVersion)
				continue;

			restorable.snapshot = snapshots[i];
			// TODO: Reenable the sanity check after TooManyFiles error is resolved
			if (false && g_network->isSimulated()) {
				// Sanity check key ranges
				state std::map<std::string, KeyRange>::iterator rit;
				for (rit = restorable.keyRanges.begin(); rit != restorable.keyRanges.end(); rit++) {
					auto it = std::find_if(restorable.ranges.begin(),
					                       restorable.ranges.end(),
					                       [file = rit->first](const RangeFile f) { return f.fileName == file; });
					ASSERT(it != restorable.ranges.end());
					KeyRange result = wait(bc->getSnapshotFileKeyRange(*it));
					ASSERT(rit->second.begin <= result.begin && rit->second.end >= result.end);
				}
			}

			// No logs needed if there is a complete filtered key space snapshot at the target version.
			if (minKeyRangeVersion == maxKeyRangeVersion && maxKeyRangeVersion == restorable.targetVersion) {
				restorable.continuousBeginVersion = restorable.continuousEndVersion = invalidVersion;
				TraceEvent("BackupContainerGetRestorableFilesWithoutLogs")
				    .detail("KeyRangeVersion", restorable.targetVersion)
				    .detail("NumberOfRangeFiles", restorable.ranges.size())
				    .detail("KeyRangesFilter", printable(keyRangesFilter));
				return Optional<RestorableFileSet>(restorable);
			}

			// FIXME: check if there are tagged logs. for each tag, there is no version gap.
			state std::vector<LogFile> logs;
			state std::vector<LogFile> plogs;
			wait(store(logs, bc->listLogFiles(minKeyRangeVersion, restorable.targetVersion, false)) &&
			     store(plogs, bc->listLogFiles(minKeyRangeVersion, restorable.targetVersion, true)));

			if (plogs.size() > 0) {
				logs.swap(plogs);
				// sort by tag ID so that filterDuplicates works.
				std::sort(logs.begin(), logs.end(), [](const LogFile& a, const LogFile& b) {
					return std::tie(a.tagId, a.beginVersion, a.endVersion) <
					       std::tie(b.tagId, b.beginVersion, b.endVersion);
				});

				// Remove duplicated log files that can happen for old epochs.
				std::vector<LogFile> filtered = filterDuplicates(logs);
				restorable.logs.swap(filtered);
				// sort by version order again for continuous analysis
				std::sort(restorable.logs.begin(), restorable.logs.end());
				if (isPartitionedLogsContinuous(restorable.logs, minKeyRangeVersion, restorable.targetVersion)) {
					restorable.continuousBeginVersion = minKeyRangeVersion;
					restorable.continuousEndVersion = restorable.targetVersion + 1; // not inclusive
					return Optional<RestorableFileSet>(restorable);
				}
				return Optional<RestorableFileSet>();
			}

			// List logs in version order so log continuity can be analyzed
			std::sort(logs.begin(), logs.end());
			// If there are logs and the first one starts at or before the snapshot begin version then proceed
			if (!logs.empty() && logs.front().beginVersion <= minKeyRangeVersion) {
				Version end = logs.begin()->endVersion;
				computeRestoreEndVersion(logs, &restorable.logs, &end, restorable.targetVersion);
				if (end >= restorable.targetVersion) {
					restorable.continuousBeginVersion = logs.begin()->beginVersion;
					restorable.continuousEndVersion = end;
					return Optional<RestorableFileSet>(restorable);
				}
			}
		}
		return Optional<RestorableFileSet>();
	}

	Future<Optional<RestorableFileSet>> getRestoreSet(Version targetVersion,
	                                                  VectorRef<KeyRangeRef> keyRangesFilter) final {
		return getRestoreSet_impl(Reference<BackupContainerFileSystem>::addRef(this), targetVersion, keyRangesFilter);
	}

private:
	struct VersionProperty {
		VersionProperty(Reference<BackupContainerFileSystem> bc, std::string name)
		  : bc(bc), path("properties/" + name) {}
		Reference<BackupContainerFileSystem> bc;
		std::string path;
		Future<Optional<Version>> get() { return readVersionProperty(bc, path); }
		Future<Void> set(Version v) { return writeVersionProperty(bc, path, v); }
		Future<Void> clear() { return bc->deleteFile(path); }
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
	VersionProperty logBeginVersion() {
		return { Reference<BackupContainerFileSystem>::addRef(this), "log_begin_version" };
	}
	VersionProperty logEndVersion() {
		return { Reference<BackupContainerFileSystem>::addRef(this), "log_end_version" };
	}
	VersionProperty expiredEndVersion() {
		return { Reference<BackupContainerFileSystem>::addRef(this), "expired_end_version" };
	}
	VersionProperty unreliableEndVersion() {
		return { Reference<BackupContainerFileSystem>::addRef(this), "unreliable_end_version" };
	}

	// Backup log types
	const static Version NON_PARTITIONED_MUTATION_LOG = 0;
	const static Version PARTITIONED_MUTATION_LOG = 1;
	VersionProperty logType() { return { Reference<BackupContainerFileSystem>::addRef(this), "mutation_log_type" }; }

	ACTOR static Future<Void> writeVersionProperty(Reference<BackupContainerFileSystem> bc,
	                                               std::string path,
	                                               Version v) {
		try {
			state Reference<IBackupFile> f = wait(bc->writeFile(path));
			std::string s = format("%lld", v);
			wait(f->append(s.data(), s.size()));
			wait(f->finish());
			return Void();
		} catch (Error& e) {
			TraceEvent(SevWarn, "BackupContainerWritePropertyFailed")
			    .error(e)
			    .detail("URL", bc->getURL())
			    .detail("Path", path);
			throw;
		}
	}

	ACTOR static Future<Optional<Version>> readVersionProperty(Reference<BackupContainerFileSystem> bc,
	                                                           std::string path) {
		try {
			state Reference<IAsyncFile> f = wait(bc->readFile(path));
			state int64_t size = wait(f->size());
			state std::string s;
			s.resize(size);
			int rs = wait(f->read((uint8_t*)s.data(), size, 0));
			Version v;
			int len;
			if (rs == size && sscanf(s.c_str(), "%" SCNd64 "%n", &v, &len) == 1 && len == size)
				return v;

			TraceEvent(SevWarn, "BackupContainerInvalidProperty").detail("URL", bc->getURL()).detail("Path", path);

			throw backup_invalid_info();
		} catch (Error& e) {
			if (e.code() == error_code_file_not_found)
				return Optional<Version>();

			TraceEvent(SevWarn, "BackupContainerReadPropertyFailed")
			    .error(e)
			    .detail("URL", bc->getURL())
			    .detail("Path", path);

			throw;
		}
	}
};

class BackupContainerLocalDirectory : public BackupContainerFileSystem,
                                      ReferenceCounted<BackupContainerLocalDirectory> {
public:
	void addref() final { return ReferenceCounted<BackupContainerLocalDirectory>::addref(); }
	void delref() final { return ReferenceCounted<BackupContainerLocalDirectory>::delref(); }

	static std::string getURLFormat() { return "file://</path/to/base/dir/>"; }

	BackupContainerLocalDirectory(std::string url) {
		std::string path;
		if (url.find("file://") != 0) {
			TraceEvent(SevWarn, "BackupContainerLocalDirectory")
			    .detail("Description", "Invalid URL for BackupContainerLocalDirectory")
			    .detail("URL", url);
		}

		path = url.substr(7);
		// Remove trailing slashes on path
		path.erase(path.find_last_not_of("\\/") + 1);

		if (!g_network->isSimulated() && path != abspath(path)) {
			TraceEvent(SevWarn, "BackupContainerLocalDirectory")
			    .detail("Description", "Backup path must be absolute (e.g. file:///some/path)")
			    .detail("URL", url)
			    .detail("Path", path);
			throw io_error();
		}

		// Finalized path written to will be will be <path>/backup-<uid>
		m_path = path;
	}

	static Future<std::vector<std::string>> listURLs(std::string url) {
		std::string path;
		if (url.find("file://") != 0) {
			TraceEvent(SevWarn, "BackupContainerLocalDirectory")
			    .detail("Description", "Invalid URL for BackupContainerLocalDirectory")
			    .detail("URL", url);
		}

		path = url.substr(7);
		// Remove trailing slashes on path
		path.erase(path.find_last_not_of("\\/") + 1);

		if (!g_network->isSimulated() && path != abspath(path)) {
			TraceEvent(SevWarn, "BackupContainerLocalDirectory")
			    .detail("Description", "Backup path must be absolute (e.g. file:///some/path)")
			    .detail("URL", url)
			    .detail("Path", path);
			throw io_error();
		}
		std::vector<std::string> dirs = platform::listDirectories(path);
		std::vector<std::string> results;

		for (auto& r : dirs) {
			if (r == "." || r == "..")
				continue;
			results.push_back(std::string("file://") + joinPath(path, r));
		}

		return results;
	}

	Future<Void> create() final {
		// Nothing should be done here because create() can be called by any process working with the container URL,
		// such as fdbbackup. Since "local directory" containers are by definition local to the machine they are
		// accessed from, the container's creation (in this case the creation of a directory) must be ensured prior to
		// every file creation, which is done in openFile(). Creating the directory here will result in unnecessary
		// directories being created on machines that run fdbbackup but not agents.
		return Void();
	}

	// The container exists if the folder it resides in exists
	Future<bool> exists() final { return directoryExists(m_path); }

	Future<Reference<IAsyncFile>> readFile(std::string path) final {
		int flags = IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED;
		// Simulation does not properly handle opening the same file from multiple machines using a shared filesystem,
		// so create a symbolic link to make each file opening appear to be unique.  This could also work in production
		// but only if the source directory is writeable which shouldn't be required for a restore.
		std::string fullPath = joinPath(m_path, path);
#ifndef _WIN32
		if (g_network->isSimulated()) {
			if (!fileExists(fullPath)) {
				throw file_not_found();
			}

			if (g_simulator.getCurrentProcess()->uid == UID()) {
				TraceEvent(SevError, "BackupContainerReadFileOnUnsetProcessID");
			}
			std::string uniquePath = fullPath + "." + g_simulator.getCurrentProcess()->uid.toString() + ".lnk";
			unlink(uniquePath.c_str());
			ASSERT(symlink(basename(path).c_str(), uniquePath.c_str()) == 0);
			fullPath = uniquePath;
		}
// Opening cached mode forces read/write mode at a lower level, overriding the readonly request.  So cached mode
// can't be used because backup files are read-only.  Cached mode can only help during restore task retries handled
// by the same process that failed the first task execution anyway, which is a very rare case.
#endif
		Future<Reference<IAsyncFile>> f = IAsyncFileSystem::filesystem()->open(fullPath, flags, 0644);

		if (g_network->isSimulated()) {
			int blockSize = 0;
			// Extract block size from the filename, if present
			size_t lastComma = path.find_last_of(',');
			if (lastComma != path.npos) {
				blockSize = atoi(path.substr(lastComma + 1).c_str());
			}
			if (blockSize <= 0) {
				blockSize = deterministicRandom()->randomInt(1e4, 1e6);
			}
			if (deterministicRandom()->random01() < .01) {
				blockSize /= deterministicRandom()->randomInt(1, 3);
			}
			ASSERT(blockSize > 0);

			return map(f, [=](Reference<IAsyncFile> fr) {
				int readAhead = deterministicRandom()->randomInt(0, 3);
				int reads = deterministicRandom()->randomInt(1, 3);
				int cacheSize = deterministicRandom()->randomInt(0, 3);
				return Reference<IAsyncFile>(new AsyncFileReadAheadCache(fr, blockSize, readAhead, reads, cacheSize));
			});
		}

		return f;
	}

	class BackupFile : public IBackupFile, ReferenceCounted<BackupFile> {
	public:
		BackupFile(std::string fileName, Reference<IAsyncFile> file, std::string finalFullPath)
		  : IBackupFile(fileName), m_file(file), m_finalFullPath(finalFullPath), m_writeOffset(0),
		    m_blockSize(CLIENT_KNOBS->BACKUP_LOCAL_FILE_WRITE_BLOCK) {
			if (BUGGIFY) {
				m_blockSize = deterministicRandom()->randomInt(100, 20000);
			}
			m_buffer.reserve(m_buffer.arena(), m_blockSize);
		}

		Future<Void> append(const void* data, int len) {
			m_buffer.append(m_buffer.arena(), (const uint8_t*)data, len);

			if (m_buffer.size() >= m_blockSize) {
				return flush(m_blockSize);
			}

			return Void();
		}

		Future<Void> flush(int size) {
			// Avoid empty write
			if (size == 0) {
				return Void();
			}

			ASSERT(size <= m_buffer.size());

			// Keep a reference to the old buffer
			Standalone<VectorRef<uint8_t>> old = m_buffer;
			// Make a new buffer, initialized with the excess bytes over the block size from the old buffer
			m_buffer = Standalone<VectorRef<uint8_t>>(old.slice(size, old.size()));

			// Write the old buffer to the underlying file and update the write offset
			Future<Void> r = uncancellable(holdWhile(old, m_file->write(old.begin(), size, m_writeOffset)));
			m_writeOffset += size;

			return r;
		}

		ACTOR static Future<Void> finish_impl(Reference<BackupFile> f) {
			wait(f->flush(f->m_buffer.size()));
			wait(f->m_file->truncate(f->size())); // Some IAsyncFile implementations extend in whole block sizes.
			wait(f->m_file->sync());
			std::string name = f->m_file->getFilename();
			f->m_file.clear();
			wait(IAsyncFileSystem::filesystem()->renameFile(name, f->m_finalFullPath));
			return Void();
		}

		int64_t size() const { return m_buffer.size() + m_writeOffset; }

		Future<Void> finish() { return finish_impl(Reference<BackupFile>::addRef(this)); }

		void addref() override { return ReferenceCounted<BackupFile>::addref(); }
		void delref() override { return ReferenceCounted<BackupFile>::delref(); }

	private:
		Reference<IAsyncFile> m_file;
		Standalone<VectorRef<uint8_t>> m_buffer;
		int64_t m_writeOffset;
		std::string m_finalFullPath;
		int m_blockSize;
	};

	Future<Reference<IBackupFile>> writeFile(std::string path) {
		int flags = IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_CREATE |
		            IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE;
		std::string fullPath = joinPath(m_path, path);
		platform::createDirectory(parentDirectory(fullPath));
		std::string temp = fullPath + "." + deterministicRandom()->randomUniqueID().toString() + ".temp";
		Future<Reference<IAsyncFile>> f = IAsyncFileSystem::filesystem()->open(temp, flags, 0644);
		return map(f,
		           [=](Reference<IAsyncFile> f) { return Reference<IBackupFile>(new BackupFile(path, f, fullPath)); });
	}

	Future<Void> deleteFile(std::string path) final {
		::deleteFile(joinPath(m_path, path));
		return Void();
	}

	Future<FilesAndSizesT> listFiles(std::string path, std::function<bool(std::string const&)>) final {
		FilesAndSizesT results;

		std::vector<std::string> files;
		platform::findFilesRecursively(joinPath(m_path, path), files);

		// Remove .lnk files from results, they are a side effect of a backup that was *read* during simulation.  See
		// openFile() above for more info on why they are created.
		if (g_network->isSimulated())
			files.erase(
			    std::remove_if(files.begin(),
			                   files.end(),
			                   [](std::string const& f) { return StringRef(f).endsWith(LiteralStringRef(".lnk")); }),
			    files.end());

		for (auto& f : files) {
			// Hide .part or .temp files.
			StringRef s(f);
			if (!s.endsWith(LiteralStringRef(".part")) && !s.endsWith(LiteralStringRef(".temp")))
				results.push_back({ f.substr(m_path.size() + 1), ::fileSize(f) });
		}

		return results;
	}

	Future<Void> deleteContainer(int* pNumDeleted) final {
		// In order to avoid deleting some random directory due to user error, first describe the backup
		// and make sure it has something in it.
		return map(describeBackup(false, invalidVersion), [=](BackupDescription const& desc) {
			// If the backup has no snapshots and no logs then it's probably not a valid backup
			if (desc.snapshots.size() == 0 && !desc.minLogBegin.present())
				throw backup_invalid_url();

			int count = platform::eraseDirectoryRecursive(m_path);
			if (pNumDeleted != nullptr)
				*pNumDeleted = count;

			return Void();
		});
	}

private:
	std::string m_path;
};

class BackupContainerBlobStore : public BackupContainerFileSystem, ReferenceCounted<BackupContainerBlobStore> {
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
	BackupContainerBlobStore(Reference<BlobStoreEndpoint> bstore,
	                         std::string name,
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

	void addref() final { return ReferenceCounted<BackupContainerBlobStore>::addref(); }
	void delref() final { return ReferenceCounted<BackupContainerBlobStore>::delref(); }

	static std::string getURLFormat() {
		return BlobStoreEndpoint::getURLFormat(true) + " (Note: The 'bucket' parameter is required.)";
	}

	virtual ~BackupContainerBlobStore() {}

	Future<Reference<IAsyncFile>> readFile(std::string path) final {
		return Reference<IAsyncFile>(new AsyncFileReadAheadCache(
		    Reference<IAsyncFile>(new AsyncFileBlobStoreRead(m_bstore, m_bucket, dataPath(path))),
		    m_bstore->knobs.read_block_size,
		    m_bstore->knobs.read_ahead_blocks,
		    m_bstore->knobs.concurrent_reads_per_file,
		    m_bstore->knobs.read_cache_blocks_per_file));
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
		BackupFile(std::string fileName, Reference<IAsyncFile> file)
		  : IBackupFile(fileName), m_file(file), m_offset(0) {}

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

		int64_t size() const { return m_offset; }

		void addref() final { return ReferenceCounted<BackupFile>::addref(); }
		void delref() final { return ReferenceCounted<BackupFile>::delref(); }

	private:
		Reference<IAsyncFile> m_file;
		int64_t m_offset;
	};

	Future<Reference<IBackupFile>> writeFile(std::string path) final {
		return Reference<IBackupFile>(new BackupFile(
		    path, Reference<IAsyncFile>(new AsyncFileBlobStoreWrite(m_bstore, m_bucket, dataPath(path)))));
	}

	Future<Void> deleteFile(std::string path) final { return m_bstore->deleteObject(m_bucket, dataPath(path)); }

	ACTOR static Future<FilesAndSizesT> listFiles_impl(Reference<BackupContainerBlobStore> bc,
	                                                   std::string path,
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
Reference<IBackupContainer> IBackupContainer::openContainer(std::string url) {
	static std::map<std::string, Reference<IBackupContainer>> m_cache;

	Reference<IBackupContainer>& r = m_cache[url];
	if (r)
		return r;

	try {
		StringRef u(url);
		if (u.startsWith(LiteralStringRef("file://")))
			r = Reference<IBackupContainer>(new BackupContainerLocalDirectory(url));
		else if (u.startsWith(LiteralStringRef("blobstore://"))) {
			std::string resource;

			// The URL parameters contain blobstore endpoint tunables as well as possible backup-specific options.
			BlobStoreEndpoint::ParametersT backupParams;
			Reference<BlobStoreEndpoint> bstore =
			    BlobStoreEndpoint::fromString(url, &resource, &lastOpenError, &backupParams);

			if (resource.empty())
				throw backup_invalid_url();
			for (auto c : resource)
				if (!isalnum(c) && c != '_' && c != '-' && c != '.' && c != '/')
					throw backup_invalid_url();
			r = Reference<IBackupContainer>(new BackupContainerBlobStore(bstore, resource, backupParams));
		} else {
			lastOpenError = "invalid URL prefix";
			throw backup_invalid_url();
		}

		r->URL = url;
		return r;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;

		TraceEvent m(SevWarn, "BackupContainer");
		m.detail("Description", "Invalid container specification.  See help.");
		m.detail("URL", url);
		m.error(e);
		if (e.code() == error_code_backup_invalid_url)
			m.detail("LastOpenError", lastOpenError);

		throw;
	}
}

// Get a list of URLS to backup containers based on some a shorter URL.  This function knows about some set of supported
// URL types which support this sort of backup discovery.
ACTOR Future<std::vector<std::string>> listContainers_impl(std::string baseURL) {
	try {
		StringRef u(baseURL);
		if (u.startsWith(LiteralStringRef("file://"))) {
			std::vector<std::string> results = wait(BackupContainerLocalDirectory::listURLs(baseURL));
			return results;
		} else if (u.startsWith(LiteralStringRef("blobstore://"))) {
			std::string resource;

			BlobStoreEndpoint::ParametersT backupParams;
			Reference<BlobStoreEndpoint> bstore =
			    BlobStoreEndpoint::fromString(baseURL, &resource, &IBackupContainer::lastOpenError, &backupParams);

			if (!resource.empty()) {
				TraceEvent(SevWarn, "BackupContainer")
				    .detail("Description", "Invalid backup container base URL, resource aka path should be blank.")
				    .detail("URL", baseURL);
				throw backup_invalid_url();
			}

			// Create a dummy container to parse the backup-specific parameters from the URL and get a final bucket name
			BackupContainerBlobStore dummy(bstore, "dummy", backupParams);

			std::vector<std::string> results = wait(BackupContainerBlobStore::listURLs(bstore, dummy.getBucket()));
			return results;
		} else {
			IBackupContainer::lastOpenError = "invalid URL prefix";
			throw backup_invalid_url();
		}

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;

		TraceEvent m(SevWarn, "BackupContainer");

		m.detail("Description", "Invalid backup container URL prefix.  See help.");
		m.detail("URL", baseURL);
		m.error(e);
		if (e.code() == error_code_backup_invalid_url)
			m.detail("LastOpenError", IBackupContainer::lastOpenError);

		throw;
	}
}

Future<std::vector<std::string>> IBackupContainer::listContainers(std::string baseURL) {
	return listContainers_impl(baseURL);
}

ACTOR Future<Version> timeKeeperVersionFromDatetime(std::string datetime, Database db) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);
	state Reference<ReadYourWritesTransaction> tr =
	    Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(db));

	state int64_t time = BackupAgentBase::parseTime(datetime);
	if (time < 0) {
		fprintf(
		    stderr, "ERROR: Incorrect date/time or format.  Format is %s.\n", BackupAgentBase::timeFormat().c_str());
		throw backup_error();
	}

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state std::vector<std::pair<int64_t, Version>> results =
			    wait(versionMap.getRange(tr, 0, time, 1, false, true));
			if (results.size() != 1) {
				// No key less than time was found in the database
				// Look for a key >= time.
				wait(store(results, versionMap.getRange(tr, time, std::numeric_limits<int64_t>::max(), 1)));

				if (results.size() != 1) {
					fprintf(stderr, "ERROR: Unable to calculate a version for given date/time.\n");
					throw backup_error();
				}
			}

			// Adjust version found by the delta between time and the time found and min with 0.
			auto& result = results[0];
			return std::max<Version>(0, result.second + (time - result.first) * CLIENT_KNOBS->CORE_VERSIONSPERSECOND);

		} catch (Error& e) {
			wait(tr->onError(e));
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
		mid = (min + max + 1) / 2; // ceiling

		// Find the highest time < mid
		state std::vector<std::pair<int64_t, Version>> results =
		    wait(versionMap.getRange(tr, min, mid, 1, false, true));

		if (results.size() != 1) {
			if (mid == min) {
				// There aren't any records having a version < v, so just look for any record having a time < now
				// and base a result on it
				wait(store(results, versionMap.getRange(tr, 0, (int64_t)now(), 1)));

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

		if (v < found.second) {
			max = found.first;
		} else {
			if (found.first == min) {
				break;
			}
			min = found.first;
		}
	}

	return found.first + (v - found.second) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
}

namespace backup_test {

int chooseFileSize(std::vector<int>& sizes) {
	if (!sizes.empty()) {
		int size = sizes.back();
		sizes.pop_back();
		return size;
	}
	return deterministicRandom()->randomInt(0, 2e6);
}

ACTOR Future<Void> writeAndVerifyFile(Reference<IBackupContainer> c,
                                      Reference<IBackupFile> f,
                                      int size,
                                      FlowLock* lock) {
	state Standalone<VectorRef<uint8_t>> content;

	wait(lock->take(TaskPriority::DefaultYield, size));
	state FlowLock::Releaser releaser(*lock, size);

	printf("writeAndVerify size=%d file=%s\n", size, f->getFileName().c_str());

	content.resize(content.arena(), size);
	for (int i = 0; i < content.size(); ++i) {
		content[i] = (uint8_t)deterministicRandom()->randomInt(0, 256);
	}

	state VectorRef<uint8_t> sendBuf = content;
	while (sendBuf.size() > 0) {
		state int n = std::min(sendBuf.size(), deterministicRandom()->randomInt(1, 16384));
		wait(f->append(sendBuf.begin(), n));
		sendBuf.pop_front(n);
	}
	wait(f->finish());

	state Reference<IAsyncFile> inputFile = wait(c->readFile(f->getFileName()));
	int64_t fileSize = wait(inputFile->size());
	ASSERT(size == fileSize);
	if (size > 0) {
		state Standalone<VectorRef<uint8_t>> buf;
		buf.resize(buf.arena(), fileSize);
		int b = wait(inputFile->read(buf.begin(), buf.size(), 0));
		ASSERT(b == buf.size());
		ASSERT(buf == content);
	}
	return Void();
}

// Randomly advance version by up to 1 second of versions
Version nextVersion(Version v) {
	int64_t increment = deterministicRandom()->randomInt64(1, CLIENT_KNOBS->CORE_VERSIONSPERSECOND);
	return v + increment;
}

ACTOR Future<Void> testBackupContainer(std::string url) {
	state FlowLock lock(100e6);

	printf("BackupContainerTest URL %s\n", url.c_str());

	state Reference<IBackupContainer> c = IBackupContainer::openContainer(url);

	// Make sure container doesn't exist, then create it.
	try {
		wait(c->deleteContainer());
	} catch (Error& e) {
		if (e.code() != error_code_backup_invalid_url && e.code() != error_code_backup_does_not_exist)
			throw;
	}

	wait(c->create());

	state std::vector<Future<Void>> writes;
	state std::map<Version, std::vector<std::string>> snapshots;
	state std::map<Version, int64_t> snapshotSizes;
	state std::map<Version, std::vector<std::pair<Key, Key>>> snapshotBeginEndKeys;
	state int nRangeFiles = 0;
	state std::map<Version, std::string> logs;
	state Version v = deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max() / 2);

	// List of sizes to use to test edge cases on underlying file implementations
	state std::vector<int> fileSizes = { 0 };
	if (StringRef(url).startsWith(LiteralStringRef("blob"))) {
		fileSizes.push_back(CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE);
		fileSizes.push_back(CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE + 10);
	}

	loop {
		state Version logStart = v;
		state int kvfiles = deterministicRandom()->randomInt(0, 3);
		state Key begin = LiteralStringRef("");
		state Key end = LiteralStringRef("");
		state int blockSize = 3 * sizeof(uint32_t) + begin.size() + end.size() + 8;

		while (kvfiles > 0) {
			if (snapshots.empty()) {
				snapshots[v] = {};
				snapshotBeginEndKeys[v] = {};
				snapshotSizes[v] = 0;
				if (deterministicRandom()->coinflip()) {
					v = nextVersion(v);
				}
			}
			Reference<IBackupFile> range = wait(c->writeRangeFile(snapshots.rbegin()->first, 0, v, blockSize));
			++nRangeFiles;
			v = nextVersion(v);
			snapshots.rbegin()->second.push_back(range->getFileName());
			snapshotBeginEndKeys.rbegin()->second.emplace_back(begin, end);

			int size = chooseFileSize(fileSizes);
			snapshotSizes.rbegin()->second += size;
			writes.push_back(writeAndVerifyFile(c, range, size, &lock));

			if (deterministicRandom()->random01() < .2) {
				writes.push_back(c->writeKeyspaceSnapshotFile(
				    snapshots.rbegin()->second, snapshotBeginEndKeys.rbegin()->second, snapshotSizes.rbegin()->second));
				snapshots[v] = {};
				snapshotBeginEndKeys[v] = {};
				snapshotSizes[v] = 0;
				break;
			}

			--kvfiles;
		}

		if (logStart == v || deterministicRandom()->coinflip()) {
			v = nextVersion(v);
		}
		state Reference<IBackupFile> log = wait(c->writeLogFile(logStart, v, 10));
		logs[logStart] = log->getFileName();
		int size = chooseFileSize(fileSizes);
		writes.push_back(writeAndVerifyFile(c, log, size, &lock));

		// Randomly stop after a snapshot has finished and all manually seeded file sizes have been used.
		if (fileSizes.empty() && !snapshots.empty() && snapshots.rbegin()->second.empty() &&
		    deterministicRandom()->random01() < .2) {
			snapshots.erase(snapshots.rbegin()->first);
			break;
		}
	}

	wait(waitForAll(writes));

	state BackupFileList listing = wait(c->dumpFileList());
	ASSERT(listing.ranges.size() == nRangeFiles);
	ASSERT(listing.logs.size() == logs.size());
	ASSERT(listing.snapshots.size() == snapshots.size());

	state BackupDescription desc = wait(c->describeBackup());
	printf("\n%s\n", desc.toString().c_str());

	// Do a series of expirations and verify resulting state
	state int i = 0;
	for (; i < listing.snapshots.size(); ++i) {
		{
			// Ensure we can still restore to the latest version
			Optional<RestorableFileSet> rest = wait(c->getRestoreSet(desc.maxRestorableVersion.get()));
			ASSERT(rest.present());
		}

		{
			// Ensure we can restore to the end version of snapshot i
			Optional<RestorableFileSet> rest = wait(c->getRestoreSet(listing.snapshots[i].endVersion));
			ASSERT(rest.present());
		}

		// Test expiring to the end of this snapshot
		state Version expireVersion = listing.snapshots[i].endVersion;

		// Expire everything up to but not including the snapshot end version
		printf("EXPIRE TO %" PRId64 "\n", expireVersion);
		state Future<Void> f = c->expireData(expireVersion);
		wait(ready(f));

		// If there is an error, it must be backup_cannot_expire and we have to be on the last snapshot
		if (f.isError()) {
			ASSERT(f.getError().code() == error_code_backup_cannot_expire);
			ASSERT(i == listing.snapshots.size() - 1);
			wait(c->expireData(expireVersion, true));
		}

		BackupDescription d = wait(c->describeBackup());
		printf("\n%s\n", d.toString().c_str());
	}

	printf("DELETING\n");
	wait(c->deleteContainer());

	state Future<BackupDescription> d = c->describeBackup();
	wait(ready(d));
	ASSERT(d.isError() && d.getError().code() == error_code_backup_does_not_exist);

	BackupFileList empty = wait(c->dumpFileList());
	ASSERT(empty.ranges.size() == 0);
	ASSERT(empty.logs.size() == 0);
	ASSERT(empty.snapshots.size() == 0);

	printf("BackupContainerTest URL=%s PASSED.\n", url.c_str());

	return Void();
}

TEST_CASE("/backup/containers/localdir") {
	if (g_network->isSimulated())
		wait(testBackupContainer(format("file://simfdb/backups/%llx", timer_int())));
	else
		wait(testBackupContainer(format("file:///private/tmp/fdb_backups/%llx", timer_int())));
	return Void();
};

TEST_CASE("/backup/containers/url") {
	if (!g_network->isSimulated()) {
		const char* url = getenv("FDB_TEST_BACKUP_URL");
		ASSERT(url != nullptr);
		wait(testBackupContainer(url));
	}
	return Void();
};

TEST_CASE("/backup/containers_list") {
	if (!g_network->isSimulated()) {
		state const char* url = getenv("FDB_TEST_BACKUP_URL");
		ASSERT(url != nullptr);
		printf("Listing %s\n", url);
		std::vector<std::string> urls = wait(IBackupContainer::listContainers(url));
		for (auto& u : urls) {
			printf("%s\n", u.c_str());
		}
	}
	return Void();
};

TEST_CASE("/backup/time") {
	// test formatTime()
	for (int i = 0; i < 1000; ++i) {
		int64_t ts = deterministicRandom()->randomInt64(0, std::numeric_limits<int32_t>::max());
		ASSERT(BackupAgentBase::parseTime(BackupAgentBase::formatTime(ts)) == ts);
	}

	ASSERT(BackupAgentBase::parseTime("2019/03/18.17:51:11-0600") ==
	       BackupAgentBase::parseTime("2019/03/18.16:51:11-0700"));
	ASSERT(BackupAgentBase::parseTime("2019/03/31.22:45:07-0700") ==
	       BackupAgentBase::parseTime("2019/04/01.03:45:07-0200"));
	ASSERT(BackupAgentBase::parseTime("2019/03/31.22:45:07+0000") ==
	       BackupAgentBase::parseTime("2019/04/01.03:45:07+0500"));
	ASSERT(BackupAgentBase::parseTime("2019/03/31.22:45:07+0030") ==
	       BackupAgentBase::parseTime("2019/04/01.03:45:07+0530"));
	ASSERT(BackupAgentBase::parseTime("2019/03/31.22:45:07+0030") ==
	       BackupAgentBase::parseTime("2019/04/01.04:00:07+0545"));

	return Void();
}

TEST_CASE("/backup/continuous") {
	std::vector<LogFile> files;

	// [0, 100) 2 tags
	files.push_back({ 0, 100, 10, "file1", 100, 0, 2 }); // Tag 0: 0-100
	ASSERT(!BackupContainerFileSystem::isPartitionedLogsContinuous(files, 0, 99));
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 0) == 0);

	files.push_back({ 0, 100, 10, "file2", 200, 1, 2 }); // Tag 1: 0-100
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 0, 99));
	ASSERT(!BackupContainerFileSystem::isPartitionedLogsContinuous(files, 0, 100));
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 0) == 99);

	// [100, 300) 3 tags
	files.push_back({ 100, 200, 10, "file3", 200, 0, 3 }); // Tag 0: 100-200
	files.push_back({ 100, 250, 10, "file4", 200, 1, 3 }); // Tag 1: 100-250
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 0, 99));
	ASSERT(!BackupContainerFileSystem::isPartitionedLogsContinuous(files, 0, 100));
	ASSERT(!BackupContainerFileSystem::isPartitionedLogsContinuous(files, 50, 150));
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 0) == 99);

	files.push_back({ 100, 300, 10, "file5", 200, 2, 3 }); // Tag 2: 100-300
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 50, 150));
	ASSERT(!BackupContainerFileSystem::isPartitionedLogsContinuous(files, 50, 200));
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 10, 199));
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 0) == 199);
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 100) == 199);

	files.push_back({ 250, 300, 10, "file6", 200, 0, 3 }); // Tag 0: 250-300, missing 200-250
	std::sort(files.begin(), files.end());
	ASSERT(!BackupContainerFileSystem::isPartitionedLogsContinuous(files, 50, 240));
	ASSERT(!BackupContainerFileSystem::isPartitionedLogsContinuous(files, 100, 280));
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 99) == 199);

	files.push_back({ 250, 300, 10, "file7", 200, 1, 3 }); // Tag 1: 250-300
	std::sort(files.begin(), files.end());
	ASSERT(!BackupContainerFileSystem::isPartitionedLogsContinuous(files, 100, 280));

	files.push_back({ 200, 250, 10, "file8", 200, 0, 3 }); // Tag 0: 200-250
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 0, 299));
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 100, 280));
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 150) == 299);

	// [300, 400) 1 tag
	// files.push_back({200, 250, 10, "file9", 200, 0, 3}); // Tag 0: 200-250, duplicate file
	files.push_back({ 300, 400, 10, "file10", 200, 0, 1 }); // Tag 1: 300-400
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 0, 399));
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 100, 399));
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 150, 399));
	ASSERT(BackupContainerFileSystem::isPartitionedLogsContinuous(files, 250, 399));
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 0) == 399);
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 99) == 399);
	ASSERT(BackupContainerFileSystem::getPartitionedLogsContinuousEndVersion(files, 250) == 399);

	return Void();
}

} // namespace backup_test
