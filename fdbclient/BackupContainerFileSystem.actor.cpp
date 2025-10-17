/*
 * BackupContainerFileSystem.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "flow/BooleanParam.h"
#ifdef BUILD_AZURE_BACKUP
#include "fdbclient/BackupContainerAzureBlobStore.h"
#endif
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BackupContainerLocalDirectory.h"
#include "fdbclient/BackupContainerS3BlobStore.h"
#include "fdbclient/JsonBuilder.h"
#include "flow/StreamCipher.h"
#include "flow/UnitTest.h"

#include <algorithm>
#include <cinttypes>

#include "flow/actorcompiler.h" // This must be the last #include.

class BackupContainerFileSystemImpl {
public:
	// TODO:  Do this more efficiently, as the range file list for a snapshot could potentially be hundreds of
	// megabytes.
	ACTOR static Future<std::pair<std::vector<RangeFile>, std::map<std::string, KeyRange>>> readKeyspaceSnapshot(
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
		if (!json_spirit::read_string(buf.toString(), json)) {
			fprintf(stderr,
			        "ERROR: Failed to read data. Verify that backup and restore encryption keys match (if provided) or "
			        "the data is corrupted.\n");
			throw restore_error();
		}

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

	// Backup log types
	static constexpr Version NON_PARTITIONED_MUTATION_LOG = 0;
	static constexpr Version PARTITIONED_MUTATION_LOG = 1;

	// Find what should be the filename of a path by finding whatever is after the last forward or backward slash, or
	// failing to find those, the whole string.
	static std::string fileNameOnly(const std::string& path) {
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

	static bool pathToRangeFile(RangeFile& out, const std::string& path, int64_t size) {
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

	ACTOR static Future<Void> writeKeyspaceSnapshotFile(Reference<BackupContainerFileSystem> bc,
	                                                    std::vector<std::string> fileNames,
	                                                    std::vector<std::pair<Key, Key>> beginEndKeys,
	                                                    int64_t totalBytes,
	                                                    IncludeKeyRangeMap includeKeyRangeMap) {
		ASSERT(!fileNames.empty() && fileNames.size() == beginEndKeys.size());

		state Version minVer = std::numeric_limits<Version>::max();
		state Version maxVer = 0;
		state RangeFile rf;
		state json_spirit::mArray fileArray;

		// Validate each filename, update version range
		for (const auto& f : fileNames) {
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

		if (includeKeyRangeMap) {
			auto ranges = doc.subDoc("keyRanges");
			for (int i = 0; i < beginEndKeys.size(); i++) {
				auto fileDoc = ranges.subDoc(fileNames[i], /*split=*/false);
				fileDoc.create("beginKey") = beginEndKeys[i].first.toString();
				fileDoc.create("endKey") = beginEndKeys[i].second.toString();
			}
		}

		wait(yield());
		state std::string docString = json_spirit::write_string(json);

		state Reference<IBackupFile> f =
		    wait(bc->writeFile(format("snapshots/snapshot,%lld,%lld,%lld", minVer, maxVer, totalBytes)));
		wait(f->append(docString.data(), docString.size()));
		wait(f->finish());

		return Void();
	}

	ACTOR static Future<BackupFileList> dumpFileList(Reference<BackupContainerFileSystem> bc,
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

	// For a list of log files specified by their indices (of the same tag),
	// returns if they are continuous in the range [begin, end]. If "tags" is not
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
				if (file.beginVersion > begin) {
					// the first version of the first file must be smaller or equal to the desired beginVersion
					return false;
				}
				if (file.endVersion > begin) {
					lastBegin = begin;
					lastTags = file.totalTags;
				} else {
					// if endVerison of file is smaller than desired beginVersion, then do not include this file
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

	// Returns the end version such that [begin, end] is continuous.
	// "logs" should be already sorted.
	static Version getPartitionedLogsContinuousEndVersion(const std::vector<LogFile>& logs, Version begin) {
		Version end = 0;

		std::map<int, std::vector<int>> tagIndices; // tagId -> indices in files
		for (int i = 0; i < logs.size(); i++) {
			ASSERT_GE(logs[i].tagId, 0);
			ASSERT_LT(logs[i].tagId, logs[i].totalTags);
			auto& indices = tagIndices[logs[i].tagId];
			// filter out if indices.back() is subset of files[i] or vice versa
			if (!indices.empty()) {
				if (logs[indices.back()].isSubset(logs[i])) {
					ASSERT_LE(logs[indices.back()].fileSize, logs[i].fileSize);
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

		// for each range in tags, check all partitions from 1 are continuous
		Version lastEnd = begin;
		for (const auto& [beginEnd, count] : tags) {
			Version tagEnd = beginEnd.second; // This range's minimum continuous partition version
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

	// Computes the continuous end version for non-partitioned mutation logs up to
	// the "targetVersion". If "outLogs" is not nullptr, it will be updated with
	// continuous log files. "*end" is updated with the continuous end version.
	static void computeRestoreEndVersion(const std::vector<LogFile>& logs,
	                                     std::vector<LogFile>* outLogs,
	                                     Version* end,
	                                     Version targetVersion) {
		auto i = logs.begin();

		// Add logs to restorable logs set until continuity is broken OR we reach targetVersion
		while (i != logs.end()) {
			if (i->beginVersion > *end || i->beginVersion > targetVersion)
				break;

			// If the next link in the log chain is found, update the end
			if (i->beginVersion == *end) {
				if (outLogs != nullptr)
					outLogs->push_back(*i);
				*end = i->endVersion;
			}
			++i;
		}
	}

	// Checks if list of sorted logfiles have the logs from snapshotBeginVersion to snapshotEndversion.
	// Which means the sorted log files(have beginVersion and endVersion) should cover
	// all the versions between snapshotBegingVersion and snapshotEndversion.
	// Note: logs should be pre-sorted according to version order.
	static bool hasContinuousLogsForSnapshot(const std::vector<LogFile>& logs,
	                                         Version snapshotBeginVersion,
	                                         Version snapshotEndVersion) {
		auto it = logs.begin();

		// find the first mutation log file that covers snapshotBeginVersion
		while (it != logs.end()) {
			if (it->beginVersion <= snapshotBeginVersion && it->endVersion > snapshotBeginVersion)
				break;
			++it;
		}

		// no log find found covering snaphostBeginVersion, return false
		if (it == logs.end())
			return false;

		// If current log entry(it), covers the entire snapshot, return true
		if (it->endVersion > snapshotEndVersion)
			return true;

		// Iterate over the next logs, check if they are continuous and if
		// the log file is covering the snapshot.
		Version prevEnd = it->endVersion;
		++it;

		while (it != logs.end()) {
			if (it->beginVersion == prevEnd &&
			    it->endVersion > snapshotEndVersion) // continuous logs until snapshot is covered
				return true;
			else if (it->beginVersion != prevEnd) // not continuous logs
				return false;

			prevEnd = it->endVersion;
			++it;
		} // comes out if the logs are not found until snapshotEndVersion.

		return prevEnd > snapshotEndVersion;
	}

	// Find the continuous log end version starting from beginVersion in the
	// given list of sorted logfiles.
	// Note: logs should be pre-sorted according to version order.
	static Version findContinuousLogEnd(const std::vector<LogFile>& logs, Version beginVersion) {
		auto it = logs.begin();

		// find the first mutation log file that covers beginVersion
		while (it != logs.end()) {
			if (it->beginVersion <= beginVersion && it->endVersion > beginVersion)
				break;
			++it;
		}

		// no log find found covering beginVersion, return invalidVersion
		if (it == logs.end())
			return invalidVersion;

		// Iterate over the next logs, check if they are continuous
		Version prevEnd = it->endVersion;
		++it;

		while (it != logs.end()) {
			if (it->beginVersion != prevEnd) // not continuous logs
				return prevEnd;

			prevEnd = it->endVersion;
			++it;
		} // out of logs.

		return prevEnd;
	}

	ACTOR static Future<BackupDescription> describeBackup(Reference<BackupContainerFileSystem> bc,
	                                                      bool deepScan,
	                                                      Version logStartVersionOverride) {
		state BackupDescription desc;
		desc.url = bc->getURL();
		desc.proxy = bc->getProxy();

		TraceEvent("BackupContainerDescribe1")
		    .detail("URL", bc->getURL())
		    .detail("LogStartVersionOverride", logStartVersionOverride)
		    .detail("DeepScan", deepScan);

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
		state Optional<Version> fileLevelEncryption;

		std::vector<Future<Void>> metaReads;
		metaReads.push_back(store(metaExpiredEnd, bc->expiredEndVersion().get()));
		metaReads.push_back(store(metaUnreliableEnd, bc->unreliableEndVersion().get()));
		metaReads.push_back(store(metaLogType, bc->logType().get()));
		metaReads.push_back(store(fileLevelEncryption, bc->fileLevelEncryption().get()));

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
		// Logs are assumed to be contiguous between metaLogBegin and metaLogEnd, so initialize desc accordingly
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
			desc.partitioned =
			    metaLogType.present() && metaLogType.get() == BackupContainerFileSystemImpl::PARTITIONED_MUTATION_LOG;
		}

		if (fileLevelEncryption.present() && fileLevelEncryption.get() != 0) {
			desc.fileLevelEncryption = true;
		} else {
			desc.fileLevelEncryption = false;
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
					desc.contiguousLogEnd = logs.begin()->beginVersion;
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
					updates =
					    updates && bc->logType().set(desc.partitioned
					                                     ? BackupContainerFileSystemImpl::PARTITIONED_MUTATION_LOG
					                                     : BackupContainerFileSystemImpl::NON_PARTITIONED_MUTATION_LOG);
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
				// If there is logs gap after contiguousLogEnd, then check whether the current snapshot
				// can be restored from the logs available after contiguousLogEnd.
				if (desc.contiguousLogEnd.present() && desc.contiguousLogEnd.get() <= s.beginVersion) {
					if (desc.partitioned)
						s.restorable = isPartitionedLogsContinuous(logs, s.beginVersion, s.endVersion);
					else
						s.restorable = hasContinuousLogsForSnapshot(logs, s.beginVersion, s.endVersion);
				}
			}

			desc.snapshotBytes += s.totalSize;

			// If the snapshot is at a single version then it requires no logs.  Update min and max restorable.
			// TODO:  Somehow check / report if the restorable range is not or may not be contiguous.
			if (s.beginVersion == s.endVersion &&
			    (!desc.contiguousLogEnd.present() || // no logs
			     (desc.contiguousLogEnd.present() &&
			      desc.contiguousLogEnd.get() >= s.beginVersion)) // have logs, then should cover snapshot
			) {
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

			// If there is logs gap after contiguousLogEnd and if current snapshot is restorable(have continuous logs)
			if (desc.contiguousLogEnd.present() &&
			    ((desc.contiguousLogEnd.get() < s.beginVersion) ||
			     // if contiguousLogEnd==s.beginVersion==s.endVersion, there is no need to check for continuous logs in
			     // single version snapshot. And this case is covered in above if condition.
			     (desc.contiguousLogEnd.get() == s.beginVersion && s.beginVersion != s.endVersion)) &&
			    s.restorable.get()) {
				if (desc.minRestorableVersion.present() && desc.maxRestorableVersion.present()) {
					ASSERT(desc.minRestorableVersion.get() < s.beginVersion);

					// check if we have contiguous logs from minRestorableVersion to current snapshot endVersion
					bool contiguousLogs = false;
					if (desc.partitioned)
						contiguousLogs =
						    isPartitionedLogsContinuous(logs, desc.minRestorableVersion.get(), s.endVersion);
					else
						contiguousLogs =
						    hasContinuousLogsForSnapshot(logs, desc.minRestorableVersion.get(), s.endVersion);

					if (contiguousLogs) {
						// The previous restorable version can be extended to current snapshot version,
						// so minRestorableVersion remain same
						desc.maxRestorableVersion = s.endVersion;
					} else {
						// Previous restorable version cannot be extended to current snapshot version,
						// means some logs are missing inbetween.
						// So set the snapshot beginversion as minRestorableVersion.
						desc.minRestorableVersion = s.endVersion;
					}
				} else {
					// There is no previous snapshot that is restorable.
					// Since the current snapshot is restorable, set the snapshot beginversion as minRestorableVersion.
					desc.minRestorableVersion = s.endVersion;
				}

				// Find the continuousLogEnd after snapshotEndVersion and set it as
				// maxRestorableVersion.
				if (desc.partitioned) {
					// TO DO: Yet to implement similar function findContinuousLogEnd for partitioned logs.
					desc.maxRestorableVersion = s.endVersion;
				} else {
					Version maxContinuousLogEnd = findContinuousLogEnd(logs, s.endVersion);
					desc.maxRestorableVersion =
					    (maxContinuousLogEnd == invalidVersion) ? s.endVersion : maxContinuousLogEnd - 1;
				}
			}
		}

		return desc;
	}

	ACTOR static Future<Void> expireData(Reference<BackupContainerFileSystem> bc,
	                                     Version expireEndVersion,
	                                     bool force,
	                                     IBackupContainer::ExpireProgress* progress,
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
				ASSERT_LE(logs[j].fileSize, logs[i].fileSize);
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

	static Optional<RestorableFileSet> getRestoreSetFromLogs(const std::vector<LogFile>& logs,
	                                                         Version targetVersion,
	                                                         RestorableFileSet restorable) {
		Version end = logs.begin()->beginVersion;
		computeRestoreEndVersion(logs, &restorable.logs, &end, targetVersion);
		if (end >= targetVersion) {
			restorable.continuousBeginVersion = logs.begin()->beginVersion;
			restorable.continuousEndVersion = end;
			return Optional<RestorableFileSet>(restorable);
		}
		return Optional<RestorableFileSet>();
	}

	// Get a set of files that can restore the given "keyRangesFilter" to the "targetVersion".
	// If "keyRangesFilter" is empty, the file set will cover all key ranges present in the backup.
	// It's generally a good idea to specify "keyRangesFilter" to reduce the number of files for
	// restore times.
	// If "logsOnly" is true, then only log files are returned and "keyRangesFilter" is ignored,
	// because the log can contain mutations of the whole key space, unlike range files that each
	// is limited to a smaller key range.
	ACTOR static Future<Optional<RestorableFileSet>> getRestoreSet(Reference<BackupContainerFileSystem> bc,
	                                                               Version targetVersion,
	                                                               VectorRef<KeyRangeRef> keyRangesFilter,
	                                                               bool logsOnly = false,
	                                                               Version beginVersion = invalidVersion) {
		for (const auto& range : keyRangesFilter) {
			TraceEvent("BackupContainerGetRestoreSet").detail("RangeFilter", printable(range));
		}

		if (logsOnly) {
			state RestorableFileSet restorableSet;
			restorableSet.targetVersion = targetVersion;
			state std::vector<LogFile> logFiles;
			Version begin = beginVersion == invalidVersion ? 0 : beginVersion;
			wait(store(logFiles, bc->listLogFiles(begin, targetVersion, false)));
			// List logs in version order so log continuity can be analyzed
			std::sort(logFiles.begin(), logFiles.end());
			if (!logFiles.empty()) {
				return getRestoreSetFromLogs(logFiles, targetVersion, restorableSet);
			}
		}

		// Find the most recent keyrange snapshot through which we can restore filtered key ranges into targetVersion.
		state std::vector<KeyspaceSnapshotFile> snapshots = wait(bc->listKeyspaceSnapshots());
		state int i = snapshots.size() - 1;
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

			// If there is no key ranges filter for the restore OR if the snapshot contains no per-file key range info
			// then return all of the range files
			if (keyRangesFilter.empty() || results.second.empty()) {
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

			// restorable.snapshot.beginVersion is set to the smallest(oldest) snapshot's beginVersion
			restorable.snapshot = snapshots[i];

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
			// If there are logs and the first one starts at or before the keyrange's snapshot begin version, then
			// it is valid restore set and proceed
			if (!logs.empty() && logs.front().beginVersion <= minKeyRangeVersion) {
				return getRestoreSetFromLogs(logs, targetVersion, restorable);
			}
		}
		return Optional<RestorableFileSet>();
	}

	static std::string versionFolderString(Version v, int smallestBucket) {
		ASSERT_LT(smallestBucket, 14);
		// Get a 0-padded fixed size representation of v
		std::string vFixedPrecision = format("%019lld", v);
		ASSERT_EQ(vFixedPrecision.size(), 19);
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
	static Version extractSnapshotBeginVersion(const std::string& path) {
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

	static bool pathToLogFile(LogFile& out, const std::string& path, int64_t size) {
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

	static bool pathToKeyspaceSnapshotFile(KeyspaceSnapshotFile& out, const std::string& path) {
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

	// fallback for using existing write api if the underlying blob store doesn't support efficient writeEntireFile
	ACTOR static Future<Void> writeEntireFileFallback(Reference<BackupContainerFileSystem> bc,
	                                                  std::string fileName,
	                                                  std::string fileContents) {
		state Reference<IBackupFile> objectFile = wait(bc->writeFile(fileName));
		wait(objectFile->append(&fileContents[0], fileContents.size()));
		wait(objectFile->finish());
		return Void();
	}

	ACTOR static Future<Void> createTestEncryptionKeyFile(std::string filename) {
		if (fileExists(filename)) {
			// Key file already exists, don't overwrite it -> only for testing between backup and restore workloads to
			// share the key.
			TraceEvent("EncryptionKeyFileExists").detail("FileName", filename);
			return Void();
		}
		state Reference<IAsyncFile> keyFile = wait(IAsyncFileSystem::filesystem()->open(
		    filename,
		    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE,
		    0600));
		StreamCipherKey testKey(AES_256_KEY_LENGTH);
		testKey.initializeRandomTestKey();
		keyFile->write(testKey.data(), testKey.size(), 0);
		wait(keyFile->sync());
		return Void();
	}

	ACTOR static Future<Void> readEncryptionKey(std::string encryptionKeyFileName) {
		state Reference<IAsyncFile> keyFile;
		state StreamCipherKey const* cipherKey = StreamCipherKey::getGlobalCipherKey();
		try {
			Reference<IAsyncFile> _keyFile = wait(IAsyncFileSystem::filesystem()->open(
			    encryptionKeyFileName,
			    IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED,
			    0400));
			keyFile = _keyFile;
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "FailedToOpenEncryptionKeyFile")
			    .error(e)
			    .detail("FileName", encryptionKeyFileName);
			throw e;
		}
		int bytesRead = wait(keyFile->read(cipherKey->data(), cipherKey->size(), 0));
		if (bytesRead != cipherKey->size()) {
			TraceEvent(SevWarnAlways, "InvalidEncryptionKeyFileSize")
			    .detail("ExpectedSize", cipherKey->size())
			    .detail("ActualSize", bytesRead);
			throw invalid_encryption_key_file();
		}
		ASSERT_EQ(bytesRead, cipherKey->size());
		return Void();
	}

	ACTOR static Future<Void> writeEncryptionMetadataIfNotExists(Reference<BackupContainerFileSystem> bc) {
		Optional<Version> existingEncryptionMetadata = wait(bc->fileLevelEncryption().get());

		if (!existingEncryptionMetadata.present()) {
			wait(bc->fileLevelEncryption().set(bc->encryptionKeyFileName.present() ? 1 : 0));
		}
		return Void();
	}

}; // class BackupContainerFileSystemImpl

Future<Reference<IBackupFile>> BackupContainerFileSystem::writeLogFile(Version beginVersion,
                                                                       Version endVersion,
                                                                       int blockSize) {
	return writeFile(BackupContainerFileSystemImpl::logVersionFolderString(beginVersion, false) +
	                 format("log,%lld,%lld,%s,%d",
	                        beginVersion,
	                        endVersion,
	                        deterministicRandom()->randomUniqueID().toString().c_str(),
	                        blockSize));
}

Future<Reference<IBackupFile>> BackupContainerFileSystem::writeTaggedLogFile(Version beginVersion,
                                                                             Version endVersion,
                                                                             int blockSize,
                                                                             uint16_t tagId,
                                                                             int totalTags) {
	return writeFile(BackupContainerFileSystemImpl::logVersionFolderString(beginVersion, true) +
	                 format("log,%lld,%lld,%s,%d-of-%d,%d",
	                        beginVersion,
	                        endVersion,
	                        deterministicRandom()->randomUniqueID().toString().c_str(),
	                        tagId,
	                        totalTags,
	                        blockSize));
}

Future<Reference<IBackupFile>> BackupContainerFileSystem::writeRangeFile(Version snapshotBeginVersion,
                                                                         int snapshotFileCount,
                                                                         Version fileVersion,
                                                                         int blockSize) {
	std::string fileName = format(
	    "range,%" PRId64 ",%s,%d", fileVersion, deterministicRandom()->randomUniqueID().toString().c_str(), blockSize);

	// In order to test backward compatibility in simulation, sometimes write to the old path format
	if (g_network->isSimulated() && deterministicRandom()->coinflip()) {
		return writeFile(BackupContainerFileSystemImpl::old_rangeVersionFolderString(fileVersion) + fileName);
	}

	return writeFile(BackupContainerFileSystemImpl::snapshotFolderString(snapshotBeginVersion) +
	                 format("/%d/", snapshotFileCount / (BUGGIFY ? 1 : 5000)) + fileName);
}

Future<std::pair<std::vector<RangeFile>, std::map<std::string, KeyRange>>>
BackupContainerFileSystem::readKeyspaceSnapshot(KeyspaceSnapshotFile snapshot) {
	return BackupContainerFileSystemImpl::readKeyspaceSnapshot(Reference<BackupContainerFileSystem>::addRef(this),
	                                                           snapshot);
}

Future<Void> BackupContainerFileSystem::writeKeyspaceSnapshotFile(const std::vector<std::string>& fileNames,
                                                                  const std::vector<std::pair<Key, Key>>& beginEndKeys,
                                                                  int64_t totalBytes,
                                                                  IncludeKeyRangeMap includeKeyRangeMap) {
	return BackupContainerFileSystemImpl::writeKeyspaceSnapshotFile(
	    Reference<BackupContainerFileSystem>::addRef(this), fileNames, beginEndKeys, totalBytes, includeKeyRangeMap);
};

Future<std::vector<LogFile>> BackupContainerFileSystem::listLogFiles(Version beginVersion,
                                                                     Version targetVersion,
                                                                     bool partitioned) {
	// The first relevant log file could have a begin version less than beginVersion based on the knobs which
	// determine log file range size, so start at an earlier version adjusted by how many versions a file could
	// contain.
	//
	// Get the cleaned (without slashes) first and last folders that could contain relevant results.
	std::string firstPath =
	    BackupContainerFileSystemImpl::cleanFolderString(BackupContainerFileSystemImpl::logVersionFolderString(
	        std::max<Version>(0,
	                          beginVersion - CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE),
	        partitioned));
	std::string lastPath = BackupContainerFileSystemImpl::cleanFolderString(
	    BackupContainerFileSystemImpl::logVersionFolderString(targetVersion, partitioned));

	std::function<bool(std::string const&)> pathFilter = [=](const std::string& folderPath) {
		// Remove slashes in the given folder path so that the '/' positions in the version folder string do not
		// matter

		std::string cleaned = BackupContainerFileSystemImpl::cleanFolderString(folderPath);
		return StringRef(firstPath).startsWith(cleaned) || StringRef(lastPath).startsWith(cleaned) ||
		       (cleaned > firstPath && cleaned < lastPath);
	};

	return map(listFiles((partitioned ? "plogs/" : "logs/"), pathFilter), [=](const FilesAndSizesT& files) {
		std::vector<LogFile> results;
		LogFile lf;
		for (auto& f : files) {
			if (BackupContainerFileSystemImpl::pathToLogFile(lf, f.first, f.second) && lf.endVersion > beginVersion &&
			    lf.beginVersion <= targetVersion)
				results.push_back(lf);
		}
		return results;
	});
}

Future<std::vector<RangeFile>> BackupContainerFileSystem::old_listRangeFiles(Version beginVersion, Version endVersion) {
	// Get the cleaned (without slashes) first and last folders that could contain relevant results.
	std::string firstPath = BackupContainerFileSystemImpl::cleanFolderString(
	    BackupContainerFileSystemImpl::old_rangeVersionFolderString(beginVersion));
	std::string lastPath = BackupContainerFileSystemImpl::cleanFolderString(
	    BackupContainerFileSystemImpl::old_rangeVersionFolderString(endVersion));

	std::function<bool(std::string const&)> pathFilter = [=](const std::string& folderPath) {
		// Remove slashes in the given folder path so that the '/' positions in the version folder string do not
		// matter
		std::string cleaned = BackupContainerFileSystemImpl::cleanFolderString(folderPath);

		return StringRef(firstPath).startsWith(cleaned) || StringRef(lastPath).startsWith(cleaned) ||
		       (cleaned > firstPath && cleaned < lastPath);
	};

	return map(listFiles("ranges/", pathFilter), [=](const FilesAndSizesT& files) {
		std::vector<RangeFile> results;
		RangeFile rf;
		for (auto& f : files) {
			if (BackupContainerFileSystemImpl::pathToRangeFile(rf, f.first, f.second) && rf.version >= beginVersion &&
			    rf.version <= endVersion)
				results.push_back(rf);
		}
		return results;
	});
}

Future<std::vector<RangeFile>> BackupContainerFileSystem::listRangeFiles(Version beginVersion, Version endVersion) {
	// Until the old folder scheme is no longer supported, read files stored using old folder scheme
	Future<std::vector<RangeFile>> oldFiles = old_listRangeFiles(beginVersion, endVersion);

	// Define filter function (for listFiles() implementations that use it) to reject any folder
	// starting after endVersion
	std::function<bool(std::string const&)> pathFilter = [=](std::string const& path) {
		return BackupContainerFileSystemImpl::extractSnapshotBeginVersion(path) <= endVersion;
	};

	Future<std::vector<RangeFile>> newFiles = map(listFiles("kvranges/", pathFilter), [=](const FilesAndSizesT& files) {
		std::vector<RangeFile> results;
		RangeFile rf;
		for (auto& f : files) {
			if (BackupContainerFileSystemImpl::pathToRangeFile(rf, f.first, f.second) && rf.version >= beginVersion &&
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

Future<std::vector<KeyspaceSnapshotFile>> BackupContainerFileSystem::listKeyspaceSnapshots(Version begin, Version end) {
	return map(listFiles("snapshots/"), [=](const FilesAndSizesT& files) {
		std::vector<KeyspaceSnapshotFile> results;
		KeyspaceSnapshotFile sf;
		for (auto& f : files) {
			if (BackupContainerFileSystemImpl::pathToKeyspaceSnapshotFile(sf, f.first) && sf.beginVersion < end &&
			    sf.endVersion >= begin)
				results.push_back(sf);
		}
		std::sort(results.begin(), results.end());
		return results;
	});
}

Future<BackupFileList> BackupContainerFileSystem::dumpFileList(Version begin, Version end) {
	return BackupContainerFileSystemImpl::dumpFileList(Reference<BackupContainerFileSystem>::addRef(this), begin, end);
}

Future<BackupDescription> BackupContainerFileSystem::describeBackup(bool deepScan, Version logStartVersionOverride) {
	return BackupContainerFileSystemImpl::describeBackup(
	    Reference<BackupContainerFileSystem>::addRef(this), deepScan, logStartVersionOverride);
}

Future<Void> BackupContainerFileSystem::expireData(Version expireEndVersion,
                                                   bool force,
                                                   ExpireProgress* progress,
                                                   Version restorableBeginVersion) {
	return BackupContainerFileSystemImpl::expireData(
	    Reference<BackupContainerFileSystem>::addRef(this), expireEndVersion, force, progress, restorableBeginVersion);
}

Future<Void> BackupContainerFileSystem::writeEncryptionMetadata() {
	return BackupContainerFileSystemImpl::writeEncryptionMetadataIfNotExists(
	    Reference<BackupContainerFileSystem>::addRef(this));
}

ACTOR static Future<KeyRange> getSnapshotFileKeyRange_impl(Reference<BackupContainerFileSystem> bc,
                                                           RangeFile file,
                                                           Database cx) {
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
				    wait(fileBackup::decodeRangeFileBlock(inFile, j, len, cx));
				if (!beginKeySet) {
					beginKey = blockData.front().key;
					beginKeySet = true;
				}
				endKey = blockData.back().key;
			}
			break;
		} catch (Error& e) {
			if (e.code() == error_code_restore_bad_read || e.code() == error_code_restore_unsupported_file_version ||
			    e.code() == error_code_restore_corrupted_data_padding) { // no retriable error
				TraceEvent(SevError, "BackupContainerGetSnapshotFileKeyRange").error(e);
				throw;
			} else if (e.code() == error_code_http_request_failed || e.code() == error_code_connection_failed ||
			           e.code() == error_code_timed_out || e.code() == error_code_lookup_failed) {
				// blob http request failure, retry
				TraceEvent(SevWarnAlways, "BackupContainerGetSnapshotFileKeyRangeConnectionFailure")
				    .error(e)
				    .detail("Retries", ++readFileRetries);
				wait(delayJittered(0.1));
			} else {
				TraceEvent(SevError, "BackupContainerGetSnapshotFileKeyRangeUnexpectedError").error(e);
				throw;
			}
		}
	}
	return KeyRange(KeyRangeRef(beginKey, endKey));
}

ACTOR static Future<Void> writeVersionProperty(Reference<BackupContainerFileSystem> bc, std::string path, Version v) {
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

ACTOR static Future<Optional<Version>> readVersionProperty(Reference<BackupContainerFileSystem> bc, std::string path) {
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

Future<KeyRange> BackupContainerFileSystem::getSnapshotFileKeyRange(const RangeFile& file, Database cx) {
	ASSERT(g_network->isSimulated());
	return getSnapshotFileKeyRange_impl(Reference<BackupContainerFileSystem>::addRef(this), file, cx);
}

Future<Optional<RestorableFileSet>> BackupContainerFileSystem::getRestoreSet(Version targetVersion,
                                                                             VectorRef<KeyRangeRef> keyRangesFilter,
                                                                             bool logsOnly,
                                                                             Version beginVersion) {
	return BackupContainerFileSystemImpl::getRestoreSet(
	    Reference<BackupContainerFileSystem>::addRef(this), targetVersion, keyRangesFilter, logsOnly, beginVersion);
}

Future<Optional<Version>> BackupContainerFileSystem::VersionProperty::get() {
	return readVersionProperty(bc, path);
}
Future<Void> BackupContainerFileSystem::VersionProperty::set(Version v) {
	return writeVersionProperty(bc, path, v);
}
Future<Void> BackupContainerFileSystem::VersionProperty::clear() {
	return bc->deleteFile(path);
}

BackupContainerFileSystem::VersionProperty BackupContainerFileSystem::logBeginVersion() {
	return { Reference<BackupContainerFileSystem>::addRef(this), "log_begin_version" };
}
BackupContainerFileSystem::VersionProperty BackupContainerFileSystem::logEndVersion() {
	return { Reference<BackupContainerFileSystem>::addRef(this), "log_end_version" };
}
BackupContainerFileSystem::VersionProperty BackupContainerFileSystem::expiredEndVersion() {
	return { Reference<BackupContainerFileSystem>::addRef(this), "expired_end_version" };
}
BackupContainerFileSystem::VersionProperty BackupContainerFileSystem::unreliableEndVersion() {
	return { Reference<BackupContainerFileSystem>::addRef(this), "unreliable_end_version" };
}
BackupContainerFileSystem::VersionProperty BackupContainerFileSystem::logType() {
	return { Reference<BackupContainerFileSystem>::addRef(this), "mutation_log_type" };
}
BackupContainerFileSystem::VersionProperty BackupContainerFileSystem::fileLevelEncryption() {
	return { Reference<BackupContainerFileSystem>::addRef(this), "file_level_encryption" };
}

bool BackupContainerFileSystem::usesEncryption() const {
	return encryptionSetupFuture.isValid();
}
Future<Void> BackupContainerFileSystem::encryptionSetupComplete() const {
	return encryptionSetupFuture;
}

Future<Void> BackupContainerFileSystem::writeEntireFileFallback(const std::string& fileName,
                                                                const std::string& fileContents) {
	return BackupContainerFileSystemImpl::writeEntireFileFallback(
	    Reference<BackupContainerFileSystem>::addRef(this), fileName, fileContents);
}

void BackupContainerFileSystem::setEncryptionKey(Optional<std::string> const& encryptionKeyFileName) {
	if (encryptionKeyFileName.present()) {
		encryptionSetupFuture = BackupContainerFileSystemImpl::readEncryptionKey(encryptionKeyFileName.get());
	}
}
Future<Void> BackupContainerFileSystem::createTestEncryptionKeyFile(std::string const& filename) {
	return BackupContainerFileSystemImpl::createTestEncryptionKeyFile(filename);
}

// Get a BackupContainerFileSystem based on a container URL string
// TODO: refactor to not duplicate IBackupContainer::openContainer. It's the exact same
// code but returning a different template type because you can't cast between them
Reference<BackupContainerFileSystem> BackupContainerFileSystem::openContainerFS(
    const std::string& url,
    const Optional<std::string>& proxy,
    const Optional<std::string>& encryptionKeyFileName,
    bool isBackup) {
	static std::map<std::string, Reference<BackupContainerFileSystem>> m_cache;

	Reference<BackupContainerFileSystem>& r = m_cache[url];
	if (r)
		return r;

	try {
		StringRef u(url);
		if (u.startsWith("file://"_sr)) {
			r = makeReference<BackupContainerLocalDirectory>(url, encryptionKeyFileName);
		} else if (u.startsWith("blobstore://"_sr)) {
			std::string resource;
			Optional<std::string> blobstoreProxy;

			// If no proxy is passed down to the openContainer method, try to fallback to the
			// fileBackupAgentProxy which is a global variable and will be set for the backup_agent.
			if (proxy.present()) {
				blobstoreProxy = proxy.get();
			} else if (fileBackupAgentProxy.present()) {
				blobstoreProxy = fileBackupAgentProxy.get();
			}

			// The URL parameters contain blobstore endpoint tunables as well as possible backup-specific options.
			S3BlobStoreEndpoint::ParametersT backupParams;
			Reference<S3BlobStoreEndpoint> bstore =
			    S3BlobStoreEndpoint::fromString(url, blobstoreProxy, &resource, &lastOpenError, &backupParams);

			if (resource.empty())
				throw backup_invalid_url();
			for (auto c : resource)
				if (!isalnum(c) && c != '_' && c != '-' && c != '.' && c != '/')
					throw backup_invalid_url();
			r = makeReference<BackupContainerS3BlobStore>(
			    bstore, resource, backupParams, encryptionKeyFileName, isBackup);
		}
#ifdef BUILD_AZURE_BACKUP
		else if (u.startsWith("azure://"_sr)) {
			u.eat("azure://"_sr);
			auto address = u.eat("/"_sr);
			if (address.endsWith(std::string(azure::storage_lite::constants::default_endpoint_suffix))) {
				CODE_PROBE(true, "Azure backup url with standard azure storage account endpoint");
				// <account>.<service>.core.windows.net/<resource_path>
				auto endPoint = address.toString();
				auto accountName = address.eat("."_sr).toString();
				auto containerName = u.eat("/"_sr).toString();
				r = makeReference<BackupContainerAzureBlobStore>(
				    endPoint, accountName, containerName, encryptionKeyFileName);
			} else {
				// resolve the network address if necessary
				std::string endpoint(address.toString());
				Optional<NetworkAddress> parsedAddress = NetworkAddress::parseOptional(endpoint);
				if (!parsedAddress.present()) {
					try {
						auto hostname = Hostname::parse(endpoint);
						auto resolvedAddress = hostname.resolveBlocking();
						if (resolvedAddress.present()) {
							CODE_PROBE(true, "Azure backup url with hostname in the endpoint");
							parsedAddress = resolvedAddress.get();
						}
					} catch (Error& e) {
						TraceEvent(SevError, "InvalidAzureBackupUrl").error(e).detail("Endpoint", endpoint);
						throw backup_invalid_url();
					}
				}
				if (!parsedAddress.present()) {
					TraceEvent(SevError, "InvalidAzureBackupUrl").detail("Endpoint", endpoint);
					throw backup_invalid_url();
				}
				auto accountName = u.eat("/"_sr).toString();
				// Avoid including ":tls" and "(fromHostname)"
				// note: the endpoint needs to contain the account name
				// so either "<account_name>.blob.core.windows.net" or "<ip>:<port>/<account_name>"
				endpoint =
				    fmt::format("{}/{}", formatIpPort(parsedAddress.get().ip, parsedAddress.get().port), accountName);
				auto containerName = u.eat("/"_sr).toString();
				r = makeReference<BackupContainerAzureBlobStore>(
				    endpoint, accountName, containerName, encryptionKeyFileName);
			}
		}
#endif
		else {
			lastOpenError = "invalid URL prefix";
			throw backup_invalid_url();
		}

		r->encryptionKeyFileName = encryptionKeyFileName;
		r->URL = url;
		return r;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;

		TraceEvent m(SevWarn, "BackupContainer");
		m.error(e);
		m.detail("Description", "Invalid container specification.  See help.");
		m.detail("URL", url);
		if (e.code() == error_code_backup_invalid_url)
			m.detail("LastOpenError", lastOpenError);

		throw;
	}
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
	ASSERT_EQ(size, fileSize);
	if (size > 0) {
		state Standalone<VectorRef<uint8_t>> buf;
		buf.resize(buf.arena(), fileSize);
		int b = wait(inputFile->read(buf.begin(), buf.size(), 0));
		ASSERT_EQ(b, buf.size());
		ASSERT(buf == content);
	}
	return Void();
}

// Randomly advance version by up to 1 second of versions
Version nextVersion(Version v) {
	int64_t increment = deterministicRandom()->randomInt64(1, CLIENT_KNOBS->CORE_VERSIONSPERSECOND);
	return v + increment;
}

// Write a snapshot file with only begin & end key
ACTOR static Future<Void> testWriteSnapshotFile(Reference<IBackupFile> file, Key begin, Key end, uint32_t blockSize) {
	ASSERT_GT(blockSize, 3 * sizeof(uint32_t) + begin.size() + end.size());

	uint32_t fileVersion = BACKUP_AGENT_SNAPSHOT_FILE_VERSION;
	// write Header
	wait(file->append((uint8_t*)&fileVersion, sizeof(fileVersion)));

	// write begin key length and key
	wait(file->appendStringRefWithLen(begin));

	// write end key length and key
	wait(file->appendStringRefWithLen(end));

	int bytesLeft = blockSize - file->size();
	if (bytesLeft > 0) {
		Value paddings = fileBackup::makePadding(bytesLeft);
		wait(file->append(paddings.begin(), bytesLeft));
	}
	wait(file->finish());
	return Void();
}

ACTOR Future<Void> testBackupContainer(std::string url,
                                       Optional<std::string> proxy,
                                       Optional<std::string> encryptionKeyFileName) {
	state FlowLock lock(100e6);

	if (encryptionKeyFileName.present()) {
		wait(BackupContainerFileSystem::createTestEncryptionKeyFile(encryptionKeyFileName.get()));
	}

	printf("BackupContainerTest URL %s\n", url.c_str());

	state Reference<IBackupContainer> c = IBackupContainer::openContainer(url, proxy, encryptionKeyFileName);

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
	if (StringRef(url).startsWith("blob"_sr)) {
		fileSizes.push_back(CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE);
		fileSizes.push_back(CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE + 10);
	}

	loop {
		state Version logStart = v;
		state int kvfiles = deterministicRandom()->randomInt(0, 3);
		state Key begin = ""_sr;
		state Key end = ""_sr;
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
			// Write in actual range file format, instead of random data.
			// writes.push_back(writeAndVerifyFile(c, range, size, &lock));
			wait(testWriteSnapshotFile(range, begin, end, blockSize));

			if (deterministicRandom()->random01() < .2) {
				writes.push_back(c->writeKeyspaceSnapshotFile(snapshots.rbegin()->second,
				                                              snapshotBeginEndKeys.rbegin()->second,
				                                              snapshotSizes.rbegin()->second,
				                                              IncludeKeyRangeMap(BUGGIFY)));
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
	ASSERT_EQ(listing.ranges.size(), nRangeFiles);
	ASSERT_EQ(listing.logs.size(), logs.size());
	ASSERT_EQ(listing.snapshots.size(), snapshots.size());

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
		fmt::print("EXPIRE TO {}\n", expireVersion);
		state Future<Void> f = c->expireData(expireVersion);
		wait(ready(f));

		// If there is an error, it must be backup_cannot_expire and we have to be on the last snapshot
		if (f.isError()) {
			ASSERT_EQ(f.getError().code(), error_code_backup_cannot_expire);
			ASSERT_EQ(i, listing.snapshots.size() - 1);
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
	ASSERT_EQ(empty.ranges.size(), 0);
	ASSERT_EQ(empty.logs.size(), 0);
	ASSERT_EQ(empty.snapshots.size(), 0);

	printf("BackupContainerTest URL=%s PASSED.\n", url.c_str());

	return Void();
}

TEST_CASE("/backup/containers/localdir/unencrypted") {
	wait(testBackupContainer(format("file://%s/fdb_backups/%llx", params.getDataDir().c_str(), timer_int()), {}, {}));
	return Void();
}

TEST_CASE("/backup/containers/localdir/encrypted") {
	wait(testBackupContainer(format("file://%s/fdb_backups/%llx", params.getDataDir().c_str(), timer_int()),
	                         {},
	                         format("%s/test_encryption_key", params.getDataDir().c_str())));
	return Void();
}

TEST_CASE("/backup/containers/url") {
	if (!g_network->isSimulated()) {
		const char* url = getenv("FDB_TEST_BACKUP_URL");
		ASSERT(url != nullptr);
		wait(testBackupContainer(url, {}, {}));
	}
	return Void();
}

TEST_CASE("/backup/containers_list") {
	if (!g_network->isSimulated()) {
		state const char* url = getenv("FDB_TEST_BACKUP_URL");
		ASSERT(url != nullptr);
		printf("Listing %s\n", url);
		std::vector<std::string> urls = wait(IBackupContainer::listContainers(url, {}));
		for (auto& u : urls) {
			printf("%s\n", u.c_str());
		}
	}
	return Void();
}

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
	ASSERT(!BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 0, 99));
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 0) == 0);

	files.push_back({ 0, 100, 10, "file2", 200, 1, 2 }); // Tag 1: 0-100
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 0, 99));
	ASSERT(!BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 0, 100));
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 0) == 99);

	// [100, 300) 3 tags
	files.push_back({ 100, 200, 10, "file3", 200, 0, 3 }); // Tag 0: 100-200
	files.push_back({ 100, 250, 10, "file4", 200, 1, 3 }); // Tag 1: 100-250
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 0, 99));
	ASSERT(!BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 0, 100));
	ASSERT(!BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 50, 150));
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 0) == 99);

	files.push_back({ 100, 300, 10, "file5", 200, 2, 3 }); // Tag 2: 100-300
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 50, 150));
	ASSERT(!BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 50, 200));
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 10, 199));
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 0) == 199);
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 100) == 199);

	files.push_back({ 250, 300, 10, "file6", 200, 0, 3 }); // Tag 0: 250-300, missing 200-250
	std::sort(files.begin(), files.end());
	ASSERT(!BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 50, 240));
	ASSERT(!BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 100, 280));
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 99) == 199);

	files.push_back({ 250, 300, 10, "file7", 200, 1, 3 }); // Tag 1: 250-300
	std::sort(files.begin(), files.end());
	ASSERT(!BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 100, 280));

	files.push_back({ 200, 250, 10, "file8", 200, 0, 3 }); // Tag 0: 200-250
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 0, 299));
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 100, 280));
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 150) == 299);

	// [300, 400) 1 tag
	// files.push_back({200, 250, 10, "file9", 200, 0, 3}); // Tag 0: 200-250, duplicate file
	files.push_back({ 300, 400, 10, "file10", 200, 0, 1 }); // Tag 1: 300-400
	std::sort(files.begin(), files.end());
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 0, 399));
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 100, 399));
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 150, 399));
	ASSERT(BackupContainerFileSystemImpl::isPartitionedLogsContinuous(files, 250, 399));
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 0) == 399);
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 99) == 399);
	ASSERT(BackupContainerFileSystemImpl::getPartitionedLogsContinuousEndVersion(files, 250) == 399);

	return Void();
}

TEST_CASE("/backup/logs_continuous") {
	std::vector<LogFile> files;

	// [10, 100)
	files.push_back({ 10, 100, 10, "file1", 100 });
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 0, 5));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 50));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 105));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 100, 101));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 101, 150));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 99));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 100));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 101));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 150));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 50, 70));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 50, 99));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 50, 100));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 11));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 98, 99));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 99, 100));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 99, 99));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 100, 100));
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 0) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 5) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 10) == 100);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 50) == 100);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 99) == 100);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 100) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 101) == invalidVersion);

	// [10, 100), [100, 200)
	files.push_back({ 100, 200, 10, "file2", 100 });
	std::sort(files.begin(), files.end());
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 0, 5));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 50));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 105));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 100, 101));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 101, 150));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 99));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 100));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 101));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 150));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 50, 70));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 50, 99));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 50, 100));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 11));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 98, 99));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 99, 100));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 99, 99));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 100, 100));

	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 150));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 205));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 200, 201));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 201, 250));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 199));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 200));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 201));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 70, 170));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 70, 200));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 199, 200));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 199, 199));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 200, 200));

	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 0) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 5) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 10) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 50) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 99) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 100) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 101) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 199) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 200) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 201) == invalidVersion);

	// [10, 100), [100, 200), [300, 400)
	files.push_back({ 300, 400, 10, "file3", 100 });
	std::sort(files.begin(), files.end());
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 0, 5));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 50));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 105));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 100, 101));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 101, 150));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 99));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 100));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 101));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 150));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 50, 70));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 50, 99));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 50, 100));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 11));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 98, 99));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 99, 100));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 99, 99));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 100, 100));

	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 150));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 5, 205));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 200, 201));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 201, 250));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 199));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 200));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 201));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 70, 170));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 70, 200));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 199, 200));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 199, 199));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 200, 200));

	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 250, 260));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 250, 310));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 250, 405));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 400, 401));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 401, 450));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 300, 399));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 300, 400));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 300, 401));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 300, 350));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 350, 370));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 350, 400));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 10, 400));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 100, 400));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 200, 400));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 299, 400));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 399, 400));
	ASSERT(BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 399, 399));
	ASSERT(!BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(files, 400, 400));

	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 0) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 5) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 10) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 50) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 99) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 100) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 101) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 199) == 200);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 200) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 201) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 250) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 299) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 300) == 400);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 301) == 400);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 399) == 400);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 400) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 401) == invalidVersion);
	ASSERT(BackupContainerFileSystemImpl::findContinuousLogEnd(files, 450) == invalidVersion);

	return Void();
}

void printFileList(BackupFileList& backupFileList) {
	printf("\nRangeFiles count:%lu", backupFileList.ranges.size());
	for (auto r : backupFileList.ranges)
		printf("\n%s", r.toString().c_str());

	printf("\nLogFiles count:%lu", backupFileList.logs.size());
	for (auto l : backupFileList.logs)
		printf("\n%s", l.toString().c_str());

	printf("\nSnapshotFiles count:%lu", backupFileList.snapshots.size());
	for (auto s : backupFileList.snapshots)
		printf("\n%" PRId64 ", %" PRId64 ", %s, %" PRId64 "\n",
		       s.beginVersion,
		       s.endVersion,
		       s.fileName.c_str(),
		       s.totalSize);
}

// Intentionally missing some log range files and checking if the snapshot can be restored.
ACTOR Future<Void> testBackupContainerWithMissingLogRanges(std::string url, Optional<std::string> proxy) {
	state FlowLock lock(100e6);
	printf("BackupContainerTest URL %s\n", url.c_str());

	state Reference<IBackupContainer> c = IBackupContainer::openContainer(url, proxy, {});
	// Make sure container doesn't exist, then create it.
	try {
		wait(c->deleteContainer());
	} catch (Error& e) {
		if (e.code() != error_code_backup_invalid_url && e.code() != error_code_backup_does_not_exist)
			throw;
	}
	wait(c->create());

	state Key begin = randomKeyBetween(normalKeys);
	state Key end = randomKeyBetween(KeyRangeRef(begin, normalKeys.end));
	state int blockSize = 3 * sizeof(uint32_t) + begin.size() + end.size() + 8;
	state std::vector<Future<Void>> writes;
	state std::pair<Key, Key> beginEndKeys = std::make_pair(begin, end);
	state std::vector<bool> snapshotsMissingLogs;
	state Version v = deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max() / 2);
	state Version tempLogEnd = 0;
	state Version logStart = v;
	state Version logEnd = v;
	state Version snapshotBeginVersion = v;
	state Version snapshotEndVersion = v;
	state Version lastMissedLogFileEnd = 0;

	// create a random number of snapshots
	state int numSnapshots = deterministicRandom()->randomInt(1, 10);
	while (numSnapshots) {
		state std::vector<std::string> rangeFileNames;
		state std::vector<std::pair<Key, Key>> snapshotBeginEndKeys;

		// create a random number of range files per snapshot
		state int numRangeFiles = deterministicRandom()->randomInt(2, 5);
		snapshotBeginVersion = v;
		while (numRangeFiles) {
			state Reference<IBackupFile> range = wait(c->writeRangeFile(v, 0, v, blockSize));
			writes.push_back(writeAndVerifyFile(c, range, deterministicRandom()->randomInt(0, 2e6), &lock));
			rangeFileNames.push_back(range->getFileName());
			snapshotBeginEndKeys.push_back(beginEndKeys);

			logEnd = v;
			v = nextVersion(v);
			--numRangeFiles;
		}
		snapshotEndVersion = logEnd;

		// writing the snapshot file
		writes.push_back(c->writeKeyspaceSnapshotFile(rangeFileNames,
		                                              snapshotBeginEndKeys,
		                                              deterministicRandom()->randomInt(0, 2e6),
		                                              IncludeKeyRangeMap(BUGGIFY)));

		// if the last missing log file overlaps with the current snapshot,
		// mark snapshotsMissingLogs for current snapshot as true.
		snapshotsMissingLogs.push_back(lastMissedLogFileEnd > snapshotBeginVersion);

		// creating log files for the snapshot range.
		while (logStart < logEnd) {
			tempLogEnd = nextVersion(logStart);
			if (deterministicRandom()->random01() < 0.5) {
				state Reference<IBackupFile> log = wait(c->writeLogFile(logStart, tempLogEnd, blockSize));
				writes.push_back(writeAndVerifyFile(c, log, deterministicRandom()->randomInt(0, 2e6), &lock));
			} else { // intentionally missing writing of some log files.
				// If the missing log range falls in the current snapshot range, mark it.
				if (!(tempLogEnd < snapshotBeginVersion || snapshotEndVersion < logStart))
					snapshotsMissingLogs.back() = true;
				lastMissedLogFileEnd = tempLogEnd;
			}
			logStart = tempLogEnd;
		}

		--numSnapshots;
	}

	wait(waitForAll(writes));
	state BackupFileList listing = wait(c->dumpFileList());
	printFileList(listing);

	printf("\n\nSnapshots missing logs:");
	state int i = 0;
	for (; i < snapshotsMissingLogs.size(); ++i)
		printf("\nSnapshot%d: %s", i, snapshotsMissingLogs[i] ? "true" : "false");

	state BackupDescription desc = wait(c->describeBackup());
	printf("\n\n%s\n", desc.toString().c_str());

	for (i = 0; i < listing.snapshots.size(); ++i) {
		// Ensure we can restore to the end version of snapshot i
		Optional<RestorableFileSet> rest = wait(c->getRestoreSet(listing.snapshots[i].endVersion));
		if (snapshotsMissingLogs[i])
			ASSERT(!rest.present());
		else
			ASSERT(rest.present());
	}

	for (i = snapshotsMissingLogs.size() - 1; i >= 0; --i) {
		if (!snapshotsMissingLogs[i]) {
			int j = i - 1;
			for (; j >= 0; --j) {
				if (snapshotsMissingLogs[j] ||
				    !BackupContainerFileSystemImpl::hasContinuousLogsForSnapshot(
				        listing.logs, listing.snapshots[j].endVersion, listing.snapshots[j + 1].beginVersion))
					break;
			}
			ASSERT(desc.minRestorableVersion.get() == listing.snapshots[j + 1].endVersion);
			ASSERT(desc.maxRestorableVersion.get() >= listing.snapshots[i].endVersion);
			if (i + 1 < snapshotsMissingLogs.size())
				ASSERT(desc.maxRestorableVersion.get() < listing.snapshots[i + 1].endVersion);
			break;
		}
	}

	printf("DELETING\n");
	wait(c->deleteContainer());

	state Future<BackupDescription> d = c->describeBackup();
	wait(ready(d));
	ASSERT(d.isError() && d.getError().code() == error_code_backup_does_not_exist);

	BackupFileList empty = wait(c->dumpFileList());
	ASSERT_EQ(empty.ranges.size(), 0);
	ASSERT_EQ(empty.logs.size(), 0);
	ASSERT_EQ(empty.snapshots.size(), 0);

	printf("BackupContainerTest URL=%s PASSED.\n", url.c_str());

	return Void();
}

TEST_CASE("/backup/containers/localdir/missingLogRangesRestorability") {
	wait(testBackupContainerWithMissingLogRanges(
	    format("file://%s/fdb_backups/%llx", params.getDataDir().c_str(), timer_int()), {}));
	return Void();
}

ACTOR Future<Void> testBackupContinuousLogEndVer(std::string url, Optional<std::string> proxy) {
	state FlowLock lock(100e6);
	printf("BackupContainerTest URL %s\n", url.c_str());
	state Reference<IBackupContainer> c = IBackupContainer::openContainer(url, proxy, {});

	// Make sure container doesn't exist, then create it.
	try {
		wait(c->deleteContainer());
	} catch (Error& e) {
		if (e.code() != error_code_backup_invalid_url && e.code() != error_code_backup_does_not_exist)
			throw;
	}

	wait(c->create());

	state int blockSize = 1024;
	state std::vector<std::string> rangeFileNames;
	state Key begin = randomKeyBetween(normalKeys);
	state Key end = randomKeyBetween(KeyRangeRef(begin, normalKeys.end));
	state std::pair<Key, Key> beginEndKeys = std::make_pair(begin, end);
	state std::vector<std::pair<Key, Key>> snapshotBeginEndKeys;

	// writing random number of range files with rangeSize 100
	state std::vector<Future<Void>> writes;
	state Version snapshotBeginVersion = 10;
	state Version snapshotEndVersion = deterministicRandom()->randomInt(500, 1000);
	state Version rangeSize = 100;
	state Version v = snapshotBeginVersion;
	state int numRangeFiles = 0;
	while (v <= snapshotEndVersion) {
		state Reference<IBackupFile> range = wait(c->writeRangeFile(v, 0, v, blockSize));
		writes.push_back(writeAndVerifyFile(c, range, 100, &lock));
		rangeFileNames.push_back(range->getFileName());
		snapshotBeginEndKeys.push_back(beginEndKeys);
		v += rangeSize;
		++numRangeFiles;
	}
	snapshotEndVersion = v - rangeSize;

	// writing random number of log files with logSize 70, covering the entire snapshot
	state Version logSize = 70;
	v = snapshotBeginVersion;
	state int numLogFiles = 0;
	while (v <= snapshotEndVersion) {
		Reference<IBackupFile> log = wait(c->writeLogFile(v, v + logSize, blockSize));
		writes.push_back(writeAndVerifyFile(c, log, 100, &lock));
		++numLogFiles;
		v += logSize;
	}

	// writing snapshot file
	writes.push_back(c->writeKeyspaceSnapshotFile(
	    rangeFileNames, snapshotBeginEndKeys, deterministicRandom()->randomInt(0, 2e6), IncludeKeyRangeMap(BUGGIFY)));
	wait(waitForAll(writes));

	state BackupFileList fileList = wait(c->dumpFileList());
	printFileList(fileList);
	ASSERT_EQ(fileList.ranges.size(), numRangeFiles);
	ASSERT_EQ(fileList.logs.size(), numLogFiles);
	ASSERT_EQ(fileList.snapshots.size(), 1);

	state BackupDescription desc = wait(c->describeBackup());
	printf("\n%s\n", desc.toString().c_str());
	ASSERT_EQ(desc.minLogBegin, snapshotBeginVersion);
	ASSERT_EQ(desc.maxLogEnd, v);
	ASSERT_EQ(desc.minRestorableVersion, snapshotEndVersion);
	ASSERT_EQ(desc.maxRestorableVersion, v - 1);
	ASSERT_EQ(desc.snapshots[0].restorable, true);
	ASSERT_EQ(desc.contiguousLogEnd, v);

	// writing random number of more continuous log files
	state int newNumLogFiles = deterministicRandom()->randomInt(2, 8);
	numLogFiles += newNumLogFiles;
	writes.clear();
	while (newNumLogFiles) {
		Reference<IBackupFile> log = wait(c->writeLogFile(v, v + logSize, blockSize));
		writes.push_back(writeAndVerifyFile(c, log, 100, &lock));
		--newNumLogFiles;
		v += logSize;
	}
	wait(waitForAll(writes));

	state BackupFileList fileList1 = wait(c->dumpFileList());
	printFileList(fileList1);
	ASSERT_EQ(fileList1.ranges.size(), numRangeFiles);
	ASSERT_EQ(fileList1.logs.size(), numLogFiles);
	ASSERT_EQ(fileList1.snapshots.size(), 1);

	state BackupDescription desc1 = wait(c->describeBackup());
	printf("\n%s\n", desc1.toString().c_str());
	ASSERT_EQ(desc1.minLogBegin, snapshotBeginVersion);
	ASSERT_EQ(desc1.maxLogEnd, v);
	ASSERT_EQ(desc1.minRestorableVersion, snapshotEndVersion);
	ASSERT_EQ(desc1.maxRestorableVersion, v - 1);
	ASSERT_EQ(desc1.snapshots[0].restorable, true);
	ASSERT_EQ(desc1.contiguousLogEnd, v);

	return Void();
}

TEST_CASE("/backup/containers/localdir/continuousLogEndVersion") {
	wait(testBackupContinuousLogEndVer(format("file://%s/fdb_backups/%llx", params.getDataDir().c_str(), timer_int()),
	                                   {}));
	return Void();
}

} // namespace backup_test
