/*
 * BlobManifest.actor.cpp
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

#include <algorithm>
#include <string>
#include <vector>

#include "fdbclient/BackupContainer.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbserver/Knobs.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/BlobConnectionProvider.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"

#include "flow/actorcompiler.h" // has to be last include

//
// This module offers routines to dump or load blob manifest file, which is used for full restore from granules
//

// Default manifest folder on external blob storage
#define MANIFEST_FOLDER "manifest"

#define ENABLE_DEBUG_PRINT true
template <typename... T>
inline void dprint(fmt::format_string<T...> fmt, T&&... args) {
	if (ENABLE_DEBUG_PRINT)
		fmt::print(fmt, std::forward<T>(args)...);
}

// Defines a manifest file. THe file name includes the epoch of blob manager and a dump sequence number.
struct BlobManifestFile {
	std::string fileName;
	int64_t epoch{ 0 };
	int64_t seqNo{ 0 };

	BlobManifestFile(const std::string& path) {
		if (sscanf(path.c_str(), MANIFEST_FOLDER "/" MANIFEST_FOLDER ".%" SCNd64 ".%" SCNd64, &epoch, &seqNo) == 2) {
			fileName = path;
		}
	}

	// Sort in descending order of {epoch, seqNo}
	bool operator<(const BlobManifestFile& rhs) const {
		return epoch == rhs.epoch ? seqNo > rhs.seqNo : epoch > rhs.epoch;
	}

	// List all blob manifest files, sorted in descending order
	ACTOR static Future<std::vector<BlobManifestFile>> list(Reference<BackupContainerFileSystem> reader) {
		std::function<bool(std::string const&)> filter = [=](std::string const& path) {
			BlobManifestFile file(path);
			return file.epoch > 0 && file.seqNo > 0;
		};
		BackupContainerFileSystem::FilesAndSizesT filesAndSizes = wait(reader->listFiles(MANIFEST_FOLDER "/", filter));

		std::vector<BlobManifestFile> result;
		for (auto& f : filesAndSizes) {
			BlobManifestFile file(f.first);
			result.push_back(file);
		}
		std::sort(result.begin(), result.end());
		return result;
	}
};

// This class dumps blob manifest to external blob storage.
class BlobManifestDumper : public ReferenceCounted<BlobManifestDumper> {
public:
	BlobManifestDumper(Database& db, Reference<BlobConnectionProvider> blobConn, int64_t epoch, int64_t seqNo)
	  : db_(db), blobConn_(blobConn), epoch_(epoch), seqNo_(seqNo) {}
	virtual ~BlobManifestDumper() {}

	// Execute the dumper
	ACTOR static Future<Void> execute(Reference<BlobManifestDumper> self) {
		try {
			state Standalone<BlobManifest> manifest;
			Standalone<VectorRef<KeyValueRef>> rows = wait(getSystemKeys(self));
			if (rows.size() == 0) {
				return Void();
			}
			manifest.rows = rows;
			Value data = encode(manifest);
			wait(writeToFile(self, data));
			wait(cleanup(self));
		} catch (Error& e) {
			dprint("WARNING: unexpected blob manifest dumper error {}\n", e.what()); // skip error handling for now
		}
		return Void();
	}

private:
	// Return system keys that to be backed up
	ACTOR static Future<Standalone<VectorRef<KeyValueRef>>> getSystemKeys(Reference<BlobManifestDumper> self) {
		state Standalone<VectorRef<KeyValueRef>> rows;
		state Transaction tr(self->db_);
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				state std::vector<KeyRangeRef> ranges = {
					blobGranuleMappingKeys, // Map granule to workers. Track the active granules
					blobGranuleFileKeys, // Map a granule version to granule files. Track files for a granule
					blobGranuleHistoryKeys, // Map granule to its parents and parent bundaries. for time-travel read
					blobRangeKeys // Key ranges managed by blob
				};
				for (auto range : ranges) {
					state GetRangeLimits limits(SERVER_KNOBS->BLOB_MANIFEST_RW_ROWS);
					limits.minRows = 0;
					state KeySelectorRef begin = firstGreaterOrEqual(range.begin);
					state KeySelectorRef end = firstGreaterOrEqual(range.end);
					loop {
						RangeResult result = wait(tr.getRange(begin, end, limits, Snapshot::True));
						for (auto& row : result) {
							rows.push_back_deep(rows.arena(), KeyValueRef(row.key, row.value));
						}
						if (!result.more) {
							break;
						}
						if (result.readThrough.present()) {
							begin = firstGreaterOrEqual(result.readThrough.get());
						} else {
							begin = firstGreaterThan(result.end()[-1].key);
						}
					}
				}
				return rows;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Write data to blob manifest file
	ACTOR static Future<Void> writeToFile(Reference<BlobManifestDumper> self, Value data) {
		static int32_t lastWrittenBytes = 0;
		if (data.size() == lastWrittenBytes) {
			dprint("Skip writting blob manifest with same size {}\n", lastWrittenBytes);
			return Void();
		}
		lastWrittenBytes = data.size();

		state Reference<BackupContainerFileSystem> writer;
		state std::string fullPath;

		std::tie(writer, fullPath) = self->blobConn_->createForWrite(MANIFEST_FOLDER);
		state std::string fileName =
		    format(MANIFEST_FOLDER "/" MANIFEST_FOLDER ".%lld.%lld", self->epoch_, self->seqNo_);
		state Reference<IBackupFile> file = wait(writer->writeFile(fileName));
		wait(file->append(data.begin(), data.size()));
		wait(file->finish());
		dprint("Write blob manifest file {} with {} bytes\n", fileName, data.size());
		return Void();
	}

	// Encode manifest as binary data
	static Value encode(BlobManifest& manifest) {
		BinaryWriter wr(IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
		wr << manifest;
		return wr.toValue();
	}

	// Remove old manifest file
	ACTOR static Future<Void> cleanup(Reference<BlobManifestDumper> self) {
		state Reference<BackupContainerFileSystem> writer;
		state std::string fullPath;
		std::tie(writer, fullPath) = self->blobConn_->createForWrite(MANIFEST_FOLDER);
		std::vector<BlobManifestFile> files = wait(BlobManifestFile::list(writer));
		if (files.size() > sMaxCount_) {
			for (auto iter = files.begin() + sMaxCount_; iter < files.end(); ++iter) {
				writer->deleteFile(iter->fileName);
				dprint("Delete manifest file {}\n", iter->fileName);
			}
		}
		return Void();
	}

	Database db_;
	Reference<BlobConnectionProvider> blobConn_;
	int64_t epoch_; // blob manager epoch
	int64_t seqNo_; // manifest seq number
	static const int sMaxCount_{ 5 }; // max number of manifest file to keep
};

// Defines filename, version, size for each granule file that interests full restore
struct GranuleFileVersion {
	Version version;
	uint8_t fileType;
	std::string filename;
	int64_t sizeInBytes;
};

// This class is to load blob manifest into system key space, which is part of for bare metal restore
class BlobManifestLoader : public ReferenceCounted<BlobManifestLoader> {
public:
	BlobManifestLoader(Database& db, Reference<BlobConnectionProvider> blobConn) : db_(db), blobConn_(blobConn) {}
	virtual ~BlobManifestLoader() {}

	// Execute the loader
	ACTOR static Future<Void> execute(Reference<BlobManifestLoader> self) {
		try {
			Value data = wait(readFromFile(self));
			if (data.empty()) {
				throw restore_missing_data();
			}
			state Standalone<BlobManifest> manifest = decode(data);
			wait(writeSystemKeys(self, manifest.rows));
			BlobGranuleRestoreVersionVector _ = wait(listGranules(self));
		} catch (Error& e) {
			dprint("WARNING: unexpected manifest loader error {}\n", e.what());
			TraceEvent("BlobManfiestError").error(e).log();
			throw;
		}
		return Void();
	}

	// Iterate active granules and return their version/sizes
	ACTOR static Future<BlobGranuleRestoreVersionVector> listGranules(Reference<BlobManifestLoader> self) {
		state Transaction tr(self->db_);
		loop {
			state BlobGranuleRestoreVersionVector results;
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			try {
				state Standalone<VectorRef<KeyRef>> blobRanges;
				// Read all granules
				state GetRangeLimits limits(SERVER_KNOBS->BLOB_MANIFEST_RW_ROWS);
				limits.minRows = 0;
				state KeySelectorRef begin = firstGreaterOrEqual(blobGranuleMappingKeys.begin);
				state KeySelectorRef end = firstGreaterOrEqual(blobGranuleMappingKeys.end);
				loop {
					RangeResult rows = wait(tr.getRange(begin, end, limits, Snapshot::True));
					for (auto& row : rows) {
						blobRanges.push_back_deep(blobRanges.arena(), row.key);
					}
					if (!rows.more) {
						break;
					}
					if (rows.readThrough.present()) {
						begin = firstGreaterOrEqual(rows.readThrough.get());
					} else {
						begin = firstGreaterThan(rows.end()[-1].key);
					}
				}

				// check each granule range
				state int i = 0;
				for (i = 0; i < blobRanges.size() - 1; i++) {
					Key startKey = blobRanges[i].removePrefix(blobGranuleMappingKeys.begin);
					Key endKey = blobRanges[i + 1].removePrefix(blobGranuleMappingKeys.begin);
					state KeyRange granuleRange = KeyRangeRef(startKey, endKey);
					try {
						Standalone<BlobGranuleRestoreVersion> granule = wait(getGranule(&tr, granuleRange));
						results.push_back_deep(results.arena(), granule);
					} catch (Error& e) {
						if (e.code() == error_code_restore_missing_data) {
							dprint("missing data for key range {} \n", granuleRange.toString());
							TraceEvent("BlobRestoreMissingData").detail("KeyRange", granuleRange.toString());
						} else {
							TraceEvent("BlobManifestError").error(e).detail("KeyRange", granuleRange.toString());
						}
						throw;
					}
				}
				return results;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Print out a summary for blob granules
	ACTOR static Future<Void> print(Reference<BlobManifestLoader> self) {
		state BlobGranuleRestoreVersionVector granules = wait(listGranules(self));
		for (auto granule : granules) {
			wait(checkGranuleFiles(self, granule));
		}
		return Void();
	}

	// Return max epoch from all manifest files
	ACTOR static Future<int64_t> lastBlobEpoc(Reference<BlobManifestLoader> self) {
		state Reference<BackupContainerFileSystem> container = self->blobConn_->getForRead(MANIFEST_FOLDER);
		std::vector<BlobManifestFile> files = wait(BlobManifestFile::list(container));
		ASSERT(!files.empty());
		return files.front().epoch;
	}

private:
	// Read data from a manifest file
	ACTOR static Future<Value> readFromFile(Reference<BlobManifestLoader> self) {
		state Reference<BackupContainerFileSystem> container = self->blobConn_->getForRead(MANIFEST_FOLDER);
		std::vector<BlobManifestFile> files = wait(BlobManifestFile::list(container));
		if (files.empty()) {
			dprint("No blob manifest files for restore\n");
			return Value();
		}
		std::string fileName = files.front().fileName;
		state Reference<IAsyncFile> reader = wait(container->readFile(fileName));
		state int64_t fileSize = wait(reader->size());
		state Arena arena;
		state uint8_t* data = new (arena) uint8_t[fileSize];
		int readSize = wait(reader->read(data, fileSize, 0));
		dprint("Blob manifest restoring {} bytes\n", readSize);
		StringRef ref = StringRef(data, readSize);
		return Value(ref, arena);
	}

	// Decode blob manifest from binary data
	static Standalone<BlobManifest> decode(Value data) {
		Standalone<BlobManifest> manifest;
		BinaryReader binaryReader(data, IncludeVersion());
		binaryReader >> manifest;
		return manifest;
	}

	// Write system keys to database
	ACTOR static Future<Void> writeSystemKeys(Reference<BlobManifestLoader> self, VectorRef<KeyValueRef> rows) {
		state int start = 0;
		state int end = 0;
		for (start = 0; start < rows.size(); start = end) {
			end = std::min(start + SERVER_KNOBS->BLOB_MANIFEST_RW_ROWS, rows.size());
			wait(writeSystemKeys(self, rows, start, end));
		}
		return Void();
	}

	// Write system keys from start index to end(exclusive), so that we don't exceed the limit of transaction limit
	ACTOR static Future<Void> writeSystemKeys(Reference<BlobManifestLoader> self,
	                                          VectorRef<KeyValueRef> rows,
	                                          int start,
	                                          int end) {
		state Transaction tr(self->db_);
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				for (int i = start; i < end; ++i) {
					tr.set(rows[i].key, rows[i].value);
				}
				wait(tr.commit());
				dprint("Blob manifest loaded rows from {} to {}\n", start, end);
				TraceEvent("BlobManifestLoader").detail("RowStart", start).detail("RowEnd", end);
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Find the newest granule for a key range. The newest granule has the max version and relevant files
	ACTOR static Future<Standalone<BlobGranuleRestoreVersion>> getGranule(Transaction* tr, KeyRangeRef range) {
		state Standalone<BlobGranuleRestoreVersion> granuleVersion;
		state KeyRange historyKeyRange = blobGranuleHistoryKeyRangeFor(range);
		loop {
			try {
				// reverse lookup so that the first row is the newest version
				state RangeResult results = wait(
				    tr->getRange(historyKeyRange, GetRangeLimits::BYTE_LIMIT_UNLIMITED, Snapshot::True, Reverse::True));
				for (KeyValueRef row : results) {
					state KeyRange keyRange;
					state Version version;
					std::tie(keyRange, version) = decodeBlobGranuleHistoryKey(row.key);
					Standalone<BlobGranuleHistoryValue> historyValue = decodeBlobGranuleHistoryValue(row.value);
					state UID granuleID = historyValue.granuleID;

					std::vector<GranuleFileVersion> files = wait(listGranuleFiles(tr, granuleID));

					granuleVersion.keyRange = KeyRangeRef(granuleVersion.arena(), keyRange);
					granuleVersion.granuleID = granuleID;
					if (files.empty()) {
						dprint("Granule {} doesn't have files for version {}\n", granuleID.toString(), version);
						granuleVersion.version = version;
						granuleVersion.sizeInBytes = 1;
					} else {
						granuleVersion.version = files.back().version;
						granuleVersion.sizeInBytes = granuleSizeInBytes(files);
					}
					dprint("Granule {}: \n", granuleVersion.granuleID.toString());
					dprint("  {} {} {}\n", keyRange.toString(), granuleVersion.version, granuleVersion.sizeInBytes);
					for (auto& file : files) {
						dprint("  File {}: {} bytes\n", file.filename, file.sizeInBytes);
					}
					return granuleVersion;
				}
				throw restore_missing_data(); // todo a better error code
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	// Return sum of last snapshot file size and delta files afterwards
	static int64_t granuleSizeInBytes(std::vector<GranuleFileVersion> files) {
		int64_t totalSize = 0;
		for (auto it = files.rbegin(); it < files.rend(); ++it) {
			totalSize += it->sizeInBytes;
			if (it->fileType == BG_FILE_TYPE_SNAPSHOT)
				break;
		}
		return totalSize;
	}

	// List all files for given granule
	ACTOR static Future<std::vector<GranuleFileVersion>> listGranuleFiles(Transaction* tr, UID granuleID) {
		state std::vector<GranuleFileVersion> files;

		state KeyRange fileKeyRange = blobGranuleFileKeyRangeFor(granuleID);
		state GetRangeLimits limits(SERVER_KNOBS->BLOB_MANIFEST_RW_ROWS);
		limits.minRows = 0;
		state KeySelectorRef begin = firstGreaterOrEqual(fileKeyRange.begin);
		state KeySelectorRef end = firstGreaterOrEqual(fileKeyRange.end);
		loop {
			RangeResult results = wait(tr->getRange(begin, end, limits, Snapshot::True));
			for (auto& row : results) {
				UID gid;
				Version version;
				uint8_t fileType;
				Standalone<StringRef> filename;
				int64_t offset;
				int64_t length;
				int64_t fullFileLength;
				Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;

				std::tie(gid, version, fileType) = decodeBlobGranuleFileKey(row.key);
				std::tie(filename, offset, length, fullFileLength, cipherKeysMeta) =
				    decodeBlobGranuleFileValue(row.value);
				GranuleFileVersion vs = { version, fileType, filename.toString(), length };
				files.push_back(vs);
			}
			if (!results.more) {
				break;
			}
			if (results.readThrough.present()) {
				begin = firstGreaterOrEqual(results.readThrough.get());
			} else {
				begin = firstGreaterThan(results.end()[-1].key);
			}
		}
		return files;
	}

	// Read data from granules and print out summary
	ACTOR static Future<Void> checkGranuleFiles(Reference<BlobManifestLoader> self, BlobGranuleRestoreVersion granule) {
		state KeyRangeRef range = granule.keyRange;
		state Version readVersion = granule.version;
		state Transaction tr(self->db_);
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				state Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
				    wait(tr.readBlobGranules(range, 0, readVersion));
				state int count = 0;
				for (const BlobGranuleChunkRef& chunk : chunks) {
					RangeResult rows = wait(readBlobGranule(chunk, range, 0, readVersion, self->blobConn_));
					count += rows.size();
				}

				dprint("Restorable blob granule {} @ {}\n", granule.granuleID.toString(), readVersion);
				dprint("  Range: {}\n", range.toString());
				dprint("  Keys : {}\n", count);
				dprint("  Size : {} bytes\n", granule.sizeInBytes);
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	Database db_;
	Reference<BlobConnectionProvider> blobConn_;
};

// API to dump a manifest copy to external storage
ACTOR Future<Void> dumpManifest(Database db, Reference<BlobConnectionProvider> blobConn, int64_t epoch, int64_t seqNo) {
	Reference<BlobManifestDumper> dumper = makeReference<BlobManifestDumper>(db, blobConn, epoch, seqNo);
	wait(BlobManifestDumper::execute(dumper));
	return Void();
}

// API to load manifest from external blob storage
ACTOR Future<Void> loadManifest(Database db, Reference<BlobConnectionProvider> blobConn) {
	Reference<BlobManifestLoader> loader = makeReference<BlobManifestLoader>(db, blobConn);
	wait(BlobManifestLoader::execute(loader));
	return Void();
}

// API to print summary for restorable granules
ACTOR Future<Void> printRestoreSummary(Database db, Reference<BlobConnectionProvider> blobConn) {
	Reference<BlobManifestLoader> loader = makeReference<BlobManifestLoader>(db, blobConn);
	wait(BlobManifestLoader::print(loader));
	return Void();
}

// API to list blob granules
ACTOR Future<BlobGranuleRestoreVersionVector> listBlobGranules(Database db,
                                                               Reference<BlobConnectionProvider> blobConn) {
	Reference<BlobManifestLoader> loader = makeReference<BlobManifestLoader>(db, blobConn);
	BlobGranuleRestoreVersionVector result = wait(BlobManifestLoader::listGranules(loader));
	return result;
}

// API to get max blob manager epoc from manifest files
ACTOR Future<int64_t> lastBlobEpoc(Database db, Reference<BlobConnectionProvider> blobConn) {
	Reference<BlobManifestLoader> loader = makeReference<BlobManifestLoader>(db, blobConn);
	int64_t epoc = wait(BlobManifestLoader::lastBlobEpoc(loader));
	return epoc;
}

// Return true if the given key range is restoring
ACTOR Future<bool> isFullRestoreMode(Database db, KeyRangeRef keys) {
	std::pair<KeyRange, BlobRestoreStatus> rangeStatus = wait(getRestoreRangeStatus(db, keys));
	return !rangeStatus.first.empty() && rangeStatus.second.phase != BlobRestorePhase::DONE;
}

// Check the given key range and return subrange that is doing restore. Returns empty range if no restoring
// for any portion of the given range.
ACTOR Future<std::pair<KeyRange, BlobRestoreStatus>> getRestoreRangeStatus(Database db, KeyRangeRef keys) {
	state Transaction tr(db);
	loop {
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			state GetRangeLimits limits(SERVER_KNOBS->BLOB_MANIFEST_RW_ROWS);
			limits.minRows = 0;
			state KeySelectorRef begin = firstGreaterOrEqual(blobRestoreCommandKeys.begin);
			state KeySelectorRef end = firstGreaterOrEqual(blobRestoreCommandKeys.end);
			loop {
				RangeResult ranges = wait(tr.getRange(begin, end, limits, Snapshot::True));
				for (auto& r : ranges) {
					KeyRange keyRange = decodeBlobRestoreCommandKeyFor(r.key);
					if (keys.intersects(keyRange)) {
						Standalone<BlobRestoreStatus> status = decodeBlobRestoreStatus(r.value);
						KeyRangeRef intersected(std::max(keys.begin, keyRange.begin), std::min(keys.end, keyRange.end));
						return std::make_pair(intersected, status);
					}
				}
				if (!ranges.more) {
					break;
				}
				if (ranges.readThrough.present()) {
					begin = firstGreaterOrEqual(ranges.readThrough.get());
				} else {
					begin = firstGreaterThan(ranges.end()[-1].key);
				}
			}
			return std::make_pair(KeyRangeRef(), BlobRestoreStatus(BlobRestorePhase::DONE));
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Update restore status
ACTOR Future<Void> updateRestoreStatus(Database db,
                                       KeyRangeRef range,
                                       BlobRestoreStatus status,
                                       Optional<BlobRestorePhase> expectedPhase) {
	state Transaction tr(db);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state Key key = blobRestoreCommandKeyFor(range);

			// check if current phase is expected
			if (expectedPhase.present()) {
				Optional<Value> oldValue = wait(tr.get(key));
				if (oldValue.present()) {
					Standalone<BlobRestoreStatus> status = decodeBlobRestoreStatus(oldValue.get());
					if (status.phase != expectedPhase.get()) {
						throw restore_error();
					}
				}
			}

			Value value = blobRestoreCommandValueFor(status);
			tr.set(key, value);
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Get restore status
ACTOR Future<Optional<BlobRestoreStatus>> getRestoreStatus(Database db, KeyRangeRef keys) {
	state Optional<BlobRestoreStatus> result;
	std::pair<KeyRange, BlobRestoreStatus> rangeStatus = wait(getRestoreRangeStatus(db, keys));
	if (!rangeStatus.first.empty()) {
		result = rangeStatus.second;
	}
	return result;
}

// Get restore argument
ACTOR Future<Optional<BlobRestoreArg>> getRestoreArg(Database db, KeyRangeRef keys) {
	state Transaction tr(db);
	state Optional<BlobRestoreArg> result;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				state GetRangeLimits limits(SERVER_KNOBS->BLOB_MANIFEST_RW_ROWS);
				limits.minRows = 0;
				state KeySelectorRef begin = firstGreaterOrEqual(blobRestoreArgKeys.begin);
				state KeySelectorRef end = firstGreaterOrEqual(blobRestoreArgKeys.end);
				loop {
					RangeResult ranges = wait(tr.getRange(begin, end, limits, Snapshot::True));
					for (auto& r : ranges) {
						KeyRange keyRange = decodeBlobRestoreArgKeyFor(r.key);
						if (keys.intersects(keyRange)) {
							Standalone<BlobRestoreArg> arg = decodeBlobRestoreArg(r.value);
							result = arg;
							return result;
						}
					}
					if (!ranges.more) {
						break;
					}
					if (ranges.readThrough.present()) {
						begin = firstGreaterOrEqual(ranges.readThrough.get());
					} else {
						begin = firstGreaterThan(ranges.back().key);
					}
				}
				return result;
			} catch (Error& e) {
				wait(tr.onError(e));
			}

		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Get restore target version. Return defaultVersion if no restore argument available for the range
ACTOR Future<Version> getRestoreTargetVersion(Database db, KeyRangeRef range, Version defaultVersion) {
	Optional<BlobRestoreArg> arg = wait(getRestoreArg(db, range));
	Version expected = defaultVersion;
	if (arg.present()) {
		if (arg.get().version.present()) {
			return arg.get().version.get();
		}
	}
	return expected;
}
