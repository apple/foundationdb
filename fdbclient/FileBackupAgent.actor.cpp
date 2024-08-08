/*
 * FileBackupAgent.actor.cpp
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

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RestoreInterface.h"
#include "fdbclient/Status.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/JsonBuilder.h"

#include <cinttypes>
#include <ctime>
#include <climits>
#include "fdbrpc/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include <numeric>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>

#include "flow/actorcompiler.h" // This must be the last #include.

FDB_DEFINE_BOOLEAN_PARAM(IncrementalBackupOnly);
FDB_DEFINE_BOOLEAN_PARAM(OnlyApplyMutationLogs);

Optional<std::string> fileBackupAgentProxy = Optional<std::string>();

#define SevFRTestInfo SevVerbose
// #define SevFRTestInfo SevInfo

static std::string boolToYesOrNo(bool val) {
	return val ? std::string("Yes") : std::string("No");
}

static std::string versionToString(Optional<Version> version) {
	if (version.present())
		return std::to_string(version.get());
	else
		return "N/A";
}

static std::string timeStampToString(Optional<int64_t> epochs) {
	if (!epochs.present())
		return "N/A";
	return BackupAgentBase::formatTime(epochs.get());
}

static Future<Optional<int64_t>> getTimestampFromVersion(Optional<Version> ver,
                                                         Reference<ReadYourWritesTransaction> tr) {
	if (!ver.present())
		return Optional<int64_t>();

	return timeKeeperEpochsFromVersion(ver.get(), tr);
}

// Time format :
// <= 59 seconds
// <= 59.99 minutes
// <= 23.99 hours
// N.NN days
std::string secondsToTimeFormat(int64_t seconds) {
	if (seconds >= 86400)
		return format("%.2f day(s)", seconds / 86400.0);
	else if (seconds >= 3600)
		return format("%.2f hour(s)", seconds / 3600.0);
	else if (seconds >= 60)
		return format("%.2f minute(s)", seconds / 60.0);
	else
		return format("%lld second(s)", seconds);
}

const Key FileBackupAgent::keyLastRestorable = LiteralStringRef("last_restorable");

// For convenience
typedef FileBackupAgent::ERestoreState ERestoreState;

StringRef FileBackupAgent::restoreStateText(ERestoreState id) {
	switch (id) {
	case ERestoreState::UNITIALIZED:
		return LiteralStringRef("unitialized");
	case ERestoreState::QUEUED:
		return LiteralStringRef("queued");
	case ERestoreState::STARTING:
		return LiteralStringRef("starting");
	case ERestoreState::RUNNING:
		return LiteralStringRef("running");
	case ERestoreState::COMPLETED:
		return LiteralStringRef("completed");
	case ERestoreState::ABORTED:
		return LiteralStringRef("aborted");
	default:
		return LiteralStringRef("Unknown");
	}
}

Key FileBackupAgent::getPauseKey() {
	FileBackupAgent backupAgent;
	return backupAgent.taskBucket->getPauseKey();
}

ACTOR Future<std::vector<KeyBackedTag>> TagUidMap::getAll_impl(TagUidMap* tagsMap,
                                                               Reference<ReadYourWritesTransaction> tr,
                                                               Snapshot snapshot) {
	state Key prefix = tagsMap->prefix; // Copying it here as tagsMap lifetime is not tied to this actor
	TagMap::PairsType tagPairs = wait(tagsMap->getRange(tr, std::string(), {}, 1e6, snapshot));
	std::vector<KeyBackedTag> results;
	for (auto& p : tagPairs)
		results.push_back(KeyBackedTag(p.first, prefix));
	return results;
}

KeyBackedTag::KeyBackedTag(std::string tagName, StringRef tagMapPrefix)
  : KeyBackedProperty<UidAndAbortedFlagT>(TagUidMap(tagMapPrefix).getProperty(tagName)), tagName(tagName),
    tagMapPrefix(tagMapPrefix) {}

class RestoreConfig : public KeyBackedConfig {
public:
	RestoreConfig(UID uid = UID()) : KeyBackedConfig(fileRestorePrefixRange.begin, uid) {}
	RestoreConfig(Reference<Task> task) : KeyBackedConfig(fileRestorePrefixRange.begin, task) {}

	KeyBackedProperty<ERestoreState> stateEnum() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	Future<StringRef> stateText(Reference<ReadYourWritesTransaction> tr) {
		return map(stateEnum().getD(tr),
		           [](ERestoreState s) -> StringRef { return FileBackupAgent::restoreStateText(s); });
	}
	KeyBackedProperty<Key> addPrefix() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	KeyBackedProperty<Key> removePrefix() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	KeyBackedProperty<bool> onlyApplyMutationLogs() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	KeyBackedProperty<bool> inconsistentSnapshotOnly() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	// XXX: Remove restoreRange() once it is safe to remove. It has been changed to restoreRanges
	KeyBackedProperty<KeyRange> restoreRange() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	KeyBackedProperty<std::vector<KeyRange>> restoreRanges() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<Key> batchFuture() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	KeyBackedProperty<Version> beginVersion() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	KeyBackedProperty<Version> restoreVersion() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	KeyBackedProperty<Version> firstConsistentVersion() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }

	KeyBackedProperty<Reference<IBackupContainer>> sourceContainer() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// Get the source container as a bare URL, without creating a container instance
	KeyBackedProperty<Value> sourceContainerURL() { return configSpace.pack(LiteralStringRef("sourceContainer")); }

	// Total bytes written by all log and range restore tasks.
	KeyBackedBinaryValue<int64_t> bytesWritten() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	// File blocks that have had tasks created for them by the Dispatch task
	KeyBackedBinaryValue<int64_t> filesBlocksDispatched() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	// File blocks whose tasks have finished
	KeyBackedBinaryValue<int64_t> fileBlocksFinished() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	// Total number of files in the fileMap
	KeyBackedBinaryValue<int64_t> fileCount() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }
	// Total number of file blocks in the fileMap
	KeyBackedBinaryValue<int64_t> fileBlockCount() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }

	Future<std::vector<KeyRange>> getRestoreRangesOrDefault(Reference<ReadYourWritesTransaction> tr) {
		return getRestoreRangesOrDefault_impl(this, tr);
	}

	ACTOR static Future<std::vector<KeyRange>> getRestoreRangesOrDefault_impl(RestoreConfig* self,
	                                                                          Reference<ReadYourWritesTransaction> tr) {
		state std::vector<KeyRange> ranges = wait(self->restoreRanges().getD(tr));
		if (ranges.empty()) {
			state KeyRange range = wait(self->restoreRange().getD(tr));
			ranges.push_back(range);
		}
		return ranges;
	}

	// Describes a file to load blocks from during restore.  Ordered by version and then fileName to enable
	// incrementally advancing through the map, saving the version and path of the next starting point.
	struct RestoreFile {
		Version version;
		std::string fileName;
		bool isRange{ false }; // false for log file
		int64_t blockSize{ 0 };
		int64_t fileSize{ 0 };
		Version endVersion{ ::invalidVersion }; // not meaningful for range files

		Tuple pack() const {
			return Tuple()
			    .append(version)
			    .append(StringRef(fileName))
			    .append(isRange)
			    .append(fileSize)
			    .append(blockSize)
			    .append(endVersion);
		}
		static RestoreFile unpack(Tuple const& t) {
			RestoreFile r;
			int i = 0;
			r.version = t.getInt(i++);
			r.fileName = t.getString(i++).toString();
			r.isRange = t.getInt(i++) != 0;
			r.fileSize = t.getInt(i++);
			r.blockSize = t.getInt(i++);
			r.endVersion = t.getInt(i++);
			return r;
		}
	};

	typedef KeyBackedSet<RestoreFile> FileSetT;
	FileSetT fileSet() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }

	Future<bool> isRunnable(Reference<ReadYourWritesTransaction> tr) {
		return map(stateEnum().getD(tr), [](ERestoreState s) -> bool {
			return s != ERestoreState::ABORTED && s != ERestoreState::COMPLETED && s != ERestoreState::UNITIALIZED;
		});
	}

	Future<Void> logError(Database cx, Error e, std::string const& details, void* taskInstance = nullptr) {
		if (!uid.isValid()) {
			TraceEvent(SevError, "FileRestoreErrorNoUID").error(e).detail("Description", details);
			return Void();
		}
		TraceEvent t(SevWarn, "FileRestoreError");
		t.error(e)
		    .detail("RestoreUID", uid)
		    .detail("Description", details)
		    .detail("TaskInstance", (uint64_t)taskInstance);
		// key_not_found could happen
		if (e.code() == error_code_key_not_found)
			t.backtrace();

		return updateErrorInfo(cx, e, details);
	}

	Key mutationLogPrefix() { return uidPrefixKey(applyLogKeys.begin, uid); }

	Key applyMutationsMapPrefix() { return uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid); }

	ACTOR static Future<int64_t> getApplyVersionLag_impl(Reference<ReadYourWritesTransaction> tr, UID uid) {
		state Future<Optional<Value>> beginVal =
		    tr->get(uidPrefixKey(applyMutationsBeginRange.begin, uid), Snapshot::True);
		state Future<Optional<Value>> endVal = tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid), Snapshot::True);
		wait(success(beginVal) && success(endVal));

		if (!beginVal.get().present() || !endVal.get().present())
			return 0;

		Version beginVersion = BinaryReader::fromStringRef<Version>(beginVal.get().get(), Unversioned());
		Version endVersion = BinaryReader::fromStringRef<Version>(endVal.get().get(), Unversioned());
		return endVersion - beginVersion;
	}

	Future<int64_t> getApplyVersionLag(Reference<ReadYourWritesTransaction> tr) {
		return getApplyVersionLag_impl(tr, uid);
	}

	void initApplyMutations(Reference<ReadYourWritesTransaction> tr, Key addPrefix, Key removePrefix) {
		// Set these because they have to match the applyMutations values.
		this->addPrefix().set(tr, addPrefix);
		this->removePrefix().set(tr, removePrefix);

		clearApplyMutationsKeys(tr);

		// Initialize add/remove prefix, range version map count and set the map's start key to InvalidVersion
		tr->set(uidPrefixKey(applyMutationsAddPrefixRange.begin, uid), addPrefix);
		tr->set(uidPrefixKey(applyMutationsRemovePrefixRange.begin, uid), removePrefix);
		int64_t startCount = 0;
		tr->set(uidPrefixKey(applyMutationsKeyVersionCountRange.begin, uid), StringRef((uint8_t*)&startCount, 8));
		Key mapStart = uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
		tr->set(mapStart, BinaryWriter::toValue<Version>(invalidVersion, Unversioned()));
	}

	void clearApplyMutationsKeys(Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

		// Clear add/remove prefix keys
		tr->clear(uidPrefixKey(applyMutationsAddPrefixRange.begin, uid));
		tr->clear(uidPrefixKey(applyMutationsRemovePrefixRange.begin, uid));

		// Clear range version map and count key
		tr->clear(uidPrefixKey(applyMutationsKeyVersionCountRange.begin, uid));
		Key mapStart = uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
		tr->clear(KeyRangeRef(mapStart, strinc(mapStart)));

		// Clear any loaded mutations that have not yet been applied
		Key mutationPrefix = mutationLogPrefix();
		tr->clear(KeyRangeRef(mutationPrefix, strinc(mutationPrefix)));

		// Clear end and begin versions (intentionally in this order)
		tr->clear(uidPrefixKey(applyMutationsEndRange.begin, uid));
		tr->clear(uidPrefixKey(applyMutationsBeginRange.begin, uid));
	}

	void setApplyBeginVersion(Reference<ReadYourWritesTransaction> tr, Version ver) {
		tr->set(uidPrefixKey(applyMutationsBeginRange.begin, uid), BinaryWriter::toValue(ver, Unversioned()));
	}

	Future<Version> getApplyBeginVersion(Reference<ReadYourWritesTransaction> tr) {
		return map(tr->get(uidPrefixKey(applyMutationsBeginRange.begin, uid)),
		           [=](Optional<Value> const& value) -> Version {
			           return value.present() ? BinaryReader::fromStringRef<Version>(value.get(), Unversioned()) : 0;
		           });
	}

	void setApplyEndVersion(Reference<ReadYourWritesTransaction> tr, Version ver) {
		tr->set(uidPrefixKey(applyMutationsEndRange.begin, uid), BinaryWriter::toValue(ver, Unversioned()));
	}

	Future<Version> getApplyEndVersion(Reference<ReadYourWritesTransaction> tr) {
		return map(tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid)),
		           [=](Optional<Value> const& value) -> Version {
			           return value.present() ? BinaryReader::fromStringRef<Version>(value.get(), Unversioned()) : 0;
		           });
	}

	ACTOR static Future<Version> getCurrentVersion_impl(RestoreConfig* self, Reference<ReadYourWritesTransaction> tr) {
		state ERestoreState status = wait(self->stateEnum().getD(tr));
		state Version version = -1;
		if (status == ERestoreState::RUNNING) {
			wait(store(version, self->getApplyBeginVersion(tr)));
		} else if (status == ERestoreState::COMPLETED) {
			wait(store(version, self->restoreVersion().getD(tr)));
		}
		return version;
	}

	Future<Version> getCurrentVersion(Reference<ReadYourWritesTransaction> tr) {
		return getCurrentVersion_impl(this, tr);
	}

	ACTOR static Future<std::string> getProgress_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr);
	Future<std::string> getProgress(Reference<ReadYourWritesTransaction> tr) { return getProgress_impl(*this, tr); }

	ACTOR static Future<std::string> getFullStatus_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr);
	Future<std::string> getFullStatus(Reference<ReadYourWritesTransaction> tr) { return getFullStatus_impl(*this, tr); }
};

typedef RestoreConfig::RestoreFile RestoreFile;

ACTOR Future<std::string> RestoreConfig::getProgress_impl(RestoreConfig restore,
                                                          Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<int64_t> fileCount = restore.fileCount().getD(tr);
	state Future<int64_t> fileBlockCount = restore.fileBlockCount().getD(tr);
	state Future<int64_t> fileBlocksDispatched = restore.filesBlocksDispatched().getD(tr);
	state Future<int64_t> fileBlocksFinished = restore.fileBlocksFinished().getD(tr);
	state Future<int64_t> bytesWritten = restore.bytesWritten().getD(tr);
	state Future<StringRef> status = restore.stateText(tr);
	state Future<Version> currentVersion = restore.getCurrentVersion(tr);
	state Future<Version> lag = restore.getApplyVersionLag(tr);
	state Future<Version> firstConsistentVersion = restore.firstConsistentVersion().getD(tr);
	state Future<std::string> tag = restore.tag().getD(tr);
	state Future<std::pair<std::string, Version>> lastError = restore.lastError().getD(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	state UID uid = restore.getUid();
	wait(success(fileCount) && success(fileBlockCount) && success(fileBlocksDispatched) &&
	     success(fileBlocksFinished) && success(bytesWritten) && success(status) && success(currentVersion) &&
	     success(lag) && success(firstConsistentVersion) && success(tag) && success(lastError));

	std::string errstr = "None";
	if (lastError.get().second != 0)
		errstr = format("'%s' %" PRId64 "s ago.\n",
		                lastError.get().first.c_str(),
		                (tr->getReadVersion().get() - lastError.get().second) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND);

	TraceEvent("FileRestoreProgress")
	    .detail("RestoreUID", uid)
	    .detail("Tag", tag.get())
	    .detail("State", status.get().toString())
	    .detail("FileCount", fileCount.get())
	    .detail("FileBlocksFinished", fileBlocksFinished.get())
	    .detail("FileBlocksTotal", fileBlockCount.get())
	    .detail("FileBlocksInProgress", fileBlocksDispatched.get() - fileBlocksFinished.get())
	    .detail("BytesWritten", bytesWritten.get())
	    .detail("CurrentVersion", currentVersion.get())
	    .detail("FirstConsistentVersion", firstConsistentVersion.get())
	    .detail("ApplyLag", lag.get())
	    .detail("TaskInstance", THIS_ADDR);

	return format("Tag: %s  UID: %s  State: %s  Blocks: %lld/%lld  BlocksInProgress: %lld  Files: %lld  BytesWritten: "
	              "%lld  CurrentVersion: %lld FirstConsistentVersion: %lld  ApplyVersionLag: %lld  LastError: %s",
	              tag.get().c_str(),
	              uid.toString().c_str(),
	              status.get().toString().c_str(),
	              fileBlocksFinished.get(),
	              fileBlockCount.get(),
	              fileBlocksDispatched.get() - fileBlocksFinished.get(),
	              fileCount.get(),
	              bytesWritten.get(),
	              currentVersion.get(),
	              firstConsistentVersion.get(),
	              lag.get(),
	              errstr.c_str());
}

ACTOR Future<std::string> RestoreConfig::getFullStatus_impl(RestoreConfig restore,
                                                            Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<std::vector<KeyRange>> ranges = restore.getRestoreRangesOrDefault(tr);
	state Future<Key> addPrefix = restore.addPrefix().getD(tr);
	state Future<Key> removePrefix = restore.removePrefix().getD(tr);
	state Future<Key> url = restore.sourceContainerURL().getD(tr);
	state Future<Version> restoreVersion = restore.restoreVersion().getD(tr);
	state Future<std::string> progress = restore.getProgress(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	wait(success(ranges) && success(addPrefix) && success(removePrefix) && success(url) && success(restoreVersion) &&
	     success(progress));

	std::string returnStr;
	returnStr = format("%s  URL: %s", progress.get().c_str(), url.get().toString().c_str());
	for (auto& range : ranges.get()) {
		returnStr += format("  Range: '%s'-'%s'", printable(range.begin).c_str(), printable(range.end).c_str());
	}
	returnStr += format("  AddPrefix: '%s'  RemovePrefix: '%s'  Version: %lld",
	                    printable(addPrefix.get()).c_str(),
	                    printable(removePrefix.get()).c_str(),
	                    restoreVersion.get());
	return returnStr;
}

FileBackupAgent::FileBackupAgent()
  : subspace(Subspace(fileBackupPrefixRange.begin))
    // The other subspaces have logUID -> value
    ,
    config(subspace.get(BackupAgentBase::keyConfig)), lastRestorable(subspace.get(FileBackupAgent::keyLastRestorable)),
    taskBucket(new TaskBucket(subspace.get(BackupAgentBase::keyTasks),
                              AccessSystemKeys::True,
                              PriorityBatch::False,
                              LockAware::True)),
    futureBucket(new FutureBucket(subspace.get(BackupAgentBase::keyFutures), AccessSystemKeys::True, LockAware::True)) {
}

namespace fileBackup {

// Return a block of contiguous padding bytes, growing if needed.
Value makePadding(int size) {
	static Value pad;
	if (pad.size() < size) {
		pad = makeString(size);
		memset(mutateString(pad), '\xff', pad.size());
	}

	return pad.substr(0, size);
}

// File Format handlers.
// Both Range and Log formats are designed to be readable starting at any 1MB boundary
// so they can be read in parallel.
//
// Writer instances must be kept alive while any member actors are in progress.
//
// RangeFileWriter must be used as follows:
//   1 - writeKey(key) the queried key range begin
//   2 - writeKV(k, v) each kv pair to restore
//   3 - writeKey(key) the queried key range end
//
// RangeFileWriter will insert the required padding, header, and extra
// end/begin keys around the 1MB boundaries as needed.
//
// Example:
//   The range a-z is queries and returns c-j which covers 3 blocks.
//   The client code writes keys in this sequence:
//             a c d e f g h i j z
//
//   H = header   P = padding   a...z = keys  v = value | = block boundary
//
//   Encoded file:  H a cv dv ev P | H e ev fv gv hv P | H h hv iv jv z
//   Decoded in blocks yields:
//           Block 1: range [a, e) with kv pairs cv, dv
//           Block 2: range [e, h) with kv pairs ev, fv, gv
//           Block 3: range [h, z) with kv pairs hv, iv, jv
//
//   NOTE: All blocks except for the final block will have one last
//   value which will not be used.  This isn't actually a waste since
//   if the next KV pair wouldn't fit within the block after the value
//   then the space after the final key to the next 1MB boundary would
//   just be padding anyway.
struct RangeFileWriter {
	RangeFileWriter(Reference<IBackupFile> file = Reference<IBackupFile>(), int blockSize = 0)
	  : file(file), blockSize(blockSize), blockEnd(0), fileVersion(BACKUP_AGENT_SNAPSHOT_FILE_VERSION) {}

	// Handles the first block and internal blocks.  Ends current block if needed.
	// The final flag is used in simulation to pad the file's final block to a whole block size
	ACTOR static Future<Void> newBlock(RangeFileWriter* self, int bytesNeeded, bool final = false) {
		// Write padding to finish current block if needed
		int bytesLeft = self->blockEnd - self->file->size();
		if (bytesLeft > 0) {
			state Value paddingFFs = makePadding(bytesLeft);
			wait(self->file->append(paddingFFs.begin(), bytesLeft));
		}

		if (final) {
			ASSERT(g_network->isSimulated());
			return Void();
		}

		// Set new blockEnd
		self->blockEnd += self->blockSize;

		// write Header
		wait(self->file->append((uint8_t*)&self->fileVersion, sizeof(self->fileVersion)));

		// If this is NOT the first block then write duplicate stuff needed from last block
		if (self->blockEnd > self->blockSize) {
			wait(self->file->appendStringRefWithLen(self->lastKey));
			wait(self->file->appendStringRefWithLen(self->lastKey));
			wait(self->file->appendStringRefWithLen(self->lastValue));
		}

		// There must now be room in the current block for bytesNeeded or the block size is too small
		if (self->file->size() + bytesNeeded > self->blockEnd)
			throw backup_bad_block_size();

		return Void();
	}

	// Used in simulation only to create backup file sizes which are an integer multiple of the block size
	Future<Void> padEnd() {
		ASSERT(g_network->isSimulated());
		if (file->size() > 0) {
			return newBlock(this, 0, true);
		}
		return Void();
	}

	// Ends the current block if necessary based on bytesNeeded.
	Future<Void> newBlockIfNeeded(int bytesNeeded) {
		if (file->size() + bytesNeeded > blockEnd)
			return newBlock(this, bytesNeeded);
		return Void();
	}

	// Start a new block if needed, then write the key and value
	ACTOR static Future<Void> writeKV_impl(RangeFileWriter* self, Key k, Value v) {
		int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
		wait(self->newBlockIfNeeded(toWrite));
		wait(self->file->appendStringRefWithLen(k));
		wait(self->file->appendStringRefWithLen(v));
		self->lastKey = k;
		self->lastValue = v;
		return Void();
	}

	Future<Void> writeKV(Key k, Value v) { return writeKV_impl(this, k, v); }

	// Write begin key or end key.
	ACTOR static Future<Void> writeKey_impl(RangeFileWriter* self, Key k) {
		int toWrite = sizeof(uint32_t) + k.size();
		wait(self->newBlockIfNeeded(toWrite));
		wait(self->file->appendStringRefWithLen(k));
		return Void();
	}

	Future<Void> writeKey(Key k) { return writeKey_impl(this, k); }

	Reference<IBackupFile> file;
	int blockSize;

private:
	int64_t blockEnd;
	uint32_t fileVersion;
	Key lastKey;
	Key lastValue;
};

static Reference<IBackupContainer> getBackupContainerWithProxy(Reference<IBackupContainer> _bc) {
	Reference<IBackupContainer> bc = IBackupContainer::openContainer(_bc->getURL(), fileBackupAgentProxy, {});
	return bc;
}

Standalone<VectorRef<KeyValueRef>> decodeRangeFileBlock(const Standalone<StringRef>& buf) {
	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	StringRefReader reader(buf, restore_corrupted_data());

	// Read header, currently only decoding BACKUP_AGENT_SNAPSHOT_FILE_VERSION
	if (reader.consume<int32_t>() != BACKUP_AGENT_SNAPSHOT_FILE_VERSION)
		throw restore_unsupported_file_version();

	// Read begin key, if this fails then block was invalid.
	uint32_t kLen = reader.consumeNetworkUInt32();
	const uint8_t* k = reader.consume(kLen);
	results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));

	// Read kv pairs and end key
	while (1) {
		// Read a key.
		kLen = reader.consumeNetworkUInt32();
		k = reader.consume(kLen);

		// If eof reached or first value len byte is 0xFF then a valid block end was reached.
		if (reader.eof() || *reader.rptr == 0xFF) {
			results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
			break;
		}

		// Read a value, which must exist or the block is invalid
		uint32_t vLen = reader.consumeNetworkUInt32();
		const uint8_t* v = reader.consume(vLen);
		results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));

		// If eof reached or first byte of next key len is 0xFF then a valid block end was reached.
		if (reader.eof() || *reader.rptr == 0xFF)
			break;
	}

	// Make sure any remaining bytes in the block are 0xFF
	for (auto b : reader.remainder())
		if (b != 0xFF)
			throw restore_corrupted_data_padding();

	return results;
}

ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file,
                                                                      int64_t offset,
                                                                      int len) {
	state Standalone<StringRef> buf = makeString(len);
	int rLen = wait(uncancellable(holdWhile(buf, file->read(mutateString(buf), len, offset))));
	if (rLen != len)
		throw restore_bad_read();

	simulateBlobFailure();

	try {
		return decodeRangeFileBlock(buf);
	} catch (Error& e) {
		TraceEvent(SevWarn, "FileRestoreDecodeRangeFileBlockFailed")
		    .error(e)
		    .detail("Filename", file->getFilename())
		    .detail("BlockOffset", offset)
		    .detail("BlockLen", len);
		throw;
	}
}

// Very simple format compared to KeyRange files.
// Header, [Key, Value]... Key len
struct LogFileWriter {
	LogFileWriter(Reference<IBackupFile> file = Reference<IBackupFile>(), int blockSize = 0)
	  : file(file), blockSize(blockSize), blockEnd(0) {}

	// Start a new block if needed, then write the key and value
	ACTOR static Future<Void> writeKV_impl(LogFileWriter* self, Key k, Value v) {
		// If key and value do not fit in this block, end it and start a new one
		int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
		if (self->file->size() + toWrite > self->blockEnd) {
			// Write padding if needed
			int bytesLeft = self->blockEnd - self->file->size();
			if (bytesLeft > 0) {
				state Value paddingFFs = makePadding(bytesLeft);
				wait(self->file->append(paddingFFs.begin(), bytesLeft));
			}

			// Set new blockEnd
			self->blockEnd += self->blockSize;

			// write the block header
			wait(self->file->append((uint8_t*)&BACKUP_AGENT_MLOG_VERSION, sizeof(BACKUP_AGENT_MLOG_VERSION)));
		}

		wait(self->file->appendStringRefWithLen(k));
		wait(self->file->appendStringRefWithLen(v));

		// At this point we should be in whatever the current block is or the block size is too small
		if (self->file->size() > self->blockEnd)
			throw backup_bad_block_size();

		return Void();
	}

	Future<Void> writeKV(Key k, Value v) { return writeKV_impl(this, k, v); }

	Reference<IBackupFile> file;
	int blockSize;

private:
	int64_t blockEnd;
};

Standalone<VectorRef<KeyValueRef>> decodeMutationLogFileBlock(const Standalone<StringRef>& buf) {
	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	StringRefReader reader(buf, restore_corrupted_data());

	// Read header, currently only decoding version BACKUP_AGENT_MLOG_VERSION
	if (reader.consume<int32_t>() != BACKUP_AGENT_MLOG_VERSION)
		throw restore_unsupported_file_version();

	// Read k/v pairs.  Block ends either at end of last value exactly or with 0xFF as first key len byte.
	while (1) {
		// If eof reached or first key len bytes is 0xFF then end of block was reached.
		if (reader.eof() || *reader.rptr == 0xFF)
			break;

		// Read key and value.  If anything throws then there is a problem.
		uint32_t kLen = reader.consumeNetworkUInt32();
		const uint8_t* k = reader.consume(kLen);
		uint32_t vLen = reader.consumeNetworkUInt32();
		const uint8_t* v = reader.consume(vLen);

		results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
	}

	// Make sure any remaining bytes in the block are 0xFF
	for (auto b : reader.remainder())
		if (b != 0xFF)
			throw restore_corrupted_data_padding();

	return results;
}

ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeMutationLogFileBlock(Reference<IAsyncFile> file,
                                                                            int64_t offset,
                                                                            int len) {
	state Standalone<StringRef> buf = makeString(len);
	int rLen = wait(file->read(mutateString(buf), len, offset));
	if (rLen != len)
		throw restore_bad_read();

	try {
		return decodeMutationLogFileBlock(buf);
	} catch (Error& e) {
		TraceEvent(SevWarn, "FileRestoreCorruptLogFileBlock")
		    .error(e)
		    .detail("Filename", file->getFilename())
		    .detail("BlockOffset", offset)
		    .detail("BlockLen", len);
		throw;
	}
}

ACTOR Future<Void> checkTaskVersion(Database cx, Reference<Task> task, StringRef name, uint32_t version) {
	uint32_t taskVersion = task->getVersion();
	if (taskVersion > version) {
		state Error err = task_invalid_version();

		TraceEvent(SevWarn, "BA_BackupRangeTaskFuncExecute")
		    .detail("TaskVersion", taskVersion)
		    .detail("Name", name)
		    .detail("Version", version);
		if (KeyBackedConfig::TaskParams.uid().exists(task)) {
			std::string msg = format("%s task version `%lu' is greater than supported version `%lu'",
			                         task->params[Task::reservedTaskParamKeyType].toString().c_str(),
			                         (unsigned long)taskVersion,
			                         (unsigned long)version);
			wait(BackupConfig(task).logError(cx, err, msg));
		}

		throw err;
	}

	return Void();
}

ACTOR static Future<Void> abortFiveZeroBackup(FileBackupAgent* backupAgent,
                                              Reference<ReadYourWritesTransaction> tr,
                                              std::string tagName) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Subspace tagNames = backupAgent->subspace.get(BackupAgentBase::keyTagName);
	Optional<Value> uidStr = wait(tr->get(tagNames.pack(Key(tagName))));
	if (!uidStr.present()) {
		TraceEvent(SevWarn, "FileBackupAbortIncompatibleBackup_TagNotFound").detail("TagName", tagName.c_str());
		return Void();
	}
	state UID uid = BinaryReader::fromStringRef<UID>(uidStr.get(), Unversioned());

	state Subspace statusSpace = backupAgent->subspace.get(BackupAgentBase::keyStates).get(uid.toString());
	state Subspace globalConfig = backupAgent->subspace.get(BackupAgentBase::keyConfig).get(uid.toString());
	state Subspace newConfigSpace =
	    uidPrefixKey(LiteralStringRef("uid->config/").withPrefix(fileBackupPrefixRange.begin), uid);

	Optional<Value> statusStr = wait(tr->get(statusSpace.pack(FileBackupAgent::keyStateStatus)));
	state EBackupState status =
	    !statusStr.present() ? EBackupState::STATE_NEVERRAN : BackupAgentBase::getState(statusStr.get().toString());

	TraceEvent(SevInfo, "FileBackupAbortIncompatibleBackup")
	    .detail("TagName", tagName.c_str())
	    .detail("Status", BackupAgentBase::getStateText(status));

	// Clear the folder id to prevent future tasks from executing at all
	tr->clear(singleKeyRange(StringRef(globalConfig.pack(FileBackupAgent::keyFolderId))));

	// Clear the mutations logging config and data
	Key configPath = uidPrefixKey(logRangesRange.begin, uid);
	Key logsPath = uidPrefixKey(backupLogKeys.begin, uid);
	tr->clear(KeyRangeRef(configPath, strinc(configPath)));
	tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));

	// Clear the new-style config space
	tr->clear(newConfigSpace.range());

	Key statusKey = StringRef(statusSpace.pack(FileBackupAgent::keyStateStatus));

	// Set old style state key to Aborted if it was Runnable
	if (backupAgent->isRunnable(status))
		tr->set(statusKey, StringRef(FileBackupAgent::getStateText(EBackupState::STATE_ABORTED)));

	return Void();
}

struct AbortFiveZeroBackupTask : TaskFuncBase {
	static StringRef name;
	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state FileBackupAgent backupAgent;
		state std::string tagName = task->params[BackupAgentBase::keyConfigBackupTag].toString();

		TEST(true); // Canceling old backup task

		TraceEvent(SevInfo, "FileBackupCancelOldTask")
		    .detail("Task", task->params[Task::reservedTaskParamKeyType])
		    .detail("TagName", tagName);
		wait(abortFiveZeroBackup(&backupAgent, tr, tagName));

		wait(taskBucket->finish(tr, task));
		return Void();
	}

	StringRef getName() const override {
		TraceEvent(SevError, "FileBackupError")
		    .detail("Cause", "AbortFiveZeroBackupTaskFunc::name() should never be called");
		ASSERT(false);
		return StringRef();
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Future<Void>(Void());
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef AbortFiveZeroBackupTask::name = LiteralStringRef("abort_legacy_backup");
REGISTER_TASKFUNC(AbortFiveZeroBackupTask);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_diff_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_log_range);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_range);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_backup_restorable);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_finish_full_backup);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_finished_full_backup);
REGISTER_TASKFUNC_ALIAS(AbortFiveZeroBackupTask, file_start_full_backup);

ACTOR static Future<Void> abortFiveOneBackup(FileBackupAgent* backupAgent,
                                             Reference<ReadYourWritesTransaction> tr,
                                             std::string tagName) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state KeyBackedTag tag = makeBackupTag(tagName);
	state UidAndAbortedFlagT current = wait(tag.getOrThrow(tr, Snapshot::False, backup_unneeded()));

	state BackupConfig config(current.first);
	EBackupState status = wait(config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN));

	if (!backupAgent->isRunnable(status)) {
		throw backup_unneeded();
	}

	TraceEvent(SevInfo, "FBA_AbortFileOneBackup")
	    .detail("TagName", tagName.c_str())
	    .detail("Status", BackupAgentBase::getStateText(status));

	// Cancel backup task through tag
	wait(tag.cancel(tr));

	Key configPath = uidPrefixKey(logRangesRange.begin, config.getUid());
	Key logsPath = uidPrefixKey(backupLogKeys.begin, config.getUid());

	tr->clear(KeyRangeRef(configPath, strinc(configPath)));
	tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));

	config.stateEnum().set(tr, EBackupState::STATE_ABORTED);

	return Void();
}

struct AbortFiveOneBackupTask : TaskFuncBase {
	static StringRef name;
	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state FileBackupAgent backupAgent;
		state BackupConfig config(task);
		state std::string tagName = wait(config.tag().getOrThrow(tr));

		TEST(true); // Canceling 5.1 backup task

		TraceEvent(SevInfo, "FileBackupCancelFiveOneTask")
		    .detail("Task", task->params[Task::reservedTaskParamKeyType])
		    .detail("TagName", tagName);
		wait(abortFiveOneBackup(&backupAgent, tr, tagName));

		wait(taskBucket->finish(tr, task));
		return Void();
	}

	StringRef getName() const override {
		TraceEvent(SevError, "FileBackupError")
		    .detail("Cause", "AbortFiveOneBackupTaskFunc::name() should never be called");
		ASSERT(false);
		return StringRef();
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Future<Void>(Void());
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef AbortFiveOneBackupTask::name = LiteralStringRef("abort_legacy_backup_5.2");
REGISTER_TASKFUNC(AbortFiveOneBackupTask);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_write_range);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_dispatch_ranges);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_write_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_erase_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_dispatch_logs);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_finished);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_write_snapshot_manifest);
REGISTER_TASKFUNC_ALIAS(AbortFiveOneBackupTask, file_backup_start);

std::function<void(Reference<Task>)> NOP_SETUP_TASK_FN = [](Reference<Task> task) { /* NOP */ };
ACTOR static Future<Key> addBackupTask(StringRef name,
                                       uint32_t version,
                                       Reference<ReadYourWritesTransaction> tr,
                                       Reference<TaskBucket> taskBucket,
                                       TaskCompletionKey completionKey,
                                       BackupConfig config,
                                       Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
                                       std::function<void(Reference<Task>)> setupTaskFn = NOP_SETUP_TASK_FN,
                                       int priority = 0,
                                       SetValidation setValidation = SetValidation::True) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	Key doneKey = wait(completionKey.get(tr, taskBucket));
	state Reference<Task> task(new Task(name, version, doneKey, priority));

	// Bind backup config to new task
	wait(config.toTask(tr, task, setValidation));

	// Set task specific params
	setupTaskFn(task);

	if (!waitFor) {
		return taskBucket->addTask(tr, task);
	}
	wait(waitFor->onSetAddTask(tr, taskBucket, task));

	return LiteralStringRef("OnSetAddTask");
}

// Clears the backup ID from "backupStartedKey" to pause backup workers.
ACTOR static Future<Void> clearBackupStartID(Reference<ReadYourWritesTransaction> tr, UID backupUid) {
	// If backup worker is not enabled, exit early.
	Optional<Value> started = wait(tr->get(backupStartedKey));
	std::vector<std::pair<UID, Version>> ids;
	if (started.present()) {
		ids = decodeBackupStartedValue(started.get());
	}
	auto it =
	    std::find_if(ids.begin(), ids.end(), [=](const std::pair<UID, Version>& p) { return p.first == backupUid; });
	if (it != ids.end()) {
		ids.erase(it);
	}

	if (ids.empty()) {
		TraceEvent("ClearBackup").detail("BackupID", backupUid);
		tr->clear(backupStartedKey);
	} else {
		tr->set(backupStartedKey, encodeBackupStartedValue(ids));
	}
	return Void();
}

// Backup and Restore taskFunc definitions will inherit from one of the following classes which
// servers to catch and log to the appropriate config any error that execute/finish didn't catch and log.
struct RestoreTaskFuncBase : TaskFuncBase {
	Future<Void> handleError(Database cx, Reference<Task> task, Error const& error) final {
		return RestoreConfig(task).logError(
		    cx,
		    error,
		    format("'%s' on '%s'", error.what(), task->params[Task::reservedTaskParamKeyType].printable().c_str()));
	}
	virtual std::string toString(Reference<Task> task) const { return ""; }
};

struct BackupTaskFuncBase : TaskFuncBase {
	Future<Void> handleError(Database cx, Reference<Task> task, Error const& error) final {
		return BackupConfig(task).logError(
		    cx,
		    error,
		    format("'%s' on '%s'", error.what(), task->params[Task::reservedTaskParamKeyType].printable().c_str()));
	}
	virtual std::string toString(Reference<Task> task) const { return ""; }
};

ACTOR static Future<Standalone<VectorRef<KeyRef>>> getBlockOfShards(Reference<ReadYourWritesTransaction> tr,
                                                                    Key beginKey,
                                                                    Key endKey,
                                                                    int limit) {

	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	state Standalone<VectorRef<KeyRef>> results;
	RangeResult values = wait(tr->getRange(
	    KeyRangeRef(keyAfter(beginKey.withPrefix(keyServersPrefix)), endKey.withPrefix(keyServersPrefix)), limit));

	for (auto& s : values) {
		KeyRef k = s.key.removePrefix(keyServersPrefix);
		results.push_back_deep(results.arena(), k);
	}

	return results;
}

struct BackupRangeTaskFunc : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Key> beginKey() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<Key> endKey() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<bool> addBackupRangeTasks() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	std::string toString(Reference<Task> task) const override {
		return format("beginKey '%s' endKey '%s' addTasks %d",
		              Params.beginKey().get(task).printable().c_str(),
		              Params.endKey().get(task).printable().c_str(),
		              Params.addBackupRangeTasks().get(task));
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	// Finish (which flushes/syncs) the file, and then in a single transaction, make some range backup progress durable.
	// This means:
	//  - increment the backup config's range bytes written
	//  - update the range file map
	//  - update the task begin key
	//  - save/extend the task with the new params
	// Returns whether or not the caller should continue executing the task.
	ACTOR static Future<bool> finishRangeFile(Reference<IBackupFile> file,
	                                          Database cx,
	                                          Reference<Task> task,
	                                          Reference<TaskBucket> taskBucket,
	                                          KeyRange range,
	                                          Version version) {
		wait(file->finish());

		// Ignore empty ranges.
		if (range.empty())
			return false;

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state BackupConfig backup(task);
		state bool usedFile = false;

		// Avoid unnecessary conflict by prevent taskbucket's automatic timeout extension
		// because the following transaction loop extends and updates the task.
		wait(task->extendMutex.take());
		state FlowLock::Releaser releaser(task->extendMutex, 1);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				// Update the start key of the task so if this transaction completes but the task then fails
				// when it is restarted it will continue where this execution left off.
				Params.beginKey().set(task, range.end);

				// Save and extend the task with the new begin parameter
				state Version newTimeout = wait(taskBucket->extendTimeout(tr, task, UpdateParams::True));

				// Update the range bytes written in the backup config
				backup.rangeBytesWritten().atomicOp(tr, file->size(), MutationRef::AddValue);
				backup.snapshotRangeFileCount().atomicOp(tr, 1, MutationRef::AddValue);

				// See if there is already a file for this key which has an earlier begin, update the map if not.
				Optional<BackupConfig::RangeSlice> s = wait(backup.snapshotRangeFileMap().get(tr, range.end));
				if (!s.present() || s.get().begin >= range.begin) {
					backup.snapshotRangeFileMap().set(
					    tr, range.end, { range.begin, version, file->getFileName(), file->size() });
					usedFile = true;
				}

				wait(tr->commit());
				task->timeoutVersion = newTimeout;
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		return usedFile;
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 int priority,
	                                 Key begin,
	                                 Key end,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
	                                 Version scheduledVersion = invalidVersion) {
		Key key = wait(addBackupTask(
		    BackupRangeTaskFunc::name,
		    BackupRangeTaskFunc::version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(parentTask),
		    waitFor,
		    [=](Reference<Task> task) {
			    Params.beginKey().set(task, begin);
			    Params.endKey().set(task, end);
			    Params.addBackupRangeTasks().set(task, false);
			    if (scheduledVersion != invalidVersion)
				    ReservedTaskParams::scheduledVersion().set(task, scheduledVersion);
		    },
		    priority));
		return key;
	}

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

		wait(checkTaskVersion(cx, task, BackupRangeTaskFunc::name, BackupRangeTaskFunc::version));

		state Key beginKey = Params.beginKey().get(task);
		state Key endKey = Params.endKey().get(task);

		TraceEvent("FileBackupRangeStart")
		    .suppressFor(60)
		    .detail("BackupUID", BackupConfig(task).getUid())
		    .detail("BeginKey", Params.beginKey().get(task).printable())
		    .detail("EndKey", Params.endKey().get(task).printable())
		    .detail("TaskKey", task->key.printable());

		// When a key range task saves the last chunk of progress and then the executor dies, when the task continues
		// its beginKey and endKey will be equal but there is no work to be done.
		if (beginKey == endKey)
			return Void();

		// Find out if there is a shard boundary in(beginKey, endKey)
		Standalone<VectorRef<KeyRef>> keys = wait(runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return getBlockOfShards(tr, beginKey, endKey, 1); }));
		if (keys.size() > 0) {
			Params.addBackupRangeTasks().set(task, true);
			return Void();
		}

		// Read everything from beginKey to endKey, write it to an output file, run the output file processor, and
		// then set on_done. If we are still writing after X seconds, end the output file and insert a new backup_range
		// task for the remainder.
		state Reference<IBackupFile> outFile;
		state Version outVersion = invalidVersion;
		state Key lastKey;

		// retrieve kvData
		state PromiseStream<RangeResultWithVersion> results;

		state Future<Void> rc = readCommitted(cx,
		                                      results,
		                                      lock,
		                                      KeyRangeRef(beginKey, endKey),
		                                      Terminator::True,
		                                      AccessSystemKeys::True,
		                                      LockAware::True);
		state RangeFileWriter rangeFile;
		state BackupConfig backup(task);

		// Don't need to check keepRunning(task) here because we will do that while finishing each output file, but if
		// bc is false then clearly the backup is no longer in progress
		Reference<IBackupContainer> _bc = wait(backup.backupContainer().getD(cx));
		if (!_bc) {
			return Void();
		}
		state Reference<IBackupContainer> bc = getBackupContainerWithProxy(_bc);
		state bool done = false;
		state int64_t nrKeys = 0;

		loop {
			state RangeResultWithVersion values;
			try {
				RangeResultWithVersion _values = waitNext(results.getFuture());
				values = _values;
				lock->release(values.first.expectedSize());
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream)
					done = true;
				else
					throw;
			}

			// If we've seen a new read version OR hit the end of the stream, then if we were writing a file finish it.
			if (values.second != outVersion || done) {
				if (outFile) {
					TEST(outVersion != invalidVersion); // Backup range task wrote multiple versions
					state Key nextKey = done ? endKey : keyAfter(lastKey);
					wait(rangeFile.writeKey(nextKey));

					if (BUGGIFY) {
						wait(rangeFile.padEnd());
					}

					bool usedFile = wait(
					    finishRangeFile(outFile, cx, task, taskBucket, KeyRangeRef(beginKey, nextKey), outVersion));
					TraceEvent("FileBackupWroteRangeFile")
					    .suppressFor(60)
					    .detail("BackupUID", backup.getUid())
					    .detail("Size", outFile->size())
					    .detail("Keys", nrKeys)
					    .detail("ReadVersion", outVersion)
					    .detail("BeginKey", beginKey.printable())
					    .detail("EndKey", nextKey.printable())
					    .detail("AddedFileToMap", usedFile);

					nrKeys = 0;
					beginKey = nextKey;
				}

				if (done)
					return Void();

				// Start writing a new file after verifying this task should keep running as of a new read version
				// (which must be >= outVersion)
				outVersion = values.second;
				// block size must be at least large enough for 3 max size keys and 2 max size values + overhead so 250k
				// conservatively.
				state int blockSize =
				    BUGGIFY ? deterministicRandom()->randomInt(250e3, 4e6) : CLIENT_KNOBS->BACKUP_RANGEFILE_BLOCK_SIZE;
				state Version snapshotBeginVersion;
				state int64_t snapshotRangeFileCount;

				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				loop {
					try {
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);

						wait(taskBucket->keepRunning(tr, task) &&
						     storeOrThrow(snapshotBeginVersion, backup.snapshotBeginVersion().get(tr)) &&
						     store(snapshotRangeFileCount, backup.snapshotRangeFileCount().getD(tr)));

						break;
					} catch (Error& e) {
						wait(tr->onError(e));
					}
				}

				Reference<IBackupFile> f =
				    wait(bc->writeRangeFile(snapshotBeginVersion, snapshotRangeFileCount, outVersion, blockSize));
				outFile = f;

				// Initialize range file writer and write begin key
				rangeFile = RangeFileWriter(outFile, blockSize);
				wait(rangeFile.writeKey(beginKey));
			}

			// write kvData to file, update lastKey and key count
			if (values.first.size() != 0) {
				state size_t i = 0;
				for (; i < values.first.size(); ++i) {
					wait(rangeFile.writeKV(values.first[i].key, values.first[i].value));
				}
				lastKey = values.first.back().key;
				nrKeys += values.first.size();
			}
		}
	}

	ACTOR static Future<Void> startBackupRangeInternal(Reference<ReadYourWritesTransaction> tr,
	                                                   Reference<TaskBucket> taskBucket,
	                                                   Reference<FutureBucket> futureBucket,
	                                                   Reference<Task> task,
	                                                   Reference<TaskFuture> onDone) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Key nextKey = Params.beginKey().get(task);
		state Key endKey = Params.endKey().get(task);

		state Standalone<VectorRef<KeyRef>> keys =
		    wait(getBlockOfShards(tr, nextKey, endKey, CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT));

		std::vector<Future<Key>> addTaskVector;
		for (int idx = 0; idx < keys.size(); ++idx) {
			if (nextKey != keys[idx]) {
				addTaskVector.push_back(addTask(tr,
				                                taskBucket,
				                                task,
				                                task->getPriority(),
				                                nextKey,
				                                keys[idx],
				                                TaskCompletionKey::joinWith(onDone)));
				TraceEvent("FileBackupRangeSplit")
				    .suppressFor(60)
				    .detail("BackupUID", BackupConfig(task).getUid())
				    .detail("BeginKey", Params.beginKey().get(task).printable())
				    .detail("EndKey", Params.endKey().get(task).printable())
				    .detail("SliceBeginKey", nextKey.printable())
				    .detail("SliceEndKey", keys[idx].printable());
			}
			nextKey = keys[idx];
		}

		wait(waitForAll(addTaskVector));

		if (nextKey != endKey) {
			// Add task to cover nextKey to the end, using the priority of the current task
			wait(success(addTask(tr,
			                     taskBucket,
			                     task,
			                     task->getPriority(),
			                     nextKey,
			                     endKey,
			                     TaskCompletionKey::joinWith(onDone),
			                     Reference<TaskFuture>(),
			                     task->getPriority())));
		}

		return Void();
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		if (Params.addBackupRangeTasks().get(task)) {
			wait(startBackupRangeInternal(tr, taskBucket, futureBucket, task, taskFuture));
		} else {
			wait(taskFuture->set(tr, taskBucket));
		}

		wait(taskBucket->finish(tr, task));

		TraceEvent("FileBackupRangeFinish")
		    .suppressFor(60)
		    .detail("BackupUID", BackupConfig(task).getUid())
		    .detail("BeginKey", Params.beginKey().get(task).printable())
		    .detail("EndKey", Params.endKey().get(task).printable())
		    .detail("TaskKey", task->key.printable());

		return Void();
	}
};
StringRef BackupRangeTaskFunc::name = LiteralStringRef("file_backup_write_range_5.2");
REGISTER_TASKFUNC(BackupRangeTaskFunc);

struct BackupSnapshotDispatchTask : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		// Set by Execute, used by Finish
		static TaskParam<int64_t> shardsBehind() { return LiteralStringRef(__FUNCTION__); }
		// Set by Execute, used by Finish
		static TaskParam<bool> snapshotFinished() { return LiteralStringRef(__FUNCTION__); }
		// Set by Execute, used by Finish
		static TaskParam<Version> nextDispatchVersion() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 int priority,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
	                                 Version scheduledVersion = invalidVersion) {
		Key key = wait(addBackupTask(
		    name,
		    version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(parentTask),
		    waitFor,
		    [=](Reference<Task> task) {
			    if (scheduledVersion != invalidVersion)
				    ReservedTaskParams::scheduledVersion().set(task, scheduledVersion);
		    },
		    priority));
		return key;
	}

	enum DispatchState { SKIP = 0, DONE = 1, NOT_DONE_MIN = 2 };

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));
		wait(checkTaskVersion(cx, task, name, version));

		state double startTime = timer();
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

		// The shard map will use 3 values classes.  Exactly SKIP, exactly DONE, then any number >= NOT_DONE_MIN which
		// will mean not done. This is to enable an efficient coalesce() call to squash adjacent ranges which are not
		// yet finished to enable efficiently finding random database shards which are not done.
		state int notDoneSequence = NOT_DONE_MIN;
		state KeyRangeMap<int> shardMap(notDoneSequence++, normalKeys.end);
		state Key beginKey = normalKeys.begin;

		// Read all shard boundaries and add them to the map
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state Future<Standalone<VectorRef<KeyRef>>> shardBoundaries =
				    getBlockOfShards(tr, beginKey, normalKeys.end, CLIENT_KNOBS->TOO_MANY);
				wait(success(shardBoundaries) && taskBucket->keepRunning(tr, task));

				if (shardBoundaries.get().size() == 0)
					break;

				for (auto& boundary : shardBoundaries.get()) {
					shardMap.rawInsert(boundary, notDoneSequence++);
				}

				beginKey = keyAfter(shardBoundaries.get().back());
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		// Read required stuff from backup config
		state BackupConfig config(task);
		state Version recentReadVersion;
		state Version snapshotBeginVersion;
		state Version snapshotTargetEndVersion;
		state int64_t snapshotIntervalSeconds;
		state Optional<Version> latestSnapshotEndVersion;
		state std::vector<KeyRange> backupRanges;
		state Optional<Key> snapshotBatchFutureKey;
		state Reference<TaskFuture> snapshotBatchFuture;
		state Optional<int64_t> snapshotBatchSize;

		tr->reset();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				wait(store(snapshotBeginVersion, config.snapshotBeginVersion().getOrThrow(tr)) &&
				     store(snapshotTargetEndVersion, config.snapshotTargetEndVersion().getOrThrow(tr)) &&
				     store(backupRanges, config.backupRanges().getOrThrow(tr)) &&
				     store(snapshotIntervalSeconds, config.snapshotIntervalSeconds().getOrThrow(tr))
				     // The next two parameters are optional
				     && store(snapshotBatchFutureKey, config.snapshotBatchFuture().get(tr)) &&
				     store(snapshotBatchSize, config.snapshotBatchSize().get(tr)) &&
				     store(latestSnapshotEndVersion, config.latestSnapshotEndVersion().get(tr)) &&
				     store(recentReadVersion, tr->getReadVersion()) && taskBucket->keepRunning(tr, task));

				// If the snapshot batch future key does not exist, this is the first execution of this dispatch task so
				//    - create and set the snapshot batch future key
				//    - initialize the batch size to 0
				//    - initialize the target snapshot end version if it is not yet set
				//    - commit
				if (!snapshotBatchFutureKey.present()) {
					snapshotBatchFuture = futureBucket->future(tr);
					config.snapshotBatchFuture().set(tr, snapshotBatchFuture->pack());
					snapshotBatchSize = 0;
					config.snapshotBatchSize().set(tr, snapshotBatchSize.get());

					// The dispatch of this batch can take multiple separate executions if the executor fails
					// so store a completion key for the dispatch finish() to set when dispatching the batch is done.
					state TaskCompletionKey dispatchCompletionKey = TaskCompletionKey::joinWith(snapshotBatchFuture);
					// this is a bad hack - but flow doesn't work well with lambda functions and caputring
					// state variables...
					auto cfg = &config;
					auto tx = &tr;
					wait(map(dispatchCompletionKey.get(tr, taskBucket), [cfg, tx](Key const& k) {
						cfg->snapshotBatchDispatchDoneKey().set(*tx, k);
						return Void();
					}));
					wait(tr->commit());
				} else {
					ASSERT(snapshotBatchSize.present());
					// Batch future key exists in the config so create future from it
					snapshotBatchFuture = makeReference<TaskFuture>(futureBucket, snapshotBatchFutureKey.get());
				}

				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		// Read all dispatched ranges
		state std::vector<std::pair<Key, bool>> dispatchBoundaries;
		tr->reset();
		beginKey = normalKeys.begin;
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state Future<std::vector<std::pair<Key, bool>>> bounds = config.snapshotRangeDispatchMap().getRange(
				    tr, beginKey, keyAfter(normalKeys.end), CLIENT_KNOBS->TOO_MANY);
				wait(success(bounds) && taskBucket->keepRunning(tr, task) &&
				     store(recentReadVersion, tr->getReadVersion()));

				if (bounds.get().empty())
					break;

				dispatchBoundaries.reserve(dispatchBoundaries.size() + bounds.get().size());
				dispatchBoundaries.insert(dispatchBoundaries.end(), bounds.get().begin(), bounds.get().end());

				beginKey = keyAfter(bounds.get().back().first);
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		// The next few sections involve combining the results above.  Yields are used after operations
		// that could have operated on many thousands of things and in loops which could have many
		// thousands of iterations.
		// Declare some common iterators which must be state vars and will be used multiple times.
		state int i;
		state RangeMap<Key, int, KeyRangeRef>::iterator iShard;
		state RangeMap<Key, int, KeyRangeRef>::iterator iShardEnd;

		// Set anything inside a dispatched range to DONE.
		// Also ensure that the boundary value are true, false, [true, false]...
		if (dispatchBoundaries.size() > 0) {
			state bool lastValue = false;
			state Key lastKey;
			for (i = 0; i < dispatchBoundaries.size(); ++i) {
				const std::pair<Key, bool>& boundary = dispatchBoundaries[i];

				// Values must alternate
				ASSERT(boundary.second == !lastValue);

				// If this was the end of a dispatched range
				if (!boundary.second) {
					// Ensure that the dispatched boundaries exist AND set all shard ranges in the dispatched range to
					// DONE.
					RangeMap<Key, int, KeyRangeRef>::Ranges shardRanges =
					    shardMap.modify(KeyRangeRef(lastKey, boundary.first));
					iShard = shardRanges.begin();
					iShardEnd = shardRanges.end();
					for (; iShard != iShardEnd; ++iShard) {
						iShard->value() = DONE;
						wait(yield());
					}
				}
				lastValue = dispatchBoundaries[i].second;
				lastKey = dispatchBoundaries[i].first;

				wait(yield());
			}
			ASSERT(lastValue == false);
		}

		// Set anything outside the backup ranges to SKIP.  We can use insert() here instead of modify()
		// because it's OK to delete shard boundaries in the skipped ranges.
		if (backupRanges.size() > 0) {
			shardMap.insert(KeyRangeRef(normalKeys.begin, backupRanges.front().begin), SKIP);
			wait(yield());

			for (i = 0; i < backupRanges.size() - 1; ++i) {
				shardMap.insert(KeyRangeRef(backupRanges[i].end, backupRanges[i + 1].begin), SKIP);
				wait(yield());
			}

			shardMap.insert(KeyRangeRef(backupRanges.back().end, normalKeys.end), SKIP);
			wait(yield());
		}

		state int countShardsDone = 0;
		state int countShardsNotDone = 0;

		// Scan through the shard map, counting the DONE and NOT_DONE shards.
		RangeMap<Key, int, KeyRangeRef>::Ranges shardRanges = shardMap.ranges();
		iShard = shardRanges.begin();
		iShardEnd = shardRanges.end();
		for (; iShard != iShardEnd; ++iShard) {
			if (iShard->value() == DONE) {
				++countShardsDone;
			} else if (iShard->value() >= NOT_DONE_MIN)
				++countShardsNotDone;

			wait(yield());
		}

		// Coalesce the shard map to make random selection below more efficient.
		shardMap.coalesce(normalKeys);
		wait(yield());

		// In this context "all" refers to all of the shards relevant for this particular backup
		state int countAllShards = countShardsDone + countShardsNotDone;

		if (countShardsNotDone == 0) {
			TraceEvent("FileBackupSnapshotDispatchFinished")
			    .detail("BackupUID", config.getUid())
			    .detail("AllShards", countAllShards)
			    .detail("ShardsDone", countShardsDone)
			    .detail("ShardsNotDone", countShardsNotDone)
			    .detail("SnapshotBeginVersion", snapshotBeginVersion)
			    .detail("SnapshotTargetEndVersion", snapshotTargetEndVersion)
			    .detail("CurrentVersion", recentReadVersion)
			    .detail("SnapshotIntervalSeconds", snapshotIntervalSeconds);
			Params.snapshotFinished().set(task, true);
			return Void();
		}

		// Decide when the next snapshot dispatch should run.
		state Version nextDispatchVersion;

		// In simulation, use snapshot interval / 5 to ensure multiple dispatches run
		// Otherwise, use the knob for the number of seconds between snapshot dispatch tasks.
		if (g_network->isSimulated())
			nextDispatchVersion =
			    recentReadVersion + CLIENT_KNOBS->CORE_VERSIONSPERSECOND * (snapshotIntervalSeconds / 5.0);
		else
			nextDispatchVersion = recentReadVersion + CLIENT_KNOBS->CORE_VERSIONSPERSECOND *
			                                              CLIENT_KNOBS->BACKUP_SNAPSHOT_DISPATCH_INTERVAL_SEC;

		// If nextDispatchVersion is greater than snapshotTargetEndVersion (which could be in the past) then just use
		// the greater of recentReadVersion or snapshotTargetEndVersion.  Any range tasks created in this dispatch will
		// be scheduled at a random time between recentReadVersion and nextDispatchVersion,
		// so nextDispatchVersion shouldn't be less than recentReadVersion.
		if (nextDispatchVersion > snapshotTargetEndVersion)
			nextDispatchVersion = std::max(recentReadVersion, snapshotTargetEndVersion);

		Params.nextDispatchVersion().set(task, nextDispatchVersion);

		// Calculate number of shards that should be done before the next interval end
		// timeElapsed is between 0 and 1 and represents what portion of the shards we should have completed by now
		double timeElapsed;
		Version snapshotScheduledVersionInterval = snapshotTargetEndVersion - snapshotBeginVersion;
		if (snapshotTargetEndVersion > snapshotBeginVersion)
			timeElapsed = std::min(
			    1.0, (double)(nextDispatchVersion - snapshotBeginVersion) / (snapshotScheduledVersionInterval));
		else
			timeElapsed = 1.0;

		state int countExpectedShardsDone = countAllShards * timeElapsed;
		state int countShardsToDispatch = std::max<int>(0, countExpectedShardsDone - countShardsDone);

		// Calculate the number of shards that would have been dispatched by a normal (on-schedule)
		// BackupSnapshotDispatchTask given the dispatch window and the start and expected-end versions of the current
		// snapshot.
		int64_t dispatchWindow = nextDispatchVersion - recentReadVersion;

		// If the scheduled snapshot interval is 0 (such as for initial, as-fast-as-possible snapshot) then all shards
		// are considered late
		int countShardsExpectedPerNormalWindow;
		if (snapshotScheduledVersionInterval == 0) {
			countShardsExpectedPerNormalWindow = 0;
		} else {
			// A dispatchWindow of 0 means the target end version is <= now which also results in all shards being
			// considered late
			countShardsExpectedPerNormalWindow =
			    (double(dispatchWindow) / snapshotScheduledVersionInterval) * countAllShards;
		}

		// The number of shards 'behind' the snapshot is the count of how may additional shards beyond normal are being
		// dispatched, if any.
		int countShardsBehind =
		    std::max<int64_t>(0, countShardsToDispatch + snapshotBatchSize.get() - countShardsExpectedPerNormalWindow);
		Params.shardsBehind().set(task, countShardsBehind);

		TraceEvent("FileBackupSnapshotDispatchStats")
		    .detail("BackupUID", config.getUid())
		    .detail("AllShards", countAllShards)
		    .detail("ShardsDone", countShardsDone)
		    .detail("ShardsNotDone", countShardsNotDone)
		    .detail("ExpectedShardsDone", countExpectedShardsDone)
		    .detail("ShardsToDispatch", countShardsToDispatch)
		    .detail("ShardsBehind", countShardsBehind)
		    .detail("SnapshotBeginVersion", snapshotBeginVersion)
		    .detail("SnapshotTargetEndVersion", snapshotTargetEndVersion)
		    .detail("NextDispatchVersion", nextDispatchVersion)
		    .detail("CurrentVersion", recentReadVersion)
		    .detail("TimeElapsed", timeElapsed)
		    .detail("SnapshotIntervalSeconds", snapshotIntervalSeconds);

		// Dispatch random shards to catch up to the expected progress
		while (countShardsToDispatch > 0) {
			// First select ranges to add
			state std::vector<KeyRange> rangesToAdd;

			// Limit number of tasks added per transaction
			int taskBatchSize = BUGGIFY ? deterministicRandom()->randomInt(1, countShardsToDispatch + 1)
			                            : CLIENT_KNOBS->BACKUP_DISPATCH_ADDTASK_SIZE;
			int added = 0;

			while (countShardsToDispatch > 0 && added < taskBatchSize && shardMap.size() > 0) {
				// Get a random range.
				auto it = shardMap.randomRange();
				// Find a NOT_DONE range and add it to rangesToAdd
				while (1) {
					if (it->value() >= NOT_DONE_MIN) {
						rangesToAdd.push_back(it->range());
						it->value() = DONE;
						shardMap.coalesce(Key(it->begin()));
						++added;
						++countShardsDone;
						--countShardsToDispatch;
						--countShardsNotDone;
						break;
					}
					if (it->end() == shardMap.mapEnd)
						break;
					++it;
				}
			}

			state int64_t oldBatchSize = snapshotBatchSize.get();
			state int64_t newBatchSize = oldBatchSize + rangesToAdd.size();

			// Now add the selected ranges in a single transaction.
			tr->reset();
			loop {
				try {
					TraceEvent("FileBackupSnapshotDispatchAddingTasks")
					    .suppressFor(2)
					    .detail("TasksToAdd", rangesToAdd.size())
					    .detail("NewBatchSize", newBatchSize);

					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					// For each range, make sure it isn't set in the dispatched range map.
					state std::vector<Future<Optional<bool>>> beginReads;
					state std::vector<Future<Optional<bool>>> endReads;

					for (auto& range : rangesToAdd) {
						beginReads.push_back(config.snapshotRangeDispatchMap().get(tr, range.begin));
						endReads.push_back(config.snapshotRangeDispatchMap().get(tr, range.end));
					}

					wait(store(snapshotBatchSize.get(), config.snapshotBatchSize().getOrThrow(tr)) &&
					     waitForAll(beginReads) && waitForAll(endReads) && taskBucket->keepRunning(tr, task));

					// Snapshot batch size should be either oldBatchSize or newBatchSize. If new, this transaction is
					// already done.
					if (snapshotBatchSize.get() == newBatchSize) {
						break;
					} else {
						ASSERT(snapshotBatchSize.get() == oldBatchSize);
						config.snapshotBatchSize().set(tr, newBatchSize);
						snapshotBatchSize = newBatchSize;
						config.snapshotDispatchLastShardsBehind().set(tr, Params.shardsBehind().get(task));
						config.snapshotDispatchLastVersion().set(tr, tr->getReadVersion().get());
					}

					state std::vector<Future<Void>> addTaskFutures;

					for (i = 0; i < beginReads.size(); ++i) {
						KeyRange& range = rangesToAdd[i];

						// This loop might have made changes to begin or end boundaries in a prior
						// iteration.  If so, the updated values exist in the RYW cache so re-read both entries.
						Optional<bool> beginValue = config.snapshotRangeDispatchMap().get(tr, range.begin).get();
						Optional<bool> endValue = config.snapshotRangeDispatchMap().get(tr, range.end).get();

						ASSERT(!beginValue.present() || !endValue.present() || beginValue != endValue);

						// If begin is present, it must be a range end so value must be false
						// If end is present, it must be a range begin so value must be true
						if ((!beginValue.present() || !beginValue.get()) && (!endValue.present() || endValue.get())) {
							if (beginValue.present()) {
								config.snapshotRangeDispatchMap().erase(tr, range.begin);
							} else {
								config.snapshotRangeDispatchMap().set(tr, range.begin, true);
							}
							if (endValue.present()) {
								config.snapshotRangeDispatchMap().erase(tr, range.end);
							} else {
								config.snapshotRangeDispatchMap().set(tr, range.end, false);
							}

							Version scheduledVersion = invalidVersion;
							// If the next dispatch version is in the future, choose a random version at which to start
							// the new task.
							if (nextDispatchVersion > recentReadVersion)
								scheduledVersion = recentReadVersion + deterministicRandom()->random01() *
								                                           (nextDispatchVersion - recentReadVersion);

							// Range tasks during the initial snapshot should run at a higher priority
							int priority = latestSnapshotEndVersion.present() ? 0 : 1;
							addTaskFutures.push_back(
							    success(BackupRangeTaskFunc::addTask(tr,
							                                         taskBucket,
							                                         task,
							                                         priority,
							                                         range.begin,
							                                         range.end,
							                                         TaskCompletionKey::joinWith(snapshotBatchFuture),
							                                         Reference<TaskFuture>(),
							                                         scheduledVersion)));

							TraceEvent("FileBackupSnapshotRangeDispatched")
							    .suppressFor(2)
							    .detail("BackupUID", config.getUid())
							    .detail("CurrentVersion", recentReadVersion)
							    .detail("ScheduledVersion", scheduledVersion)
							    .detail("BeginKey", range.begin.printable())
							    .detail("EndKey", range.end.printable());
						} else {
							// This shouldn't happen because if the transaction was already done or if another execution
							// of this task is making progress it should have been detected above.
							ASSERT(false);
						}
					}

					wait(waitForAll(addTaskFutures));
					wait(tr->commit());
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}

		if (countShardsNotDone == 0) {
			TraceEvent("FileBackupSnapshotDispatchFinished")
			    .detail("BackupUID", config.getUid())
			    .detail("AllShards", countAllShards)
			    .detail("ShardsDone", countShardsDone)
			    .detail("ShardsNotDone", countShardsNotDone)
			    .detail("SnapshotBeginVersion", snapshotBeginVersion)
			    .detail("SnapshotTargetEndVersion", snapshotTargetEndVersion)
			    .detail("CurrentVersion", recentReadVersion)
			    .detail("SnapshotIntervalSeconds", snapshotIntervalSeconds)
			    .detail("DispatchTimeSeconds", timer() - startTime);
			Params.snapshotFinished().set(task, true);
		}

		return Void();
	}

	// This function is just a wrapper for BackupSnapshotManifest::addTask() which is defined below.
	// The BackupSnapshotDispatchTask and BackupSnapshotManifest tasks reference each other so in order to keep their
	// execute and finish phases defined together inside their class definitions this wrapper is declared here but
	// defined after BackupSnapshotManifest is defined.
	static Future<Key> addSnapshotManifestTask(Reference<ReadYourWritesTransaction> tr,
	                                           Reference<TaskBucket> taskBucket,
	                                           Reference<Task> parentTask,
	                                           TaskCompletionKey completionKey,
	                                           Reference<TaskFuture> waitFor = Reference<TaskFuture>());

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state BackupConfig config(task);

		// Get the batch future and dispatch done keys, then clear them.
		state Key snapshotBatchFutureKey;
		state Key snapshotBatchDispatchDoneKey;

		wait(store(snapshotBatchFutureKey, config.snapshotBatchFuture().getOrThrow(tr)) &&
		     store(snapshotBatchDispatchDoneKey, config.snapshotBatchDispatchDoneKey().getOrThrow(tr)));

		state Reference<TaskFuture> snapshotBatchFuture = futureBucket->unpack(snapshotBatchFutureKey);
		state Reference<TaskFuture> snapshotBatchDispatchDoneFuture =
		    futureBucket->unpack(snapshotBatchDispatchDoneKey);
		config.snapshotBatchFuture().clear(tr);
		config.snapshotBatchDispatchDoneKey().clear(tr);
		config.snapshotBatchSize().clear(tr);

		// Update shardsBehind here again in case the execute phase did not actually have to create any shard tasks
		config.snapshotDispatchLastShardsBehind().set(tr, Params.shardsBehind().getOrDefault(task, 0));
		config.snapshotDispatchLastVersion().set(tr, tr->getReadVersion().get());

		state Reference<TaskFuture> snapshotFinishedFuture = task->getDoneFuture(futureBucket);

		// If the snapshot is finished, the next task is to write a snapshot manifest, otherwise it's another snapshot
		// dispatch task. In either case, the task should wait for snapshotBatchFuture. The snapshot done key, passed to
		// the current task, is also passed on.
		if (Params.snapshotFinished().getOrDefault(task, false)) {
			wait(success(addSnapshotManifestTask(
			    tr, taskBucket, task, TaskCompletionKey::signal(snapshotFinishedFuture), snapshotBatchFuture)));
		} else {
			wait(success(addTask(tr,
			                     taskBucket,
			                     task,
			                     1,
			                     TaskCompletionKey::signal(snapshotFinishedFuture),
			                     snapshotBatchFuture,
			                     Params.nextDispatchVersion().get(task))));
		}

		// This snapshot batch is finished, so set the batch done future.
		wait(snapshotBatchDispatchDoneFuture->set(tr, taskBucket));

		wait(taskBucket->finish(tr, task));

		return Void();
	}
};
StringRef BackupSnapshotDispatchTask::name = LiteralStringRef("file_backup_dispatch_ranges_5.2");
REGISTER_TASKFUNC(BackupSnapshotDispatchTask);

struct BackupLogRangeTaskFunc : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<bool> addBackupLogRangeTasks() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<int64_t> fileSize() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<Version> beginVersion() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<Version> endVersion() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

		wait(checkTaskVersion(cx, task, BackupLogRangeTaskFunc::name, BackupLogRangeTaskFunc::version));

		state Version beginVersion = Params.beginVersion().get(task);
		state Version endVersion = Params.endVersion().get(task);

		state BackupConfig config(task);
		state Reference<IBackupContainer> bc;

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			// Wait for the read version to pass endVersion
			try {
				wait(taskBucket->keepRunning(tr, task));

				if (!bc) {
					// Backup container must be present if we're still here
					Reference<IBackupContainer> _bc = wait(config.backupContainer().getOrThrow(tr));
					bc = getBackupContainerWithProxy(_bc);
				}

				Version currentVersion = tr->getReadVersion().get();
				if (endVersion < currentVersion)
					break;

				wait(delay(std::max(CLIENT_KNOBS->BACKUP_RANGE_MINWAIT,
				                    (double)(endVersion - currentVersion) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND)));
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		Key destUidValue = wait(config.destUidValue().getOrThrow(tr));

		// Get the set of key ranges that hold mutations for (beginVersion, endVersion).  They will be queried in
		// parallel below and there is a limit on how many we want to process in a single BackupLogRangeTask so if that
		// limit is exceeded then set the addBackupLogRangeTasks boolean in Params and stop, signalling the finish()
		// step to break up the (beginVersion, endVersion) range into smaller intervals which are then processed by
		// individual BackupLogRangeTasks.
		state Standalone<VectorRef<KeyRangeRef>> ranges = getLogRanges(beginVersion, endVersion, destUidValue);
		if (ranges.size() > CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
			Params.addBackupLogRangeTasks().set(task, true);
			return Void();
		}

		// Block size must be at least large enough for 1 max size key, 1 max size value, and overhead, so
		// conservatively 125k.
		state int blockSize =
		    BUGGIFY ? deterministicRandom()->randomInt(125e3, 4e6) : CLIENT_KNOBS->BACKUP_LOGFILE_BLOCK_SIZE;
		state Reference<IBackupFile> outFile = wait(bc->writeLogFile(beginVersion, endVersion, blockSize));
		state LogFileWriter logFile(outFile, blockSize);

		// Query all key ranges covering (beginVersion, endVersion) in parallel, writing their results to the results
		// promise stream as they are received.  Note that this means the records read from the results stream are not
		// likely to be in increasing Version order.
		state PromiseStream<RangeResultWithVersion> results;
		state std::vector<Future<Void>> rc;

		for (auto& range : ranges) {
			rc.push_back(
			    readCommitted(cx, results, lock, range, Terminator::False, AccessSystemKeys::True, LockAware::True));
		}

		state Future<Void> sendEOS = map(errorOr(waitForAll(rc)), [=](ErrorOr<Void> const& result) {
			if (result.isError())
				results.sendError(result.getError());
			else
				results.sendError(end_of_stream());
			return Void();
		});

		state Version lastVersion;
		try {
			loop {
				state RangeResultWithVersion r = waitNext(results.getFuture());
				lock->release(r.first.expectedSize());

				state int i = 0;
				for (; i < r.first.size(); ++i) {
					// Remove the backupLogPrefix + UID bytes from the key
					wait(logFile.writeKV(r.first[i].key.substr(backupLogPrefixBytes + 16), r.first[i].value));
					lastVersion = r.second;
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;

			if (e.code() != error_code_end_of_stream) {
				state Error err = e;
				wait(config.logError(cx, err, format("Failed to write to file `%s'", outFile->getFileName().c_str())));
				throw err;
			}
		}

		// Make sure this task is still alive, if it's not then the data read above could be incomplete.
		wait(taskBucket->keepRunning(cx, task));

		wait(outFile->finish());

		TraceEvent("FileBackupWroteLogFile")
		    .suppressFor(60)
		    .detail("BackupUID", config.getUid())
		    .detail("Size", outFile->size())
		    .detail("BeginVersion", beginVersion)
		    .detail("EndVersion", endVersion)
		    .detail("LastReadVersion", lastVersion);

		Params.fileSize().set(task, outFile->size());

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 int priority,
	                                 Version beginVersion,
	                                 Version endVersion,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = wait(addBackupTask(
		    BackupLogRangeTaskFunc::name,
		    BackupLogRangeTaskFunc::version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(parentTask),
		    waitFor,
		    [=](Reference<Task> task) {
			    Params.beginVersion().set(task, beginVersion);
			    Params.endVersion().set(task, endVersion);
			    Params.addBackupLogRangeTasks().set(task, false);
		    },
		    priority));
		return key;
	}

	ACTOR static Future<Void> startBackupLogRangeInternal(Reference<ReadYourWritesTransaction> tr,
	                                                      Reference<TaskBucket> taskBucket,
	                                                      Reference<FutureBucket> futureBucket,
	                                                      Reference<Task> task,
	                                                      Reference<TaskFuture> taskFuture,
	                                                      Version beginVersion,
	                                                      Version endVersion) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		std::vector<Future<Key>> addTaskVector;
		int tasks = 0;
		for (int64_t vblock = beginVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
		     vblock < (endVersion + CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE - 1) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
		     vblock += CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
			Version bv = std::max(beginVersion, vblock * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);

			if (tasks >= CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT) {
				addTaskVector.push_back(addTask(tr,
				                                taskBucket,
				                                task,
				                                task->getPriority(),
				                                bv,
				                                endVersion,
				                                TaskCompletionKey::joinWith(taskFuture)));
				break;
			}

			Version ev = std::min(endVersion,
			                      (vblock + CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
			addTaskVector.push_back(
			    addTask(tr, taskBucket, task, task->getPriority(), bv, ev, TaskCompletionKey::joinWith(taskFuture)));
			tasks++;
		}

		wait(waitForAll(addTaskVector));

		return Void();
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Version beginVersion = Params.beginVersion().get(task);
		state Version endVersion = Params.endVersion().get(task);
		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		state BackupConfig config(task);

		if (Params.fileSize().exists(task)) {
			config.logBytesWritten().atomicOp(tr, Params.fileSize().get(task), MutationRef::AddValue);
		}

		if (Params.addBackupLogRangeTasks().get(task)) {
			wait(startBackupLogRangeInternal(tr, taskBucket, futureBucket, task, taskFuture, beginVersion, endVersion));
		} else {
			wait(taskFuture->set(tr, taskBucket));
		}

		wait(taskBucket->finish(tr, task));
		return Void();
	}
};

StringRef BackupLogRangeTaskFunc::name = LiteralStringRef("file_backup_write_logs_5.2");
REGISTER_TASKFUNC(BackupLogRangeTaskFunc);

// This task stopped being used in 6.2, however the code remains here to handle upgrades.
struct EraseLogRangeTaskFunc : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	static struct {
		static TaskParam<Version> beginVersion() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<Version> endVersion() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<Key> destUidValue() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 UID logUid,
	                                 TaskCompletionKey completionKey,
	                                 Key destUidValue,
	                                 Version endVersion = 0,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = wait(addBackupTask(
		    EraseLogRangeTaskFunc::name,
		    EraseLogRangeTaskFunc::version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(logUid),
		    waitFor,
		    [=](Reference<Task> task) {
			    Params.beginVersion().set(task, 1); // FIXME: remove in 6.X, only needed for 5.2 backward compatibility
			    Params.endVersion().set(task, endVersion);
			    Params.destUidValue().set(task, destUidValue);
		    },
		    0,
		    SetValidation::False));

		return key;
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		wait(checkTaskVersion(tr->getDatabase(), task, EraseLogRangeTaskFunc::name, EraseLogRangeTaskFunc::version));

		state Version endVersion = Params.endVersion().get(task);
		state Key destUidValue = Params.destUidValue().get(task);

		state BackupConfig config(task);
		state Key logUidValue = config.getUidAsKey();

		wait(taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task) &&
		     eraseLogData(
		         tr, logUidValue, destUidValue, endVersion != 0 ? Optional<Version>(endVersion) : Optional<Version>()));

		return Void();
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef EraseLogRangeTaskFunc::name = LiteralStringRef("file_backup_erase_logs_5.2");
REGISTER_TASKFUNC(EraseLogRangeTaskFunc);

struct BackupLogsDispatchTask : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Version> prevBeginVersion() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<Version> beginVersion() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		wait(checkTaskVersion(tr->getDatabase(), task, BackupLogsDispatchTask::name, BackupLogsDispatchTask::version));

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		if (CLIENT_KNOBS->BACKUP_AGENT_VERBOSE_LOGGING) {
			tr->debugTransaction(deterministicRandom()->randomUniqueID());
		}

		state Reference<TaskFuture> onDone = task->getDoneFuture(futureBucket);
		state Version prevBeginVersion = Params.prevBeginVersion().get(task);
		state Version beginVersion = Params.beginVersion().get(task);
		state BackupConfig config(task);
		config.latestLogEndVersion().set(tr, beginVersion);

		state bool stopWhenDone;
		state Optional<Version> restorableVersion;
		state EBackupState backupState;
		state Optional<std::string> tag;
		state Optional<Version> latestSnapshotEndVersion;
		state Optional<bool> partitionedLog;

		wait(store(stopWhenDone, config.stopWhenDone().getOrThrow(tr)) &&
		     store(restorableVersion, config.getLatestRestorableVersion(tr)) &&
		     store(backupState, config.stateEnum().getOrThrow(tr)) && store(tag, config.tag().get(tr)) &&
		     store(latestSnapshotEndVersion, config.latestSnapshotEndVersion().get(tr)) &&
		     store(partitionedLog, config.partitionedLogEnabled().get(tr)));

		// If restorable, update the last restorable version for this tag
		if (restorableVersion.present() && tag.present()) {
			FileBackupAgent().setLastRestorable(tr, StringRef(tag.get()), restorableVersion.get());
		}

		// If the backup is restorable but the state is not differential then set state to differential
		if (restorableVersion.present() && backupState != EBackupState::STATE_RUNNING_DIFFERENTIAL)
			config.stateEnum().set(tr, EBackupState::STATE_RUNNING_DIFFERENTIAL);

		// If stopWhenDone is set and there is a restorable version, set the done future and do not create further
		// tasks.
		if (stopWhenDone && restorableVersion.present()) {
			wait(onDone->set(tr, taskBucket) && taskBucket->finish(tr, task));

			TraceEvent("FileBackupLogsDispatchDone")
			    .detail("BackupUID", config.getUid())
			    .detail("BeginVersion", beginVersion)
			    .detail("RestorableVersion", restorableVersion.orDefault(-1));

			return Void();
		}

		state Version endVersion = std::max<Version>(tr->getReadVersion().get() + 1,
		                                             beginVersion + (CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES - 1) *
		                                                                CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);

		TraceEvent("FileBackupLogDispatch")
		    .suppressFor(60)
		    .detail("BeginVersion", beginVersion)
		    .detail("EndVersion", endVersion)
		    .detail("RestorableVersion", restorableVersion.orDefault(-1));

		state Reference<TaskFuture> logDispatchBatchFuture = futureBucket->future(tr);

		// If a snapshot has ended for this backup then mutations are higher priority to reduce backup lag
		state int priority = latestSnapshotEndVersion.present() ? 1 : 0;

		if (!partitionedLog.present() || !partitionedLog.get()) {
			// Add the initial log range task to read/copy the mutations and the next logs dispatch task which will run
			// after this batch is done
			wait(success(BackupLogRangeTaskFunc::addTask(tr,
			                                             taskBucket,
			                                             task,
			                                             priority,
			                                             beginVersion,
			                                             endVersion,
			                                             TaskCompletionKey::joinWith(logDispatchBatchFuture))));
			wait(success(BackupLogsDispatchTask::addTask(tr,
			                                             taskBucket,
			                                             task,
			                                             priority,
			                                             beginVersion,
			                                             endVersion,
			                                             TaskCompletionKey::signal(onDone),
			                                             logDispatchBatchFuture)));

			// Do not erase at the first time
			if (prevBeginVersion > 0) {
				state Key destUidValue = wait(config.destUidValue().getOrThrow(tr));
				wait(eraseLogData(tr, config.getUidAsKey(), destUidValue, Optional<Version>(beginVersion)));
			}
		} else {
			// Skip mutation copy and erase backup mutations. Just check back periodically.
			Version scheduledVersion = tr->getReadVersion().get() +
			                           CLIENT_KNOBS->BACKUP_POLL_PROGRESS_SECONDS * CLIENT_KNOBS->VERSIONS_PER_SECOND;
			wait(success(BackupLogsDispatchTask::addTask(tr,
			                                             taskBucket,
			                                             task,
			                                             1,
			                                             beginVersion,
			                                             endVersion,
			                                             TaskCompletionKey::signal(onDone),
			                                             Reference<TaskFuture>(),
			                                             scheduledVersion)));
		}

		wait(taskBucket->finish(tr, task));

		TraceEvent("FileBackupLogsDispatchContinuing")
		    .suppressFor(60)
		    .detail("BackupUID", config.getUid())
		    .detail("BeginVersion", beginVersion)
		    .detail("EndVersion", endVersion);

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 int priority,
	                                 Version prevBeginVersion,
	                                 Version beginVersion,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
	                                 Version scheduledVersion = invalidVersion) {
		Key key = wait(addBackupTask(
		    BackupLogsDispatchTask::name,
		    BackupLogsDispatchTask::version,
		    tr,
		    taskBucket,
		    completionKey,
		    BackupConfig(parentTask),
		    waitFor,
		    [=](Reference<Task> task) {
			    Params.prevBeginVersion().set(task, prevBeginVersion);
			    Params.beginVersion().set(task, beginVersion);
			    if (scheduledVersion != invalidVersion) {
				    ReservedTaskParams::scheduledVersion().set(task, scheduledVersion);
			    }
		    },
		    priority));
		return key;
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef BackupLogsDispatchTask::name = LiteralStringRef("file_backup_dispatch_logs_5.2");
REGISTER_TASKFUNC(BackupLogsDispatchTask);

struct FileBackupFinishedTask : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	StringRef getName() const override { return name; };

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		wait(checkTaskVersion(tr->getDatabase(), task, FileBackupFinishedTask::name, FileBackupFinishedTask::version));

		state BackupConfig backup(task);
		state UID uid = backup.getUid();

		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);
		state Key destUidValue = wait(backup.destUidValue().getOrThrow(tr));

		wait(eraseLogData(tr, backup.getUidAsKey(), destUidValue) && clearBackupStartID(tr, uid));

		backup.stateEnum().set(tr, EBackupState::STATE_COMPLETED);

		wait(taskBucket->finish(tr, task));

		TraceEvent("FileBackupFinished").detail("BackupUID", uid);

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = wait(addBackupTask(FileBackupFinishedTask::name,
		                             FileBackupFinishedTask::version,
		                             tr,
		                             taskBucket,
		                             completionKey,
		                             BackupConfig(parentTask),
		                             waitFor));
		return key;
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef FileBackupFinishedTask::name = LiteralStringRef("file_backup_finished_5.2");
REGISTER_TASKFUNC(FileBackupFinishedTask);

struct BackupSnapshotManifest : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	static struct {
		static TaskParam<Version> endVersion() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state BackupConfig config(task);
		state Reference<IBackupContainer> bc;

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

		// Read the entire range file map into memory, then walk it backwards from its last entry to produce a list of
		// non overlapping key range files
		state std::map<Key, BackupConfig::RangeSlice> localmap;
		state Key startKey;
		state int batchSize = BUGGIFY ? 1 : 1000000;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				wait(taskBucket->keepRunning(tr, task));

				if (!bc) {
					// Backup container must be present if we're still here
					Reference<IBackupContainer> _bc = wait(config.backupContainer().getOrThrow(tr));
					bc = getBackupContainerWithProxy(_bc);
				}

				BackupConfig::RangeFileMapT::PairsType rangeresults =
				    wait(config.snapshotRangeFileMap().getRange(tr, startKey, {}, batchSize));

				for (auto& p : rangeresults) {
					localmap.insert(p);
				}

				if (rangeresults.size() < batchSize)
					break;

				startKey = keyAfter(rangeresults.back().first);
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		std::vector<std::string> files;
		std::vector<std::pair<Key, Key>> beginEndKeys;
		state Version maxVer = 0;
		state Version minVer = std::numeric_limits<Version>::max();
		state int64_t totalBytes = 0;

		if (!localmap.empty()) {
			// Get iterator that points to greatest key, start there.
			auto ri = localmap.rbegin();
			auto i = (++ri).base();

			while (1) {
				const BackupConfig::RangeSlice& r = i->second;

				// Add file to final file list
				files.push_back(r.fileName);

				// Add (beginKey, endKey) pairs to the list
				beginEndKeys.emplace_back(i->second.begin, i->first);

				// Update version range seen
				if (r.version < minVer)
					minVer = r.version;
				if (r.version > maxVer)
					maxVer = r.version;

				// Update total bytes counted.
				totalBytes += r.fileSize;

				// Jump to file that either ends where this file begins or has the greatest end that is less than
				// the begin of this file.  In other words find the map key that is <= begin of this file.  To do this
				// find the first end strictly greater than begin and then back up one.
				i = localmap.upper_bound(i->second.begin);
				// If we get begin then we're done, there are no more ranges that end at or before the last file's begin
				if (i == localmap.begin())
					break;
				--i;
			}
		}

		Params.endVersion().set(task, maxVer);
		wait(bc->writeKeyspaceSnapshotFile(files, beginEndKeys, totalBytes));

		TraceEvent(SevInfo, "FileBackupWroteSnapshotManifest")
		    .detail("BackupUID", config.getUid())
		    .detail("BeginVersion", minVer)
		    .detail("EndVersion", maxVer)
		    .detail("TotalBytes", totalBytes);

		return Void();
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		wait(checkTaskVersion(tr->getDatabase(), task, BackupSnapshotManifest::name, BackupSnapshotManifest::version));

		state BackupConfig config(task);

		// Set the latest snapshot end version, which was set during the execute phase
		config.latestSnapshotEndVersion().set(tr, Params.endVersion().get(task));

		state bool stopWhenDone;
		state EBackupState backupState;
		state Optional<Version> restorableVersion;
		state Optional<Version> firstSnapshotEndVersion;
		state Optional<std::string> tag;

		wait(store(stopWhenDone, config.stopWhenDone().getOrThrow(tr)) &&
		     store(backupState, config.stateEnum().getOrThrow(tr)) &&
		     store(restorableVersion, config.getLatestRestorableVersion(tr)) &&
		     store(firstSnapshotEndVersion, config.firstSnapshotEndVersion().get(tr)) &&
		     store(tag, config.tag().get(tr)));

		// If restorable, update the last restorable version for this tag
		if (restorableVersion.present() && tag.present()) {
			FileBackupAgent().setLastRestorable(tr, StringRef(tag.get()), restorableVersion.get());
		}

		if (!firstSnapshotEndVersion.present()) {
			config.firstSnapshotEndVersion().set(tr, Params.endVersion().get(task));
		}

		// If the backup is restorable and the state isn't differential the set state to differential
		if (restorableVersion.present() && backupState != EBackupState::STATE_RUNNING_DIFFERENTIAL)
			config.stateEnum().set(tr, EBackupState::STATE_RUNNING_DIFFERENTIAL);

		// Unless we are to stop, start the next snapshot using the default interval
		Reference<TaskFuture> snapshotDoneFuture = task->getDoneFuture(futureBucket);
		if (!stopWhenDone) {
			wait(config.initNewSnapshot(tr) &&
			     success(BackupSnapshotDispatchTask::addTask(
			         tr, taskBucket, task, 1, TaskCompletionKey::signal(snapshotDoneFuture))));
		} else {
			// Set the done future as the snapshot is now complete.
			wait(snapshotDoneFuture->set(tr, taskBucket));
		}

		wait(taskBucket->finish(tr, task));
		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = wait(addBackupTask(BackupSnapshotManifest::name,
		                             BackupSnapshotManifest::version,
		                             tr,
		                             taskBucket,
		                             completionKey,
		                             BackupConfig(parentTask),
		                             waitFor,
		                             NOP_SETUP_TASK_FN,
		                             1));
		return key;
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef BackupSnapshotManifest::name = LiteralStringRef("file_backup_write_snapshot_manifest_5.2");
REGISTER_TASKFUNC(BackupSnapshotManifest);

Future<Key> BackupSnapshotDispatchTask::addSnapshotManifestTask(Reference<ReadYourWritesTransaction> tr,
                                                                Reference<TaskBucket> taskBucket,
                                                                Reference<Task> parentTask,
                                                                TaskCompletionKey completionKey,
                                                                Reference<TaskFuture> waitFor) {
	return BackupSnapshotManifest::addTask(tr, taskBucket, parentTask, completionKey, waitFor);
}

struct StartFullBackupTaskFunc : BackupTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Version> beginVersion() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		wait(checkTaskVersion(cx, task, StartFullBackupTaskFunc::name, StartFullBackupTaskFunc::version));

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state BackupConfig config(task);
		state Future<Optional<bool>> partitionedLog;
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				partitionedLog = config.partitionedLogEnabled().get(tr);
				state Future<Version> startVersionFuture = tr->getReadVersion();
				wait(success(partitionedLog) && success(startVersionFuture));

				Params.beginVersion().set(task, startVersionFuture.get());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		// Check if backup worker is enabled
		DatabaseConfiguration dbConfig = wait(getDatabaseConfiguration(cx));
		state bool backupWorkerEnabled = dbConfig.backupWorkerEnabled;
		if (!backupWorkerEnabled && partitionedLog.get().present() && partitionedLog.get().get()) {
			// Change configuration only when we set to use partitioned logs and
			// the flag was not set before.
			wait(success(ManagementAPI::changeConfig(cx.getReference(), "backup_worker_enabled:=1", true)));
			backupWorkerEnabled = true;
		}

		// Set the "backupStartedKey" and wait for all backup worker started
		tr->reset();
		loop {
			state Future<Void> watchFuture;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				state Future<Void> keepRunning = taskBucket->keepRunning(tr, task);

				state Future<Optional<Value>> started = tr->get(backupStartedKey);
				state Future<Optional<Value>> taskStarted = tr->get(config.allWorkerStarted().key);
				partitionedLog = config.partitionedLogEnabled().get(tr);
				wait(success(started) && success(taskStarted) && success(partitionedLog));

				if (!partitionedLog.get().present() || !partitionedLog.get().get()) {
					return Void(); // Skip if not using partitioned logs
				}

				std::vector<std::pair<UID, Version>> ids;
				if (started.get().present()) {
					ids = decodeBackupStartedValue(started.get().get());
				}
				const UID uid = config.getUid();
				auto it = std::find_if(
				    ids.begin(), ids.end(), [uid](const std::pair<UID, Version>& p) { return p.first == uid; });
				if (it == ids.end()) {
					ids.emplace_back(uid, Params.beginVersion().get(task));
				} else {
					Params.beginVersion().set(task, it->second);
				}

				tr->set(backupStartedKey, encodeBackupStartedValue(ids));
				if (backupWorkerEnabled) {
					config.backupWorkerEnabled().set(tr, true);
				}

				// The task may be restarted. Set the watch if started key has NOT been set.
				if (!taskStarted.get().present()) {
					watchFuture = tr->watch(config.allWorkerStarted().key);
				}

				wait(keepRunning);
				wait(tr->commit());
				if (!taskStarted.get().present()) {
					wait(watchFuture);
				}
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state BackupConfig config(task);
		state Version beginVersion = Params.beginVersion().get(task);

		state Future<std::vector<KeyRange>> backupRangesFuture = config.backupRanges().getOrThrow(tr);
		state Future<Key> destUidValueFuture = config.destUidValue().getOrThrow(tr);
		state Future<Optional<bool>> partitionedLog = config.partitionedLogEnabled().get(tr);
		state Future<Optional<bool>> incrementalBackupOnly = config.incrementalBackupOnly().get(tr);
		wait(success(backupRangesFuture) && success(destUidValueFuture) && success(partitionedLog) &&
		     success(incrementalBackupOnly));
		std::vector<KeyRange> backupRanges = backupRangesFuture.get();
		Key destUidValue = destUidValueFuture.get();

		// Start logging the mutations for the specified ranges of the tag if needed
		if (!partitionedLog.get().present() || !partitionedLog.get().get()) {
			for (auto& backupRange : backupRanges) {
				config.startMutationLogs(tr, backupRange, destUidValue);
			}
		}

		config.stateEnum().set(tr, EBackupState::STATE_RUNNING);

		state Reference<TaskFuture> backupFinished = futureBucket->future(tr);

		// Initialize the initial snapshot and create tasks to continually write logs and snapshots.
		state Optional<int64_t> initialSnapshotIntervalSeconds = wait(config.initialSnapshotIntervalSeconds().get(tr));
		wait(config.initNewSnapshot(tr, initialSnapshotIntervalSeconds.orDefault(0)));

		// Using priority 1 for both of these to at least start both tasks soon
		// Do not add snapshot task if we only want the incremental backup
		if (!incrementalBackupOnly.get().present() || !incrementalBackupOnly.get().get()) {
			wait(success(BackupSnapshotDispatchTask::addTask(
			    tr, taskBucket, task, 1, TaskCompletionKey::joinWith(backupFinished))));
		}
		wait(success(BackupLogsDispatchTask::addTask(
		    tr, taskBucket, task, 1, 0, beginVersion, TaskCompletionKey::joinWith(backupFinished))));

		// If a clean stop is requested, the log and snapshot tasks will quit after the backup is restorable, then the
		// following task will clean up and set the completed state.
		wait(success(
		    FileBackupFinishedTask::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal(), backupFinished)));

		wait(taskBucket->finish(tr, task));

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 UID uid,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key key = wait(addBackupTask(StartFullBackupTaskFunc::name,
		                             StartFullBackupTaskFunc::version,
		                             tr,
		                             taskBucket,
		                             completionKey,
		                             BackupConfig(uid),
		                             waitFor));
		return key;
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef StartFullBackupTaskFunc::name = LiteralStringRef("file_backup_start_5.2");
REGISTER_TASKFUNC(StartFullBackupTaskFunc);

struct RestoreCompleteTaskFunc : RestoreTaskFuncBase {
	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		wait(checkTaskVersion(tr->getDatabase(), task, name, version));

		state RestoreConfig restore(task);
		restore.stateEnum().set(tr, ERestoreState::COMPLETED);
		tr->atomicOp(metadataVersionKey, metadataVersionRequiredValue, MutationRef::SetVersionstampedValue);
		// Clear the file map now since it could be huge.
		restore.fileSet().clear(tr);

		// TODO:  Validate that the range version map has exactly the restored ranges in it.  This means that for any
		// restore operation the ranges to restore must be within the backed up ranges, otherwise from the restore
		// perspective it will appear that some key ranges were missing and so the backup set is incomplete and the
		// restore has failed. This validation cannot be done currently because Restore only supports a single restore
		// range but backups can have many ranges.

		// Clear the applyMutations stuff, including any unapplied mutations from versions beyond the restored version.
		restore.clearApplyMutationsKeys(tr);

		wait(taskBucket->finish(tr, task));
		wait(unlockDatabase(tr, restore.getUid()));

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		state Reference<Task> task(new Task(RestoreCompleteTaskFunc::name, RestoreCompleteTaskFunc::version, doneKey));

		// Get restore config from parent task and bind it to new task
		wait(RestoreConfig(parentTask).toTask(tr, task));

		if (!waitFor) {
			return taskBucket->addTask(tr, task);
		}

		wait(waitFor->onSetAddTask(tr, taskBucket, task));
		return LiteralStringRef("OnSetAddTask");
	}

	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreCompleteTaskFunc::name = LiteralStringRef("restore_complete");
REGISTER_TASKFUNC(RestoreCompleteTaskFunc);

struct RestoreFileTaskFuncBase : RestoreTaskFuncBase {
	struct InputParams {
		static TaskParam<RestoreFile> inputFile() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<int64_t> readOffset() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<int64_t> readLen() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	std::string toString(Reference<Task> task) const override {
		return format("fileName '%s' readLen %lld readOffset %lld",
		              Params.inputFile().get(task).fileName.c_str(),
		              Params.readLen().get(task),
		              Params.readOffset().get(task));
	}
};

struct RestoreRangeTaskFunc : RestoreFileTaskFuncBase {
	static struct : InputParams {
		// The range of data that the (possibly empty) data represented, which is set if it intersects the target
		// restore range
		static TaskParam<KeyRange> originalFileRange() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<std::vector<KeyRange>> originalFileRanges() { return LiteralStringRef(__FUNCTION__); }

		static std::vector<KeyRange> getOriginalFileRanges(Reference<Task> task) {
			if (originalFileRanges().exists(task)) {
				return Params.originalFileRanges().get(task);
			} else {
				std::vector<KeyRange> range;
				if (originalFileRange().exists(task))
					range.push_back(Params.originalFileRange().get(task));
				return range;
			}
		}
	} Params;

	std::string toString(Reference<Task> task) const override {
		std::string returnStr = RestoreFileTaskFuncBase::toString(task);
		for (auto& range : Params.getOriginalFileRanges(task))
			returnStr += format("  originalFileRange '%s'", printable(range).c_str());
		return returnStr;
	}

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state RestoreConfig restore(task);

		state RestoreFile rangeFile = Params.inputFile().get(task);
		state int64_t readOffset = Params.readOffset().get(task);
		state int64_t readLen = Params.readLen().get(task);

		TraceEvent("FileRestoreRangeStart")
		    .suppressFor(60)
		    .detail("RestoreUID", restore.getUid())
		    .detail("FileName", rangeFile.fileName)
		    .detail("FileVersion", rangeFile.version)
		    .detail("FileSize", rangeFile.fileSize)
		    .detail("ReadOffset", readOffset)
		    .detail("ReadLen", readLen)
		    .detail("TaskInstance", THIS_ADDR);

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state Future<Reference<IBackupContainer>> bc;
		state Future<std::vector<KeyRange>> restoreRanges;
		state Future<Key> addPrefix;
		state Future<Key> removePrefix;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				Reference<IBackupContainer> _bc = wait(restore.sourceContainer().getOrThrow(tr));
				bc = getBackupContainerWithProxy(_bc);
				restoreRanges = restore.getRestoreRangesOrDefault(tr);
				addPrefix = restore.addPrefix().getD(tr);
				removePrefix = restore.removePrefix().getD(tr);

				wait(taskBucket->keepRunning(tr, task));

				wait(success(bc) && success(restoreRanges) && success(addPrefix) && success(removePrefix) &&
				     checkTaskVersion(tr->getDatabase(), task, name, version));
				break;

			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		state Reference<IAsyncFile> inFile = wait(bc.get()->readFile(rangeFile.fileName));
		state Standalone<VectorRef<KeyValueRef>> blockData = wait(decodeRangeFileBlock(inFile, readOffset, readLen));

		// First and last key are the range for this file
		state KeyRange fileRange;
		state std::vector<KeyRange> originalFileRanges;
		// If fileRange doesn't intersect restore range then we're done.
		state int index;
		for (index = 0; index < restoreRanges.get().size(); index++) {
			auto& restoreRange = restoreRanges.get()[index];
			fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);
			if (!fileRange.intersects(restoreRange))
				continue;

			// We know the file range intersects the restore range but there could still be keys outside the restore
			// range. Find the subvector of kv pairs that intersect the restore range.  Note that the first and last
			// keys are just the range endpoints for this file
			int rangeStart = 1;
			int rangeEnd = blockData.size() - 1;
			// Slide start forward, stop if something in range is found
			while (rangeStart < rangeEnd && !restoreRange.contains(blockData[rangeStart].key))
				++rangeStart;
			// Side end backward, stop if something in range is found
			while (rangeEnd > rangeStart && !restoreRange.contains(blockData[rangeEnd - 1].key))
				--rangeEnd;

			state VectorRef<KeyValueRef> data = blockData.slice(rangeStart, rangeEnd);

			// Shrink file range to be entirely within restoreRange and translate it to the new prefix
			// First, use the untranslated file range to create the shrunk original file range which must be used in the
			// kv range version map for applying mutations
			state KeyRange originalFileRange =
			    KeyRangeRef(std::max(fileRange.begin, restoreRange.begin), std::min(fileRange.end, restoreRange.end));
			originalFileRanges.push_back(originalFileRange);

			// Now shrink and translate fileRange
			Key fileEnd = std::min(fileRange.end, restoreRange.end);
			if (fileEnd == (removePrefix.get() == StringRef() ? normalKeys.end : strinc(removePrefix.get()))) {
				fileEnd = addPrefix.get() == StringRef() ? normalKeys.end : strinc(addPrefix.get());
			} else {
				fileEnd = fileEnd.removePrefix(removePrefix.get()).withPrefix(addPrefix.get());
			}
			fileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.begin)
			                            .removePrefix(removePrefix.get())
			                            .withPrefix(addPrefix.get()),
			                        fileEnd);

			state int start = 0;
			state int end = data.size();
			state int dataSizeLimit =
			    BUGGIFY ? deterministicRandom()->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

			tr->reset();
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					state int i = start;
					state int txBytes = 0;
					state int iend = start;

					// find iend that results in the desired transaction size
					for (; iend < end && txBytes < dataSizeLimit; ++iend) {
						txBytes += data[iend].key.expectedSize();
						txBytes += data[iend].value.expectedSize();
					}

					// Clear the range we are about to set.
					// If start == 0 then use fileBegin for the start of the range, else data[start]
					// If iend == end then use fileEnd for the end of the range, else data[iend]
					state KeyRange trRange = KeyRangeRef(
					    (start == 0) ? fileRange.begin
					                 : data[start].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()),
					    (iend == end) ? fileRange.end
					                  : data[iend].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()));

					tr->clear(trRange);

					for (; i < iend; ++i) {
						tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
						tr->set(data[i].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()),
						        data[i].value);
					}

					// Add to bytes written count
					restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

					state Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

					wait(taskBucket->keepRunning(tr, task));

					wait(checkLock);

					wait(tr->commit());

					TraceEvent("FileRestoreCommittedRange")
					    .suppressFor(60)
					    .detail("RestoreUID", restore.getUid())
					    .detail("FileName", rangeFile.fileName)
					    .detail("FileVersion", rangeFile.version)
					    .detail("FileSize", rangeFile.fileSize)
					    .detail("ReadOffset", readOffset)
					    .detail("ReadLen", readLen)
					    .detail("CommitVersion", tr->getCommittedVersion())
					    .detail("BeginRange", trRange.begin)
					    .detail("EndRange", trRange.end)
					    .detail("StartIndex", start)
					    .detail("EndIndex", i)
					    .detail("DataSize", data.size())
					    .detail("Bytes", txBytes)
					    .detail("OriginalFileRange", originalFileRange)
					    .detail("TaskInstance", THIS_ADDR);

					// Commit succeeded, so advance starting point
					start = i;

					if (start == end)
						break;
					tr->reset();
				} catch (Error& e) {
					if (e.code() == error_code_transaction_too_large)
						dataSizeLimit /= 2;
					else
						wait(tr->onError(e));
				}
			}
		}
		if (!originalFileRanges.empty()) {
			if (BUGGIFY && restoreRanges.get().size() == 1) {
				Params.originalFileRange().set(task, originalFileRanges[0]);
			} else {
				Params.originalFileRanges().set(task, originalFileRanges);
			}
		}
		return Void();
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state RestoreConfig restore(task);
		restore.fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

		// Update the KV range map if originalFileRange is set
		std::vector<Future<Void>> updateMap;
		std::vector<KeyRange> ranges = Params.getOriginalFileRanges(task);
		for (auto& range : ranges) {
			Value versionEncoded = BinaryWriter::toValue(Params.inputFile().get(task).version, Unversioned());
			updateMap.push_back(krmSetRange(tr, restore.applyMutationsMapPrefix(), range, versionEncoded));
		}

		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		wait(taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task) && waitForAll(updateMap));

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 RestoreFile rf,
	                                 int64_t offset,
	                                 int64_t len,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		state Reference<Task> task(new Task(RestoreRangeTaskFunc::name, RestoreRangeTaskFunc::version, doneKey));

		// Create a restore config from the current task and bind it to the new task.
		wait(RestoreConfig(parentTask).toTask(tr, task));

		Params.inputFile().set(task, rf);
		Params.readOffset().set(task, offset);
		Params.readLen().set(task, len);

		if (!waitFor) {
			return taskBucket->addTask(tr, task);
		}

		wait(waitFor->onSetAddTask(tr, taskBucket, task));
		return LiteralStringRef("OnSetAddTask");
	}

	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreRangeTaskFunc::name = LiteralStringRef("restore_range_data");
REGISTER_TASKFUNC(RestoreRangeTaskFunc);

// Decodes a mutation log key, which contains (hash, commitVersion, chunkNumber) and
// returns (commitVersion, chunkNumber)
std::pair<Version, int32_t> decodeMutationLogKey(const StringRef& key) {
	ASSERT(key.size() == sizeof(uint8_t) + sizeof(Version) + sizeof(int32_t));

	uint8_t hash;
	Version version;
	int32_t part;
	BinaryReader rd(key, Unversioned());
	rd >> hash >> version >> part;
	version = bigEndian64(version);
	part = bigEndian32(part);

	int32_t v = version / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
	ASSERT(((uint8_t)hashlittle(&v, sizeof(v), 0)) == hash);

	return std::make_pair(version, part);
}

// Decodes an encoded list of mutations in the format of:
//   [includeVersion:uint64_t][val_length:uint32_t][mutation_1][mutation_2]...[mutation_k],
// where a mutation is encoded as:
//   [type:uint32_t][keyLength:uint32_t][valueLength:uint32_t][param1][param2]
std::vector<MutationRef> decodeMutationLogValue(const StringRef& value) {
	StringRefReader reader(value, restore_corrupted_data());

	Version protocolVersion = reader.consume<uint64_t>();
	if (protocolVersion <= 0x0FDB00A200090001) {
		throw incompatible_protocol_version();
	}

	uint32_t val_length = reader.consume<uint32_t>();
	if (val_length != value.size() - sizeof(uint64_t) - sizeof(uint32_t)) {
		TraceEvent(SevError, "FileRestoreLogValueError")
		    .detail("ValueLen", val_length)
		    .detail("ValueSize", value.size())
		    .detail("Value", printable(value));
	}

	std::vector<MutationRef> mutations;
	while (1) {
		if (reader.eof())
			break;

		// Deserialization of a MutationRef, which was packed by MutationListRef::push_back_deep()
		uint32_t type, p1len, p2len;
		type = reader.consume<uint32_t>();
		p1len = reader.consume<uint32_t>();
		p2len = reader.consume<uint32_t>();

		const uint8_t* key = reader.consume(p1len);
		const uint8_t* val = reader.consume(p2len);

		mutations.emplace_back((MutationRef::Type)type, StringRef(key, p1len), StringRef(val, p2len));
	}
	return mutations;
}

void AccumulatedMutations::addChunk(int chunkNumber, const KeyValueRef& kv) {
	if (chunkNumber == lastChunkNumber + 1) {
		lastChunkNumber = chunkNumber;
		serializedMutations += kv.value.toString();
	} else {
		lastChunkNumber = -2;
		serializedMutations.clear();
	}
	kvs.push_back(kv);
}

bool AccumulatedMutations::isComplete() const {
	if (lastChunkNumber >= 0) {
		StringRefReader reader(serializedMutations, restore_corrupted_data());

		Version protocolVersion = reader.consume<uint64_t>();
		if (protocolVersion <= 0x0FDB00A200090001) {
			throw incompatible_protocol_version();
		}

		uint32_t vLen = reader.consume<uint32_t>();
		return vLen == reader.remainder().size();
	}

	return false;
}

// Returns true if a complete chunk contains any MutationRefs which intersect with any
// range in ranges.
// It is undefined behavior to run this if isComplete() does not return true.
bool AccumulatedMutations::matchesAnyRange(const RangeMapFilters& filters) const {
	std::vector<MutationRef> mutations = decodeMutationLogValue(serializedMutations);
	for (auto& m : mutations) {
		if (filters.match(m)) {
			return true;
		}
	}

	return false;
}

bool RangeMapFilters::match(const MutationRef& m) const {
	if (isSingleKeyMutation((MutationRef::Type)m.type)) {
		if (match(singleKeyRange(m.param1))) {
			return true;
		}
	} else if (m.type == MutationRef::ClearRange) {
		if (match(KeyRangeRef(m.param1, m.param2))) {
			return true;
		}
	} else {
		ASSERT(false);
	}
	return false;
}

bool RangeMapFilters::match(const KeyValueRef& kv) const {
	return match(singleKeyRange(kv.key));
}

bool RangeMapFilters::match(const KeyRangeRef& range) const {
	auto ranges = rangeMap.intersectingRanges(range);
	for (const auto& r : ranges) {
		if (r.cvalue() == 1) {
			return true;
		}
	}
	return false;
}

// Returns a vector of filtered KV refs from data which are either part of incomplete mutation groups OR complete
// and have data relevant to one of the KV ranges in ranges
std::vector<KeyValueRef> filterLogMutationKVPairs(VectorRef<KeyValueRef> data, const RangeMapFilters& filters) {
	std::unordered_map<Version, AccumulatedMutations> mutationBlocksByVersion;

	for (auto& kv : data) {
		auto versionAndChunkNumber = decodeMutationLogKey(kv.key);
		mutationBlocksByVersion[versionAndChunkNumber.first].addChunk(versionAndChunkNumber.second, kv);
	}

	std::vector<KeyValueRef> output;

	for (auto& vb : mutationBlocksByVersion) {
		AccumulatedMutations& m = vb.second;

		// If the mutations are incomplete or match one of the ranges, include in results.
		if (!m.isComplete() || m.matchesAnyRange(filters)) {
			output.insert(output.end(), m.kvs.begin(), m.kvs.end());
		}
	}

	return output;
}
struct RestoreLogDataTaskFunc : RestoreFileTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	static struct : InputParams {
	} Params;

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state RestoreConfig restore(task);

		state RestoreFile logFile = Params.inputFile().get(task);
		state int64_t readOffset = Params.readOffset().get(task);
		state int64_t readLen = Params.readLen().get(task);

		TraceEvent("FileRestoreLogStart")
		    .suppressFor(60)
		    .detail("RestoreUID", restore.getUid())
		    .detail("FileName", logFile.fileName)
		    .detail("FileBeginVersion", logFile.version)
		    .detail("FileEndVersion", logFile.endVersion)
		    .detail("FileSize", logFile.fileSize)
		    .detail("ReadOffset", readOffset)
		    .detail("ReadLen", readLen)
		    .detail("TaskInstance", THIS_ADDR);

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state Reference<IBackupContainer> bc;
		state std::vector<KeyRange> ranges;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				Reference<IBackupContainer> _bc = wait(restore.sourceContainer().getOrThrow(tr));
				bc = getBackupContainerWithProxy(_bc);

				wait(store(ranges, restore.getRestoreRangesOrDefault(tr)));

				wait(checkTaskVersion(tr->getDatabase(), task, name, version));
				wait(taskBucket->keepRunning(tr, task));

				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		state Key mutationLogPrefix = restore.mutationLogPrefix();
		state Reference<IAsyncFile> inFile = wait(bc->readFile(logFile.fileName));
		state Standalone<VectorRef<KeyValueRef>> dataOriginal =
		    wait(decodeMutationLogFileBlock(inFile, readOffset, readLen));

		// Filter the KV pairs extracted from the log file block to remove any records known to not be needed for this
		// restore based on the restore range set.
		RangeMapFilters filters(ranges);
		state std::vector<KeyValueRef> dataFiltered = filterLogMutationKVPairs(dataOriginal, filters);

		state int start = 0;
		state int end = dataFiltered.size();
		state int dataSizeLimit =
		    BUGGIFY ? deterministicRandom()->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

		tr->reset();
		loop {
			try {
				if (start == end)
					return Void();

				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state int i = start;
				state int txBytes = 0;
				for (; i < end && txBytes < dataSizeLimit; ++i) {
					Key k = dataFiltered[i].key.withPrefix(mutationLogPrefix);
					ValueRef v = dataFiltered[i].value;
					tr->set(k, v);
					txBytes += k.expectedSize();
					txBytes += v.expectedSize();
				}

				state Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

				wait(taskBucket->keepRunning(tr, task));
				wait(checkLock);

				// Add to bytes written count
				restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

				wait(tr->commit());

				TraceEvent("FileRestoreCommittedLog")
				    .suppressFor(60)
				    .detail("RestoreUID", restore.getUid())
				    .detail("FileName", logFile.fileName)
				    .detail("FileBeginVersion", logFile.version)
				    .detail("FileEndVersion", logFile.endVersion)
				    .detail("FileSize", logFile.fileSize)
				    .detail("ReadOffset", readOffset)
				    .detail("ReadLen", readLen)
				    .detail("CommitVersion", tr->getCommittedVersion())
				    .detail("StartIndex", start)
				    .detail("EndIndex", i)
				    .detail("RecordCountOriginal", dataOriginal.size())
				    .detail("RecordCountFiltered", dataFiltered.size())
				    .detail("Bytes", txBytes)
				    .detail("TaskInstance", THIS_ADDR);

				// Commit succeeded, so advance starting point
				start = i;
				tr->reset();
			} catch (Error& e) {
				if (e.code() == error_code_transaction_too_large)
					dataSizeLimit /= 2;
				else
					wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		RestoreConfig(task).fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		// TODO:  Check to see if there is a leak in the FutureBucket since an invalid task (validation key fails) will
		// never set its taskFuture.
		wait(taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task));

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 RestoreFile lf,
	                                 int64_t offset,
	                                 int64_t len,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		state Reference<Task> task(new Task(RestoreLogDataTaskFunc::name, RestoreLogDataTaskFunc::version, doneKey));

		// Create a restore config from the current task and bind it to the new task.
		wait(RestoreConfig(parentTask).toTask(tr, task));
		Params.inputFile().set(task, lf);
		Params.readOffset().set(task, offset);
		Params.readLen().set(task, len);

		if (!waitFor) {
			return taskBucket->addTask(tr, task);
		}

		wait(waitFor->onSetAddTask(tr, taskBucket, task));
		return LiteralStringRef("OnSetAddTask");
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreLogDataTaskFunc::name = LiteralStringRef("restore_log_data");
REGISTER_TASKFUNC(RestoreLogDataTaskFunc);

struct RestoreDispatchTaskFunc : RestoreTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	StringRef getName() const override { return name; };

	static struct {
		static TaskParam<Version> beginVersion() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<std::string> beginFile() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<int64_t> beginBlock() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<int64_t> batchSize() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<int64_t> remainingInBatch() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state RestoreConfig restore(task);

		state Version beginVersion = Params.beginVersion().get(task);
		state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		state int64_t remainingInBatch = Params.remainingInBatch().get(task);
		state bool addingToExistingBatch = remainingInBatch > 0;
		state Version restoreVersion;
		state Future<Optional<bool>> onlyApplyMutationLogs = restore.onlyApplyMutationLogs().get(tr);

		wait(store(restoreVersion, restore.restoreVersion().getOrThrow(tr)) && success(onlyApplyMutationLogs) &&
		     checkTaskVersion(tr->getDatabase(), task, name, version));

		// If not adding to an existing batch then update the apply mutations end version so the mutations from the
		// previous batch can be applied.  Only do this once beginVersion is > 0 (it will be 0 for the initial
		// dispatch).
		if (!addingToExistingBatch && beginVersion > 0) {
			restore.setApplyEndVersion(tr, std::min(beginVersion, restoreVersion + 1));
		}

		// The applyLag must be retrieved AFTER potentially updating the apply end version.
		state int64_t applyLag = wait(restore.getApplyVersionLag(tr));
		state int64_t batchSize = Params.batchSize().get(task);

		// If starting a new batch and the apply lag is too large then re-queue and wait
		if (!addingToExistingBatch && applyLag > (BUGGIFY ? 1 : CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 300)) {
			// Wait a small amount of time and then re-add this same task.
			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			wait(success(RestoreDispatchTaskFunc::addTask(
			    tr, taskBucket, task, beginVersion, "", 0, batchSize, remainingInBatch)));

			TraceEvent("FileRestoreDispatch")
			    .detail("RestoreUID", restore.getUid())
			    .detail("BeginVersion", beginVersion)
			    .detail("ApplyLag", applyLag)
			    .detail("BatchSize", batchSize)
			    .detail("Decision", "too_far_behind")
			    .detail("TaskInstance", THIS_ADDR);

			wait(taskBucket->finish(tr, task));
			return Void();
		}

		state std::string beginFile = Params.beginFile().getOrDefault(task);
		// Get a batch of files.  We're targeting batchSize blocks being dispatched so query for batchSize files (each
		// of which is 0 or more blocks).
		state int taskBatchSize = BUGGIFY ? 1 : CLIENT_KNOBS->RESTORE_DISPATCH_ADDTASK_SIZE;
		state RestoreConfig::FileSetT::Values files =
		    wait(restore.fileSet().getRange(tr, { beginVersion, beginFile }, {}, taskBatchSize));

		// allPartsDone will be set once all block tasks in the current batch are finished.
		state Reference<TaskFuture> allPartsDone;

		// If adding to existing batch then join the new block tasks to the existing batch future
		if (addingToExistingBatch) {
			Key fKey = wait(restore.batchFuture().getD(tr));
			allPartsDone = Reference<TaskFuture>(new TaskFuture(futureBucket, fKey));
		} else {
			// Otherwise create a new future for the new batch
			allPartsDone = futureBucket->future(tr);
			restore.batchFuture().set(tr, allPartsDone->pack());
			// Set batch quota remaining to batch size
			remainingInBatch = batchSize;
		}

		// If there were no files to load then this batch is done and restore is almost done.
		if (files.size() == 0) {
			// If adding to existing batch then blocks could be in progress so create a new Dispatch task that waits for
			// them to finish
			if (addingToExistingBatch) {
				// Setting next begin to restoreVersion + 1 so that any files in the file map at the restore version
				// won't be dispatched again.
				wait(success(RestoreDispatchTaskFunc::addTask(tr,
				                                              taskBucket,
				                                              task,
				                                              restoreVersion + 1,
				                                              "",
				                                              0,
				                                              batchSize,
				                                              0,
				                                              TaskCompletionKey::noSignal(),
				                                              allPartsDone)));

				TraceEvent("FileRestoreDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("BeginFile", Params.beginFile().get(task))
				    .detail("BeginBlock", Params.beginBlock().get(task))
				    .detail("RestoreVersion", restoreVersion)
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "end_of_final_batch")
				    .detail("TaskInstance", THIS_ADDR);
			} else if (beginVersion < restoreVersion) {
				// If beginVersion is less than restoreVersion then do one more dispatch task to get there
				wait(success(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, restoreVersion, "", 0, batchSize)));

				TraceEvent("FileRestoreDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("BeginFile", Params.beginFile().get(task))
				    .detail("BeginBlock", Params.beginBlock().get(task))
				    .detail("RestoreVersion", restoreVersion)
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "apply_to_restore_version")
				    .detail("TaskInstance", THIS_ADDR);
			} else if (applyLag == 0) {
				// If apply lag is 0 then we are done so create the completion task
				wait(success(RestoreCompleteTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal())));

				TraceEvent("FileRestoreDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("BeginFile", Params.beginFile().get(task))
				    .detail("BeginBlock", Params.beginBlock().get(task))
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "restore_complete")
				    .detail("TaskInstance", THIS_ADDR);
			} else {
				// Applying of mutations is not yet finished so wait a small amount of time and then re-add this same
				// task.
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				wait(success(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, beginVersion, "", 0, batchSize)));

				TraceEvent("FileRestoreDispatch")
				    .detail("RestoreUID", restore.getUid())
				    .detail("BeginVersion", beginVersion)
				    .detail("ApplyLag", applyLag)
				    .detail("Decision", "apply_still_behind")
				    .detail("TaskInstance", THIS_ADDR);
			}

			// If adding to existing batch then task is joined with a batch future so set done future
			// Note that this must be done after joining at least one task with the batch future in case all other
			// blockers already finished.
			Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

			wait(taskBucket->finish(tr, task) && setDone);
			return Void();
		}

		// Start moving through the file list and queuing up blocks.  Only queue up to RESTORE_DISPATCH_ADDTASK_SIZE
		// blocks per Dispatch task and target batchSize total per batch but a batch must end on a complete version
		// boundary so exceed the limit if necessary to reach the end of a version of files.
		state std::vector<Future<Key>> addTaskFutures;
		state Version endVersion = files[0].version;
		state int blocksDispatched = 0;
		state int64_t beginBlock = Params.beginBlock().getOrDefault(task);
		state int i = 0;

		for (; i < files.size(); ++i) {
			RestoreConfig::RestoreFile& f = files[i];

			// Here we are "between versions" (prior to adding the first block of the first file of a new version) so
			// this is an opportunity to end the current dispatch batch (which must end on a version boundary) if the
			// batch size has been reached or exceeded
			if (f.version != endVersion && remainingInBatch <= 0) {
				// Next start will be at the first version after endVersion at the first file first block
				++endVersion;
				beginFile = "";
				beginBlock = 0;
				break;
			}

			// Set the starting point for the next task in case we stop inside this file
			endVersion = f.version;
			beginFile = f.fileName;

			state int64_t j = beginBlock * f.blockSize;
			// For each block of the file
			for (; j < f.fileSize; j += f.blockSize) {
				// Stop if we've reached the addtask limit
				if (blocksDispatched == taskBatchSize)
					break;

				if (f.isRange) {
					addTaskFutures.push_back(
					    RestoreRangeTaskFunc::addTask(tr,
					                                  taskBucket,
					                                  task,
					                                  f,
					                                  j,
					                                  std::min<int64_t>(f.blockSize, f.fileSize - j),
					                                  TaskCompletionKey::joinWith(allPartsDone)));
				} else {
					addTaskFutures.push_back(
					    RestoreLogDataTaskFunc::addTask(tr,
					                                    taskBucket,
					                                    task,
					                                    f,
					                                    j,
					                                    std::min<int64_t>(f.blockSize, f.fileSize - j),
					                                    TaskCompletionKey::joinWith(allPartsDone)));
				}

				// Increment beginBlock for the file and total blocks dispatched for this task
				++beginBlock;
				++blocksDispatched;
				--remainingInBatch;
			}

			// Stop if we've reached the addtask limit
			if (blocksDispatched == taskBatchSize)
				break;

			// We just completed an entire file so the next task should start at the file after this one within
			// endVersion (or later) if this iteration ends up being the last for this task
			beginFile = beginFile + '\x00';
			beginBlock = 0;

			TraceEvent("FileRestoreDispatchedFile")
			    .suppressFor(60)
			    .detail("RestoreUID", restore.getUid())
			    .detail("FileName", f.fileName)
			    .detail("TaskInstance", THIS_ADDR);
		}

		// If no blocks were dispatched then the next dispatch task should run now and be joined with the allPartsDone
		// future
		if (blocksDispatched == 0) {
			std::string decision;

			// If no files were dispatched either then the batch size wasn't large enough to catch all of the files at
			// the next lowest non-dispatched version, so increase the batch size.
			if (i == 0) {
				batchSize *= 2;
				decision = "increased_batch_size";
			} else
				decision = "all_files_were_empty";

			TraceEvent("FileRestoreDispatch")
			    .detail("RestoreUID", restore.getUid())
			    .detail("BeginVersion", beginVersion)
			    .detail("BeginFile", Params.beginFile().get(task))
			    .detail("BeginBlock", Params.beginBlock().get(task))
			    .detail("EndVersion", endVersion)
			    .detail("ApplyLag", applyLag)
			    .detail("BatchSize", batchSize)
			    .detail("Decision", decision)
			    .detail("TaskInstance", THIS_ADDR)
			    .detail("RemainingInBatch", remainingInBatch);

			wait(success(RestoreDispatchTaskFunc::addTask(tr,
			                                              taskBucket,
			                                              task,
			                                              endVersion,
			                                              beginFile,
			                                              beginBlock,
			                                              batchSize,
			                                              remainingInBatch,
			                                              TaskCompletionKey::joinWith((allPartsDone)))));

			// If adding to existing batch then task is joined with a batch future so set done future.
			// Note that this must be done after joining at least one task with the batch future in case all other
			// blockers already finished.
			Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

			wait(setDone && taskBucket->finish(tr, task));

			return Void();
		}

		// Increment the number of blocks dispatched in the restore config
		restore.filesBlocksDispatched().atomicOp(tr, blocksDispatched, MutationRef::Type::AddValue);

		// If beginFile is not empty then we had to stop in the middle of a version (possibly within a file) so we
		// cannot end the batch here because we do not know if we got all of the files and blocks from the last version
		// queued, so make sure remainingInBatch is at least 1.
		if (!beginFile.empty())
			remainingInBatch = std::max<int64_t>(1, remainingInBatch);

		// If more blocks need to be dispatched in this batch then add a follow-on task that is part of the allPartsDone
		// group which will won't wait to run and will add more block tasks.
		if (remainingInBatch > 0)
			addTaskFutures.push_back(RestoreDispatchTaskFunc::addTask(tr,
			                                                          taskBucket,
			                                                          task,
			                                                          endVersion,
			                                                          beginFile,
			                                                          beginBlock,
			                                                          batchSize,
			                                                          remainingInBatch,
			                                                          TaskCompletionKey::joinWith(allPartsDone)));
		else // Otherwise, add a follow-on task to continue after all previously dispatched blocks are done
			addTaskFutures.push_back(RestoreDispatchTaskFunc::addTask(tr,
			                                                          taskBucket,
			                                                          task,
			                                                          endVersion,
			                                                          beginFile,
			                                                          beginBlock,
			                                                          batchSize,
			                                                          0,
			                                                          TaskCompletionKey::noSignal(),
			                                                          allPartsDone));

		wait(waitForAll(addTaskFutures));

		// If adding to existing batch then task is joined with a batch future so set done future.
		Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

		wait(setDone && taskBucket->finish(tr, task));

		TraceEvent("FileRestoreDispatch")
		    .detail("RestoreUID", restore.getUid())
		    .detail("BeginVersion", beginVersion)
		    .detail("BeginFile", Params.beginFile().get(task))
		    .detail("BeginBlock", Params.beginBlock().get(task))
		    .detail("EndVersion", endVersion)
		    .detail("ApplyLag", applyLag)
		    .detail("BatchSize", batchSize)
		    .detail("Decision", "dispatched_files")
		    .detail("FilesDispatched", i)
		    .detail("BlocksDispatched", blocksDispatched)
		    .detail("TaskInstance", THIS_ADDR)
		    .detail("RemainingInBatch", remainingInBatch);

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 Version beginVersion,
	                                 std::string beginFile,
	                                 int64_t beginBlock,
	                                 int64_t batchSize,
	                                 int64_t remainingInBatch = 0,
	                                 TaskCompletionKey completionKey = TaskCompletionKey::noSignal(),
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));

		// Use high priority for dispatch tasks that have to queue more blocks for the current batch
		unsigned int priority = (remainingInBatch > 0) ? 1 : 0;
		state Reference<Task> task(
		    new Task(RestoreDispatchTaskFunc::name, RestoreDispatchTaskFunc::version, doneKey, priority));

		// Create a config from the parent task and bind it to the new task
		wait(RestoreConfig(parentTask).toTask(tr, task));
		Params.beginVersion().set(task, beginVersion);
		Params.batchSize().set(task, batchSize);
		Params.remainingInBatch().set(task, remainingInBatch);
		Params.beginBlock().set(task, beginBlock);
		Params.beginFile().set(task, beginFile);

		if (!waitFor) {
			return taskBucket->addTask(tr, task);
		}

		wait(waitFor->onSetAddTask(tr, taskBucket, task));
		return LiteralStringRef("OnSetAddTask");
	}

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return Void();
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef RestoreDispatchTaskFunc::name = LiteralStringRef("restore_dispatch");
REGISTER_TASKFUNC(RestoreDispatchTaskFunc);

ACTOR Future<std::string> restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state std::vector<KeyBackedTag> tags;
	if (tagName.size() == 0) {
		std::vector<KeyBackedTag> t = wait(getAllRestoreTags(tr));
		tags = t;
	} else
		tags.push_back(makeRestoreTag(tagName.toString()));

	state std::string result;
	state int i = 0;

	for (; i < tags.size(); ++i) {
		UidAndAbortedFlagT u = wait(tags[i].getD(tr));
		std::string s = wait(RestoreConfig(u.first).getFullStatus(tr));
		result.append(s);
		result.append("\n\n");
	}

	return result;
}

ACTOR Future<ERestoreState> abortRestore(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

	state KeyBackedTag tag = makeRestoreTag(tagName.toString());
	state Optional<UidAndAbortedFlagT> current = wait(tag.get(tr));
	if (!current.present())
		return ERestoreState::UNITIALIZED;

	state RestoreConfig restore(current.get().first);

	state ERestoreState status = wait(restore.stateEnum().getD(tr));
	state bool runnable = wait(restore.isRunnable(tr));

	if (!runnable)
		return status;

	restore.stateEnum().set(tr, ERestoreState::ABORTED);

	// Clear all of the ApplyMutations stuff
	restore.clearApplyMutationsKeys(tr);

	// Cancel the backup tasks on this tag
	wait(tag.cancel(tr));
	wait(unlockDatabase(tr, current.get().first));
	return ERestoreState::ABORTED;
}

ACTOR Future<ERestoreState> abortRestore(Database cx, Key tagName) {
	state Reference<ReadYourWritesTransaction> tr =
	    Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));

	loop {
		try {
			ERestoreState estate = wait(abortRestore(tr, tagName));
			if (estate != ERestoreState::ABORTED) {
				return estate;
			}
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));

	// Commit a dummy transaction before returning success, to ensure the mutation applier has stopped submitting
	// mutations
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);
			tr->addReadConflictRange(singleKeyRange(KeyRef()));
			tr->addWriteConflictRange(singleKeyRange(KeyRef()));
			wait(tr->commit());
			return ERestoreState::ABORTED;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

struct StartFullRestoreTaskFunc : RestoreTaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<Version> firstVersion() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	// Find all files needed for the restore and save them in the RestoreConfig for the task.
	// Update the total number of files and blocks and change state to starting.
	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state RestoreConfig restore(task);
		state Version restoreVersion;
		state Version beginVersion;
		state Reference<IBackupContainer> bc;
		state std::vector<KeyRange> ranges;
		state bool logsOnly;
		state bool inconsistentSnapshotOnly;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				wait(checkTaskVersion(tr->getDatabase(), task, name, version));
				wait(store(beginVersion, restore.beginVersion().getD(tr, Snapshot::False, ::invalidVersion)));

				wait(store(restoreVersion, restore.restoreVersion().getOrThrow(tr)));
				wait(store(ranges, restore.getRestoreRangesOrDefault(tr)));
				wait(store(logsOnly, restore.onlyApplyMutationLogs().getD(tr, Snapshot::False, false)));
				wait(store(inconsistentSnapshotOnly,
				           restore.inconsistentSnapshotOnly().getD(tr, Snapshot::False, false)));

				wait(taskBucket->keepRunning(tr, task));

				ERestoreState oldState = wait(restore.stateEnum().getD(tr));
				if (oldState != ERestoreState::QUEUED && oldState != ERestoreState::STARTING) {
					wait(restore.logError(cx,
					                      restore_error(),
					                      format("StartFullRestore: Encountered unexpected state(%d)", oldState),
					                      THIS));
					return Void();
				}
				restore.stateEnum().set(tr, ERestoreState::STARTING);
				restore.fileSet().clear(tr);
				restore.fileBlockCount().clear(tr);
				restore.fileCount().clear(tr);
				Reference<IBackupContainer> _bc = wait(restore.sourceContainer().getOrThrow(tr));
				bc = getBackupContainerWithProxy(_bc);

				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		tr->reset();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				Version destVersion = wait(tr->getReadVersion());
				TraceEvent("FileRestoreVersionUpgrade")
				    .detail("RestoreVersion", restoreVersion)
				    .detail("Dest", destVersion);
				if (destVersion <= restoreVersion) {
					TEST(true); // Forcing restored cluster to higher version
					tr->set(minRequiredCommitVersionKey, BinaryWriter::toValue(restoreVersion + 1, Unversioned()));
					wait(tr->commit());
				} else {
					break;
				}
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		state Version firstConsistentVersion = invalidVersion;
		if (beginVersion == invalidVersion) {
			beginVersion = 0;
		}
		state Standalone<VectorRef<KeyRangeRef>> keyRangesFilter;
		for (auto const& r : ranges) {
			keyRangesFilter.push_back_deep(keyRangesFilter.arena(), KeyRangeRef(r));
		}
		state Optional<RestorableFileSet> restorable =
		    wait(bc->getRestoreSet(restoreVersion, keyRangesFilter, logsOnly, beginVersion));
		if (!restorable.present())
			throw restore_missing_data();

		// Convert the two lists in restorable (logs and ranges) to a single list of RestoreFiles.
		// Order does not matter, they will be put in order when written to the restoreFileMap below.
		state std::vector<RestoreConfig::RestoreFile> files;
		if (!logsOnly) {
			beginVersion = restorable.get().snapshot.beginVersion;
			if (!inconsistentSnapshotOnly) {
				for (const RangeFile& f : restorable.get().ranges) {
					files.push_back({ f.version, f.fileName, true, f.blockSize, f.fileSize });
					// In a restore with both snapshots and logs, the firstConsistentVersion is the highest version of
					// any range file.
					firstConsistentVersion = std::max(firstConsistentVersion, f.version);
				}
			} else {
				for (int i = 0; i < restorable.get().ranges.size(); ++i) {
					const RangeFile& f = restorable.get().ranges[i];
					files.push_back({ f.version, f.fileName, true, f.blockSize, f.fileSize });
					// In inconsistentSnapshotOnly mode, if all range files have the same version, then it is the
					// firstConsistentVersion, otherwise unknown (use -1).
					if (i != 0 && f.version != firstConsistentVersion) {
						firstConsistentVersion = invalidVersion;
					} else {
						firstConsistentVersion = f.version;
					}
				}
			}
		} else {
			// In logs-only (incremental) mode, the firstConsistentVersion should just be restore.beginVersion().
			firstConsistentVersion = beginVersion;
		}
		if (!inconsistentSnapshotOnly) {
			for (const LogFile& f : restorable.get().logs) {
				files.push_back({ f.beginVersion, f.fileName, false, f.blockSize, f.fileSize, f.endVersion });
			}
		}
		// First version for which log data should be applied
		Params.firstVersion().set(task, beginVersion);

		tr->reset();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				restore.firstConsistentVersion().set(tr, firstConsistentVersion);
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		state std::vector<RestoreConfig::RestoreFile>::iterator start = files.begin();
		state std::vector<RestoreConfig::RestoreFile>::iterator end = files.end();

		tr->reset();
		while (start != end) {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				wait(taskBucket->keepRunning(tr, task));

				state std::vector<RestoreConfig::RestoreFile>::iterator i = start;

				state int txBytes = 0;
				state int nFileBlocks = 0;
				state int nFiles = 0;
				auto fileSet = restore.fileSet();
				for (; i != end && txBytes < 1e6; ++i) {
					txBytes += fileSet.insert(tr, *i);
					nFileBlocks += (i->fileSize + i->blockSize - 1) / i->blockSize;
					++nFiles;
				}

				restore.fileCount().atomicOp(tr, nFiles, MutationRef::Type::AddValue);
				restore.fileBlockCount().atomicOp(tr, nFileBlocks, MutationRef::Type::AddValue);

				wait(tr->commit());

				TraceEvent("FileRestoreLoadedFiles")
				    .detail("RestoreUID", restore.getUid())
				    .detail("FileCount", nFiles)
				    .detail("FileBlockCount", nFileBlocks)
				    .detail("TransactionBytes", txBytes)
				    .detail("TaskInstance", THIS_ADDR);

				start = i;
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state RestoreConfig restore(task);

		state Version firstVersion = Params.firstVersion().getOrDefault(task, invalidVersion);
		if (firstVersion == invalidVersion) {
			wait(restore.logError(
			    tr->getDatabase(), restore_missing_data(), "StartFullRestore: The backup had no data.", THIS));
			std::string tag = wait(restore.tag().getD(tr));
			wait(success(abortRestore(tr, StringRef(tag))));
			return Void();
		}

		restore.stateEnum().set(tr, ERestoreState::RUNNING);

		// Set applyMutation versions

		restore.setApplyBeginVersion(tr, firstVersion);
		restore.setApplyEndVersion(tr, firstVersion);

		// Apply range data and log data in order
		wait(success(RestoreDispatchTaskFunc::addTask(
		    tr, taskBucket, task, 0, "", 0, CLIENT_KNOBS->RESTORE_DISPATCH_BATCH_SIZE)));

		wait(taskBucket->finish(tr, task));
		state Future<Optional<bool>> logsOnly = restore.onlyApplyMutationLogs().get(tr);
		wait(success(logsOnly));
		if (logsOnly.get().present() && logsOnly.get().get()) {
			// If this is an incremental restore, we need to set the applyMutationsMapPrefix
			// to the earliest log version so no mutations are missed
			Value versionEncoded = BinaryWriter::toValue(Params.firstVersion().get(task), Unversioned());
			wait(krmSetRange(tr, restore.applyMutationsMapPrefix(), normalKeys, versionEncoded));
		}
		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 UID uid,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		Key doneKey = wait(completionKey.get(tr, taskBucket));
		state Reference<Task> task(
		    new Task(StartFullRestoreTaskFunc::name, StartFullRestoreTaskFunc::version, doneKey));

		state RestoreConfig restore(uid);
		// Bind the restore config to the new task
		wait(restore.toTask(tr, task));

		if (!waitFor) {
			return taskBucket->addTask(tr, task);
		}

		wait(waitFor->onSetAddTask(tr, taskBucket, task));
		return LiteralStringRef("OnSetAddTask");
	}

	StringRef getName() const override { return name; };

	Future<Void> execute(Database cx,
	                     Reference<TaskBucket> tb,
	                     Reference<FutureBucket> fb,
	                     Reference<Task> task) override {
		return _execute(cx, tb, fb, task);
	};
	Future<Void> finish(Reference<ReadYourWritesTransaction> tr,
	                    Reference<TaskBucket> tb,
	                    Reference<FutureBucket> fb,
	                    Reference<Task> task) override {
		return _finish(tr, tb, fb, task);
	};
};
StringRef StartFullRestoreTaskFunc::name = LiteralStringRef("restore_start");
REGISTER_TASKFUNC(StartFullRestoreTaskFunc);
} // namespace fileBackup

struct LogInfo : public ReferenceCounted<LogInfo> {
	std::string fileName;
	Reference<IAsyncFile> logFile;
	Version beginVersion;
	Version endVersion;
	int64_t offset;

	LogInfo() : offset(0){};
};

class FileBackupAgentImpl {
public:
	static constexpr int MAX_RESTORABLE_FILE_METASECTION_BYTES = 1024 * 8;

	// Parallel restore
	ACTOR static Future<Void> parallelRestoreFinish(Database cx, UID randomUID, UnlockDB unlockDB = UnlockDB::True) {
		state ReadYourWritesTransaction tr(cx);
		state Optional<Value> restoreRequestDoneKeyValue;
		TraceEvent("FastRestoreToolWaitForRestoreToFinish").detail("DBLock", randomUID);
		// TODO: register watch first and then check if the key exist
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> _restoreRequestDoneKeyValue = wait(tr.get(restoreRequestDoneKey));
				restoreRequestDoneKeyValue = _restoreRequestDoneKeyValue;
				// Restore may finish before restoreTool waits on the restore finish event.
				if (restoreRequestDoneKeyValue.present()) {
					break;
				} else {
					state Future<Void> watchForRestoreRequestDone = tr.watch(restoreRequestDoneKey);
					wait(tr.commit());
					wait(watchForRestoreRequestDone);
					break;
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		TraceEvent("FastRestoreToolRestoreFinished")
		    .detail("ClearRestoreRequestDoneKey", restoreRequestDoneKeyValue.present());
		// Only this agent can clear the restoreRequestDoneKey
		wait(runRYWTransaction(cx, [](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->clear(restoreRequestDoneKey);
			return Void();
		}));

		if (unlockDB) {
			TraceEvent("FastRestoreToolRestoreFinished").detail("UnlockDBStart", randomUID);
			wait(unlockDatabase(cx, randomUID));
			TraceEvent("FastRestoreToolRestoreFinished").detail("UnlockDBFinish", randomUID);
		} else {
			TraceEvent("FastRestoreToolRestoreFinished").detail("DBLeftLockedAfterRestore", randomUID);
		}

		return Void();
	}

	ACTOR static Future<Void> submitParallelRestore(Database cx,
	                                                Key backupTag,
	                                                Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                                                Key bcUrl,
	                                                Optional<std::string> proxy,
	                                                Version targetVersion,
	                                                LockDB lockDB,
	                                                UID randomUID,
	                                                Key addPrefix,
	                                                Key removePrefix) {
		// Sanity check backup is valid
		state Reference<IBackupContainer> bc = IBackupContainer::openContainer(bcUrl.toString(), proxy, {});
		state BackupDescription desc = wait(bc->describeBackup());
		wait(desc.resolveVersionTimes(cx));

		if (targetVersion == invalidVersion && desc.maxRestorableVersion.present()) {
			targetVersion = desc.maxRestorableVersion.get();
			TraceEvent(SevWarn, "FastRestoreSubmitRestoreRequestWithInvalidTargetVersion")
			    .detail("OverrideTargetVersion", targetVersion);
		}

		Optional<RestorableFileSet> restoreSet = wait(bc->getRestoreSet(targetVersion));

		if (!restoreSet.present()) {
			TraceEvent(SevWarn, "FileBackupAgentRestoreNotPossible")
			    .detail("BackupContainer", bc->getURL())
			    .detail("TargetVersion", targetVersion);
			throw restore_invalid_version();
		}

		TraceEvent("FastRestoreSubmitRestoreRequest")
		    .detail("BackupDesc", desc.toString())
		    .detail("TargetVersion", targetVersion);

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state int restoreIndex = 0;
		state int numTries = 0;
		// lock DB for restore
		loop {
			try {
				if (lockDB) {
					wait(lockDatabase(cx, randomUID));
				}
				wait(checkDatabaseLock(tr, randomUID));

				TraceEvent("FastRestoreToolSubmitRestoreRequests").detail("DBIsLocked", randomUID);
				break;
			} catch (Error& e) {
				TraceEvent(numTries > 50 ? SevError : SevInfo, "FastRestoreToolSubmitRestoreRequestsMayFail")
				    .error(e)
				    .detail("Reason", "DB is not properly locked")
				    .detail("ExpectedLockID", randomUID);
				numTries++;
				wait(tr->onError(e));
			}
		}

		// set up restore request
		tr->reset();
		numTries = 0;
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				// Note: we always lock DB here in case DB is modified at the bacupRanges boundary.
				for (restoreIndex = 0; restoreIndex < backupRanges.size(); restoreIndex++) {
					auto range = backupRanges[restoreIndex];
					Standalone<StringRef> restoreTag(backupTag.toString() + "_" + std::to_string(restoreIndex));
					// Register the request request in DB, which will be picked up by restore worker leader
					struct RestoreRequest restoreRequest(restoreIndex,
					                                     restoreTag,
					                                     bcUrl,
					                                     proxy,
					                                     targetVersion,
					                                     range,
					                                     deterministicRandom()->randomUniqueID(),
					                                     addPrefix,
					                                     removePrefix);
					tr->set(restoreRequestKeyFor(restoreRequest.index), restoreRequestValue(restoreRequest));
				}
				tr->set(restoreRequestTriggerKey,
				        restoreRequestTriggerValue(deterministicRandom()->randomUniqueID(), backupRanges.size()));
				wait(tr->commit()); // Trigger restore
				break;
			} catch (Error& e) {
				TraceEvent(numTries > 50 ? SevError : SevInfo, "FastRestoreToolSubmitRestoreRequestsRetry")
				    .error(e)
				    .detail("RestoreIndex", restoreIndex);
				numTries++;
				wait(tr->onError(e));
			}
		}
		return Void();
	}

	// This method will return the final status of the backup at tag, and return the URL that was used on the tag
	// when that status value was read.
	ACTOR static Future<EBackupState> waitBackup(FileBackupAgent* backupAgent,
	                                             Database cx,
	                                             std::string tagName,
	                                             StopWhenDone stopWhenDone,
	                                             Reference<IBackupContainer>* pContainer = nullptr,
	                                             UID* pUID = nullptr) {
		state std::string backTrace;
		state KeyBackedTag tag = makeBackupTag(tagName);

		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			try {
				state Optional<UidAndAbortedFlagT> oldUidAndAborted = wait(tag.get(tr));
				if (!oldUidAndAborted.present()) {
					return EBackupState::STATE_NEVERRAN;
				}

				state BackupConfig config(oldUidAndAborted.get().first);
				state EBackupState status =
				    wait(config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN));

				// Break, if one of the following is true
				//  - no longer runnable
				//  - in differential mode (restorable) and stopWhenDone is not enabled
				if (!FileBackupAgent::isRunnable(status) ||
				    ((!stopWhenDone) && (EBackupState::STATE_RUNNING_DIFFERENTIAL == status))) {

					if (pContainer != nullptr) {
						Reference<IBackupContainer> c =
						    wait(config.backupContainer().getOrThrow(tr, Snapshot::False, backup_invalid_info()));
						*pContainer = fileBackup::getBackupContainerWithProxy(c);
					}

					if (pUID != nullptr) {
						*pUID = oldUidAndAborted.get().first;
					}

					return status;
				}

				state Future<Void> watchFuture = tr->watch(config.stateEnum().key);
				wait(tr->commit());
				wait(watchFuture);
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	// TODO: Get rid of all of these confusing boolean flags
	ACTOR static Future<Void> submitBackup(FileBackupAgent* backupAgent,
	                                       Reference<ReadYourWritesTransaction> tr,
	                                       Key outContainer,
	                                       Optional<std::string> proxy,
	                                       int initialSnapshotIntervalSeconds,
	                                       int snapshotIntervalSeconds,
	                                       std::string tagName,
	                                       Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                                       StopWhenDone stopWhenDone,
	                                       UsePartitionedLog partitionedLog,
	                                       IncrementalBackupOnly incrementalBackupOnly,
	                                       Optional<std::string> encryptionKeyFileName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

		TraceEvent(SevInfo, "FBA_SubmitBackup")
		    .detail("TagName", tagName.c_str())
		    .detail("StopWhenDone", stopWhenDone)
		    .detail("UsePartitionedLog", partitionedLog)
		    .detail("OutContainer", outContainer.toString());

		state KeyBackedTag tag = makeBackupTag(tagName);
		Optional<UidAndAbortedFlagT> uidAndAbortedFlag = wait(tag.get(tr));
		if (uidAndAbortedFlag.present()) {
			state BackupConfig prevConfig(uidAndAbortedFlag.get().first);
			state EBackupState prevBackupStatus =
			    wait(prevConfig.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN));
			if (FileBackupAgent::isRunnable(prevBackupStatus)) {
				throw backup_duplicate();
			}

			// Now is time to clear prev backup config space. We have no more use for it.
			prevConfig.clear(tr);
		}

		state BackupConfig config(deterministicRandom()->randomUniqueID());
		state UID uid = config.getUid();

		// This check will ensure that current backupUid is later than the last backup Uid
		state Standalone<StringRef> nowStr = BackupAgentBase::getCurrentTime();
		state std::string backupContainer = outContainer.toString();

		// To be consistent with directory handling behavior since FDB backup was first released, if the container
		// string describes a local directory then "/backup-<timestamp>" will be added to it.
		if (backupContainer.find("file://") == 0) {
			backupContainer = joinPath(backupContainer, std::string("backup-") + nowStr.toString());
		}

		state Reference<IBackupContainer> bc =
		    IBackupContainer::openContainer(backupContainer, proxy, encryptionKeyFileName);
		try {
			wait(timeoutError(bc->create(), 30));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			fprintf(stderr, "ERROR: Could not create backup container: %s\n", e.what());
			throw backup_error();
		}

		Optional<Value> lastBackupTimestamp = wait(backupAgent->lastBackupTimestamp().get(tr));

		if ((lastBackupTimestamp.present()) && (lastBackupTimestamp.get() >= nowStr)) {
			fprintf(stderr,
			        "ERROR: The last backup `%s' happened in the future.\n",
			        printable(lastBackupTimestamp.get()).c_str());
			throw backup_error();
		}

		KeyRangeMap<int> backupRangeSet;
		for (auto& backupRange : backupRanges) {
			backupRangeSet.insert(backupRange, 1);
		}

		backupRangeSet.coalesce(allKeys);
		state std::vector<KeyRange> normalizedRanges;

		for (auto& backupRange : backupRangeSet.ranges()) {
			if (backupRange.value()) {
				normalizedRanges.push_back(KeyRange(KeyRangeRef(backupRange.range().begin, backupRange.range().end)));
			}
		}

		config.clear(tr);

		state Key destUidValue(BinaryWriter::toValue(uid, Unversioned()));
		if (normalizedRanges.size() == 1) {
			RangeResult existingDestUidValues = wait(
			    tr->getRange(KeyRangeRef(destUidLookupPrefix, strinc(destUidLookupPrefix)), CLIENT_KNOBS->TOO_MANY));
			bool found = false;
			for (auto it : existingDestUidValues) {
				if (BinaryReader::fromStringRef<KeyRange>(it.key.removePrefix(destUidLookupPrefix), IncludeVersion()) ==
				    normalizedRanges[0]) {
					destUidValue = it.value;
					found = true;
					break;
				}
			}
			if (!found) {
				destUidValue = BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned());
				tr->set(
				    BinaryWriter::toValue(normalizedRanges[0], IncludeVersion(ProtocolVersion::withSharedMutations()))
				        .withPrefix(destUidLookupPrefix),
				    destUidValue);
			}
		}

		tr->set(config.getUidAsKey().withPrefix(destUidValue).withPrefix(backupLatestVersionsPrefix),
		        BinaryWriter::toValue<Version>(tr->getReadVersion().get(), Unversioned()));
		config.destUidValue().set(tr, destUidValue);

		// Point the tag to this new uid
		tag.set(tr, { uid, false });

		backupAgent->lastBackupTimestamp().set(tr, nowStr);

		// Set the backup keys
		config.tag().set(tr, tagName);
		config.stateEnum().set(tr, EBackupState::STATE_SUBMITTED);
		config.backupContainer().set(tr, bc);
		config.stopWhenDone().set(tr, stopWhenDone);
		config.backupRanges().set(tr, normalizedRanges);
		config.initialSnapshotIntervalSeconds().set(tr, initialSnapshotIntervalSeconds);
		config.snapshotIntervalSeconds().set(tr, snapshotIntervalSeconds);
		config.partitionedLogEnabled().set(tr, partitionedLog);
		config.incrementalBackupOnly().set(tr, incrementalBackupOnly);

		Key taskKey = wait(fileBackup::StartFullBackupTaskFunc::addTask(
		    tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal()));

		return Void();
	}

	ACTOR static Future<Void> submitRestore(FileBackupAgent* backupAgent,
	                                        Reference<ReadYourWritesTransaction> tr,
	                                        Key tagName,
	                                        Key backupURL,
	                                        Optional<std::string> proxy,
	                                        Standalone<VectorRef<KeyRangeRef>> ranges,
	                                        Version restoreVersion,
	                                        Key addPrefix,
	                                        Key removePrefix,
	                                        LockDB lockDB,
	                                        OnlyApplyMutationLogs onlyApplyMutationLogs,
	                                        InconsistentSnapshotOnly inconsistentSnapshotOnly,
	                                        Version beginVersion,
	                                        UID uid) {
		KeyRangeMap<int> restoreRangeSet;
		for (auto& range : ranges) {
			restoreRangeSet.insert(range, 1);
		}
		restoreRangeSet.coalesce(allKeys);
		state std::vector<KeyRange> restoreRanges;
		for (auto& restoreRange : restoreRangeSet.ranges()) {
			if (restoreRange.value()) {
				restoreRanges.push_back(KeyRange(KeyRangeRef(restoreRange.range().begin, restoreRange.range().end)));
			}
		}
		for (auto& restoreRange : restoreRanges) {
			ASSERT(restoreRange.begin.startsWith(removePrefix) && restoreRange.end.startsWith(removePrefix));
		}

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		// Get old restore config for this tag
		state KeyBackedTag tag = makeRestoreTag(tagName.toString());
		state Optional<UidAndAbortedFlagT> oldUidAndAborted = wait(tag.get(tr));
		if (oldUidAndAborted.present()) {
			if (oldUidAndAborted.get().first == uid) {
				if (oldUidAndAborted.get().second) {
					throw restore_duplicate_uid();
				} else {
					return Void();
				}
			}

			state RestoreConfig oldRestore(oldUidAndAborted.get().first);

			// Make sure old restore for this tag is not runnable
			bool runnable = wait(oldRestore.isRunnable(tr));

			if (runnable) {
				throw restore_duplicate_tag();
			}

			// Clear the old restore config
			oldRestore.clear(tr);
		}

		state int index;
		for (index = 0; index < restoreRanges.size(); index++) {
			KeyRange restoreIntoRange = KeyRangeRef(restoreRanges[index].begin, restoreRanges[index].end)
			                                .removePrefix(removePrefix)
			                                .withPrefix(addPrefix);
			RangeResult existingRows = wait(tr->getRange(restoreIntoRange, 1));
			if (existingRows.size() > 0 && !onlyApplyMutationLogs) {
				throw restore_destination_not_empty();
			}
		}
		// Make new restore config
		state RestoreConfig restore(uid);

		// Point the tag to the new uid
		tag.set(tr, { uid, false });

		Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupURL.toString(), proxy, {});

		// Configure the new restore
		restore.tag().set(tr, tagName.toString());
		restore.sourceContainer().set(tr, bc);
		restore.stateEnum().set(tr, ERestoreState::QUEUED);
		restore.restoreVersion().set(tr, restoreVersion);
		restore.onlyApplyMutationLogs().set(tr, onlyApplyMutationLogs);
		restore.inconsistentSnapshotOnly().set(tr, inconsistentSnapshotOnly);
		restore.beginVersion().set(tr, beginVersion);
		if (BUGGIFY && restoreRanges.size() == 1) {
			restore.restoreRange().set(tr, restoreRanges[0]);
		} else {
			restore.restoreRanges().set(tr, restoreRanges);
		}
		// this also sets restore.add/removePrefix.
		restore.initApplyMutations(tr, addPrefix, removePrefix);

		Key taskKey = wait(fileBackup::StartFullRestoreTaskFunc::addTask(
		    tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal()));

		if (lockDB)
			wait(lockDatabase(tr, uid));
		else
			wait(checkDatabaseLock(tr, uid));

		return Void();
	}

	// This method will return the final status of the backup
	ACTOR static Future<ERestoreState> waitRestore(Database cx, Key tagName, Verbose verbose) {
		state ERestoreState status;
		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state KeyBackedTag tag = makeRestoreTag(tagName.toString());
				Optional<UidAndAbortedFlagT> current = wait(tag.get(tr));
				if (!current.present()) {
					if (verbose)
						printf("waitRestore: Tag: %s  State: %s\n",
						       tagName.toString().c_str(),
						       FileBackupAgent::restoreStateText(ERestoreState::UNITIALIZED).toString().c_str());
					return ERestoreState::UNITIALIZED;
				}

				state RestoreConfig restore(current.get().first);

				if (verbose) {
					state std::string details = wait(restore.getProgress(tr));
					printf("%s\n", details.c_str());
				}

				ERestoreState status_ = wait(restore.stateEnum().getD(tr));
				status = status_;
				state bool runnable = wait(restore.isRunnable(tr));

				// State won't change from here
				if (!runnable)
					break;

				// Wait for a change
				state Future<Void> watchFuture = tr->watch(restore.stateEnum().key);
				wait(tr->commit());
				if (verbose)
					wait(watchFuture || delay(1));
				else
					wait(watchFuture);
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		return status;
	}

	ACTOR static Future<Void> discontinueBackup(FileBackupAgent* backupAgent,
	                                            Reference<ReadYourWritesTransaction> tr,
	                                            Key tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state KeyBackedTag tag = makeBackupTag(tagName.toString());
		state UidAndAbortedFlagT current = wait(tag.getOrThrow(tr, Snapshot::False, backup_unneeded()));
		state BackupConfig config(current.first);
		state EBackupState status = wait(config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN));

		if (!FileBackupAgent::isRunnable(status)) {
			throw backup_unneeded();
		}

		// If the backup is already restorable then 'mostly' abort it - cancel all tasks via the tag
		// and clear the mutation logging config and data - but set its state as COMPLETED instead of ABORTED.
		state Optional<Version> latestRestorableVersion = wait(config.getLatestRestorableVersion(tr));

		TraceEvent(SevInfo, "FBA_DiscontinueBackup")
		    .detail("AlreadyRestorable", latestRestorableVersion.present() ? "Yes" : "No")
		    .detail("TagName", tag.tagName.c_str())
		    .detail("Status", BackupAgentBase::getStateText(status));

		if (latestRestorableVersion.present()) {
			// Cancel all backup tasks through tag
			wait(tag.cancel(tr));

			tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

			state Key destUidValue = wait(config.destUidValue().getOrThrow(tr));
			wait(success(tr->getReadVersion()));
			wait(eraseLogData(tr, config.getUidAsKey(), destUidValue) &&
			     fileBackup::clearBackupStartID(tr, config.getUid()));

			config.stateEnum().set(tr, EBackupState::STATE_COMPLETED);

			return Void();
		}

		state bool stopWhenDone = wait(config.stopWhenDone().getOrThrow(tr));

		if (stopWhenDone) {
			throw backup_duplicate();
		}

		config.stopWhenDone().set(tr, true);

		return Void();
	}

	ACTOR static Future<Void> abortBackup(FileBackupAgent* backupAgent,
	                                      Reference<ReadYourWritesTransaction> tr,
	                                      std::string tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state KeyBackedTag tag = makeBackupTag(tagName);
		state UidAndAbortedFlagT current = wait(tag.getOrThrow(tr, Snapshot::False, backup_unneeded()));

		state BackupConfig config(current.first);
		state Key destUidValue = wait(config.destUidValue().getOrThrow(tr));
		EBackupState status = wait(config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN));

		if (!backupAgent->isRunnable(status)) {
			throw backup_unneeded();
		}

		TraceEvent(SevInfo, "FBA_AbortBackup")
		    .detail("TagName", tagName.c_str())
		    .detail("Status", BackupAgentBase::getStateText(status));

		// Cancel backup task through tag
		wait(tag.cancel(tr));

		wait(eraseLogData(tr, config.getUidAsKey(), destUidValue) &&
		     fileBackup::clearBackupStartID(tr, config.getUid()));

		config.stateEnum().set(tr, EBackupState::STATE_ABORTED);

		return Void();
	}

	ACTOR static Future<Void> changePause(FileBackupAgent* backupAgent, Database db, bool pause) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(db));
		state Future<Void> change = backupAgent->taskBucket->changePause(db, pause);

		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			try {
				tr->set(backupPausedKey, pause ? LiteralStringRef("1") : LiteralStringRef("0"));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
		wait(change);
		TraceEvent("FileBackupAgentChangePaused").detail("Action", pause ? "Paused" : "Resumed");
		return Void();
	}

	struct TimestampedVersion {
		Optional<Version> version;
		Optional<int64_t> epochs;

		bool present() const { return version.present(); }

		JsonBuilderObject toJSON() const {
			JsonBuilderObject doc;
			if (version.present()) {
				doc.setKey("Version", version.get());
				if (epochs.present()) {
					doc.setKey("EpochSeconds", epochs.get());
					doc.setKey("Timestamp", timeStampToString(epochs));
				}
			}
			return doc;
		}
	};

	// Helper actor for generating status
	// If f is present, lookup epochs using timekeeper and tr, return TimestampedVersion
	ACTOR static Future<TimestampedVersion> getTimestampedVersion(Reference<ReadYourWritesTransaction> tr,
	                                                              Future<Optional<Version>> f) {
		state TimestampedVersion tv;
		wait(store(tv.version, f));
		if (tv.version.present()) {
			wait(store(tv.epochs, timeKeeperEpochsFromVersion(tv.version.get(), tr)));
		}
		return tv;
	}

	ACTOR static Future<std::string> getStatusJSON(FileBackupAgent* backupAgent, Database cx, std::string tagName) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

		loop {
			try {
				state JsonBuilderObject doc;
				doc.setKey("SchemaVersion", "1.0.0");

				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state KeyBackedTag tag = makeBackupTag(tagName);
				state Optional<UidAndAbortedFlagT> uidAndAbortedFlag;
				state Optional<Value> paused;
				state Version recentReadVersion;

				wait(store(paused, tr->get(backupAgent->taskBucket->getPauseKey())) &&
				     store(uidAndAbortedFlag, tag.get(tr)) && store(recentReadVersion, tr->getReadVersion()));

				doc.setKey("BackupAgentsPaused", paused.present());
				doc.setKey("Tag", tag.tagName);

				if (uidAndAbortedFlag.present()) {
					doc.setKey("UID", uidAndAbortedFlag.get().first.toString());

					state BackupConfig config(uidAndAbortedFlag.get().first);

					state EBackupState backupState =
					    wait(config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN));
					JsonBuilderObject statusDoc;
					statusDoc.setKey("Name", BackupAgentBase::getStateName(backupState));
					statusDoc.setKey("Description", BackupAgentBase::getStateText(backupState));
					statusDoc.setKey("Completed", backupState == EBackupState::STATE_COMPLETED);
					statusDoc.setKey("Running", BackupAgentBase::isRunnable(backupState));
					doc.setKey("Status", statusDoc);

					state Future<Void> done = Void();

					if (backupState != EBackupState::STATE_NEVERRAN) {
						state Reference<IBackupContainer> bc;
						state TimestampedVersion latestRestorable;

						wait(
						    store(latestRestorable, getTimestampedVersion(tr, config.getLatestRestorableVersion(tr))) &&
						    store(bc, config.backupContainer().getOrThrow(tr)));
						bc = fileBackup::getBackupContainerWithProxy(bc);

						doc.setKey("Restorable", latestRestorable.present());

						if (latestRestorable.present()) {
							JsonBuilderObject o = latestRestorable.toJSON();
							if (backupState != EBackupState::STATE_COMPLETED) {
								o.setKey("LagSeconds",
								         (recentReadVersion - latestRestorable.version.get()) /
								             CLIENT_KNOBS->CORE_VERSIONSPERSECOND);
							}
							doc.setKey("LatestRestorablePoint", o);
						}
						doc.setKey("DestinationURL", bc->getURL());
					}

					if (backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL ||
					    backupState == EBackupState::STATE_RUNNING) {
						state int64_t snapshotInterval;
						state int64_t logBytesWritten;
						state int64_t rangeBytesWritten;
						state bool stopWhenDone;
						state TimestampedVersion snapshotBegin;
						state TimestampedVersion snapshotTargetEnd;
						state TimestampedVersion latestLogEnd;
						state TimestampedVersion latestSnapshotEnd;
						state TimestampedVersion snapshotLastDispatch;
						state Optional<int64_t> snapshotLastDispatchShardsBehind;

						wait(
						    store(snapshotInterval, config.snapshotIntervalSeconds().getOrThrow(tr)) &&
						    store(logBytesWritten, config.logBytesWritten().getD(tr)) &&
						    store(rangeBytesWritten, config.rangeBytesWritten().getD(tr)) &&
						    store(stopWhenDone, config.stopWhenDone().getOrThrow(tr)) &&
						    store(snapshotBegin, getTimestampedVersion(tr, config.snapshotBeginVersion().get(tr))) &&
						    store(snapshotTargetEnd,
						          getTimestampedVersion(tr, config.snapshotTargetEndVersion().get(tr))) &&
						    store(latestLogEnd, getTimestampedVersion(tr, config.latestLogEndVersion().get(tr))) &&
						    store(latestSnapshotEnd,
						          getTimestampedVersion(tr, config.latestSnapshotEndVersion().get(tr))) &&
						    store(snapshotLastDispatch,
						          getTimestampedVersion(tr, config.snapshotDispatchLastVersion().get(tr))) &&
						    store(snapshotLastDispatchShardsBehind, config.snapshotDispatchLastShardsBehind().get(tr)));

						doc.setKey("StopAfterSnapshot", stopWhenDone);
						doc.setKey("SnapshotIntervalSeconds", snapshotInterval);
						doc.setKey("LogBytesWritten", logBytesWritten);
						doc.setKey("RangeBytesWritten", rangeBytesWritten);

						if (latestLogEnd.present()) {
							doc.setKey("LatestLogEnd", latestLogEnd.toJSON());
						}

						if (latestSnapshotEnd.present()) {
							doc.setKey("LatestSnapshotEnd", latestSnapshotEnd.toJSON());
						}

						JsonBuilderObject snapshot;

						if (snapshotBegin.present()) {
							snapshot.setKey("Begin", snapshotBegin.toJSON());

							if (snapshotTargetEnd.present()) {
								snapshot.setKey("EndTarget", snapshotTargetEnd.toJSON());

								Version interval = snapshotTargetEnd.version.get() - snapshotBegin.version.get();
								snapshot.setKey("IntervalSeconds", interval / CLIENT_KNOBS->CORE_VERSIONSPERSECOND);

								Version elapsed = recentReadVersion - snapshotBegin.version.get();
								double progress = (interval > 0) ? (100.0 * elapsed / interval) : 100;
								snapshot.setKey("ExpectedProgress", progress);
							}

							JsonBuilderObject dispatchDoc = snapshotLastDispatch.toJSON();
							if (snapshotLastDispatchShardsBehind.present()) {
								dispatchDoc.setKey("ShardsBehind", snapshotLastDispatchShardsBehind.get());
							}
							snapshot.setKey("LastDispatch", dispatchDoc);
						}

						doc.setKey("CurrentSnapshot", snapshot);
					}

					KeyBackedMap<int64_t, std::pair<std::string, Version>>::PairsType errors =
					    wait(config.lastErrorPerType().getRange(
					        tr, 0, std::numeric_limits<int>::max(), CLIENT_KNOBS->TOO_MANY));
					JsonBuilderArray errorList;
					for (auto& e : errors) {
						std::string msg = e.second.first;
						Version ver = e.second.second;

						JsonBuilderObject errDoc;
						errDoc.setKey("Message", msg.c_str());
						errDoc.setKey("RelativeSeconds",
						              (ver - recentReadVersion) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND);
					}
					doc.setKey("Errors", errorList);
				}

				return doc.getJson();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<std::string> getStatus(FileBackupAgent* backupAgent,
	                                           Database cx,
	                                           ShowErrors showErrors,
	                                           std::string tagName) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state std::string statusText;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state KeyBackedTag tag;
				state BackupConfig config;
				state EBackupState backupState;

				statusText = "";
				tag = makeBackupTag(tagName);
				state Optional<UidAndAbortedFlagT> uidAndAbortedFlag = wait(tag.get(tr));
				state Future<Optional<Value>> fPaused = tr->get(backupAgent->taskBucket->getPauseKey());
				if (uidAndAbortedFlag.present()) {
					config = BackupConfig(uidAndAbortedFlag.get().first);
					EBackupState status =
					    wait(config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN));
					backupState = status;
				}

				if (!uidAndAbortedFlag.present() || backupState == EBackupState::STATE_NEVERRAN) {
					statusText += "No previous backups found.\n";
				} else {
					state std::string backupStatus(BackupAgentBase::getStateText(backupState));
					state Reference<IBackupContainer> bc;
					state Optional<Version> latestRestorableVersion;
					state Version recentReadVersion;

					wait(store(latestRestorableVersion, config.getLatestRestorableVersion(tr)) &&
					     store(bc, config.backupContainer().getOrThrow(tr)) &&
					     store(recentReadVersion, tr->getReadVersion()));
					bc = fileBackup::getBackupContainerWithProxy(bc);

					bool snapshotProgress = false;

					switch (backupState) {
					case EBackupState::STATE_SUBMITTED:
						statusText += "The backup on tag `" + tagName + "' is in progress (just started) to " +
						              bc->getURL() + ".\n";
						break;
					case EBackupState::STATE_RUNNING:
						statusText += "The backup on tag `" + tagName + "' is in progress to " + bc->getURL() + ".\n";
						snapshotProgress = true;
						break;
					case EBackupState::STATE_RUNNING_DIFFERENTIAL:
						statusText += "The backup on tag `" + tagName + "' is restorable but continuing to " +
						              bc->getURL() + ".\n";
						snapshotProgress = true;
						break;
					case EBackupState::STATE_COMPLETED:
						statusText += "The previous backup on tag `" + tagName + "' at " + bc->getURL() +
						              " completed at version " + format("%lld", latestRestorableVersion.orDefault(-1)) +
						              ".\n";
						break;
					default:
						statusText += "The previous backup on tag `" + tagName + "' at " + bc->getURL() + " " +
						              backupStatus + ".\n";
						break;
					}
					statusText += format("BackupUID: %s\n", uidAndAbortedFlag.get().first.toString().c_str());
					statusText += format("BackupURL: %s\n", bc->getURL().c_str());

					if (snapshotProgress) {
						state int64_t snapshotInterval;
						state Version snapshotBeginVersion;
						state Version snapshotTargetEndVersion;
						state Optional<Version> latestSnapshotEndVersion;
						state Optional<Version> latestLogEndVersion;
						state Optional<int64_t> logBytesWritten;
						state Optional<int64_t> rangeBytesWritten;
						state Optional<int64_t> latestSnapshotEndVersionTimestamp;
						state Optional<int64_t> latestLogEndVersionTimestamp;
						state Optional<int64_t> snapshotBeginVersionTimestamp;
						state Optional<int64_t> snapshotTargetEndVersionTimestamp;
						state bool stopWhenDone;

						wait(store(snapshotBeginVersion, config.snapshotBeginVersion().getOrThrow(tr)) &&
						     store(snapshotTargetEndVersion, config.snapshotTargetEndVersion().getOrThrow(tr)) &&
						     store(snapshotInterval, config.snapshotIntervalSeconds().getOrThrow(tr)) &&
						     store(logBytesWritten, config.logBytesWritten().get(tr)) &&
						     store(rangeBytesWritten, config.rangeBytesWritten().get(tr)) &&
						     store(latestLogEndVersion, config.latestLogEndVersion().get(tr)) &&
						     store(latestSnapshotEndVersion, config.latestSnapshotEndVersion().get(tr)) &&
						     store(stopWhenDone, config.stopWhenDone().getOrThrow(tr)));

						wait(store(latestSnapshotEndVersionTimestamp,
						           getTimestampFromVersion(latestSnapshotEndVersion, tr)) &&
						     store(latestLogEndVersionTimestamp, getTimestampFromVersion(latestLogEndVersion, tr)) &&
						     store(snapshotBeginVersionTimestamp,
						           timeKeeperEpochsFromVersion(snapshotBeginVersion, tr)) &&
						     store(snapshotTargetEndVersionTimestamp,
						           timeKeeperEpochsFromVersion(snapshotTargetEndVersion, tr)));

						statusText += format("Snapshot interval is %lld seconds.  ", snapshotInterval);
						if (backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL)
							statusText += format("Current snapshot progress target is %3.2f%% (>100%% means the "
							                     "snapshot is supposed to be done)\n",
							                     100.0 * (recentReadVersion - snapshotBeginVersion) /
							                         (snapshotTargetEndVersion - snapshotBeginVersion));
						else
							statusText += "The initial snapshot is still running.\n";

						statusText += format("\nDetails:\n LogBytes written - %lld\n RangeBytes written - %lld\n "
						                     "Last complete log version and timestamp        - %s, %s\n "
						                     "Last complete snapshot version and timestamp   - %s, %s\n "
						                     "Current Snapshot start version and timestamp   - %s, %s\n "
						                     "Expected snapshot end version and timestamp    - %s, %s\n "
						                     "Backup supposed to stop at next snapshot completion - %s\n",
						                     logBytesWritten.orDefault(0),
						                     rangeBytesWritten.orDefault(0),
						                     versionToString(latestLogEndVersion).c_str(),
						                     timeStampToString(latestLogEndVersionTimestamp).c_str(),
						                     versionToString(latestSnapshotEndVersion).c_str(),
						                     timeStampToString(latestSnapshotEndVersionTimestamp).c_str(),
						                     versionToString(snapshotBeginVersion).c_str(),
						                     timeStampToString(snapshotBeginVersionTimestamp).c_str(),
						                     versionToString(snapshotTargetEndVersion).c_str(),
						                     timeStampToString(snapshotTargetEndVersionTimestamp).c_str(),
						                     boolToYesOrNo(stopWhenDone).c_str());
					}

					// Append the errors, if requested
					if (showErrors) {
						KeyBackedMap<int64_t, std::pair<std::string, Version>>::PairsType errors =
						    wait(config.lastErrorPerType().getRange(
						        tr, 0, std::numeric_limits<int>::max(), CLIENT_KNOBS->TOO_MANY));
						std::string recentErrors;
						std::string pastErrors;

						for (auto& e : errors) {
							Version v = e.second.second;
							std::string msg = format(
							    "%s ago : %s\n",
							    secondsToTimeFormat((recentReadVersion - v) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND)
							        .c_str(),
							    e.second.first.c_str());

							// If error version is at or more recent than the latest restorable version then it could be
							// inhibiting progress
							if (v >= latestRestorableVersion.orDefault(0)) {
								recentErrors += msg;
							} else {
								pastErrors += msg;
							}
						}

						if (!recentErrors.empty()) {
							if (latestRestorableVersion.present())
								statusText +=
								    format("Recent Errors (since latest restorable point %s ago)\n",
								           secondsToTimeFormat((recentReadVersion - latestRestorableVersion.get()) /
								                               CLIENT_KNOBS->CORE_VERSIONSPERSECOND)
								               .c_str()) +
								    recentErrors;
							else
								statusText += "Recent Errors (since initialization)\n" + recentErrors;
						}
						if (!pastErrors.empty())
							statusText += "Older Errors\n" + pastErrors;
					}
				}

				Optional<Value> paused = wait(fPaused);
				if (paused.present()) {
					statusText += format("\nAll backup agents have been paused.\n");
				}

				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		return statusText;
	}

	ACTOR static Future<Optional<Version>> getLastRestorable(FileBackupAgent* backupAgent,
	                                                         Reference<ReadYourWritesTransaction> tr,
	                                                         Key tagName,
	                                                         Snapshot snapshot) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Optional<Value> version = wait(tr->get(backupAgent->lastRestorable.pack(tagName), snapshot));

		return (version.present())
		           ? Optional<Version>(BinaryReader::fromStringRef<Version>(version.get(), Unversioned()))
		           : Optional<Version>();
	}

	static StringRef read(StringRef& data, int bytes) {
		if (bytes > data.size())
			throw restore_error();
		StringRef r = data.substr(0, bytes);
		data = data.substr(bytes);
		return r;
	}

	// Submits the restore request to the database and throws "restore_invalid_version" error if
	// restore is not possible. Parameters:
	//   cx: the database to be restored to
	//   cxOrig: if present, is used to resolve the restore timestamp into a version.
	//   tagName: restore tag
	//   url: the backup container's URL that contains all backup files
	//   ranges: the restored key ranges; if empty, restore all key ranges in the backup
	//   waitForComplete: if set, wait until the restore is completed before returning; otherwise,
	//                    return when the request is submitted to the database.
	//   targetVersion: the version to be restored.
	//   verbose: print verbose information.
	//   addPrefix: each key is added this prefix during restore.
	//   removePrefix: for each key to be restored, remove this prefix first.
	//   lockDB: if set lock the database with randomUid before performing restore;
	//           otherwise, check database is locked with the randomUid
	//   onlyApplyMutationLogs: only perform incremental restore, by only applying mutation logs
	//   inconsistentSnapshotOnly: Ignore mutation log files during the restore to speedup the process.
	//                             When set to true, gives an inconsistent snapshot, thus not recommended
	//   beginVersion: restore's begin version
	//   randomUid: the UID for lock the database
	ACTOR static Future<Version> restore(FileBackupAgent* backupAgent,
	                                     Database cx,
	                                     Optional<Database> cxOrig,
	                                     Key tagName,
	                                     Key url,
	                                     Optional<std::string> proxy,
	                                     Standalone<VectorRef<KeyRangeRef>> ranges,
	                                     WaitForComplete waitForComplete,
	                                     Version targetVersion,
	                                     Verbose verbose,
	                                     Key addPrefix,
	                                     Key removePrefix,
	                                     LockDB lockDB,
	                                     OnlyApplyMutationLogs onlyApplyMutationLogs,
	                                     InconsistentSnapshotOnly inconsistentSnapshotOnly,
	                                     Version beginVersion,
	                                     Optional<std::string> encryptionKeyFileName,
	                                     UID randomUid) {
		// The restore command line tool won't allow ranges to be empty, but correctness workloads somehow might.
		if (ranges.empty()) {
			throw restore_error();
		}

		state Reference<IBackupContainer> bc = IBackupContainer::openContainer(url.toString(), proxy, {});

		state BackupDescription desc = wait(bc->describeBackup(true));
		if (cxOrig.present()) {
			wait(desc.resolveVersionTimes(cxOrig.get()));
		}

		printf("Backup Description\n%s", desc.toString().c_str());
		if (targetVersion == invalidVersion && desc.maxRestorableVersion.present())
			targetVersion = desc.maxRestorableVersion.get();

		if (targetVersion == invalidVersion && onlyApplyMutationLogs && desc.contiguousLogEnd.present()) {
			targetVersion = desc.contiguousLogEnd.get() - 1;
		}

		Optional<RestorableFileSet> restoreSet =
		    wait(bc->getRestoreSet(targetVersion, ranges, onlyApplyMutationLogs, beginVersion));

		if (!restoreSet.present()) {
			TraceEvent(SevWarn, "FileBackupAgentRestoreNotPossible")
			    .detail("BackupContainer", bc->getURL())
			    .detail("BeginVersion", beginVersion)
			    .detail("TargetVersion", targetVersion);
			fmt::print(stderr, "ERROR: Restore version {0} is not possible from {1}\n", targetVersion, bc->getURL());
			throw restore_invalid_version();
		}

		if (verbose) {
			printf("Restoring backup to version: %lld\n", (long long)targetVersion);
		}

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(submitRestore(backupAgent,
				                   tr,
				                   tagName,
				                   url,
				                   proxy,
				                   ranges,
				                   targetVersion,
				                   addPrefix,
				                   removePrefix,
				                   lockDB,
				                   onlyApplyMutationLogs,
				                   inconsistentSnapshotOnly,
				                   beginVersion,
				                   randomUid));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				if (e.code() == error_code_restore_duplicate_tag) {
					throw;
				}
				wait(tr->onError(e));
			}
		}

		if (waitForComplete) {
			ERestoreState finalState = wait(waitRestore(cx, tagName, verbose));
			if (finalState != ERestoreState::COMPLETED)
				throw restore_error();
		}

		return targetVersion;
	}

	// used for correctness only, locks the database before discontinuing the backup and that same lock is then used
	// while doing the restore. the tagname of the backup must be the same as the restore.
	ACTOR static Future<Version> atomicRestore(FileBackupAgent* backupAgent,
	                                           Database cx,
	                                           Key tagName,
	                                           Standalone<VectorRef<KeyRangeRef>> ranges,
	                                           Key addPrefix,
	                                           Key removePrefix,
	                                           UsePartitionedLog fastRestore) {
		state Reference<ReadYourWritesTransaction> ryw_tr =
		    Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
		state BackupConfig backupConfig;
		loop {
			try {
				ryw_tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ryw_tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				state KeyBackedTag tag = makeBackupTag(tagName.toString());
				UidAndAbortedFlagT uidFlag = wait(tag.getOrThrow(ryw_tr));
				backupConfig = BackupConfig(uidFlag.first);
				state EBackupState status = wait(backupConfig.stateEnum().getOrThrow(ryw_tr));

				if (status != EBackupState::STATE_RUNNING_DIFFERENTIAL) {
					throw backup_duplicate();
				}

				break;
			} catch (Error& e) {
				wait(ryw_tr->onError(e));
			}
		}

		// Lock src, record commit version
		state Transaction tr(cx);
		state Version commitVersion;
		state UID randomUid = deterministicRandom()->randomUniqueID();
		loop {
			try {
				// We must get a commit version so add a conflict range that won't likely cause conflicts
				// but will ensure that the transaction is actually submitted.
				tr.addWriteConflictRange(backupConfig.snapshotRangeDispatchMap().space.range());
				wait(lockDatabase(&tr, randomUid));
				wait(tr.commit());
				commitVersion = tr.getCommittedVersion();
				TraceEvent("AS_Locked").detail("CommitVer", commitVersion);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		ryw_tr->reset();
		loop {
			try {
				Optional<Version> restoreVersion = wait(backupConfig.getLatestRestorableVersion(ryw_tr));
				if (restoreVersion.present() && restoreVersion.get() >= commitVersion) {
					TraceEvent("AS_RestoreVersion").detail("RestoreVer", restoreVersion.get());
					break;
				} else {
					ryw_tr->reset();
					wait(delay(0.2));
				}
			} catch (Error& e) {
				wait(ryw_tr->onError(e));
			}
		}

		ryw_tr->reset();
		loop {
			try {
				wait(discontinueBackup(backupAgent, ryw_tr, tagName));
				wait(ryw_tr->commit());
				TraceEvent("AS_DiscontinuedBackup").log();
				break;
			} catch (Error& e) {
				if (e.code() == error_code_backup_unneeded || e.code() == error_code_backup_duplicate) {
					break;
				}
				wait(ryw_tr->onError(e));
			}
		}

		wait(success(waitBackup(backupAgent, cx, tagName.toString(), StopWhenDone::True)));
		TraceEvent("AS_BackupStopped").log();

		ryw_tr->reset();
		loop {

			try {
				ryw_tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ryw_tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				for (auto& range : ranges) {
					ryw_tr->addReadConflictRange(range);
					ryw_tr->clear(range);
				}
				wait(ryw_tr->commit());
				TraceEvent("AS_ClearedRange").log();
				break;
			} catch (Error& e) {
				wait(ryw_tr->onError(e));
			}
		}

		Reference<IBackupContainer> _bc = wait(backupConfig.backupContainer().getOrThrow(cx));
		Reference<IBackupContainer> bc = fileBackup::getBackupContainerWithProxy(_bc);

		if (fastRestore) {
			TraceEvent("AtomicParallelRestoreStartRestore").log();
			Version targetVersion = ::invalidVersion;
			wait(submitParallelRestore(cx,
			                           tagName,
			                           ranges,
			                           KeyRef(bc->getURL()),
			                           bc->getProxy(),
			                           targetVersion,
			                           LockDB::True,
			                           randomUid,
			                           addPrefix,
			                           removePrefix));
			state bool hasPrefix = (addPrefix.size() > 0 || removePrefix.size() > 0);
			TraceEvent("AtomicParallelRestoreWaitForRestoreFinish").detail("HasPrefix", hasPrefix);
			wait(parallelRestoreFinish(cx, randomUid, UnlockDB{ !hasPrefix }));
			// If addPrefix or removePrefix set, we want to transform the effect by copying data
			if (hasPrefix) {
				wait(transformRestoredDatabase(cx, ranges, addPrefix, removePrefix));
				wait(unlockDatabase(cx, randomUid));
			}
			return -1;
		} else {
			TraceEvent("AS_StartRestore").log();
			Version ver = wait(restore(backupAgent,
			                           cx,
			                           cx,
			                           tagName,
			                           KeyRef(bc->getURL()),
			                           bc->getProxy(),
			                           ranges,
			                           WaitForComplete::True,
			                           ::invalidVersion,
			                           Verbose::True,
			                           addPrefix,
			                           removePrefix,
			                           LockDB::True,
			                           OnlyApplyMutationLogs::False,
			                           InconsistentSnapshotOnly::False,
			                           ::invalidVersion,
			                           {},
			                           randomUid));
			return ver;
		}
	}

	// Similar to atomicRestore, only used in simulation test.
	// locks the database before discontinuing the backup and that same lock is then used while doing the restore.
	// the tagname of the backup must be the same as the restore.
	static Future<Void> atomicParallelRestore(FileBackupAgent* backupAgent,
	                                          Database cx,
	                                          Key tagName,
	                                          Standalone<VectorRef<KeyRangeRef>> ranges,
	                                          Key addPrefix,
	                                          Key removePrefix) {
		return success(
		    atomicRestore(backupAgent, cx, tagName, ranges, addPrefix, removePrefix, UsePartitionedLog::True));
	}
};

const int FileBackupAgent::dataFooterSize = 20;

// Return if parallel restore has finished
Future<Void> FileBackupAgent::parallelRestoreFinish(Database cx, UID randomUID, UnlockDB unlockDB) {
	return FileBackupAgentImpl::parallelRestoreFinish(cx, randomUID, unlockDB);
}

Future<Void> FileBackupAgent::submitParallelRestore(Database cx,
                                                    Key backupTag,
                                                    Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                                    Key bcUrl,
                                                    Optional<std::string> proxy,
                                                    Version targetVersion,
                                                    LockDB lockDB,
                                                    UID randomUID,
                                                    Key addPrefix,
                                                    Key removePrefix) {
	return FileBackupAgentImpl::submitParallelRestore(
	    cx, backupTag, backupRanges, bcUrl, proxy, targetVersion, lockDB, randomUID, addPrefix, removePrefix);
}

Future<Void> FileBackupAgent::atomicParallelRestore(Database cx,
                                                    Key tagName,
                                                    Standalone<VectorRef<KeyRangeRef>> ranges,
                                                    Key addPrefix,
                                                    Key removePrefix) {
	return FileBackupAgentImpl::atomicParallelRestore(this, cx, tagName, ranges, addPrefix, removePrefix);
}

Future<Version> FileBackupAgent::restore(Database cx,
                                         Optional<Database> cxOrig,
                                         Key tagName,
                                         Key url,
                                         Optional<std::string> proxy,
                                         Standalone<VectorRef<KeyRangeRef>> ranges,
                                         WaitForComplete waitForComplete,
                                         Version targetVersion,
                                         Verbose verbose,
                                         Key addPrefix,
                                         Key removePrefix,
                                         LockDB lockDB,
                                         OnlyApplyMutationLogs onlyApplyMutationLogs,
                                         InconsistentSnapshotOnly inconsistentSnapshotOnly,
                                         Version beginVersion,
                                         Optional<std::string> const& encryptionKeyFileName) {
	return FileBackupAgentImpl::restore(this,
	                                    cx,
	                                    cxOrig,
	                                    tagName,
	                                    url,
	                                    proxy,
	                                    ranges,
	                                    waitForComplete,
	                                    targetVersion,
	                                    verbose,
	                                    addPrefix,
	                                    removePrefix,
	                                    lockDB,
	                                    onlyApplyMutationLogs,
	                                    inconsistentSnapshotOnly,
	                                    beginVersion,
	                                    encryptionKeyFileName,
	                                    deterministicRandom()->randomUniqueID());
}

Future<Version> FileBackupAgent::atomicRestore(Database cx,
                                               Key tagName,
                                               Standalone<VectorRef<KeyRangeRef>> ranges,
                                               Key addPrefix,
                                               Key removePrefix) {
	return FileBackupAgentImpl::atomicRestore(
	    this, cx, tagName, ranges, addPrefix, removePrefix, UsePartitionedLog::False);
}

Future<ERestoreState> FileBackupAgent::abortRestore(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return fileBackup::abortRestore(tr, tagName);
}

Future<ERestoreState> FileBackupAgent::abortRestore(Database cx, Key tagName) {
	return fileBackup::abortRestore(cx, tagName);
}

Future<std::string> FileBackupAgent::restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return fileBackup::restoreStatus(tr, tagName);
}

Future<ERestoreState> FileBackupAgent::waitRestore(Database cx, Key tagName, Verbose verbose) {
	return FileBackupAgentImpl::waitRestore(cx, tagName, verbose);
};

Future<Void> FileBackupAgent::submitBackup(Reference<ReadYourWritesTransaction> tr,
                                           Key outContainer,
                                           Optional<std::string> proxy,
                                           int initialSnapshotIntervalSeconds,
                                           int snapshotIntervalSeconds,
                                           std::string const& tagName,
                                           Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                           StopWhenDone stopWhenDone,
                                           UsePartitionedLog partitionedLog,
                                           IncrementalBackupOnly incrementalBackupOnly,
                                           Optional<std::string> const& encryptionKeyFileName) {
	return FileBackupAgentImpl::submitBackup(this,
	                                         tr,
	                                         outContainer,
	                                         proxy,
	                                         initialSnapshotIntervalSeconds,
	                                         snapshotIntervalSeconds,
	                                         tagName,
	                                         backupRanges,
	                                         stopWhenDone,
	                                         partitionedLog,
	                                         incrementalBackupOnly,
	                                         encryptionKeyFileName);
}

Future<Void> FileBackupAgent::discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return FileBackupAgentImpl::discontinueBackup(this, tr, tagName);
}

Future<Void> FileBackupAgent::abortBackup(Reference<ReadYourWritesTransaction> tr, std::string tagName) {
	return FileBackupAgentImpl::abortBackup(this, tr, tagName);
}

Future<std::string> FileBackupAgent::getStatus(Database cx, ShowErrors showErrors, std::string tagName) {
	return FileBackupAgentImpl::getStatus(this, cx, showErrors, tagName);
}

Future<std::string> FileBackupAgent::getStatusJSON(Database cx, std::string tagName) {
	return FileBackupAgentImpl::getStatusJSON(this, cx, tagName);
}

Future<Optional<Version>> FileBackupAgent::getLastRestorable(Reference<ReadYourWritesTransaction> tr,
                                                             Key tagName,
                                                             Snapshot snapshot) {
	return FileBackupAgentImpl::getLastRestorable(this, tr, tagName, snapshot);
}

void FileBackupAgent::setLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName, Version version) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->set(lastRestorable.pack(tagName), BinaryWriter::toValue<Version>(version, Unversioned()));
}

Future<EBackupState> FileBackupAgent::waitBackup(Database cx,
                                                 std::string tagName,
                                                 StopWhenDone stopWhenDone,
                                                 Reference<IBackupContainer>* pContainer,
                                                 UID* pUID) {
	return FileBackupAgentImpl::waitBackup(this, cx, tagName, stopWhenDone, pContainer, pUID);
}

Future<Void> FileBackupAgent::changePause(Database db, bool pause) {
	return FileBackupAgentImpl::changePause(this, db, pause);
}

// Fast Restore addPrefix test helper functions
static std::pair<bool, bool> insideValidRange(KeyValueRef kv,
                                              Standalone<VectorRef<KeyRangeRef>> restoreRanges,
                                              Standalone<VectorRef<KeyRangeRef>> backupRanges) {
	bool insideRestoreRange = false;
	bool insideBackupRange = false;
	for (auto& range : restoreRanges) {
		TraceEvent(SevFRTestInfo, "InsideValidRestoreRange")
		    .detail("Key", kv.key)
		    .detail("Range", range)
		    .detail("Inside", (kv.key >= range.begin && kv.key < range.end));
		if (kv.key >= range.begin && kv.key < range.end) {
			insideRestoreRange = true;
			break;
		}
	}
	for (auto& range : backupRanges) {
		TraceEvent(SevFRTestInfo, "InsideValidBackupRange")
		    .detail("Key", kv.key)
		    .detail("Range", range)
		    .detail("Inside", (kv.key >= range.begin && kv.key < range.end));
		if (kv.key >= range.begin && kv.key < range.end) {
			insideBackupRange = true;
			break;
		}
	}
	return std::make_pair(insideBackupRange, insideRestoreRange);
}

// Write [begin, end) in kvs to DB
ACTOR static Future<Void> writeKVs(Database cx, Standalone<VectorRef<KeyValueRef>> kvs, int begin, int end) {
	wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		int index = begin;
		while (index < end) {
			TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKV")
			    .detail("Index", index)
			    .detail("KVs", kvs.size())
			    .detail("Key", kvs[index].key)
			    .detail("Value", kvs[index].value);
			tr->set(kvs[index].key, kvs[index].value);
			++index;
		}
		return Void();
	}));

	// Sanity check data has been written to DB
	state ReadYourWritesTransaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			KeyRef k1 = kvs[begin].key;
			KeyRef k2 = end < kvs.size() ? kvs[end].key : normalKeys.end;
			TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKVReadBack")
			    .detail("Range", KeyRangeRef(k1, k2))
			    .detail("Begin", begin)
			    .detail("End", end);
			RangeResult readKVs = wait(tr.getRange(KeyRangeRef(k1, k2), CLIENT_KNOBS->TOO_MANY));
			ASSERT(readKVs.size() > 0 || begin == end);
			break;
		} catch (Error& e) {
			TraceEvent("TransformDatabaseContentsWriteKVReadBackError").error(e);
			wait(tr.onError(e));
		}
	}

	TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKVDone").detail("Begin", begin).detail("End", end);

	return Void();
}

// restoreRanges is the actual range that has applied removePrefix and addPrefix processed by restore system
// Assume: restoreRanges do not overlap which is achieved by ensuring backup ranges do not overlap
ACTOR static Future<Void> transformDatabaseContents(Database cx,
                                                    Key addPrefix,
                                                    Key removePrefix,
                                                    Standalone<VectorRef<KeyRangeRef>> restoreRanges) {
	state ReadYourWritesTransaction tr(cx);
	state Standalone<VectorRef<KeyValueRef>> oldData;

	TraceEvent("FastRestoreWorkloadTransformDatabaseContents")
	    .detail("AddPrefix", addPrefix)
	    .detail("RemovePrefix", removePrefix);
	state int i = 0;
	loop { // Read all data from DB
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			for (i = 0; i < restoreRanges.size(); ++i) {
				RangeResult kvs = wait(tr.getRange(restoreRanges[i], CLIENT_KNOBS->TOO_MANY));
				ASSERT(!kvs.more);
				for (auto kv : kvs) {
					oldData.push_back_deep(oldData.arena(), KeyValueRef(kv.key, kv.value));
				}
			}
			break;
		} catch (Error& e) {
			TraceEvent("FastRestoreWorkloadTransformDatabaseContentsGetAllKeys")
			    .error(e)
			    .detail("Index", i)
			    .detail("RestoreRange", restoreRanges[i]);
			oldData = Standalone<VectorRef<KeyValueRef>>(); // clear the vector
			wait(tr.onError(e));
		}
	}

	// Convert data by removePrefix and addPrefix in memory
	state Standalone<VectorRef<KeyValueRef>> newKVs;
	for (int i = 0; i < oldData.size(); ++i) {
		Key newKey(oldData[i].key);
		TraceEvent(SevFRTestInfo, "TransformDatabaseContents")
		    .detail("Keys", oldData.size())
		    .detail("Index", i)
		    .detail("GetKey", oldData[i].key)
		    .detail("GetValue", oldData[i].value);
		if (newKey.size() < removePrefix.size()) { // If true, must check why.
			TraceEvent(SevError, "TransformDatabaseContents")
			    .detail("Key", newKey)
			    .detail("RemovePrefix", removePrefix);
			continue;
		}
		newKey = newKey.removePrefix(removePrefix).withPrefix(addPrefix);
		newKVs.push_back_deep(newKVs.arena(), KeyValueRef(newKey.contents(), oldData[i].value));
		TraceEvent(SevFRTestInfo, "TransformDatabaseContents")
		    .detail("Keys", newKVs.size())
		    .detail("Index", i)
		    .detail("NewKey", newKVs.back().key)
		    .detail("NewValue", newKVs.back().value);
	}

	state Standalone<VectorRef<KeyRangeRef>> backupRanges; // dest. ranges
	for (auto& range : restoreRanges) {
		KeyRange tmpRange = range;
		backupRanges.push_back_deep(backupRanges.arena(), tmpRange.removePrefix(removePrefix).withPrefix(addPrefix));
	}

	// Clear the transformed data (original data with removePrefix and addPrefix) in restoreRanges
	wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		for (int i = 0; i < restoreRanges.size(); i++) {
			TraceEvent(SevFRTestInfo, "TransformDatabaseContents")
			    .detail("ClearRestoreRange", restoreRanges[i])
			    .detail("ClearBackupRange", backupRanges[i]);
			tr->clear(restoreRanges[i]); // Clear the range.removePrefix().withPrefix()
			tr->clear(backupRanges[i]);
		}
		return Void();
	}));

	// Sanity check to ensure no data in the ranges
	tr.reset();
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult emptyData = wait(tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
			for (int i = 0; i < emptyData.size(); ++i) {
				TraceEvent(SevError, "ExpectEmptyData")
				    .detail("Index", i)
				    .detail("Key", emptyData[i].key)
				    .detail("Value", emptyData[i].value);
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	// Write transformed KVs (i.e., kv backup took) back to DB
	state std::vector<Future<Void>> fwrites;
	loop {
		try {
			state int begin = 0;
			state int len = 0;
			while (begin < newKVs.size()) {
				len = std::min(100, newKVs.size() - begin);
				fwrites.push_back(writeKVs(cx, newKVs, begin, begin + len));
				begin = begin + len;
			}
			wait(waitForAll(fwrites));
			break;
		} catch (Error& e) {
			TraceEvent(SevError, "FastRestoreWorkloadTransformDatabaseContentsUnexpectedErrorOnWriteKVs").error(e);
			wait(tr.onError(e));
		}
	}

	// Sanity check
	tr.reset();
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult allData = wait(tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
			TraceEvent(SevFRTestInfo, "SanityCheckData").detail("Size", allData.size());
			for (int i = 0; i < allData.size(); ++i) {
				std::pair<bool, bool> backupRestoreValid = insideValidRange(allData[i], restoreRanges, backupRanges);
				TraceEvent(backupRestoreValid.first ? SevFRTestInfo : SevError, "SanityCheckData")
				    .detail("Index", i)
				    .detail("Key", allData[i].key)
				    .detail("Value", allData[i].value)
				    .detail("InsideBackupRange", backupRestoreValid.first)
				    .detail("InsideRestoreRange", backupRestoreValid.second);
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	TraceEvent("FastRestoreWorkloadTransformDatabaseContentsFinish")
	    .detail("AddPrefix", addPrefix)
	    .detail("RemovePrefix", removePrefix);

	return Void();
}

// addPrefix and removePrefix are the options used in the restore request:
// every backup key applied removePrefix and addPrefix in restore;
// transformRestoredDatabase actor will revert it by remove addPrefix and add removePrefix.
ACTOR Future<Void> transformRestoredDatabase(Database cx,
                                             Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                             Key addPrefix,
                                             Key removePrefix) {
	try {
		Standalone<VectorRef<KeyRangeRef>> restoreRanges;
		for (int i = 0; i < backupRanges.size(); ++i) {
			KeyRange range(backupRanges[i]);
			Key begin = range.begin.removePrefix(removePrefix).withPrefix(addPrefix);
			Key end = range.end.removePrefix(removePrefix).withPrefix(addPrefix);
			TraceEvent("FastRestoreTransformRestoredDatabase")
			    .detail("From", KeyRangeRef(begin.contents(), end.contents()))
			    .detail("To", range);
			restoreRanges.push_back_deep(restoreRanges.arena(), KeyRangeRef(begin.contents(), end.contents()));
		}
		wait(transformDatabaseContents(cx, removePrefix, addPrefix, restoreRanges));
	} catch (Error& e) {
		TraceEvent(SevError, "FastRestoreTransformRestoredDatabaseUnexpectedError").error(e);
		throw;
	}

	return Void();
}

void simulateBlobFailure() {
	if (BUGGIFY && deterministicRandom()->random01() < 0.01) { // Simulate blob failures
		double i = deterministicRandom()->random01();
		if (i < 0.4) {
			throw http_request_failed();
		} else if (i < 0.5) {
			throw http_bad_request();
		} else if (i < 0.7) {
			throw connection_failed();
		} else if (i < 0.8) {
			throw timed_out();
		} else if (i < 0.9) {
			throw lookup_failed();
		}
	}
}
