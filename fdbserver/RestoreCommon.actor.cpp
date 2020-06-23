/*
 * RestoreCommon.actor.cpp
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

// This file implements the functions defined in RestoreCommon.actor.h
// The functions in this file are copied from BackupAgent

#include "fdbserver/RestoreCommon.actor.h"

// Backup agent header
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define SevFRTestInfo SevVerbose
//#define SevFRTestInfo SevInfo

// Split RestoreConfigFR defined in FileBackupAgent.actor.cpp to declaration in Restore.actor.h and implementation in
// RestoreCommon.actor.cpp
KeyBackedProperty<ERestoreState> RestoreConfigFR::stateEnum() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
Future<StringRef> RestoreConfigFR::stateText(Reference<ReadYourWritesTransaction> tr) {
	return map(stateEnum().getD(tr), [](ERestoreState s) -> StringRef { return FileBackupAgent::restoreStateText(s); });
}
KeyBackedProperty<Key> RestoreConfigFR::addPrefix() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
KeyBackedProperty<Key> RestoreConfigFR::removePrefix() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
// XXX: Remove restoreRange() once it is safe to remove. It has been changed to restoreRanges
KeyBackedProperty<KeyRange> RestoreConfigFR::restoreRange() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
KeyBackedProperty<std::vector<KeyRange>> RestoreConfigFR::restoreRanges() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
KeyBackedProperty<Key> RestoreConfigFR::batchFuture() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
KeyBackedProperty<Version> RestoreConfigFR::restoreVersion() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}

KeyBackedProperty<Reference<IBackupContainer>> RestoreConfigFR::sourceContainer() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
// Get the source container as a bare URL, without creating a container instance
KeyBackedProperty<Value> RestoreConfigFR::sourceContainerURL() {
	return configSpace.pack(LiteralStringRef("sourceContainer"));
}

// Total bytes written by all log and range restore tasks.
KeyBackedBinaryValue<int64_t> RestoreConfigFR::bytesWritten() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
// File blocks that have had tasks created for them by the Dispatch task
KeyBackedBinaryValue<int64_t> RestoreConfigFR::filesBlocksDispatched() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
// File blocks whose tasks have finished
KeyBackedBinaryValue<int64_t> RestoreConfigFR::fileBlocksFinished() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
// Total number of files in the fileMap
KeyBackedBinaryValue<int64_t> RestoreConfigFR::fileCount() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}
// Total number of file blocks in the fileMap
KeyBackedBinaryValue<int64_t> RestoreConfigFR::fileBlockCount() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}

Future<std::vector<KeyRange>> RestoreConfigFR::getRestoreRangesOrDefault(Reference<ReadYourWritesTransaction> tr) {
	return getRestoreRangesOrDefault_impl(this, tr);
}

ACTOR Future<std::vector<KeyRange>> RestoreConfigFR::getRestoreRangesOrDefault_impl(
    RestoreConfigFR* self, Reference<ReadYourWritesTransaction> tr) {
	state std::vector<KeyRange> ranges = wait(self->restoreRanges().getD(tr));
	if (ranges.empty()) {
		state KeyRange range = wait(self->restoreRange().getD(tr));
		ranges.push_back(range);
	}
	return ranges;
}

KeyBackedSet<RestoreConfigFR::RestoreFile> RestoreConfigFR::fileSet() {
	return configSpace.pack(LiteralStringRef(__FUNCTION__));
}

Future<bool> RestoreConfigFR::isRunnable(Reference<ReadYourWritesTransaction> tr) {
	return map(stateEnum().getD(tr), [](ERestoreState s) -> bool {
		return s != ERestoreState::ABORTED && s != ERestoreState::COMPLETED && s != ERestoreState::UNITIALIZED;
	});
}

Future<Void> RestoreConfigFR::logError(Database cx, Error e, std::string const& details, void* taskInstance) {
	if (!uid.isValid()) {
		TraceEvent(SevError, "FileRestoreErrorNoUID").error(e).detail("Description", details);
		return Void();
	}
	TraceEvent t(SevWarn, "FileRestoreError");
	t.error(e).detail("RestoreUID", uid).detail("Description", details).detail("TaskInstance", (uint64_t)taskInstance);
	// key_not_found could happen
	if (e.code() == error_code_key_not_found) t.backtrace();

	return updateErrorInfo(cx, e, details);
}

Key RestoreConfigFR::mutationLogPrefix() {
	return uidPrefixKey(applyLogKeys.begin, uid);
}

Key RestoreConfigFR::applyMutationsMapPrefix() {
	return uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
}

ACTOR Future<int64_t> RestoreConfigFR::getApplyVersionLag_impl(Reference<ReadYourWritesTransaction> tr, UID uid) {
	// Both of these are snapshot reads
	state Future<Optional<Value>> beginVal = tr->get(uidPrefixKey(applyMutationsBeginRange.begin, uid), true);
	state Future<Optional<Value>> endVal = tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid), true);
	wait(success(beginVal) && success(endVal));

	if (!beginVal.get().present() || !endVal.get().present()) return 0;

	Version beginVersion = BinaryReader::fromStringRef<Version>(beginVal.get().get(), Unversioned());
	Version endVersion = BinaryReader::fromStringRef<Version>(endVal.get().get(), Unversioned());
	return endVersion - beginVersion;
}

Future<int64_t> RestoreConfigFR::getApplyVersionLag(Reference<ReadYourWritesTransaction> tr) {
	return getApplyVersionLag_impl(tr, uid);
}

void RestoreConfigFR::initApplyMutations(Reference<ReadYourWritesTransaction> tr, Key addPrefix, Key removePrefix) {
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

void RestoreConfigFR::clearApplyMutationsKeys(Reference<ReadYourWritesTransaction> tr) {
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

void RestoreConfigFR::setApplyBeginVersion(Reference<ReadYourWritesTransaction> tr, Version ver) {
	tr->set(uidPrefixKey(applyMutationsBeginRange.begin, uid), BinaryWriter::toValue(ver, Unversioned()));
}

void RestoreConfigFR::setApplyEndVersion(Reference<ReadYourWritesTransaction> tr, Version ver) {
	tr->set(uidPrefixKey(applyMutationsEndRange.begin, uid), BinaryWriter::toValue(ver, Unversioned()));
}

Future<Version> RestoreConfigFR::getApplyEndVersion(Reference<ReadYourWritesTransaction> tr) {
	return map(tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid)), [=](Optional<Value> const& value) -> Version {
		return value.present() ? BinaryReader::fromStringRef<Version>(value.get(), Unversioned()) : 0;
	});
}

// Meng: Change RestoreConfigFR to Reference<RestoreConfigFR> because FastRestore pass the Reference<RestoreConfigFR> around
ACTOR Future<std::string> RestoreConfigFR::getProgress_impl(Reference<RestoreConfigFR> restore,
                                                          Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<int64_t> fileCount = restore->fileCount().getD(tr);
	state Future<int64_t> fileBlockCount = restore->fileBlockCount().getD(tr);
	state Future<int64_t> fileBlocksDispatched = restore->filesBlocksDispatched().getD(tr);
	state Future<int64_t> fileBlocksFinished = restore->fileBlocksFinished().getD(tr);
	state Future<int64_t> bytesWritten = restore->bytesWritten().getD(tr);
	state Future<StringRef> status = restore->stateText(tr);
	state Future<Version> lag = restore->getApplyVersionLag(tr);
	state Future<std::string> tag = restore->tag().getD(tr);
	state Future<std::pair<std::string, Version>> lastError = restore->lastError().getD(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	state UID uid = restore->getUid();
	wait(success(fileCount) && success(fileBlockCount) && success(fileBlocksDispatched) &&
	     success(fileBlocksFinished) && success(bytesWritten) && success(status) && success(lag) && success(tag) &&
	     success(lastError));

	std::string errstr = "None";
	if (lastError.get().second != 0)
		errstr = format("'%s' %llds ago.\n", lastError.get().first.c_str(),
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
	    .detail("ApplyLag", lag.get())
	    .detail("TaskInstance", THIS_ADDR)
	    .backtrace();

	return format("Tag: %s  UID: %s  State: %s  Blocks: %lld/%lld  BlocksInProgress: %lld  Files: %lld  BytesWritten: "
	              "%lld  ApplyVersionLag: %lld  LastError: %s",
	              tag.get().c_str(), uid.toString().c_str(), status.get().toString().c_str(), fileBlocksFinished.get(),
	              fileBlockCount.get(), fileBlocksDispatched.get() - fileBlocksFinished.get(), fileCount.get(),
	              bytesWritten.get(), lag.get(), errstr.c_str());
}
Future<std::string> RestoreConfigFR::getProgress(Reference<ReadYourWritesTransaction> tr) {
	Reference<RestoreConfigFR> restore = Reference<RestoreConfigFR>(this);
	return getProgress_impl(restore, tr);
}

// Meng: Change RestoreConfigFR to Reference<RestoreConfigFR>
ACTOR Future<std::string> RestoreConfigFR::getFullStatus_impl(Reference<RestoreConfigFR> restore,
                                                            Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<std::vector<KeyRange>> ranges = restore->getRestoreRangesOrDefault(tr);
	state Future<Key> addPrefix = restore->addPrefix().getD(tr);
	state Future<Key> removePrefix = restore->removePrefix().getD(tr);
	state Future<Key> url = restore->sourceContainerURL().getD(tr);
	state Future<Version> restoreVersion = restore->restoreVersion().getD(tr);
	state Future<std::string> progress = restore->getProgress(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	wait(success(ranges) && success(addPrefix) && success(removePrefix) &&
		 success(url) && success(restoreVersion) && success(progress));

	std::string returnStr;
	returnStr = format("%s  URL: %s", progress.get().c_str(), url.get().toString().c_str());
	for (auto& range : ranges.get()) {
		returnStr += format("  Range: '%s'-'%s'", printable(range.begin).c_str(), printable(range.end).c_str());
	}
	returnStr += format("  AddPrefix: '%s'  RemovePrefix: '%s'  Version: %lld", printable(addPrefix.get()).c_str(),
	                    printable(removePrefix.get()).c_str(), restoreVersion.get());
	return returnStr;
}
Future<std::string> RestoreConfigFR::getFullStatus(Reference<ReadYourWritesTransaction> tr) {
	Reference<RestoreConfigFR> restore = Reference<RestoreConfigFR>(this);
	return getFullStatus_impl(restore, tr);
}

std::string RestoreConfigFR::toString() {
	std::stringstream ss;
	ss << "uid:" << uid.toString() << " prefix:" << prefix.contents().toString();
	return ss.str();
}

// parallelFileRestore is copied from FileBackupAgent.actor.cpp for the same reason as RestoreConfigFR is copied
// The implementation of parallelFileRestore is copied from FileBackupAgent.actor.cpp
// parallelFileRestore is copied from FileBackupAgent.actor.cpp for the same reason as RestoreConfigFR is copied
namespace parallelFileRestore {

ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeLogFileBlock(Reference<IAsyncFile> file, int64_t offset,
                                                                    int len) {
	state Standalone<StringRef> buf = makeString(len);
	int rLen = wait(file->read(mutateString(buf), len, offset));
	if (rLen != len) throw restore_bad_read();

	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	state StringRefReader reader(buf, restore_corrupted_data());

	try {
		// Read header, currently only decoding version BACKUP_AGENT_MLOG_VERSION
		if (reader.consume<int32_t>() != BACKUP_AGENT_MLOG_VERSION) throw restore_unsupported_file_version();

		// Read k/v pairs.  Block ends either at end of last value exactly or with 0xFF as first key len byte.
		while (1) {
			// If eof reached or first key len bytes is 0xFF then end of block was reached.
			if (reader.eof() || *reader.rptr == 0xFF) break;

			// Read key and value.  If anything throws then there is a problem.
			uint32_t kLen = reader.consumeNetworkUInt32();
			const uint8_t* k = reader.consume(kLen);
			uint32_t vLen = reader.consumeNetworkUInt32();
			const uint8_t* v = reader.consume(vLen);

			results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
		}

		// Make sure any remaining bytes in the block are 0xFF
		for (auto b : reader.remainder())
			if (b != 0xFF) throw restore_corrupted_data_padding();

		return results;

	} catch (Error& e) {
		TraceEvent(SevError, "FileRestoreCorruptLogFileBlock")
		    .error(e)
		    .detail("Filename", file->getFilename())
		    .detail("BlockOffset", offset)
		    .detail("BlockLen", len)
		    .detail("ErrorRelativeOffset", reader.rptr - buf.begin())
		    .detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset);
		throw;
	}
}

} // namespace parallelFileRestore

static std::pair<bool, bool> insideValidRange(KeyValueRef kv, Standalone<VectorRef<KeyRangeRef>> restoreRanges,
                                              Standalone<VectorRef<KeyRangeRef>> backupRanges) {
	bool insideRestoreRange = false;
	bool insideBackupRange = false;
	for (auto& range : restoreRanges) {
		TraceEvent("InsideValidRestoreRange")
		    .detail("Key", kv.key)
		    .detail("Range", range)
		    .detail("Inside", (kv.key >= range.begin && kv.key < range.end));
		if (kv.key >= range.begin && kv.key < range.end) {
			insideRestoreRange = true;
			break;
		}
	}
	for (auto& range : backupRanges) {
		TraceEvent("InsideValidBackupRange")
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
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			KeyRef k1 = kvs[begin].key;
			KeyRef k2 = end < kvs.size() ? kvs[end].key : normalKeys.end;
			TraceEvent(SevFRTestInfo, "TransformDatabaseContentsWriteKVReadBack")
			    .detail("Range", KeyRangeRef(k1, k2))
			    .detail("Begin", begin)
			    .detail("End", end);
			Standalone<RangeResultRef> readKVs = wait(tr.getRange(KeyRangeRef(k1, k2), CLIENT_KNOBS->TOO_MANY));
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
ACTOR Future<Void> transformDatabaseContents(Database cx, Key addPrefix, Key removePrefix,
                                             Standalone<VectorRef<KeyRangeRef>> restoreRanges) {
	state ReadYourWritesTransaction tr(cx);
	state Standalone<VectorRef<KeyValueRef>> oldData;

	loop { // Read all data from DB
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			TraceEvent("FastRestoreWorkloadTransformDatabaseContents")
			    .detail("AddPrefix", addPrefix)
			    .detail("RemovePrefix", removePrefix);

			state int i = 0;
			for (i = 0; i < restoreRanges.size(); ++i) {
				Standalone<RangeResultRef> kvs = wait(tr.getRange(restoreRanges[i], CLIENT_KNOBS->TOO_MANY));
				ASSERT(!kvs.more);
				for (auto kv : kvs) {
					oldData.push_back_deep(oldData.arena(), KeyValueRef(kv.key, kv.value));
				}
			}
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
			oldData = Standalone<VectorRef<KeyValueRef>>(); // clear the vector
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
		if (newKey.size() < removePrefix.size()) { // If true, must check why?!
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
	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Standalone<RangeResultRef> emptyData = wait(tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
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
	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Standalone<RangeResultRef> allData = wait(tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
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
ACTOR Future<Void> transformRestoredDatabase(Database cx, Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                             Key addPrefix, Key removePrefix) {
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