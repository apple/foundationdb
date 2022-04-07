/*
 * RestoreCommon.actor.cpp
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
    RestoreConfigFR* self,
    Reference<ReadYourWritesTransaction> tr) {
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
	if (e.code() == error_code_key_not_found)
		t.backtrace();

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
	state Future<Optional<Value>> beginVal = tr->get(uidPrefixKey(applyMutationsBeginRange.begin, uid), Snapshot::True);
	state Future<Optional<Value>> endVal = tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid), Snapshot::True);
	wait(success(beginVal) && success(endVal));

	if (!beginVal.get().present() || !endVal.get().present())
		return 0;

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

// Meng: Change RestoreConfigFR to Reference<RestoreConfigFR> because FastRestore pass the Reference<RestoreConfigFR>
// around
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
		errstr = format("'%s' %llds ago.\n",
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
	    .detail("ApplyLag", lag.get())
	    .detail("TaskInstance", THIS_ADDR)
	    .backtrace();

	return format("Tag: %s  UID: %s  State: %s  Blocks: %lld/%lld  BlocksInProgress: %lld  Files: %lld  BytesWritten: "
	              "%lld  ApplyVersionLag: %lld  LastError: %s",
	              tag.get().c_str(),
	              uid.toString().c_str(),
	              status.get().toString().c_str(),
	              fileBlocksFinished.get(),
	              fileBlockCount.get(),
	              fileBlocksDispatched.get() - fileBlocksFinished.get(),
	              fileCount.get(),
	              bytesWritten.get(),
	              lag.get(),
	              errstr.c_str());
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

ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeLogFileBlock(Reference<IAsyncFile> file,
                                                                    int64_t offset,
                                                                    int len) {
	state Standalone<StringRef> buf = makeString(len);
	int rLen = wait(file->read(mutateString(buf), len, offset));
	if (rLen != len)
		throw restore_bad_read();

	simulateBlobFailure();

	Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
	state StringRefReader reader(buf, restore_corrupted_data());

	try {
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
