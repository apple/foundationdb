/*
 * FileBackupAgent.actor.cpp
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

#include "BackupAgent.h"
#include "BackupContainer.h"
#include "DatabaseContext.h"
#include "ManagementAPI.h"
#include "Status.h"
#include "KeyBackedTypes.h"

#include <ctime>
#include <climits>
#include "fdbrpc/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include <numeric>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <algorithm>

const Key FileBackupAgent::keyLastRestorable = LiteralStringRef("last_restorable");

// For convenience
typedef FileBackupAgent::ERestoreState ERestoreState;

StringRef FileBackupAgent::restoreStateText(ERestoreState id) {
	switch(id) {
		case ERestoreState::UNITIALIZED: return LiteralStringRef("unitialized");
		case ERestoreState::QUEUED: return LiteralStringRef("queued");
		case ERestoreState::STARTING: return LiteralStringRef("starting");
		case ERestoreState::RUNNING: return LiteralStringRef("running");
		case ERestoreState::COMPLETED: return LiteralStringRef("completed");
		case ERestoreState::ABORTED: return LiteralStringRef("aborted");
		default: return LiteralStringRef("Unknown");
	}
}

template<> Tuple Codec<ERestoreState>::pack(ERestoreState const &val) { return Tuple().append(val); }
template<> ERestoreState Codec<ERestoreState>::unpack(Tuple const &val) { return (ERestoreState)val.getInt(0); }

typedef BackupAgentBase::enumState EBackupState;
template<> Tuple Codec<EBackupState>::pack(EBackupState const &val) { return Tuple().append(val); }
template<> EBackupState Codec<EBackupState>::unpack(Tuple const &val) { return (EBackupState)val.getInt(0); }

// Key backed tags are a single-key slice of the TagUidMap, defined below.
// The Value type of the key is a UidAndAbortedFlagT which is a pair of {UID, aborted_flag}
// All tasks on the UID will have a validation key/value that requires aborted_flag to be
// false, so changing that value, such as changing the UID or setting aborted_flag to true,
// will kill all of the active tasks on that backup/restore UID.
typedef std::pair<UID, bool> UidAndAbortedFlagT;
class KeyBackedTag : public KeyBackedProperty<UidAndAbortedFlagT> {
public:
	KeyBackedTag() : KeyBackedProperty(StringRef()) {}
	KeyBackedTag(StringRef tagName, StringRef tagMapPrefix);

	Future<Void> cancel(Reference<ReadYourWritesTransaction> tr) {
		Key tag = tagName;
		Key tagMapPrefix = tagMapPrefix;
		return map(get(tr), [tag, tagMapPrefix, tr](Optional<UidAndAbortedFlagT> up) -> Void {
			if (up.present()) {
				// Set aborted flag to true
				up.get().second = true;
				KeyBackedTag(tag, tagMapPrefix).set(tr, up.get());
			}
			return Void();
		});
	}

	Key tagName;
	Key tagMapPrefix;
};

// Map of tagName to {UID, aborted_flag} located in the fileRestorePrefixRange keyspace.
class TagUidMap : public KeyBackedMap<Key, UidAndAbortedFlagT> {
public:
	typedef KeyBackedMap<Key, UidAndAbortedFlagT> TagMap;

	TagUidMap(const StringRef & prefix) : TagMap(LiteralStringRef("tag->uid/").withPrefix(prefix)), prefix(prefix) {}

	ACTOR static Future<std::vector<KeyBackedTag>> getAll_impl(TagUidMap *tagsMap, Reference<ReadYourWritesTransaction> tr) {
		TagMap::PairsType tagPairs = wait(tagsMap->getRange(tr, KeyRef(), {}, 1e6));
		std::vector<KeyBackedTag> results;
		for(auto &p : tagPairs)
			results.push_back(KeyBackedTag(p.first, tagsMap->prefix));
		return results;
	}

	Future<std::vector<KeyBackedTag>> getAll(Reference<ReadYourWritesTransaction> tr) {
		return getAll_impl(this, tr);
	}

	Key prefix;
};

KeyBackedTag::KeyBackedTag(StringRef tagName, StringRef tagMapPrefix)
		: KeyBackedProperty<UidAndAbortedFlagT>(TagUidMap(tagMapPrefix).getProperty(tagName)), tagName(tagName), tagMapPrefix(tagMapPrefix) {}

KeyBackedTag makeRestoreTag(StringRef tagName) {
	return KeyBackedTag(tagName, fileRestorePrefixRange.begin);
}

KeyBackedTag makeBackupTag(StringRef tagName) {
	return KeyBackedTag(tagName, fileBackupPrefixRange.begin);
}

Future<std::vector<KeyBackedTag>> getAllRestoreTags(Reference<ReadYourWritesTransaction> tr) {
	return TagUidMap(fileRestorePrefixRange.begin).getAll(tr);
}

Future<std::vector<KeyBackedTag>> getAllBackupTags(Reference<ReadYourWritesTransaction> tr) {
	return TagUidMap(fileBackupPrefixRange.begin).getAll(tr);
}

class KeyBackedConfig {
public:
	static struct {
		static TaskParam<UID> uid() {return LiteralStringRef(__FUNCTION__); }
	} TaskParams;

	KeyBackedConfig(StringRef prefix, UID uid = UID()) :
			uid(uid),
			prefix(prefix),
			configSpace(uidPrefixKey(LiteralStringRef("uid->config/").withPrefix(prefix), uid)) {}

	KeyBackedConfig(StringRef prefix, Reference<Task> task) : KeyBackedConfig(prefix, TaskParams.uid().get(task)) {}

	Future<Void> toTask(Reference<ReadYourWritesTransaction> tr, Reference<Task> task) {
		// Set the uid task parameter
		TaskParams.uid().set(task, uid);
		// Set the validation condition for the task which is that the restore uid's tag's uid is the same as the restore uid.
		// Get this uid's tag, then get the KEY for the tag's uid but don't read it.  That becomes the validation key
		// which TaskBucket will check, and its value must be this restore config's uid.
		UID u = uid;  // 'this' could be invalid in lambda
		Key p = prefix;
		return map(tag().get(tr), [u,p,task](Optional<Value> const &tag) -> Void {
			if(!tag.present())
				throw restore_error();
			// Validation contition is that the uidPair key must be exactly {u, false}
			TaskBucket::setValidationCondition(task, KeyBackedTag(tag.get(), p).key, Codec<UidAndAbortedFlagT>::pack({u, false}).pack());
			return Void();
		});
	}

	KeyBackedProperty<Value> tag() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	UID getUid() { return uid; }

	void clear(Reference<ReadYourWritesTransaction> tr) {
		tr->clear(configSpace.range());
	}

protected:
	UID uid;
	Key prefix;
	Subspace configSpace;
};

class RestoreConfig : public KeyBackedConfig {
public:
	RestoreConfig(UID uid = UID()) : KeyBackedConfig(fileRestorePrefixRange.begin, uid) {}
	RestoreConfig(Reference<Task> task) : KeyBackedConfig(fileRestorePrefixRange.begin, task) {}

	KeyBackedProperty<ERestoreState> stateEnum() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	Future<StringRef> stateText(Reference<ReadYourWritesTransaction> tr) {
		return map(stateEnum().getD(tr), [](ERestoreState s) -> StringRef { return FileBackupAgent::restoreStateText(s); });
	}
	KeyBackedProperty<Key> addPrefix() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<Key> removePrefix() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<KeyRange> restoreRange() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<Key> batchFuture() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<Version> restoreVersion() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	KeyBackedProperty<Value> sourceURL() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Total bytes written by all log and range restore tasks.
	KeyBackedBinaryValue<int64_t> bytesWritten() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// File blocks that have had tasks created for them by the Dispatch task
	KeyBackedBinaryValue<int64_t> filesBlocksDispatched() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// File blocks whose tasks have finished
	KeyBackedBinaryValue<int64_t> fileBlocksFinished() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// Total number of files in the fileMap
	KeyBackedBinaryValue<int64_t> fileCount() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
	// Total number of file blocks in the fileMap
	KeyBackedBinaryValue<int64_t> fileBlockCount() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	typedef std::pair<Version, Value> VersionAndValueT;
	typedef KeyBackedMap<VersionAndValueT, bool> FileMapT;
	FileMapT fileMap() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// lastError is a pair of error message and timestamp expressed as an int64_t
	KeyBackedProperty<std::pair<Value, int64_t>> lastError() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	Future<bool> isRunnable(Reference<ReadYourWritesTransaction> tr) {
		return map(stateEnum().getD(tr), [](ERestoreState s) -> bool { return   s != ERestoreState::ABORTED
																			&& s != ERestoreState::COMPLETED
																			&& s != ERestoreState::UNITIALIZED;
		});
	}

	Future<Void> logError(Database cx, Error e, std::string details, void *taskInstance = nullptr) {
		if(!uid.isValid()) {
			TraceEvent(SevError, "FileRestoreErrorNoUID").error(e).detail("Description", details);
			return Void();
		}
		// Lambda might outlive 'this' so save uid, capture by value, and make a new RestoreConfig from it later.
		UID u = uid;
		return runRYWTransaction(cx, [u, cx, e, details, taskInstance](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			TraceEvent(SevWarn, "FileRestoreError").error(e).detail("RestoreUID", u).detail("Description", details).detail("TaskInstance", (uint64_t)taskInstance);
			std::string msg = format("ERROR: %s %s", e.what(), details.c_str());
			RestoreConfig restore(u);
			restore.lastError().set(tr, {StringRef(msg), (int64_t)now()});
			return Void();
		});
	}

	Key mutationLogPrefix() {
		return uidPrefixKey(applyLogKeys.begin, uid);
	}

	Key applyMutationsMapPrefix() {
		 return uidPrefixKey(applyMutationsKeyVersionMapRange.begin, uid);
	}

	ACTOR static Future<int64_t> getApplyVersionLag_impl(Reference<ReadYourWritesTransaction> tr, UID uid) {
		// Both of these are snapshot reads
		state Future<Optional<Value>> beginVal = tr->get(uidPrefixKey(applyMutationsBeginRange.begin, uid), true);
		state Future<Optional<Value>> endVal = tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid), true);
		Void _ = wait(success(beginVal) && success(endVal));

		if(!beginVal.get().present() || !endVal.get().present())
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

	void setApplyEndVersion(Reference<ReadYourWritesTransaction> tr, Version ver) {
		tr->set(uidPrefixKey(applyMutationsEndRange.begin, uid), BinaryWriter::toValue(ver, Unversioned()));
	}

	Future<Version> getApplyEndVersion(Reference<ReadYourWritesTransaction> tr) {
		return map(tr->get(uidPrefixKey(applyMutationsEndRange.begin, uid)), [=](Optional<Value> const &value) -> Version {
			return value.present() ? BinaryReader::fromStringRef<Version>(value.get(), Unversioned()) : 0;
		});
	}

	static Future<std::string> getProgress_impl(RestoreConfig const &restore, Reference<ReadYourWritesTransaction> const &tr);
	Future<std::string> getProgress(Reference<ReadYourWritesTransaction> tr) {
		return getProgress_impl(*this, tr);
	}

	static Future<std::string> getFullStatus_impl(RestoreConfig const &restore, Reference<ReadYourWritesTransaction> const &tr);
	Future<std::string> getFullStatus(Reference<ReadYourWritesTransaction> tr) {
		return getFullStatus_impl(*this, tr);
	}
};

ACTOR Future<std::string> RestoreConfig::getProgress_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<int64_t> fileCount = restore.fileCount().getD(tr);
	state Future<int64_t> fileBlockCount = restore.fileBlockCount().getD(tr);
	state Future<int64_t> fileBlocksDispatched = restore.filesBlocksDispatched().getD(tr);
	state Future<int64_t> fileBlocksFinished = restore.fileBlocksFinished().getD(tr);
	state Future<int64_t> bytesWritten = restore.bytesWritten().getD(tr);
	state Future<StringRef> status = restore.stateText(tr);
	state Future<Version> lag = restore.getApplyVersionLag(tr);
	state Future<Key> tag = restore.tag().getD(tr);
	state Future<std::pair<Value, int64_t>> lastError = restore.lastError().getD(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	state UID uid = restore.getUid();
	Void _ = wait(success(fileCount) && success(fileBlockCount) && success(fileBlocksDispatched) && success(fileBlocksFinished) && success(bytesWritten) && success(status) && success(lag) && success(tag) && success(lastError));

	std::string errstr = "None";
	if(lastError.get().second != 0)
		errstr = format("'%s' %llds ago.\n", lastError.get().first.toString().c_str(), (int64_t)now() - lastError.get().second);

	TraceEvent("FileRestoreProgress")
		.detail("UID", uid)
		.detail("Tag", tag.get().toString())
		.detail("State", status.get().toString())
		.detail("FileCount", fileCount.get())
		.detail("FileBlocksFinished", fileBlocksFinished.get())
		.detail("FileBlocksTotal", fileBlockCount.get())
		.detail("FileBlocksInProgress", fileBlocksDispatched.get() - fileBlocksFinished.get())
		.detail("BytesWritten", bytesWritten.get())
		.detail("ApplyLag", lag.get())
		.detail("TaskInstance", (uint64_t)this);


	return format("Tag: %s  UID: %s  State: %s  Blocks: %lld/%lld  BlocksInProgress: %lld  Files: %lld  BytesWritten: %lld  ApplyVersionLag: %lld  LastError: %s",
					tag.get().toString().c_str(),
					uid.toString().c_str(),
					status.get().toString().c_str(),
					fileBlocksFinished.get(),
					fileBlockCount.get(),
					fileBlocksDispatched.get() - fileBlocksFinished.get(),
					fileCount.get(),
					bytesWritten.get(),
					lag.get(),
					errstr.c_str()
				);
}

ACTOR Future<std::string> RestoreConfig::getFullStatus_impl(RestoreConfig restore, Reference<ReadYourWritesTransaction> tr) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);

	state Future<KeyRange> range = restore.restoreRange().getD(tr);
	state Future<Key> addPrefix = restore.addPrefix().getD(tr);
	state Future<Key> removePrefix = restore.removePrefix().getD(tr);
	state Future<Key> url = restore.sourceURL().getD(tr);
	state Future<Version> restoreVersion = restore.restoreVersion().getD(tr);
	state Future<std::string> progress = restore.getProgress(tr);

	// restore might no longer be valid after the first wait so make sure it is not needed anymore.
	state UID uid = restore.getUid();
	Void _ = wait(success(range) && success(addPrefix) && success(removePrefix) && success(url) && success(restoreVersion) && success(progress));

	return format("%s  URL: %s  Begin: '%s'  End: '%s'  AddPrefix: '%s'  RemovePrefix: '%s'  Version: %lld",
					progress.get().c_str(),
					url.get().toString().c_str(),
					printable(range.get().begin).c_str(),
					printable(range.get().end).c_str(),
					printable(addPrefix.get()).c_str(),
					printable(removePrefix.get()).c_str(),
					restoreVersion.get()
	);
}

// TODO:  Grow this class so it does for backup what RestoreConfig does for restore, such as holding all useful backup metadata
// and handling binding to a Task, initializing from a Task, and setting Task validation parameters.
// NOTE that when doing this backup configs MUST be changed to use UNIQUE uids for each backup or bad things can happened
// such as stale backup tasks writing data into newer backups or reading the wrong kv manifest data or writing it to the wrong backup.
class BackupConfig : public KeyBackedConfig {
public:
	BackupConfig(UID uid = UID()) : KeyBackedConfig(fileBackupPrefixRange.begin, uid) {}
	BackupConfig(Reference<Task> task) : KeyBackedConfig(fileBackupPrefixRange.begin, task) {}

	// rangeFileMap maps a keyrange file's End to its Begin and Filename
	typedef std::pair<Key, Key> KeyAndFilenameT;
	typedef KeyBackedMap<Key, KeyAndFilenameT> RangeFileMapT;
	RangeFileMapT rangeFileMap() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedBinaryValue<int64_t> rangeBytesWritten() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedBinaryValue<int64_t> logBytesWritten() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<EBackupState> stateEnum() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<Key> backupContainer() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}
};

FileBackupAgent::FileBackupAgent()
	: subspace(Subspace(fileBackupPrefixRange.begin))
	// tagNames has tagName => logUID
	, tagNames(subspace.get(BackupAgentBase::keyTagName))
	// The other subspaces have logUID -> value
	, lastRestorable(subspace.get(FileBackupAgent::keyLastRestorable))
	, states(subspace.get(BackupAgentBase::keyStates))
	, config(subspace.get(BackupAgentBase::keyConfig))
	, errors(subspace.get(BackupAgentBase::keyErrors))
	, ranges(subspace.get(BackupAgentBase::keyRanges))
	, taskBucket(new TaskBucket(subspace.get(BackupAgentBase::keyTasks), true, false, true))
	, futureBucket(new FutureBucket(subspace.get(BackupAgentBase::keyFutures), true, true))
	, sourceStates(subspace.get(BackupAgentBase::keySourceStates))
{
}

namespace fileBackup {

	ACTOR static Future<Void> writeString(Reference<IAsyncFile> file, Standalone<StringRef> s, int64_t *pOffset) {
		state uint32_t lenBuf = bigEndian32((uint32_t)s.size());
		Void _ = wait(file->write(&lenBuf, sizeof(lenBuf), *pOffset));
		*pOffset += sizeof(lenBuf);
		Void _ = wait(file->write(s.begin(), s.size(), *pOffset));
		*pOffset += s.size();
		return Void();
	}

	// Padding bytes for backup files.  The largest padded area that could ever have to be written is
	// the size of two 32 bit ints and the largest key size and largest value size.  Since CLIENT_KNOBS
	// may not be initialized yet a conservative constant is being used.
	std::string paddingFFs(128 * 1024, 0xFF);

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
		RangeFileWriter(Reference<IAsyncFile> file = Reference<IAsyncFile>(), int blockSize = 0) : file(file), blockSize(blockSize), offset(0), blockEnd(0), fileVersion(1001) {}

		// Handles the first block and internal blocks.  Ends current block if needed.
		ACTOR static Future<Void> newBlock(RangeFileWriter *self, int bytesNeeded) {
			// Write padding to finish current block if needed
			int bytesLeft = self->blockEnd - self->offset;
			if(bytesLeft > 0) {
				Void _ = wait(self->file->write(paddingFFs.data(), bytesLeft, self->offset));
				self->offset = self->blockEnd;
			}

			// Set new blockEnd
			self->blockEnd += self->blockSize;

			// write Header
			Void _ = wait(self->file->write(&self->fileVersion, sizeof(self->fileVersion), self->offset));
			self->offset += sizeof(self->fileVersion);

			// If this is NOT the first block then write duplicate stuff needed from last block
			if(self->blockEnd > self->blockSize) {
				Void _ = wait(self->write(self->lastKey));
				Void _ = wait(self->write(self->lastKey));
				Void _ = wait(self->write(self->lastValue));
			}

			// There must now be room in the current block for bytesNeeded or the block size is too small
			if(self->offset + bytesNeeded > self->blockEnd)
				throw backup_bad_block_size();

			return Void();
		}

		// Ends the current block if necessary based on bytesNeeded.
		Future<Void> newBlockIfNeeded(int bytesNeeded) {
			if(offset + bytesNeeded > blockEnd)
				return newBlock(this, bytesNeeded);
			return Void();
		}

		// Start a new block if needed, then write the key and value
		ACTOR static Future<Void> writeKV_impl(RangeFileWriter *self, Key k, Value v) {
			int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
			Void _ = wait(self->newBlockIfNeeded(toWrite));
			Void _ = wait(self->write(k));
			Void _ = wait(self->write(v));
			self->lastKey = k;
			self->lastValue = v;
			return Void();
		}

		Future<Void> writeKV(Key k, Value v) { return writeKV_impl(this, k, v); }

		// Write begin key or end key.
		ACTOR static Future<Void> writeKey_impl(RangeFileWriter *self, Key k) {
			int toWrite = sizeof(int32_t) + k.size();
			Void _ = wait(self->newBlockIfNeeded(toWrite));
			Void _ = wait(self->write(k));
			return Void();
		}

		Future<Void> writeKey(Key k) { return writeKey_impl(this, k); }

		Reference<IAsyncFile> file;
		int blockSize;
		int64_t offset;

	private:
		Future<Void> write(Standalone<StringRef> s) { return writeString(file, s, &offset); }
		int64_t blockEnd;
		uint32_t fileVersion;
		Key lastKey;
		Key lastValue;
	};

	// Helper class for reading restore data from a buffer and throwing the right errors.
	struct StringRefReader {
		StringRefReader(StringRef s = StringRef(), Error e = Error()) : rptr(s.begin()), end(s.end()), failure_error(e) {}

		// Return remainder of data as a StringRef
		StringRef remainder() {
			return StringRef(rptr, end - rptr);
		}

		// Return a pointer to len bytes at the current read position and advance read pos
		const uint8_t * consume(unsigned int len) {
			if(rptr == end && len != 0)
				throw end_of_stream();
			const uint8_t *p = rptr;
			rptr += len;
			if(rptr > end)
				throw failure_error;
			return p;
		}

		// Return a T from the current read position and advance read pos
		template<typename T> const T consume() {
			return *(const T *)consume(sizeof(T));
		}

		// Functions for consuming big endian (network byte order) integers.
		// Consumes a big endian number, swaps it to little endian, and returns it.
		const int32_t  consumeNetworkInt32()  { return (int32_t)bigEndian32((uint32_t)consume< int32_t>());}
		const uint32_t consumeNetworkUInt32() { return          bigEndian32(          consume<uint32_t>());}

		bool eof() { return rptr == end; }

		const uint8_t *rptr, *end;
		Error failure_error;
	};

	ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file, int64_t offset, int len) {
		state Standalone<StringRef> buf = makeString(len);
		int rLen = wait(file->read(mutateString(buf), len, offset));
		if(rLen != len)
			throw restore_bad_read();

		Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
		state StringRefReader reader(buf, restore_corrupted_data());

		try {
			// Read header, currently only decoding version 1001
			if(reader.consume<int32_t>() != 1001)
				throw restore_unsupported_file_version();

			// Read begin key, if this fails then block was invalid.
			uint32_t kLen = reader.consumeNetworkUInt32();
			const uint8_t *k = reader.consume(kLen);
			results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));

			// Read kv pairs and end key
			while(1) {
				// Read a key.
				kLen = reader.consumeNetworkUInt32();
				k = reader.consume(kLen);

				// If eof reached or first value len byte is 0xFF then a valid block end was reached.
				if(reader.eof() || *reader.rptr == 0xFF) {
					results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef()));
					break;
				}

				// Read a value, which must exist or the block is invalid
				uint32_t vLen = reader.consumeNetworkUInt32();
				const uint8_t *v = reader.consume(vLen);
				results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));

				// If eof reached or first byte of next key len is 0xFF then a valid block end was reached.
				if(reader.eof() || *reader.rptr == 0xFF)
					break;
			}

			// Make sure any remaining bytes in the block are 0xFF
			for(auto b : reader.remainder())
				if(b != 0xFF)
					throw restore_corrupted_data_padding();

			return results;

		} catch(Error &e) {
			TraceEvent(SevError, "FileRestoreCorruptRangeFileBlock")
				.detail("Filename", file->getFilename())
				.detail("BlockOffset", offset)
				.detail("BlockLen", len)
				.detail("ErrorRelativeOffset", reader.rptr - buf.begin())
				.detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset)
				.error(e);
			throw;
		}
	}


	// Very simple format compared to KeyRange files.
	// Header, [Key, Value]... Key len
	struct LogFileWriter {
		static const std::string &FFs;

		LogFileWriter(Reference<IAsyncFile> file = Reference<IAsyncFile>(), int blockSize = 0) : file(file), blockSize(blockSize), offset(0), blockEnd(0), fileVersion(2001) {}

		// Start a new block if needed, then write the key and value
		ACTOR static Future<Void> writeKV_impl(LogFileWriter *self, Key k, Value v) {
			// If key and value do not fit in this block, end it and start a new one
			int toWrite = sizeof(int32_t) + k.size() + sizeof(int32_t) + v.size();
			if(self->offset + toWrite > self->blockEnd) {
				// Write padding if needed
				int bytesLeft = self->blockEnd - self->offset;
				if(bytesLeft > 0) {
					Void _ = wait(self->file->write(paddingFFs.data(), bytesLeft, self->offset));
					self->offset = self->blockEnd;
				}

				// Set new blockEnd
				self->blockEnd += self->blockSize;

				// write Header
				Void _ = wait(self->file->write(&self->fileVersion, sizeof(self->fileVersion), self->offset));
				self->offset += sizeof(self->fileVersion);
			}

			Void _ = wait(self->write(k));
			Void _ = wait(self->write(v));

			// At this point we should be in whatever the current block is or the block size is too small
			if(self->offset > self->blockEnd)
				throw backup_bad_block_size();

			return Void();
		}

		Future<Void> writeKV(Key k, Value v) { return writeKV_impl(this, k, v); }

		Reference<IAsyncFile> file;
		int blockSize;
		int64_t offset;

	private:
		Future<Void> write(Standalone<StringRef> s) { return writeString(file, s, &offset); }
		int64_t blockEnd;
		uint32_t fileVersion;
	};

	ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeLogFileBlock(Reference<IAsyncFile> file, int64_t offset, int len) {
		state Standalone<StringRef> buf = makeString(len);
		int rLen = wait(file->read(mutateString(buf), len, offset));
		if(rLen != len)
			throw restore_bad_read();

		Standalone<VectorRef<KeyValueRef>> results({}, buf.arena());
		state StringRefReader reader(buf, restore_corrupted_data());

		try {
			// Read header, currently only decoding version 2001
			if(reader.consume<int32_t>() != 2001)
				throw restore_unsupported_file_version();

			// Read k/v pairs.  Block ends either at end of last value exactly or with 0xFF as first key len byte.
			while(1) {
				// If eof reached or first key len bytes is 0xFF then end of block was reached.
				if(reader.eof() || *reader.rptr == 0xFF)
					break;

				// Read key and value.  If anything throws then there is a problem.
				uint32_t kLen = reader.consumeNetworkUInt32();
				const uint8_t *k = reader.consume(kLen);
				uint32_t vLen = reader.consumeNetworkUInt32();
				const uint8_t *v = reader.consume(vLen);

				results.push_back(results.arena(), KeyValueRef(KeyRef(k, kLen), ValueRef(v, vLen)));
			}

			// Make sure any remaining bytes in the block are 0xFF
			for(auto b : reader.remainder())
				if(b != 0xFF)
					throw restore_corrupted_data_padding();

			return results;

		} catch(Error &e) {
			TraceEvent(SevError, "FileRestoreCorruptLogFileBlock")
				.detail("Filename", file->getFilename())
				.detail("BlockOffset", offset)
				.detail("BlockLen", len)
				.detail("ErrorRelativeOffset", reader.rptr - buf.begin())
				.detail("ErrorAbsoluteOffset", reader.rptr - buf.begin() + offset)
				.error(e);
			throw;
		}
	}

	bool copyDefaultParameters(Reference<Task> source, Reference<Task> dest) {
		if (source) {
			copyParameter(source, dest, BackupAgentBase::keyStateStop);
			copyParameter(source, dest, BackupAgentBase::keyConfigBackupTag);
			copyParameter(source, dest, BackupAgentBase::keyConfigLogUid);
			copyParameter(source, dest, BackupAgentBase::keyErrors);

			return true;
		}

		return false;
	}

	// This is used to open files during BACKUP tasks.
	ACTOR static Future<Reference<IAsyncFile>> doOpenBackupFile(bool writeMode, std::string backupContainer, std::string fileName, Key keyErrors, Database cx) {
		try
		{
			Reference<IBackupContainer> container = IBackupContainer::openContainer(backupContainer);
			IBackupContainer::EMode mode = writeMode ? IBackupContainer::WRITEONLY : IBackupContainer::READONLY;
			state Reference<IAsyncFile>	backupFile = wait(container->openFile(fileName, mode));
			return backupFile;
		}
		catch (Error &e) {
			state Error err = e;
			Void _ = wait(logError(cx, keyErrors, format("ERROR: Failed to open file `%s' because of error %s", fileName.c_str(), err.what())));
			throw err;
		}
	}

	static Future<Reference<IAsyncFile>> openBackupFile(bool writeMode, std::string backupContainer, std::string fileName, Key keyErrors, Database cx) {
		return doOpenBackupFile(writeMode, backupContainer, fileName, keyErrors, cx);
	}

	ACTOR Future<Void> truncateCloseFile(Database cx, Key keyErrors, std::string backupContainer, std::string fileName, Reference<IAsyncFile> file, int64_t truncateSize = -1) {
		if (truncateSize == -1) {
			int64_t size = wait(file->size());
			truncateSize = size;
		}

		// Never write an empty file, write one null byte instead. This is just to make blob store happy (or any other underlying fs with no empty file concept)
		if(truncateSize == 0) {
			char c = '\0';
			Void _ = wait(file->write(&c, 1, 0));
			truncateSize += 1;
		}

		state Future<Void> truncate = file->truncate(truncateSize);
		state Future<Void> sync = file->sync();

		try {
			Void _ = wait(truncate);
		} catch( Error &e ) {
			if(e.code() == error_code_actor_cancelled)
				throw;

			state Error err = e;
			Void _ = wait(logError(cx, keyErrors, format("ERROR: Failed to write to file `%s' in container '%s' because of error %s", fileName.c_str(), backupContainer.c_str(), err.what())));
			throw err;
		}

		try {
			Void _ = wait(sync);
		} catch( Error &e ) {
			if(e.code() == error_code_actor_cancelled)
				throw;
			TraceEvent("FBA_TruncateCloseFileSyncError").error(e);

			state Transaction tr(cx);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
					Tuple t;
					t.append(std::numeric_limits<Version>::max());
					t.append(StringRef());
					tr.set(t.pack().withPrefix(keyErrors), format("WARNING: Cannot sync file `%s' in container '%s'", fileName.c_str(), backupContainer.c_str()));
					Void _ = wait( tr.commit() );
					break;
				} catch( Error &e ) {
					Void _ = wait( tr.onError(e));
				}
			}
		}
		file = Reference<IAsyncFile>();
		return Void();
	}

	ACTOR static Future<std::string> getPath(Reference<ReadYourWritesTransaction> tr, Key configOutputPath) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Optional<Value> dir = wait(tr->get(configOutputPath));

		return dir.present() ? dir.get().toString() : "";
	}

	ACTOR template <class Tr>
	Future<Void> checkTaskVersion(Tr tr, Reference<Task> task, StringRef name, uint32_t version) {
		uint32_t taskVersion = task->getVersion();
		if (taskVersion > version) {
			TraceEvent(SevError, "BA_BackupRangeTaskFunc_execute").detail("taskVersion", taskVersion).detail("Name", printable(name)).detail("Version", version);
			Void _ = wait(logError(tr, task->params[FileBackupAgent::keyErrors],
				format("ERROR: %s task version `%lu' is greater than supported version `%lu'", task->params[Task::reservedTaskParamKeyType].toString().c_str(), (unsigned long)taskVersion, (unsigned long)version)));

			throw task_invalid_version();
		}

		return Void();
	}

	ACTOR static Future<Void> writeRestoreFile(Reference<ReadYourWritesTransaction> tr, Key keyErrors, std::string backupContainer, UID logUid,
		Version stopVersion, Key keyConfigBackupTag, Key keyConfigLogUid, Key keyConfigBackupRanges)
	{
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state std::string filename = "restorable";
		state std::string tempFileName = FileBackupAgent::getTempFilename();
		state Reference<IAsyncFile> f = wait(openBackupFile(true, backupContainer, tempFileName, keyErrors, tr->getDatabase()));

		if (f) {
			state int64_t offset = wait(f->size());
			state std::string msg;

			TraceEvent(SevInfo, "BA_FinishFileWrite").detail("tempfile", tempFileName).detail("backupContainer", backupContainer);

			msg += format("%-15s %ld\n", "fdbbackupver:", backupVersion);
			msg += format("%-15s %lld\n", "restorablever:", stopVersion);
			msg += format("%-15s %s\n", "tag:", printable(keyConfigBackupTag).c_str());
			msg += format("%-15s %s\n", "logUid:", logUid.toString().c_str());
			msg += format("%-15s %s\n", "logUidValue:", printable(keyConfigLogUid).c_str());

			// Deserialize the backup ranges
			state Standalone<VectorRef<KeyRangeRef>> backupRanges;
			BinaryReader br(keyConfigBackupRanges, IncludeVersion());
			br >> backupRanges;

			msg += format("%-15s %d\n", "ranges:", backupRanges.size());

			for (auto &backupRange : backupRanges) {
				msg += format("%-15s %s\n", "rangebegin:", printable(backupRange.begin).c_str());
				msg += format("%-15s %s\n", "rangeend:", printable(backupRange.end).c_str());
			}

			msg += format("%-15s %s\n", "time:", BackupAgentBase::getCurrentTime().toString().c_str());
			msg += format("%-15s %lld\n", "timesecs:", (long long) now());

			// Variable boundary
			msg += "\n\n----------------------------------------------------\n\n\n";

			msg += format("This backup can be restored beginning at version %lld.\n", stopVersion);

			// Write the message to the file
			try {
				f->write(msg.c_str(), msg.size(), offset);
			} catch( Error &e ) {
				if( e.code() == error_code_actor_cancelled)
					throw;

				state Error err = e;
				Void _ = wait(logError(tr->getDatabase(), keyErrors, format("ERROR: Failed to write to file `%s' because of error %s", filename.c_str(), err.what())));

				throw err;
			}
			Void _ = wait(truncateCloseFile(tr->getDatabase(), keyErrors, backupContainer, filename, f, offset + msg.size()));
			Void _ = wait(IBackupContainer::openContainer(backupContainer)->renameFile(tempFileName, filename));
			tr->set(FileBackupAgent().lastRestorable.get(keyConfigBackupTag).pack(), BinaryWriter::toValue(stopVersion, Unversioned()));
		}
		else {
			Void _ = wait(logError(tr, keyErrors, "ERROR: Failed to open restorable file for unknown reason."));
			throw io_error();
		}

		TraceEvent("BA_WriteRestoreFile").detail("logUid", logUid).detail("stopVersion", stopVersion)
			.detail("backupContainer", backupContainer)
			.detail("backupTag", printable(StringRef(keyConfigBackupTag)));

		return Void();
	}

	struct BackupRangeTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		static const Key keyAddBackupRangeTasks;
		static const Key keyBackupRangeBeginKey;
		static const Key keyFileSize;

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };

		// Record a range file to the range file map in the backup.
		ACTOR static Future<bool> recordRangeFile(BackupConfig backup, Database cx, Reference<Task> task, Reference<TaskBucket> taskBucket, KeyRange range, std::string fileName) {
			// Ignore empty ranges.
			if(range.empty())
				return true;

			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			loop {
				try {
					tr->reset();
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	
					// It's VERY important to check this because currently backup re-uses UIDs so we could be writing this 
					// into a completed backup's map or worse, the map for a differnet and newer backup which would then
					// be corrupted.
					bool keepGoing = wait(taskBucket->keepRunning(tr, task));
					if(!keepGoing)
						return false;

					// See if there is already a file for this key which has an earlier begin
					Optional<BackupConfig::KeyAndFilenameT> beginAndFilename = wait(backup.rangeFileMap().get(tr, range.end));
					if(beginAndFilename.present() && beginAndFilename.get().first < range.begin)
						break;

					backup.rangeFileMap().set(tr, range.end, {range.begin, StringRef(fileName)});

					Void _ = wait(tr->commit());
					break;
				} catch(Error &e) {
					Void _ = wait(tr->onError(e));
				}
			}

			return true;
		}

		ACTOR static Future<Standalone<VectorRef<KeyRef>>> getBlockOfShards(Reference<ReadYourWritesTransaction> tr, Key beginKey, Key endKey, int limit) {

			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state Standalone<VectorRef<KeyRef>> results;
			Standalone<RangeResultRef> values = wait(tr->getRange(KeyRangeRef(keyAfter(beginKey.withPrefix(keyServersPrefix)), endKey.withPrefix(keyServersPrefix)), limit));

			for (auto &s : values) {
				KeyRef k = s.key.removePrefix(keyServersPrefix);
				results.push_back_deep(results.arena(), k);
			}

			return results;
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, Key begin, Key end, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>(), int priority = 0) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(BackupRangeTaskFunc::name, BackupRangeTaskFunc::version, doneKey, priority));

			// Get backup config from parent task and bind to new task
			Void _ = wait(BackupConfig(parentTask).toTask(tr, task));

			copyDefaultParameters(parentTask, task);

			task->params[BackupAgentBase::keyBeginKey] = begin;
			task->params[BackupAgentBase::keyEndKey] = end;

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		ACTOR static Future<Void> endKeyRangeFile(Database cx, Key keyErrors, RangeFileWriter *rangeFile, std::string backupContainer, std::string *outFileName, Key endKey, Version atVersion) {
			ASSERT(outFileName != nullptr);

			if (!rangeFile->file){
				return Void();
			}

			Void _ = wait(rangeFile->writeKey(endKey));

			state std::string finalName = FileBackupAgent::getDataFilename(atVersion, rangeFile->offset, rangeFile->blockSize);
			Void _ = wait(truncateCloseFile(cx, keyErrors, backupContainer, finalName, rangeFile->file, rangeFile->offset));

			Void _ = wait(IBackupContainer::openContainer(backupContainer)->renameFile(*outFileName, finalName));
			*outFileName = finalName;

			return Void();
		}

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

			Void _ = wait(checkTaskVersion(cx, task, BackupRangeTaskFunc::name, BackupRangeTaskFunc::version));

			// Find out if there is a shard boundary in(beginKey, endKey)
			Standalone<VectorRef<KeyRef>> keys = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getBlockOfShards(tr, task->params[FileBackupAgent::keyBeginKey], task->params[FileBackupAgent::keyEndKey], 1); }));
			if (keys.size() > 0) {
				task->params[BackupRangeTaskFunc::keyAddBackupRangeTasks] = StringRef();
				return Void();
			}

			// Read everything from beginKey to endKey, write it to an output file, run the output file processor, and
			// then set on_done. If we are still writing after X seconds, end the output file and insert a new backup_range
			// task for the remainder.
			state double timeout = now() + CLIENT_KNOBS->BACKUP_RANGE_TIMEOUT;
			state Reference<IAsyncFile> outFile;
			state Version outVersion = -1;

			state std::string outFileName;
			state Key beginKey = task->params[FileBackupAgent::keyBeginKey];
			state Key endKey = task->params[FileBackupAgent::keyEndKey];
			state Key lastKey;

			state KeyRange range(KeyRangeRef(beginKey, endKey));

			// retrieve kvData
			state PromiseStream<RangeResultWithVersion> results;
			state Future<Void> rc = readCommitted(cx, results, lock, range, true, true, true);
			state RangeFileWriter rangeFile;
			state int64_t fileSize = 0;

			state BackupConfig backup(task);
			state std::string backupContainer;
			loop {
				try {
					state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					Key backupContainerBytes = wait(backup.backupContainer().getOrThrow(tr));
					backupContainer = backupContainerBytes.toString();

					break;
				} catch (Error &e) {
					Void _ = wait(tr->onError(e));
				}
			}

			loop{
				try{
					state RangeResultWithVersion values = waitNext(results.getFuture());

					lock->release(values.first.expectedSize());

					//TraceEvent("FBA_Range").detail("range", values.first.size() ? printable(KeyRangeRef(values.first[0].key, values.first.end()[-1].key)) : "endOfRange").detail("version", values.second).detail("size", values.first.size());

					if ((now() >= timeout) || (values.second != outVersion)){
						if (outFile){
							TEST(true); // Backup range task wrote multiple versions

							bool isFinished = wait(taskBucket->isFinished(cx, task));
							if (isFinished){
								Void _ = wait(truncateCloseFile(cx, task->params[FileBackupAgent::keyErrors], backupContainer, outFileName, outFile));
								return Void();
							}
							state Key nextKey = keyAfter(lastKey);
							Void _ = wait(endKeyRangeFile(cx, task->params[FileBackupAgent::keyErrors], &rangeFile, backupContainer, &outFileName, nextKey, outVersion));

							// outFileName has now been modified to be the file's final (non temporary) name.
							bool keepGoing = wait(recordRangeFile(backup, cx, task, taskBucket, KeyRangeRef(beginKey, nextKey), outFileName));
							if(!keepGoing)
								return Void();

							fileSize += rangeFile.offset;
							beginKey = nextKey;
						}

						if (now() >= timeout) {
							TEST(true); // Backup range task did not finish before timeout
							task->params[BackupRangeTaskFunc::keyBackupRangeBeginKey] = beginKey;
							task->params[BackupRangeTaskFunc::keyFileSize] = BinaryWriter::toValue<int64_t>(fileSize, Unversioned());
							return Void();
						}

						outFileName = FileBackupAgent::getTempFilename();
						Reference<IAsyncFile> f = wait(openBackupFile(true, backupContainer, outFileName, task->params[FileBackupAgent::keyErrors], cx));
						outFile = f;
						outVersion = values.second;

						// Initialize range file writer and write begin key
						// block size must be at least large enough for 3 max size keys and 2 max size values + overhead so 250k conservatively.
						rangeFile = RangeFileWriter(outFile, (BUGGIFY ? g_random->randomInt(250e3, 4e6) : CLIENT_KNOBS->BACKUP_RANGEFILE_BLOCK_SIZE));
						Void _ = wait(rangeFile.writeKey(beginKey));
					}

					// write kvData to file
					state size_t i = 0;
					for (; i < values.first.size(); ++i) {
						lastKey = values.first[i].key;
						Void _ = wait(rangeFile.writeKV(lastKey, values.first[i].value));
					}
				}
				catch (Error &e) {
					state Error err = e;
					if (err.code() == error_code_actor_cancelled) {
						throw err;
					}

					if (err.code() == error_code_end_of_stream) {
						if (outFile) {
							bool isFinished = wait(taskBucket->isFinished(cx, task));
							if (isFinished){
								Void _ = wait(truncateCloseFile(cx, task->params[FileBackupAgent::keyErrors], backupContainer, outFileName, outFile));
								return Void();
							}
							try {
								Void _ = wait(endKeyRangeFile(cx, task->params[FileBackupAgent::keyErrors], &rangeFile, backupContainer, &outFileName, endKey, outVersion));

								// outFileName has now been modified to be the file's final (non temporary) name.
								bool keepGoing = wait(recordRangeFile(backup, cx, task, taskBucket, KeyRangeRef(beginKey, endKey), outFileName));
								if(!keepGoing)
									return Void();

								fileSize += rangeFile.offset;
							} catch( Error &e ) {
								state Error e2 = e;
								if (e2.code() == error_code_actor_cancelled) {
									throw e2;
								}

								Void _ = wait(logError(cx, task->params[FileBackupAgent::keyErrors], format("ERROR: Failed to write to file `%s' because of error %s", outFileName.c_str(), e2.what())));
								throw e2;
							}
						}
						task->params[BackupRangeTaskFunc::keyFileSize] = BinaryWriter::toValue<int64_t>(fileSize, Unversioned());
						return Void();
					}

					Void _ = wait(logError(cx, task->params[FileBackupAgent::keyErrors], format("ERROR: Failed to write to file `%s' because of error %s", outFileName.c_str(), err.what())));

					throw err;
				}
			}
		}

		ACTOR static Future<Void> startBackupRangeInternal(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task, Reference<TaskFuture> onDone) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state Key nextKey = task->params[BackupAgentBase::keyBeginKey];
			state Standalone<VectorRef<KeyRef>> keys = wait(getBlockOfShards(tr, nextKey, task->params[FileBackupAgent::keyEndKey], CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT));

			std::vector<Future<Key>> addTaskVector;
			for (int idx = 0; idx < keys.size(); ++idx) {
				if (nextKey != keys[idx]) {
					addTaskVector.push_back(addTask(tr, taskBucket, task, nextKey, keys[idx], TaskCompletionKey::joinWith(onDone)));
				}
				nextKey = keys[idx];
			}

			Void _ = wait(waitForAll(addTaskVector));

			if (nextKey != task->params[BackupAgentBase::keyEndKey]) {
				// Add task to cover nextKey to the end, using the priority of the current task
				Key _ = wait(addTask(tr, taskBucket, task, nextKey, task->params[BackupAgentBase::keyEndKey], TaskCompletionKey::joinWith(onDone), Reference<TaskFuture>(), task->getPriority()));
			}

			return Void();
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

			if(task->params.find(BackupRangeTaskFunc::keyFileSize) != task->params.end()) {
				int64_t fileSize = BinaryReader::fromStringRef<int64_t>(task->params[BackupRangeTaskFunc::keyFileSize], Unversioned());
				BackupConfig(task).rangeBytesWritten().atomicOp(tr, fileSize, MutationRef::AddValue);
			}

			if (task->params.find(BackupRangeTaskFunc::keyAddBackupRangeTasks) != task->params.end()) {
				Void _ = wait(startBackupRangeInternal(tr, taskBucket, futureBucket, task, taskFuture));
			}
			else if (task->params.find(BackupRangeTaskFunc::keyBackupRangeBeginKey) != task->params.end() && task->params[BackupRangeTaskFunc::keyBackupRangeBeginKey] < task->params[BackupAgentBase::keyEndKey]) {
				ASSERT(taskFuture->key.size() > 0);
				Key _ = wait(BackupRangeTaskFunc::addTask(tr, taskBucket, task, task->params[BackupRangeTaskFunc::keyBackupRangeBeginKey], task->params[BackupAgentBase::keyEndKey], TaskCompletionKey::signal(taskFuture->key)));
			}
			else {
				Void _ = wait(taskFuture->set(tr, taskBucket));
			}

			Void _ = wait(taskBucket->finish(tr, task));
			return Void();
		}

	};
	StringRef BackupRangeTaskFunc::name = LiteralStringRef("file_backup_range");
	const uint32_t BackupRangeTaskFunc::version = 1;
	const Key BackupRangeTaskFunc::keyAddBackupRangeTasks = LiteralStringRef("addBackupRangeTasks");
	const Key BackupRangeTaskFunc::keyBackupRangeBeginKey = LiteralStringRef("backupRangeBeginKey");
	const Key BackupRangeTaskFunc::keyFileSize = LiteralStringRef("fileSize");
	REGISTER_TASKFUNC(BackupRangeTaskFunc);

	struct FinishFullBackupTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			Void _ = wait(checkTaskVersion(tr, task, FinishFullBackupTaskFunc::name, FinishFullBackupTaskFunc::version));

			// Enable the stop key
			state Version readVersion = wait(tr->getReadVersion());
			tr->set(task->params[FileBackupAgent::keyStateStop], BinaryWriter::toValue(readVersion, Unversioned()));

			Void _ = wait(taskBucket->finish(tr, task));

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			// After the BackupRangeTask completes, set the stop key which will stop the BackupLogsTask
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(FinishFullBackupTaskFunc::name, FinishFullBackupTaskFunc::version, doneKey));

			// Get backup config from parent task and bind to new task
			Void _ = wait(BackupConfig(parentTask).toTask(tr, task));

			copyDefaultParameters(parentTask, task);

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };

	};
	StringRef FinishFullBackupTaskFunc::name = LiteralStringRef("file_finish_full_backup");
	const uint32_t FinishFullBackupTaskFunc::version = 1;
	REGISTER_TASKFUNC(FinishFullBackupTaskFunc);

	struct BackupLogRangeTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		static const Key keyNextBeginVersion;
		static const Key keyAddBackupLogRangeTasks;
		static const Key keyFileSize;

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };

		ACTOR static Future<Version> dumpData(Database cx, Reference<Task> task, PromiseStream<RCGroup> results, LogFileWriter *outFile,
			std::string fileName, FlowLock* lock, double timeout) {
			loop{
				try{
					state RCGroup group = waitNext(results.getFuture());

					// release lock
					lock->release(group.items.expectedSize());

					state uint32_t len = 0;
					state int i = 0;

					for (; i < group.items.size(); ++i) {
						// Remove the backupLogPrefix + UID bytes from the key
						Void _ = wait(outFile->writeKV(group.items[i].key.substr(backupLogPrefixBytes + 16), group.items[i].value));
					}
					
					if(now() >= timeout)
						return group.groupKey + 1;
				}
				catch (Error &e) {
					if(e.code() == error_code_actor_cancelled)
						throw;

					if (e.code() == error_code_end_of_stream) {
						return invalidVersion;
					}

					state Error err = e;
					Void _ = wait(logError(cx, task->params[FileBackupAgent::keyErrors], format("ERROR: Failed to write to file `%s' because of error %s", fileName.c_str(), err.what())));

					throw err;
				}
			}
		}

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

			Void _ = wait(checkTaskVersion(cx, task, BackupLogRangeTaskFunc::name, BackupLogRangeTaskFunc::version));

			state double timeout = now() + CLIENT_KNOBS->BACKUP_RANGE_TIMEOUT;
			state Version beginVersion = BinaryReader::fromStringRef<Version>(task->params[FileBackupAgent::keyBeginVersion], Unversioned());
			state Version endVersion = BinaryReader::fromStringRef<Version>(task->params[FileBackupAgent::keyEndVersion], Unversioned());

			state BackupConfig config(task);
			state std::string backupContainer;
			loop{
				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				// Wait for the read version to pass endVersion
				try {
					Key backupContainerBytes = wait(config.backupContainer().getOrThrow(tr));
					backupContainer = backupContainerBytes.toString();

					Version currentVersion = wait(tr->getReadVersion());
					if(endVersion < currentVersion)
						break;

					Void _ = wait(delay(std::max(CLIENT_KNOBS->BACKUP_RANGE_MINWAIT, (double) (endVersion-currentVersion)/CLIENT_KNOBS->CORE_VERSIONSPERSECOND)));
				}
				catch (Error &e) {
					Void _ = wait(tr->onError(e));
				}
			}

			if (now() >= timeout) {
				task->params[BackupLogRangeTaskFunc::keyNextBeginVersion] = task->params[FileBackupAgent::keyBeginVersion];
				return Void();
			}


			state Standalone<VectorRef<KeyRangeRef>> ranges = getLogRanges(beginVersion, endVersion, task->params[FileBackupAgent::keyConfigLogUid]);
			if (ranges.size() > CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
				task->params[BackupLogRangeTaskFunc::keyAddBackupLogRangeTasks] = StringRef();
				return Void();
			}

			state std::string tempFileName = FileBackupAgent::getTempFilename();
			state Reference<IAsyncFile> outFile = wait(openBackupFile(true, backupContainer, tempFileName, task->params[FileBackupAgent::keyErrors], cx));
			// Block size must be at least large enough for 1 max size key, 1 max size value, and overhead, so conservatively 125k.
			state LogFileWriter logFile(outFile, (BUGGIFY ? g_random->randomInt(125e3, 4e6) : CLIENT_KNOBS->BACKUP_LOGFILE_BLOCK_SIZE));
			state size_t idx;

			state std::vector<PromiseStream<RCGroup>> results;
			state std::vector<Future<Void>> rc;
			state std::vector<Promise<Void>> active;

			for (int i = 0; i < ranges.size(); ++i) {
				results.push_back(PromiseStream<RCGroup>());
				active.push_back(Promise<Void>());
				rc.push_back(readCommitted(cx, results[i], active[i].getFuture(), lock, ranges[i], decodeBKMutationLogKey, false, true, true, nullptr));
			}

			for (idx = 0; idx < ranges.size(); ++idx) {
				active[idx].send(Void());

				Version stopVersion = wait(dumpData(cx, task, results[idx], &logFile, tempFileName, lock.getPtr(), timeout));

				if( stopVersion != invalidVersion || now() >= timeout) {
					if(stopVersion != invalidVersion)
						endVersion = stopVersion;
					else
						endVersion = std::min<Version>(endVersion, ((beginVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE) + idx + 1) * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);

					task->params[BackupLogRangeTaskFunc::keyNextBeginVersion] = BinaryWriter::toValue(endVersion, Unversioned());
					break;
				}
			}

			task->params[BackupLogRangeTaskFunc::keyFileSize] = BinaryWriter::toValue<int64_t>(logFile.offset, Unversioned());
			std::string logFileName = FileBackupAgent::getLogFilename(beginVersion, endVersion, logFile.offset, logFile.blockSize);
			Void _ = wait(endLogFile(cx, taskBucket, futureBucket, task, outFile, tempFileName, logFileName, logFile.offset, backupContainer));

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, Version beginVersion, Version endVersion, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(BackupLogRangeTaskFunc::name, BackupLogRangeTaskFunc::version, doneKey, 1));

			// Get backup config from parent task and bind to new task
			Void _ = wait(BackupConfig(parentTask).toTask(tr, task));

			copyDefaultParameters(parentTask, task);

			task->params[FileBackupAgent::keyBeginVersion] = BinaryWriter::toValue(beginVersion, Unversioned());
			task->params[FileBackupAgent::keyEndVersion] = BinaryWriter::toValue(endVersion, Unversioned());

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		ACTOR static Future<Void> startBackupLogRangeInternal(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task, Reference<TaskFuture> taskFuture, Version beginVersion, Version endVersion ) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			std::vector<Future<Key>> addTaskVector;
			int tasks = 0;
			for (int64_t vblock = beginVersion / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE; vblock < (endVersion + CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE - 1) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE; vblock += CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) {
				Version bv = std::max(beginVersion, vblock * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);

				if( tasks >= CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT ) {
					addTaskVector.push_back(addTask(tr, taskBucket, task, bv, endVersion, TaskCompletionKey::joinWith(taskFuture)));
					break;
				}

				Version ev = std::min(endVersion, (vblock + CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES) * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
				addTaskVector.push_back(addTask(tr, taskBucket, task, bv, ev, TaskCompletionKey::joinWith(taskFuture)));
				tasks++;
			}

			Void _ = wait(waitForAll(addTaskVector));

			return Void();
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Version beginVersion = BinaryReader::fromStringRef<Version>(task->params[FileBackupAgent::keyBeginVersion], Unversioned());
			state Version endVersion = BinaryReader::fromStringRef<Version>(task->params[FileBackupAgent::keyEndVersion], Unversioned());
			state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

			if(task->params.find(BackupLogRangeTaskFunc::keyFileSize) != task->params.end()) {
				int64_t fileSize = BinaryReader::fromStringRef<int64_t>(task->params[BackupLogRangeTaskFunc::keyFileSize], Unversioned());
				BackupConfig(task).logBytesWritten().atomicOp(tr, fileSize, MutationRef::AddValue);
			}

			if (task->params.find(BackupLogRangeTaskFunc::keyAddBackupLogRangeTasks) != task->params.end()) {
				Void _ = wait(startBackupLogRangeInternal(tr, taskBucket, futureBucket, task, taskFuture, beginVersion, endVersion));
				endVersion = beginVersion;
			}
			else if (task->params.find(BackupLogRangeTaskFunc::keyNextBeginVersion) != task->params.end()) {
				state Version nextVersion = BinaryReader::fromStringRef<Version>(task->params[BackupLogRangeTaskFunc::keyNextBeginVersion], Unversioned());
				Key _ = wait(BackupLogRangeTaskFunc::addTask(tr, taskBucket, task, nextVersion, endVersion, TaskCompletionKey::joinWith(taskFuture)));
				endVersion = nextVersion;
			} else {
				Void _ = wait(taskFuture->set(tr, taskBucket));
			}

			if(endVersion > beginVersion) {
				Standalone<VectorRef<KeyRangeRef>> ranges = getLogRanges(beginVersion, endVersion, task->params[FileBackupAgent::keyConfigLogUid]);
				for (auto & rng : ranges)
					tr->clear(rng);
			}

			Void _ = wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Void> endLogFile(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task, Reference<IAsyncFile> tempFile, std::string tempFileName, std::string logFileName, int64_t size, std::string backupContainer) {
			try {
				if (tempFile) {
					Void _ = wait(truncateCloseFile(cx, task->params[FileBackupAgent::keyErrors], backupContainer, logFileName, tempFile, size));
				}

				bool isFinished = wait(taskBucket->isFinished(cx, task));
				if (isFinished)
					return Void();

				Void _ = wait(IBackupContainer::openContainer(backupContainer)->renameFile(tempFileName, logFileName));
			}
			catch (Error &e) {
				TraceEvent(SevError, "BA_BackupLogRangeTaskFunc_endLogFileError").error(e).detail("backupContainer", backupContainer).detail("Rename_file_from", tempFileName);
				throw;
			}

			return Void();
		}

	};

	StringRef BackupLogRangeTaskFunc::name = LiteralStringRef("file_backup_log_range");
	const uint32_t BackupLogRangeTaskFunc::version = 1;
	const Key BackupLogRangeTaskFunc::keyNextBeginVersion = LiteralStringRef("nextBeginVersion");
	const Key BackupLogRangeTaskFunc::keyAddBackupLogRangeTasks = LiteralStringRef("addBackupLogRangeTasks");
	const Key BackupLogRangeTaskFunc::keyFileSize = LiteralStringRef("fileSize");
	REGISTER_TASKFUNC(BackupLogRangeTaskFunc);

	struct BackupLogsTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			Void _ = wait(checkTaskVersion(tr, task, BackupLogsTaskFunc::name, BackupLogsTaskFunc::version));

			state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
			state Reference<TaskFuture> allPartsDone;

			state Optional<Value> stopValue = wait(tr->get(task->params[FileBackupAgent::keyStateStop]));
			state Version stopVersionData = stopValue.present() ? BinaryReader::fromStringRef<Version>(stopValue.get(), Unversioned()) : -1;

			state Version beginVersion = BinaryReader::fromStringRef<Version>(task->params[FileBackupAgent::keyBeginVersion], Unversioned());
			state Version endVersion = std::max<Version>( tr->getReadVersion().get() + 1, beginVersion + (CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES-1)*CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE );

			if(endVersion - beginVersion > g_random->randomInt64(0, CLIENT_KNOBS->BACKUP_VERSION_DELAY)) {
				TraceEvent("FBA_BackupLogs").detail("beginVersion", beginVersion).detail("endVersion", endVersion).detail("stopVersionData", stopVersionData);
			}

			// Only consider stopping, if the stop key is set and less than begin version
			if ((stopVersionData > 0) && (stopVersionData < beginVersion)) {
				allPartsDone = onDone;

				// Updated the stop version
				tr->set(task->params[FileBackupAgent::keyStateStop], BinaryWriter::toValue(endVersion, Unversioned()));
			}
			else {
				allPartsDone = futureBucket->future(tr);
				Key _ = wait(BackupLogsTaskFunc::addTask(tr, taskBucket, task, endVersion, TaskCompletionKey::signal(onDone), allPartsDone));
			}

			Key _ = wait(BackupLogRangeTaskFunc::addTask(tr, taskBucket, task, beginVersion, endVersion, TaskCompletionKey::joinWith(allPartsDone)));

			Void _ = wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, Version beginVersion, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(BackupLogsTaskFunc::name, BackupLogsTaskFunc::version, doneKey, 1));

			// Get backup config from parent task and bind to new task
			Void _ = wait(BackupConfig(parentTask).toTask(tr, task));

			copyDefaultParameters(parentTask, task);
			copyParameter(parentTask, task, FileBackupAgent::keyConfigStopWhenDoneKey);
			copyParameter(parentTask, task, FileBackupAgent::keyConfigBackupRanges);

			task->params[BackupAgentBase::keyBeginVersion] = BinaryWriter::toValue(beginVersion, Unversioned());

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef BackupLogsTaskFunc::name = LiteralStringRef("file_backup_logs");
	const uint32_t BackupLogsTaskFunc::version = 1;
	REGISTER_TASKFUNC(BackupLogsTaskFunc);

	struct FinishedFullBackupTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		StringRef getName() const { return name; };

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			Void _ = wait(checkTaskVersion(tr, task, FinishedFullBackupTaskFunc::name, FinishedFullBackupTaskFunc::version));

			state UID logUid = BinaryReader::fromStringRef<UID>(task->params[FileBackupAgent::keyConfigLogUid], Unversioned());

			state Key configPath = uidPrefixKey(logRangesRange.begin, logUid);
			state Key logsPath = uidPrefixKey(backupLogKeys.begin, logUid);

			state BackupConfig backup(task);
			backup.clear(tr);
			tr->clear(KeyRangeRef(configPath, strinc(configPath)));
			tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));
			backup.stateEnum().set(tr, EBackupState::STATE_COMPLETED);

			Void _ = wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(FinishedFullBackupTaskFunc::name, FinishedFullBackupTaskFunc::version, doneKey));

			// Get backup config from parent task and bind to new task
			Void _ = wait(BackupConfig(parentTask).toTask(tr, task));

			copyDefaultParameters(parentTask, task);
			copyParameter(parentTask, task, BackupAgentBase::keyConfigBackupRanges);

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef FinishedFullBackupTaskFunc::name = LiteralStringRef("file_finished_full_backup");
	const uint32_t FinishedFullBackupTaskFunc::version = 1;
	REGISTER_TASKFUNC(FinishedFullBackupTaskFunc);

	struct BackupDiffLogsTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			Void _ = wait(checkTaskVersion(tr, task, BackupDiffLogsTaskFunc::name, BackupDiffLogsTaskFunc::version));

			state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
			state Reference<TaskFuture> allPartsDone;

			state Optional<Value> stopWhenDone = wait(tr->get(task->params[FileBackupAgent::keyConfigStopWhenDoneKey]));

			state Version beginVersion = BinaryReader::fromStringRef<Version>(task->params[FileBackupAgent::keyBeginVersion], Unversioned());
			state Version endVersion = std::max<Version>( tr->getReadVersion().get() + 1, beginVersion + (CLIENT_KNOBS->BACKUP_MAX_LOG_RANGES-1)*CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE );

			tr->set(FileBackupAgent().lastRestorable.get(task->params[FileBackupAgent::keyConfigBackupTag]).pack(), BinaryWriter::toValue(beginVersion, Unversioned()));

			if(endVersion - beginVersion > g_random->randomInt64(0, CLIENT_KNOBS->BACKUP_VERSION_DELAY)) {
				TraceEvent("FBA_DiffLogs").detail("beginVersion", beginVersion).detail("endVersion", endVersion).detail("stopWhenDone", stopWhenDone.present());
			}

			if (stopWhenDone.present()) {
				allPartsDone = onDone;
				tr->set(task->params[FileBackupAgent::keyStateStop], BinaryWriter::toValue(endVersion, Unversioned()));
			}
			else {
				allPartsDone = futureBucket->future(tr);
				Key _ = wait(BackupDiffLogsTaskFunc::addTask(tr, taskBucket, task, endVersion, TaskCompletionKey::signal(onDone), allPartsDone));
			}

			Key _ = wait(BackupLogRangeTaskFunc::addTask(tr, taskBucket, task, beginVersion, endVersion, TaskCompletionKey::joinWith(allPartsDone)));

			Void _ = wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, Version beginVersion, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(BackupDiffLogsTaskFunc::name, BackupDiffLogsTaskFunc::version, doneKey, 1));

			// Get backup config from parent task and bind to new task
			Void _ = wait(BackupConfig(parentTask).toTask(tr, task));

			copyDefaultParameters(parentTask, task);
			copyParameter(parentTask, task, BackupAgentBase::keyConfigBackupRanges);
			copyParameter(parentTask, task, BackupAgentBase::keyConfigStopWhenDoneKey);

			task->params[FileBackupAgent::keyBeginVersion] = BinaryWriter::toValue(beginVersion, Unversioned());

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef BackupDiffLogsTaskFunc::name = LiteralStringRef("file_backup_diff_logs");
	const uint32_t BackupDiffLogsTaskFunc::version = 1;
	REGISTER_TASKFUNC(BackupDiffLogsTaskFunc);

	struct FileInfo {
		FileInfo(std::string fname = "") { init(fname); }

		enum EType { UNKNOWN=0, LOG=10, KVRANGE=20, KVMANIFEST=30 };
		std::string filename;
		EType type;
		Version beginVersion;
		Version endVersion;
		std::string uid;
		int64_t size;
		int blockSize;

		bool valid() const { return type != EType::UNKNOWN; }

		// Comparator for ordering files by beginVersion, type, endVersion, filename
		bool operator<(const FileInfo &rhs) const {
			int64_t cmp = beginVersion - rhs.beginVersion;
			if(cmp == 0) {
				cmp = type - rhs.type;
				if(cmp == 0) {
					cmp = endVersion - rhs.endVersion;
					if(cmp == 0)
						cmp = filename.compare(rhs.filename);
				}
			}
			return cmp < 0;
		}
		
		bool init(std::string fname) {
			filename = fname;
			uid.clear();
			type = FileInfo::UNKNOWN;
			beginVersion = invalidVersion;
			endVersion = invalidVersion;
			size = 0;
			blockSize = 0;
			
			// Filenames are comma separated lists, split by comma into parts
			std::vector<std::string> parts;
			boost::split(parts, fname, boost::is_any_of(","));
			if(parts.empty())
				return false;

			// Filename forms:
			//   kvrange,    version,      uid,        size, blocksize
			//   log,        beginVersion, endVersion, size, blocksize
			//   kvmanifest, beginVersion, endVersion, uid,  totalRangesSize
			// All file forms have 5 parts
			if(parts.size() != 5)
				return false;

			beginVersion = getVersionFromString(parts[1]);
			std::string &ftype = parts[0];

			// KV Range manifest
			if(ftype == "kvmanifest") {
				type = EType::KVMANIFEST;
				endVersion = getVersionFromString(parts[2]);
				uid = parts[3];
				size = atoi(parts[4].c_str());
			}
			// KV range data
			else if(ftype == "kvrange") {
				type = EType::KVRANGE;
				endVersion = beginVersion;  // kvrange files are single version
				uid = parts[2];
				size = atoll(parts[3].c_str());
				blockSize = atoi(parts[4].c_str());
			}
			// Transaction log file
			else if(ftype == "log") {
				type = EType::LOG;
				endVersion = getVersionFromString(parts[2]);
				size = atoll(parts[3].c_str());
				blockSize = atoi(parts[4].c_str());
			}

			return valid();
		}
	};

	struct BackupRestorableTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state BackupConfig backup(task);
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

			// Read the entire range file map into memory, then walk it backwards from its last entry to produce a list of non overlapping key range files
			// Map is endKey -> (beginKey, filename)
			state std::map<Key, std::pair<Key, Key>> localmap;
			state Key startKey;
			state int batchSize = BUGGIFY ? 1 : 1000000;
			state std::string backupContainer;

			loop {
				try {
					tr->reset();
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					Key backupContainerBytes = wait(backup.backupContainer().getOrThrow(tr));
					backupContainer = backupContainerBytes.toString();

					// Calling isFinished instead of keepRunning because if this backup was cancelled completely but we got
					// all the way to this part we may as well write out the kvmanifest.
					bool isFinished = wait(taskBucket->isFinished(tr, task));
					if(isFinished)
						return Void();

					BackupConfig::RangeFileMapT::PairsType rangeresults = wait(backup.rangeFileMap().getRange(tr, startKey, {}, batchSize));

					for(auto &p : rangeresults)
						localmap.insert(p);

					if(rangeresults.size() < batchSize)
						break;

					startKey = keyAfter(rangeresults.back().first);
				} catch(Error &e) {
					Void _ = wait(tr->onError(e));
				}
			}

			// TODO:  Make sure the range coverage matches the configured backup ranges, which can be disjoint.
			std::vector<Key> files;
			state Version maxVer = 0;
			state Version minVer = std::numeric_limits<Version>::max();
			state int64_t totalBytes = 0;

			if(!localmap.empty()) {
				// Get iterator that points to greatest key, start there.
				auto ri = localmap.rbegin();
				auto i = (++ri).base();
				FileInfo fi;

				while(1) {
					// Add file to list and update version range seen
					files.push_back(i->second.second);
					FileInfo fi(i->second.second.toString());
					ASSERT(fi.type == FileInfo::KVRANGE);
					if(fi.beginVersion < minVer)
						minVer = fi.beginVersion;
					if(fi.endVersion > maxVer)
						maxVer = fi.endVersion;
					totalBytes += fi.size;

					// Jump to file that either ends where this file begins or has the greatest end that is less than
					// the begin of this file.  In other words find the map key that is <= begin of this file.  To do this
					// find the first end strictly greater than begin and then back up one.
					i = localmap.upper_bound(i->second.first);
					// If we get begin then we're done, there are no more ranges that end at or before the last file's begin
					if(i == localmap.begin())
						break;
					--i;
				}
			}

			json_spirit::Value doc = json_spirit::Array();
			json_spirit::Array &array = doc.get_array();			
			for(auto &f : files)
				array.push_back(f.toString());
			state std::string docString = json_spirit::write_string(doc);

			state Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupContainer);
			state std::string tempFile = FileBackupAgent::getTempFilename();
			state Reference<IAsyncFile> mf = wait(bc->openFile(tempFile, IBackupContainer::WRITEONLY));
			Void _ = wait(mf->write(docString.data(), docString.size(), 0));
			Void _ = wait(mf->sync());

			std::string fileName = format("kvmanifest,%lld,%lld,%lld,%s", minVer, maxVer, totalBytes, g_random->randomUniqueID().toString().c_str());
			Void _ = wait(bc->renameFile(tempFile, fileName));

			return Void();
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			Void _ = wait(checkTaskVersion(tr, task, BackupRestorableTaskFunc::name, BackupRestorableTaskFunc::version));

			state BackupConfig config(task);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			Key backupContainerBytes = wait(config.backupContainer().getOrThrow(tr));
			state std::string backupContainer = backupContainerBytes.toString();

			state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

			state Optional<Value> stopValue = wait(tr->get(task->params[FileBackupAgent::keyStateStop]));
			state Version restoreVersion = stopValue.present() ? BinaryReader::fromStringRef<Version>(stopValue.get(), Unversioned()) : -1;

			state Optional<Value> stopWhenDone = wait(tr->get(task->params[FileBackupAgent::keyConfigStopWhenDoneKey]));
			state Reference<TaskFuture> allPartsDone;

			state UID logUid = BinaryReader::fromStringRef<UID>(task->params[FileBackupAgent::keyConfigLogUid], Unversioned());

			Void _ = wait(writeRestoreFile(tr, task->params[FileBackupAgent::keyErrors], backupContainer, logUid, restoreVersion,
				task->params[FileBackupAgent::keyConfigBackupTag], task->params[BackupAgentBase::keyConfigLogUid], task->params[BackupAgentBase::keyConfigBackupRanges]));

			TraceEvent("FBA_Complete").detail("restoreVersion", restoreVersion).detail("differential", stopWhenDone.present());

			// Start the complete task, if differential is not enabled
			if (stopWhenDone.present()) {
				// After the Backup completes, clear the backup subspace and update the status
				Key _ = wait(FinishedFullBackupTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal()));
			}
			else { // Start the writing of logs, if differential
				config.stateEnum().set(tr, EBackupState::STATE_DIFFERENTIAL);

				allPartsDone = futureBucket->future(tr);

				Key _ = wait(BackupDiffLogsTaskFunc::addTask(tr, taskBucket, task, restoreVersion, TaskCompletionKey::joinWith(allPartsDone)));

				// After the Backup completes, clear the backup subspace and update the status
				Key _ = wait(FinishedFullBackupTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal(), allPartsDone));
			}

			Void _ = wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(BackupRestorableTaskFunc::name, BackupRestorableTaskFunc::version, doneKey));

			// Get backup config from parent task and bind to new task
			Void _ = wait(BackupConfig(parentTask).toTask(tr, task));

			copyDefaultParameters(parentTask, task);
			copyParameter(parentTask, task, BackupAgentBase::keyConfigBackupRanges);
			copyParameter(parentTask, task, BackupAgentBase::keyConfigStopWhenDoneKey);

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef BackupRestorableTaskFunc::name = LiteralStringRef("file_backup_restorable");
	const uint32_t BackupRestorableTaskFunc::version = 1;
	REGISTER_TASKFUNC(BackupRestorableTaskFunc);

	struct StartFullBackupTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			Void _ = wait(checkTaskVersion(cx, task, StartFullBackupTaskFunc::name, StartFullBackupTaskFunc::version));

			loop{
				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					Version startVersion = wait(tr->getReadVersion());

					task->params[FileBackupAgent::keyBeginVersion] = BinaryWriter::toValue(startVersion, Unversioned());
					break;
				}
				catch (Error &e) {
					Void _ = wait(tr->onError(e));
				}
			}

			return Void();
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state BackupConfig config(task);
			state UID logUid = BinaryReader::fromStringRef<UID>(task->params[FileBackupAgent::keyConfigLogUid], Unversioned());
			state Key logUidDest = uidPrefixKey(backupLogKeys.begin, logUid);
			state Version beginVersion = BinaryReader::fromStringRef<Version>(task->params[BackupAgentBase::keyBeginVersion], Unversioned());

			ASSERT(config.getUid() == logUid);

			state Standalone<VectorRef<KeyRangeRef>> backupRanges;
			BinaryReader br(task->params[FileBackupAgent::keyConfigBackupRanges], IncludeVersion());
			br >> backupRanges;

			// Start logging the mutations for the specified ranges of the tag
			for (auto &backupRange : backupRanges) {
				tr->set(logRangesEncodeKey(backupRange.begin, logUid), logRangesEncodeValue(backupRange.end, logUidDest));
			}

			config.stateEnum().set(tr, EBackupState::STATE_BACKUP);

			state Reference<TaskFuture>	kvBackupRangeComplete = futureBucket->future(tr);
			state Reference<TaskFuture>	kvBackupComplete = futureBucket->future(tr);
			state int rangeCount = 0;

			for (; rangeCount < backupRanges.size(); ++rangeCount) {
				// Add the initial range task as high priority.
				Key _ = wait(BackupRangeTaskFunc::addTask(tr, taskBucket, task, backupRanges[rangeCount].begin, backupRanges[rangeCount].end, TaskCompletionKey::joinWith(kvBackupRangeComplete), Reference<TaskFuture>(), 1));
			}

			// After the BackupRangeTask completes, set the stop key which will stop the BackupLogsTask
			Key _ = wait(FinishFullBackupTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal(), kvBackupRangeComplete));

			// Backup the logs which will create BackupLogRange tasks
			Key _ = wait(BackupLogsTaskFunc::addTask(tr, taskBucket, task, beginVersion, TaskCompletionKey::joinWith(kvBackupComplete)));

			// After the Backup completes, clear the backup subspace and update the status
			Key _ = wait(BackupRestorableTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal(), kvBackupComplete));

			Void _ = wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Key keyStateStop,
			UID uid, Key keyConfigLogUid, Key keyConfigBackupTag, Key keyConfigBackupRanges, Key keyConfigStopWhenDoneKey, Key keyErrors, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>())
		{
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(StartFullBackupTaskFunc::name, StartFullBackupTaskFunc::version, doneKey));

			state BackupConfig config(uid);
			// Bind backup config to the new task
			Void _ = wait(config.toTask(tr, task));

			task->params[BackupAgentBase::keyStateStop] = keyStateStop;
			task->params[BackupAgentBase::keyConfigLogUid] = keyConfigLogUid;
			task->params[BackupAgentBase::keyConfigBackupTag] = keyConfigBackupTag;
			task->params[BackupAgentBase::keyConfigBackupRanges] = keyConfigBackupRanges;
			task->params[BackupAgentBase::keyConfigStopWhenDoneKey] = keyConfigStopWhenDoneKey;
			task->params[BackupAgentBase::keyErrors] = keyErrors;

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef StartFullBackupTaskFunc::name = LiteralStringRef("file_start_full_backup");
	const uint32_t StartFullBackupTaskFunc::version = 1;
	REGISTER_TASKFUNC(StartFullBackupTaskFunc);

	typedef std::set<FileInfo> BackupFileSetT;
	typedef std::set<Version> VersionSetT;

	// Finalize the file set for the target restore version.  This means:
	// Replace the kvmanifest files in the set with the contents of the ONE file actually required
	// Remove unnecessary log files.
	// Also verifies that the first log begin and last log end are sufficient for this restore version.
	// DOES NOT verify that the log file set form a continuous chain, this should already be done because
	// the file list itself should have come from scanBackupContents().
	ACTOR static Future<Void> finalizeBackupFileSet(Reference<IBackupContainer> container, BackupFileSetT *files, Version restoreVersion) {

		// First, find the 'greatest' kvmanifest file in the set with an endVersion that is <= restoreVersion.
		// The files are first sorted by beginVersion, so this will find the latest-starting kvmanifest file
		// that ends at or equal to restoreVersion
		state FileInfo kvmanifest;
		for(auto i = files->rbegin(); !(i == files->rend()); ) {
			bool keep;
			// If we already chose a kvmanifest then delete the current file unless it is a log file that ends
			// after the kvmanifest range begins.
			if(kvmanifest.valid()) {
				keep = i->type == FileInfo::LOG && i->endVersion > kvmanifest.beginVersion;
			}
			// If this is a kvmanifest file then don't keep it but possibly copy its value to kvmanifest
			else if(i->type == FileInfo::KVMANIFEST) {
				keep = false;
				// If this is the kvmanifest we're looking for then save it.
				if(i->endVersion <= restoreVersion)
					kvmanifest = *i;
			}
			else {
				// The only files to keep now are logs that begin at or before restoreVersion
				keep = i->type == FileInfo::LOG && i->beginVersion <= restoreVersion;
			}

			++i;
			if(!keep)
				i = BackupFileSetT::reverse_iterator(files->erase(i.base()));
		}

		if(!kvmanifest.valid())
			throw restore_missing_data();

		// The file set should now ONLY contain logs, and at least 1 of them.
		if(files->empty())
			throw restore_missing_data();
		
		if(files->rbegin()->type != FileInfo::LOG || files->begin()->type != FileInfo::LOG)
			throw restore_unknown_file_type();

		// Verify that the first log begin is <= kvmanifest begin		
		if(files->begin()->beginVersion > kvmanifest.beginVersion)
			throw restore_missing_data();

		// Verify that the last log end is > restoreVersion
		if(files->rbegin()->endVersion <= restoreVersion)
			throw restore_missing_data();

		// Next, read the kvmanifest and add its files to the 
		state json_spirit::mValue mfDoc;
		try {
			state Reference<IAsyncFile> mf = wait(container->openFile(kvmanifest.filename, IBackupContainer::READONLY));
			state int64_t mfSize = wait(mf->size());
			state Standalone<StringRef> buf = makeString(mfSize);
			int bRead = wait(mf->read(mutateString(buf), mfSize, 0));
			if(bRead != mfSize)
				throw restore_corrupted_data();
			
			json_spirit::read_string(buf.toString(), mfDoc);
			if(mfDoc.type() != json_spirit::array_type)
				throw restore_corrupted_data();

			for(auto &v : mfDoc.get_array()) {
				if(v.type() != json_spirit::str_type)
					throw restore_corrupted_data();
				FileInfo fi(v.get_str());
				if(fi.type != FileInfo::KVRANGE)
					throw restore_corrupted_data();

				files->insert(fi);
			}

		} catch(Error &e) {
			TraceEvent(SevError, "FileRestoreKVManifestError").detail("FileName", kvmanifest.filename).error(e);
			throw restore_missing_data();
		}
		
		return Void();
	}

	// Given a backup container, scan its file list to see if it appears to be complete.
	// Optionally return:
	//   * minimum restorable version
	//   * maximum restorable version
	//   * name of the restorable file
	//   * a set of ONLY kvmanifest and log files which can later be finalized to a minimal log and kvrange file set
	//     required to restore to a specific version.
	ACTOR static Future<Void> scanBackupContents(Reference<IBackupContainer> container, Version* pMinRestoreVersion, Version* pMaxRestoreVersion,
		BackupFileSetT *pRestoreFiles, std::string *pRestorableFile, bool verbose) {

		vector<std::string> existingFiles = wait(container->listFiles());

		// Use a local restore files set if one was not provided.
		state BackupFileSetT restoreFiles;
		if(pRestoreFiles == nullptr)
			pRestoreFiles = &restoreFiles;
		pRestoreFiles->clear();

		state std::string restorableFile;  // name of restoreable file if present

		// Map of each log file end version to its lowest seen begin version, used to figure out which log files to use
		state std::map<Version, Version> end_begin;  // Map of log file end versions to begin versions
		FileInfo earliestKVManifest;                 // KVManifest with the earliest begin version

		for (auto& filename : existingFiles) {
			if (filename == "restorable")
				restorableFile = filename;
			else {
				FileInfo fi(filename);
				if(fi.valid()) {
					// In this pass we will only collect log files and kvmanifest files.
					// The kvrange files to use will be determined by the kvmanifest file selected later.
					if(fi.type == FileInfo::LOG) {
						Version beginVer = fi.beginVersion;
						if(end_begin.count(fi.endVersion))
							beginVer = std::min(beginVer, end_begin[fi.endVersion]);
						end_begin[fi.endVersion] = beginVer;
						pRestoreFiles->insert(fi);
					}
					else if(fi.type == FileInfo::KVMANIFEST) {
						// Set earliest KV manifest if this is it or there wasn't one before
						if(!earliestKVManifest.valid() || fi < earliestKVManifest)
							earliestKVManifest = fi;
						pRestoreFiles->insert(fi);
					}
				}
				else {
					if(!StringRef(filename).startsWith(LiteralStringRef("temp.")))
						TraceEvent(SevWarn, "BA_restore").detail("UnexpectedFileName", filename);
				}
			
			}
		}
		
		if(!earliestKVManifest.valid()) {
			TraceEvent(SevError, "FileRestoreKVManifestNotFound");
			if(verbose)
				fprintf(stderr, "ERROR: The backup does not contain a kvmanifest file.\n");
			throw restore_missing_data();
		}

		if (restorableFile.empty()){
			TraceEvent(SevError, "BA_restore").detail("RestorableNotFound", "This backup does not contain all of the necessary files");
			if(verbose)
				fprintf(stderr, "ERROR: The backup is missing the 'restoreable' file.\n");
			throw restore_missing_data();
		}

		// Trim the log begin/end version pairs to the longest contiguous chain that reaches all the way back to the earliest log version
		// There are a few steps to this process.  First, clean up the end_begin map.
		Version lastBegin = std::numeric_limits<Version>::max();
		Version lastEnd = std::numeric_limits<Version>::max();
		std::set<Version> toRemove;
		// Iterate over the (end, begin) pairs in reverse order by end version
		for (auto ver = end_begin.rbegin(); !(ver == end_begin.rend()); ++ver) {
			// If the end version is greater than the previous begin version then discard this entry
			if (ver->first > lastBegin) {
				toRemove.insert(ver->first);
			}
			else {
				// If the end version is less than the previous begin version then there is a non-contiguous gap so discard
				// everything from the previous end version forward and clear removals since everything iterated over
				// so far is now gone
				if(ver->first < lastBegin) {
					end_begin.erase(end_begin.lower_bound(lastEnd), end_begin.end());
					toRemove.clear();
					ver = end_begin.rbegin();
				}

				lastEnd = ver->first;
				lastBegin = ver->second;
			}
		}

		// Now remove the unneeded end_begin pairs from the map
		for(auto r : toRemove)
			end_begin.erase(r);
		toRemove.clear();

		// Now remove log files from the restore file set which do not have version range pairs in the version map
		for(auto i = pRestoreFiles->begin(); i != pRestoreFiles->end(); ) {
			auto j = i;
			++i;
			if(j->type == FileInfo::LOG) {
				Version beginVer = j->beginVersion;
				Version endVer = j->endVersion;
				// If endVer is not found or beginVer is not equal to endVer then remove the file from set
				auto ev = end_begin.find(endVer);
				if(ev == end_begin.end() || beginVer != ev->second)
					pRestoreFiles->erase(j);
			}
		}

		// Minimum restore version is the end version of the earliest kvmanifest
		Version minRestoreVersion = earliestKVManifest.endVersion;
		// First data version is the begin version of the earliest kvmanifest
		Version firstDataVersion = earliestKVManifest.beginVersion;

		Version maxRestoreVersion = -1;
		Version firstLogVersion = std::numeric_limits<Version>::max();

		// Get max restorable version from the end of the log chain
		if(!(end_begin.rbegin() == end_begin.rend())) {
			maxRestoreVersion = end_begin.rbegin()->first - 1;
			firstLogVersion = end_begin.begin()->second;
		}

		if (firstLogVersion > firstDataVersion) {
			TraceEvent(SevError, "BA_restore").detail("LogVersion_greateer_than_data_version", true)
				.detail("firstLogVersion", firstLogVersion).detail("firstDataVersion", firstDataVersion)
				.detail("minRestoreVersion", minRestoreVersion).detail("maxRestoreVersion", maxRestoreVersion)
				.detail("restorableFile", restorableFile);
			fprintf(stderr, "ERROR: The first log version %lld is greater than the first data version %lld\n",
				(long long) firstLogVersion, (long long) firstDataVersion);
			throw restore_invalid_version();
		}

		if (minRestoreVersion > maxRestoreVersion) {
			TraceEvent(SevError, "BA_restore").detail("minRestoreVersion", minRestoreVersion).detail("maxRestoreVersion", maxRestoreVersion)
				.detail("firstLogVersion", firstLogVersion).detail("firstDataVersion", firstDataVersion);
			if(verbose)
				fprintf(stderr, "ERROR: The last data version %lld is greater than last log version %lld\n",
					(long long) minRestoreVersion, (long long) maxRestoreVersion);
			throw restore_invalid_version();
		}

		if (pMinRestoreVersion) {
			*pMinRestoreVersion = minRestoreVersion;
		}

		if (pMaxRestoreVersion) {
			*pMaxRestoreVersion = maxRestoreVersion;
		}

		if (pRestorableFile) {
			*pRestorableFile = restorableFile;
		}

		return Void();
	}

	struct RestoreCompleteTaskFunc : TaskFuncBase {
		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			Void _ = wait(checkTaskVersion(tr, task, name, version));

			state RestoreConfig restore(task);
			restore.stateEnum().set(tr, ERestoreState::COMPLETED);
			// Clear the file map now since it could be huge.
			restore.fileMap().clear(tr);

			// TODO:  Validate that the range version map has exactly the restored ranges in it.  This means that for any restore operation
			// the ranges to restore must be within the backed up ranges, otherwise from the restore perspective it will appear that some
			// key ranges were missing and so the backup set is incomplete and the restore has failed.
			// This validation cannot be done currently because Restore only supports a single restore range but backups can have many ranges.

			// Clear the applyMutations stuff, including any unapplied mutations from versions beyond the restored version.
			restore.clearApplyMutationsKeys(tr);

			Void _ = wait(taskBucket->finish(tr, task));
			Void _ = wait(unlockDatabase(tr, restore.getUid()));

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(RestoreCompleteTaskFunc::name, RestoreCompleteTaskFunc::version, doneKey));

			// Get restore config from parent task and bind it to new task
			Void _ = wait(RestoreConfig(parentTask).toTask(tr, task));

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		static StringRef name;
		static const uint32_t version;
		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };

	};
	StringRef RestoreCompleteTaskFunc::name = LiteralStringRef("restore_complete");
	const uint32_t RestoreCompleteTaskFunc::version = 1;
	REGISTER_TASKFUNC(RestoreCompleteTaskFunc);

	// This is for opening files to read during RESTORE tasks
	ACTOR Future<Reference<IAsyncFile>> openRestoreFile(Database cx, RestoreConfig restore, std::string url, std::string fileName) {
		state std::string errstr;
		try {
			state Reference<IAsyncFile> inFile = wait(IBackupContainer::openContainer(url, &errstr)->openFile(fileName, IBackupContainer::EMode::READONLY));
			return inFile;
		} catch(Error &e) {
			state Error err = e;
			Void _ = wait(restore.logError(cx, e, format("opening '%s' from '%s': %s", fileName.c_str(), url.c_str(), errstr.c_str())));
			throw err;
		}
	}

	struct FilePartParams {
		static TaskParam<Value> inputFile() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<int64_t> fileOffset() { return LiteralStringRef(__FUNCTION__); }
		static TaskParam<int64_t> fileLen() { return LiteralStringRef(__FUNCTION__); }
	};

	struct RestoreRangeTaskFunc : TaskFuncBase {
		static struct : public FilePartParams {
			static TaskParam<KeyRange> originalFileRange() { return LiteralStringRef(__FUNCTION__); }
		} Params;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state double startTime = now();
			state RestoreConfig restore(task);

			try {
				state Value inputFile = Params.inputFile().get(task);
				state int64_t fileOffset = Params.fileOffset().get(task);
				state int64_t fileLen = Params.fileLen().get(task);

				TraceEvent("FileRestoreRangeStart")
					.detail("UID", restore.getUid())
					.detail("FileName", inputFile.toString())
					.detail("FileOffset", fileOffset)
					.detail("FileLen", fileLen)
					.detail("TaskInstance", (uint64_t)this);

				state FileInfo fi(inputFile.toString());
				if(!fi.valid())
					throw restore_unknown_file_type();

				state Reference<ReadYourWritesTransaction> tr( new ReadYourWritesTransaction(cx) );
				loop {
					try {
						tr->reset();
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);

						state Future<Value> url = restore.sourceURL().getD(tr);
						state Future<KeyRange> restoreRange = restore.restoreRange().getD(tr);
						state Future<Key> addPrefix = restore.addPrefix().getD(tr);
						state Future<Key> removePrefix = restore.removePrefix().getD(tr);

						Void _ = wait(success(url) && success(restoreRange) && success(addPrefix) && success(removePrefix) && checkTaskVersion(tr, task, name, version));
						bool go = wait(taskBucket->keepRunning(tr, task));
						if(!go)
							return Void();
						break;

					} catch(Error &e) {
						Void _ = wait(tr->onError(e));
					}
				}

				state Reference<IAsyncFile> inFile = wait(openRestoreFile(cx, restore, url.get().toString(), inputFile.toString()));
				state Standalone<VectorRef<KeyValueRef>> blockData = wait(decodeRangeFileBlock(inFile, fileOffset, fileLen));

				// First and last key are the range for this file
				state KeyRange fileRange = KeyRangeRef(blockData.front().key, blockData.back().key);

				// If fileRange doesn't intersect restore range then we're done.
				if(!fileRange.intersects(restoreRange.get()))
					return Void();

				// We know the file range intersects the restore range but there could still be keys outside the restore range.
				// Find the subvector of kv pairs that intersect the restore range.  Note that the first and last keys are just the range endpoints for this file
				int rangeStart = 1;
				int rangeEnd = blockData.size() - 1;
				// Slide start forward, stop if something in range is found
				while(rangeStart < rangeEnd && !restoreRange.get().contains(blockData[rangeStart].key))
					++rangeStart;
				// Side end backward, stop if something in range is found
				while(rangeEnd > rangeStart && !restoreRange.get().contains(blockData[rangeEnd - 1].key))
					--rangeEnd;

				state VectorRef<KeyValueRef> data = blockData.slice(rangeStart, rangeEnd);

				// Shrink file range to be entirely within restoreRange and translate it to the new prefix
				// First, use the untranslated file range to create the shrunk original file range which must be used in the kv range version map for applying mutations
				state KeyRange originalFileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.get().begin),std::min(fileRange.end,   restoreRange.get().end));
				Params.originalFileRange().set(task, originalFileRange);

				// Now shrink and translate fileRange
				Key fileEnd = std::min(fileRange.end,   restoreRange.get().end);
				if(fileEnd == (removePrefix.get() == StringRef() ? normalKeys.end : strinc(removePrefix.get())) ) {
					fileEnd = addPrefix.get() == StringRef() ? normalKeys.end : strinc(addPrefix.get());
				} else {
					fileEnd = fileEnd.removePrefix(removePrefix.get()).withPrefix(addPrefix.get());
				}
				fileRange = KeyRangeRef(std::max(fileRange.begin, restoreRange.get().begin).removePrefix(removePrefix.get()).withPrefix(addPrefix.get()),fileEnd);

				state int start = 0;
				state int end = data.size();
				state int dataSizeLimit = BUGGIFY ? g_random->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

				loop {
					try {
						tr->reset();
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);

						state int i = start;
						state int txBytes = 0;
						state int iend = start;

						// find iend that results in the desired transaction size
						for(; iend < end && txBytes < dataSizeLimit; ++iend) {
							txBytes += data[iend].key.expectedSize();
							txBytes += data[iend].value.expectedSize();
						}

						// Clear the range we are about to set.
						// If start == 0 then use fileBegin for the start of the range, else data[start]
						// If iend == end then use fileEnd for the end of the range, else data[iend]
						state KeyRange trRange = KeyRangeRef((start == 0 ) ? fileRange.begin : data[start].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get())
														   , (iend == end) ? fileRange.end   : data[iend ].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()));

						tr->clear(trRange);

						for(; i < iend; ++i) {
							tr->setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
							tr->set(data[i].key.removePrefix(removePrefix.get()).withPrefix(addPrefix.get()), data[i].value);
						}

						// Add to bytes written count
						restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

						state Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

						// Save and extend this task periodically in case database is slow, which checks keepRunning, or check it directly.
						Future<bool> goFuture;
						if(now() - startTime > 30) {
							// TODO: Optionally, task params could be modified here to save progress so far.
							startTime = now();
							goFuture = taskBucket->saveAndExtend(tr, task);
						}
						else
							goFuture = taskBucket->keepRunning(tr, task);

						bool go = wait(goFuture);
						if(!go)
							return Void();

						Void _ = wait( checkLock );

						Void _ = wait(tr->commit());

						TraceEvent("FileRestoreCommittedRange")
							.detail("UID", restore.getUid())
							.detail("FileName", inputFile.toString())
							.detail("FileSize", fi.size)
							.detail("FileOffset", fileOffset)
							.detail("FileLen", fileLen)
							.detail("FileVersion", fi.beginVersion)
							.detail("CommitVersion", tr->getCommittedVersion())
							.detail("BeginRange", printable(trRange.begin))
							.detail("EndRange", printable(trRange.end))
							.detail("StartIndex", start)
							.detail("EndIndex", i)
							.detail("DataSize", data.size())
							.detail("Bytes", txBytes)
							.detail("OriginalFileRange", printable(originalFileRange))
							.detail("TaskInstance", (uint64_t)this);

						// Commit succeeded, so advance starting point
						start = i;

						if(start == end)
							return Void();
					} catch(Error &e) {
						TraceEvent(SevWarn, "FileRestoreErrorRangeWrite")
							.detail("UID", restore.getUid())
							.detail("FileName", inputFile.toString())
							.detail("FileSize", fi.size)
							.detail("FileOffset", fileOffset)
							.detail("FileLen", fileLen)
							.detail("FileVersion", fi.beginVersion)
							.detail("BeginRange", printable(trRange.begin))
							.detail("EndRange", printable(trRange.end))
							.detail("StartIndex", start)
							.detail("EndIndex", i)
							.detail("DataSize", data.size())
							.detail("Bytes", txBytes)
							.error(e)
							.detail("TaskInstance", (uint64_t)this);

						if(e.code() == error_code_transaction_too_large)
							dataSizeLimit /= 2;
						else
							Void _ = wait(tr->onError(e));
					}
				}
			} catch(Error &e) {
				state Error err2 = e;
				Void _ = wait(restore.logError(cx, e, format("RestoreRange: Error on %s", Params.inputFile().get(task).toString().c_str()), this));
				throw err2;
			}
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state RestoreConfig restore(task);
			restore.fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

			// Update the KV range map if originalFileRange is set
			Future<Void> updateMap = Void();
			if(Params.originalFileRange().exists(task)) {
				Value versionEncoded = BinaryWriter::toValue(FileInfo(Params.inputFile().get(task).toString()).beginVersion, Unversioned());
				updateMap = krmSetRange(tr, restore.applyMutationsMapPrefix(), Params.originalFileRange().get(task), versionEncoded);
			}

			state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
			Void _ = wait(taskFuture->set(tr, taskBucket) &&
					        taskBucket->finish(tr, task) && updateMap);

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, Value inputFile, int64_t offset, int64_t len, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(RestoreRangeTaskFunc::name, RestoreRangeTaskFunc::version, doneKey));

			// Create a restore config from the current task and bind it to the new task.
			Void _ = wait(RestoreConfig(parentTask).toTask(tr, task));
			Params.inputFile().set(task, inputFile);
			Params.fileOffset().set(task, offset);
			Params.fileLen().set(task, len);

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		static StringRef name;
		static const uint32_t version;
		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef RestoreRangeTaskFunc::name = LiteralStringRef("restore_range_data");
	const uint32_t RestoreRangeTaskFunc::version = 1;
	REGISTER_TASKFUNC(RestoreRangeTaskFunc);

	struct RestoreLogDataTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;
		StringRef getName() const { return name; };

		static struct : public FilePartParams {
		} Params;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state double startTime = now();
			state RestoreConfig restore(task);

			try {
				state Value inputFile = Params.inputFile().get(task);
				state int64_t fileOffset = Params.fileOffset().get(task);
				state int64_t fileLen = Params.fileLen().get(task);

				TraceEvent("FileRestoreLogStart")
					.detail("UID", restore.getUid())
					.detail("FileName", inputFile.toString())
					.detail("FileOffset", fileOffset)
					.detail("FileLen", fileLen)
					.detail("TaskInstance", (uint64_t)this);

				state FileInfo fi(inputFile.toString());
				if(!fi.valid())
					throw restore_unknown_file_type();

				state Reference<ReadYourWritesTransaction> tr( new ReadYourWritesTransaction(cx) );
				loop {
					try {
						tr->reset();
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);

						state Value url = wait(restore.sourceURL().getD(tr));

						Void _ = wait(checkTaskVersion(tr, task, name, version));
						bool go = wait(taskBucket->keepRunning(tr, task));
						if(!go)
							return Void();

						break;
					} catch(Error &e) {
						Void _ = wait(tr->onError(e));
					}
				}

				state Key mutationLogPrefix = restore.mutationLogPrefix();
				state Reference<IAsyncFile> inFile = wait(openRestoreFile(cx, restore, url.toString(), inputFile.toString()));
				state Standalone<VectorRef<KeyValueRef>> data = wait(decodeLogFileBlock(inFile, fileOffset, fileLen));

				state int start = 0;
				state int end = data.size();
				state int dataSizeLimit = BUGGIFY ? g_random->randomInt(256 * 1024, 10e6) : CLIENT_KNOBS->RESTORE_WRITE_TX_SIZE;

				loop {
					try {
						if(start == end)
							return Void();

						tr->reset();
						tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						tr->setOption(FDBTransactionOptions::LOCK_AWARE);

						state int i = start;
						state int txBytes = 0;
						for(; i < end && txBytes < dataSizeLimit; ++i) {
							Key k = data[i].key.withPrefix(mutationLogPrefix);
							ValueRef v = data[i].value;
							tr->set(k, v);
							txBytes += k.expectedSize();
							txBytes += v.expectedSize();
						}

						state Future<Void> checkLock = checkDatabaseLock(tr, restore.getUid());

						// Save and extend this task periodically in case database is slow, which checks keepRunning, or check it directly.
						Future<bool> goFuture;
						if(now() - startTime > 30) {
							// TODO: Optionally, task params could be modified here to save progress so far.
							startTime = now();
							goFuture = taskBucket->saveAndExtend(tr, task);
						}
						else
							goFuture = taskBucket->keepRunning(tr, task);

						bool go = wait(goFuture);
						if(!go)
							return Void();

						Void _ = wait( checkLock );

						// Add to bytes written count
						restore.bytesWritten().atomicOp(tr, txBytes, MutationRef::Type::AddValue);

						Void _ = wait(tr->commit());

						TraceEvent("FileRestoreCommittedLog")
							.detail("UID", restore.getUid())
							.detail("FileName", inputFile.toString())
							.detail("FileSize", fi.size)
							.detail("FileOffset", fileOffset)
							.detail("FileLen", fileLen)
							.detail("FileBeginVersion", fi.beginVersion)
							.detail("FileEndVersion", fi.endVersion)
							.detail("CommitVersion", tr->getCommittedVersion())
							.detail("StartIndex", start)
							.detail("EndIndex", i)
							.detail("DataSize", data.size())
							.detail("Bytes", txBytes)
							.detail("TaskInstance", (uint64_t)this);

						// Commit succeeded, so advance starting point
						start = i;
					} catch(Error &e) {
						TraceEvent(SevWarn, "FileRestoreErrorLogWrite")
							.detail("UID", restore.getUid())
							.detail("FileName", inputFile.toString())
							.detail("FileSize", fi.size)
							.detail("FileOffset", fileOffset)
							.detail("FileLen", fileLen)
							.detail("FileBeginVersion", fi.beginVersion)
							.detail("FileEndVersion", fi.endVersion)
							.detail("StartIndex", start)
							.detail("EndIndex", i)
							.detail("DataSize", data.size())
							.detail("Bytes", txBytes)
							.error(e)
							.detail("TaskInstance", (uint64_t)this);

						if(e.code() == error_code_transaction_too_large)
							dataSizeLimit /= 2;
						else
							Void _ = wait(tr->onError(e));
					}
				}
			} catch(Error &e) {
				state Error err2 = e;
				Void _ = wait(restore.logError(cx, e, format("RestoreLogData: Error on %s", Params.inputFile().get(task).toString().c_str()), this));
				throw err2;
			}
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			RestoreConfig(task).fileBlocksFinished().atomicOp(tr, 1, MutationRef::Type::AddValue);

			state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

			// TODO:  Check to see if there is a leak in the FutureBucket since an invalid task (validation key fails) will never set its taskFuture.
			Void _ = wait(taskFuture->set(tr, taskBucket) &&
					        taskBucket->finish(tr, task));

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, Value inputFile, int64_t offset, int64_t len, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(RestoreLogDataTaskFunc::name, RestoreLogDataTaskFunc::version, doneKey));

			// Create a restore config from the current task and bind it to the new task.
			Void _ = wait(RestoreConfig(parentTask).toTask(tr, task));
			Params.inputFile().set(task, inputFile);
			Params.fileOffset().set(task, offset);
			Params.fileLen().set(task, len);

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef RestoreLogDataTaskFunc::name = LiteralStringRef("restore_log_data");
	const uint32_t RestoreLogDataTaskFunc::version = 1;
	REGISTER_TASKFUNC(RestoreLogDataTaskFunc);

	struct RestoreDispatchTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;
		StringRef getName() const { return name; };

		static struct {
			static TaskParam<Version> beginVersion() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<Key> beginFile() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<int64_t> beginBlock() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<int64_t> batchSize() { return LiteralStringRef(__FUNCTION__); }
			static TaskParam<int64_t> remainingInBatch() { return LiteralStringRef(__FUNCTION__); }
		} Params;

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state RestoreConfig restore(task);

			try {
				state Version beginVersion = Params.beginVersion().get(task);
				state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

				Void _ = wait(checkTaskVersion(tr, task, name, version));
				state int64_t remainingInBatch = Params.remainingInBatch().get(task);
				state bool addingToExistingBatch = remainingInBatch > 0;

				// If not adding to an existing batch then update the apply mutations end version so the mutations from the
				// previous batch can be applied.
				if(!addingToExistingBatch)
					restore.setApplyEndVersion(tr, beginVersion);

				// Get the apply version lag
				state int64_t applyLag = wait(restore.getApplyVersionLag(tr));
				state int64_t batchSize = Params.batchSize().get(task);

				// If starting a new batch and the apply lag is too large then re-queue and wait
				if(!addingToExistingBatch && applyLag > (BUGGIFY ? 1 : CLIENT_KNOBS->CORE_VERSIONSPERSECOND * 300)) {
					// Wait a small amount of time and then re-add this same task.
					Void _ = wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
					Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, beginVersion, StringRef(), 0, batchSize, remainingInBatch));

					TraceEvent("FileRestoreDispatch")
						.detail("UID", restore.getUid())
						.detail("BeginVersion", beginVersion)
						.detail("ApplyLag", applyLag)
						.detail("BatchSize", batchSize)
						.detail("Decision", "too_far_behind")
						.detail("TaskInstance", (uint64_t)this);

					Void _ = wait(taskBucket->finish(tr, task));
					return Void();
				}

				state Key beginFile = Params.beginFile().getOrDefault(task);
				// Get a batch of files.  We're targeting batchSize blocks being dispatched so query for batchSize files (each of which is 0 or more blocks).
				state RestoreConfig::FileMapT::PairsType files = wait(restore.fileMap().getRange(tr, {beginVersion, beginFile}, {}, CLIENT_KNOBS->RESTORE_DISPATCH_ADDTASK_SIZE));

				// allPartsDone will be set once all block tasks in the current batch are finished.
				state Reference<TaskFuture> allPartsDone;

				// If adding to existing batch then join the new block tasks to the existing batch future
				if(addingToExistingBatch) {
					Key fKey = wait(restore.batchFuture().getD(tr));
					allPartsDone = Reference<TaskFuture>(new TaskFuture(futureBucket, fKey));
				}
				else {
					// Otherwise create a new future for the new batch
					allPartsDone = futureBucket->future(tr);
					restore.batchFuture().set(tr, allPartsDone->pack());
					// Set batch quota remaining to batch size
					remainingInBatch = batchSize;
				}

				// If there were no files to load then this batch is done and restore is almost done.
				if(files.size() == 0) {
					state Version restoreVersion = wait(restore.restoreVersion().getD(tr));

					// If adding to existing batch then blocks could be in progress so create a new Dispatch task that waits for them to finish
					if(addingToExistingBatch) {
						Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, restoreVersion, StringRef(), 0, batchSize, 0, TaskCompletionKey::noSignal(), allPartsDone));

						TraceEvent("FileRestoreDispatch")
							.detail("UID", restore.getUid())
							.detail("BeginVersion", beginVersion)
							.detail("BeginFile", printable(Params.beginFile().get(task)))
							.detail("BeginBlock", Params.beginBlock().get(task))
							.detail("RestoreVersion", restoreVersion)
							.detail("ApplyLag", applyLag)
							.detail("Decision", "end_of_final_batch")
							.detail("TaskInstance", (uint64_t)this);
					}
					else if(beginVersion < restoreVersion) {
						// If beginVersion is less than restoreVersion then do one more dispatch task to get there
						Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, restoreVersion, StringRef(), 0, batchSize));

						TraceEvent("FileRestoreDispatch")
							.detail("UID", restore.getUid())
							.detail("BeginVersion", beginVersion)
							.detail("BeginFile", printable(Params.beginFile().get(task)))
							.detail("BeginBlock", Params.beginBlock().get(task))
							.detail("RestoreVersion", restoreVersion)
							.detail("ApplyLag", applyLag)
							.detail("Decision", "apply_to_restore_version")
							.detail("TaskInstance", (uint64_t)this);
					}
					else if(applyLag == 0) {
						// If apply lag is 0 then we are done so create the completion task
						Key _ = wait(RestoreCompleteTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal()));

						TraceEvent("FileRestoreDispatch")
							.detail("UID", restore.getUid())
							.detail("BeginVersion", beginVersion)
							.detail("BeginFile", printable(Params.beginFile().get(task)))
							.detail("BeginBlock", Params.beginBlock().get(task))
							.detail("ApplyLag", applyLag)
							.detail("Decision", "restore_complete")
							.detail("TaskInstance", (uint64_t)this);
					} else {
						// Applying of mutations is not yet finished so wait a small amount of time and then re-add this same task.
						Void _ = wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
						Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, beginVersion, StringRef(), 0, batchSize));

						TraceEvent("FileRestoreDispatch")
							.detail("UID", restore.getUid())
							.detail("BeginVersion", beginVersion)
							.detail("ApplyLag", applyLag)
							.detail("Decision", "apply_still_behind")
							.detail("TaskInstance", (uint64_t)this);
					}

					// If adding to existing batch then task is joined with a batch future so set done future
					// Note that this must be done after joining at least one task with the batch future in case all other blockers already finished.
					Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

					Void _ = wait(taskBucket->finish(tr, task) && setDone);
					return Void();
				}

				// Start moving through the file list and queuing up blocks.  Only queue up to RESTORE_DISPATCH_ADDTASK_SIZE blocks per Dispatch task
				// and target batchSize total per batch but a batch must end on a complete version boundary so exceed the limit if necessary 
				// to reach the end of a version of files.
				state std::vector<Future<Key>> addTaskFutures;
				state Version endVersion = files[0].first.first;
				state int blocksDispatched = 0;
				state int64_t beginBlock = Params.beginBlock().getOrDefault(task);
				state int i = 0;

				for(; i < files.size(); ++i) {
					state FileInfo fi(files[i].first.second.toString());
					if(!fi.valid())
						throw restore_unknown_file_type();

					// Here we are "between versions" (prior to adding the first block of the first file of a new version) so this is an opportunity
					// to end the current dispatch batch (which must end on a version boundary) if the batch size has been reached or exceeded
					if(fi.beginVersion != endVersion && remainingInBatch <= 0) {
						// Next start will be at the first version after endVersion at the first file first block
						++endVersion;
						beginFile = StringRef();
						beginBlock = 0;
						break;
					}

					// Set the starting point for the next task in case we stop inside this file
					endVersion = fi.beginVersion;
					beginFile = fi.filename;

					state int64_t j = beginBlock * fi.blockSize;
					// For each block of the file
					for(; j < fi.size; j += fi.blockSize) {
						// Stop if we've reached the addtask limit
						if(blocksDispatched == CLIENT_KNOBS->RESTORE_DISPATCH_ADDTASK_SIZE)
							break;

						if(fi.type == FileInfo::LOG) {
							addTaskFutures.push_back(RestoreLogDataTaskFunc::addTask(tr, taskBucket, task,
								KeyRef(fi.filename), j, std::min<int64_t>(fi.blockSize, fi.size - j),
								TaskCompletionKey::joinWith(allPartsDone)));
						}
						else if(fi.type == FileInfo::KVRANGE) {
							addTaskFutures.push_back(RestoreRangeTaskFunc::addTask(tr, taskBucket, task,
								KeyRef(fi.filename), j, std::min<int64_t>(fi.blockSize, fi.size - j),
								TaskCompletionKey::joinWith(allPartsDone)));
						}
						else {
							Void _ = wait(restore.logError(tr->getDatabase(), Error(), format("RestoreDispatch: Unknown file type: %s", fi.filename.c_str()), this));
							throw restore_unknown_file_type();
						}

						// Increment beginBlock for the file and total blocks dispatched for this task
						++beginBlock;
						++blocksDispatched;
						--remainingInBatch;
					}
					
					// Stop if we've reached the addtask limit
					if(blocksDispatched == CLIENT_KNOBS->RESTORE_DISPATCH_ADDTASK_SIZE)
						break;

					// We just completed an entire file so the next task should start at the file after this one within endVersion (or later) 
					// if this iteration ends up being the last for this task
					beginFile = StringRef(beginFile.toString() + "\x01");
					beginBlock = 0;

					//TraceEvent("FileRestoreDispatchedFile").detail("UID", restore.getUid()).detail("FileName", fi.filename).detail("TaskInstance", (uint64_t)this);
				}

				// If no blocks were dispatched then the next dispatch task should run now and be joined with the allPartsDone future
				if(blocksDispatched == 0) {
					std::string decision;

					// If no files were dispatched either then the batch size wasn't large enough to catch all of the files at the next lowest non-dispatched
					// version, so increase the batch size.
					if(i == 0) {
						batchSize *= 2;
						decision = "increased_batch_size";
					}
					else
						decision = "all_files_were_empty";

					TraceEvent("FileRestoreDispatch")
						.detail("UID", restore.getUid())
						.detail("BeginVersion", beginVersion)
						.detail("BeginFile", printable(Params.beginFile().get(task)))
						.detail("BeginBlock", Params.beginBlock().get(task))
						.detail("EndVersion", endVersion)
						.detail("ApplyLag", applyLag)
						.detail("BatchSize", batchSize)
						.detail("Decision", decision)
						.detail("TaskInstance", (uint64_t)this)
						.detail("RemainingInBatch", remainingInBatch);

					Void _ = wait(success(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, endVersion, beginFile, beginBlock, batchSize, remainingInBatch, TaskCompletionKey::joinWith((allPartsDone)))));
					
					// If adding to existing batch then task is joined with a batch future so set done future.
					// Note that this must be done after joining at least one task with the batch future in case all other blockers already finished.
					Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

					Void _ = wait(setDone && taskBucket->finish(tr, task));

					return Void();
				}

				// If adding to existing batch then task is joined with a batch future so set done future.
				Future<Void> setDone = addingToExistingBatch ? onDone->set(tr, taskBucket) : Void();

				// Increment the number of blocks dispatched in the restore config
				restore.filesBlocksDispatched().atomicOp(tr, blocksDispatched, MutationRef::Type::AddValue);

				// If beginFile is not empty then we had to stop in the middle of a version (possibly within a file) so we cannot end 
				// the batch here because we do not know if we got all of the files and blocks from the last version queued, so
				// make sure remainingInBatch is at least 1.
				if(beginFile.size() != 0)
					remainingInBatch = std::max<int64_t>(1, remainingInBatch);

				// If more blocks need to be dispatched in this batch then add a follow-on task that is part of the allPartsDone group which will won't wait
				// to run and will add more block tasks.
				if(remainingInBatch > 0)
					addTaskFutures.push_back(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, endVersion, beginFile, beginBlock, batchSize, remainingInBatch, TaskCompletionKey::joinWith(allPartsDone)));
				else // Otherwise, add a follow-on task to continue after all previously dispatched blocks are done
					addTaskFutures.push_back(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, endVersion, beginFile, beginBlock, batchSize, 0, TaskCompletionKey::noSignal(), allPartsDone));

				Void _ = wait(setDone && waitForAll(addTaskFutures) && taskBucket->finish(tr, task));

				TraceEvent("FileRestoreDispatch")
					.detail("BeginVersion", beginVersion)
					.detail("BeginFile", printable(Params.beginFile().get(task)))
					.detail("BeginBlock", Params.beginBlock().get(task))
					.detail("EndVersion", endVersion)
					.detail("ApplyLag", applyLag)
					.detail("BatchSize", batchSize)
					.detail("Decision", "dispatched_files")
					.detail("FilesDispatched", i)
					.detail("BlocksDispatched", blocksDispatched)
					.detail("TaskInstance", (uint64_t)this)
					.detail("RemainingInBatch", remainingInBatch);

			} catch(Error &e) {
				state Error err = e;
				Void _ = wait(restore.logError(tr->getDatabase(), e, "RestoreDispatch: Unexpected error.", this));
				throw err;
			}

			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<Task> parentTask, Version beginVersion, Key beginFile, int64_t beginBlock, int64_t batchSize, int64_t remainingInBatch = 0, TaskCompletionKey completionKey = TaskCompletionKey::noSignal(), Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
			Key doneKey = wait(completionKey.get(tr, taskBucket));

			// Use high priority for dispatch tasks that have to queue more blocks for the current batch
			unsigned int priority = (remainingInBatch > 0) ? 1 : 0;
 			state Reference<Task> task(new Task(RestoreDispatchTaskFunc::name, RestoreDispatchTaskFunc::version, doneKey, priority));

 			// Create a config from the parent task and bind it to the new task
			Void _ = wait(RestoreConfig(parentTask).toTask(tr, task));
			Params.beginVersion().set(task, beginVersion);
			Params.batchSize().set(task, batchSize);
			Params.remainingInBatch().set(task, remainingInBatch);
			Params.beginBlock().set(task, beginBlock);
			Params.beginFile().set(task, beginFile);

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return Void(); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef RestoreDispatchTaskFunc::name = LiteralStringRef("restore_dispatch");
	const uint32_t RestoreDispatchTaskFunc::version = 1;
	REGISTER_TASKFUNC(RestoreDispatchTaskFunc);

	ACTOR Future<std::string> restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName) {
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state std::vector<KeyBackedTag> tags;
		if(tagName.size() == 0) {
			std::vector<KeyBackedTag> t = wait(getAllRestoreTags(tr));
			tags = t;
		}
		else
			tags.push_back(makeRestoreTag(tagName));

		state std::string result;
		state int i = 0;

		for(; i < tags.size(); ++i) {
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

		state KeyBackedTag tag = makeRestoreTag(tagName);
		state Optional<UidAndAbortedFlagT> current = wait(tag.get(tr));
		if(!current.present())
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
		Void _ = wait(tag.cancel(tr));
		Void _ = wait(unlockDatabase(tr, current.get().first));

		return ERestoreState::ABORTED;
	}

 	struct StartFullRestoreTaskFunc : TaskFuncBase {
		static StringRef name;
		static const uint32_t version;

		static struct {
			static TaskParam<Version> firstVersion() { return LiteralStringRef(__FUNCTION__); }
		} Params;

		ACTOR static Future<Void> _execute(Database cx, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			state RestoreConfig restore(task);

			loop {
				try {
					tr->reset();
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					Void _ = wait(checkTaskVersion(tr, task, name, version));
					state Version restoreVersion = wait(restore.restoreVersion().getD(tr));
					bool go = wait(taskBucket->keepRunning(tr, task));
					if(!go)
						return Void();

					ERestoreState oldState = wait(restore.stateEnum().getD(tr));
					if(oldState != ERestoreState::QUEUED && oldState != ERestoreState::STARTING) {
						Void _ = wait(restore.logError(cx, restore_error(), format("StartFullRestore: Encountered unexpected state(%d)", oldState), this));
						return Void();
					}
					restore.stateEnum().set(tr, ERestoreState::STARTING);
					restore.fileMap().clear(tr);
					restore.fileBlockCount().clear(tr);
					restore.fileCount().clear(tr);
					state Value url = wait(restore.sourceURL().getD(tr));

					Void _ = wait(tr->commit());
					break;
				} catch(Error &e) {
					Void _ = wait(tr->onError(e));
				}
			}

			state std::string errstr;
			state Reference<IBackupContainer> bc;

			try {
				Reference<IBackupContainer> c = IBackupContainer::openContainer(url.toString(), &errstr);
				bc = c;
			} catch(Error &e) {
				state Error err = e;
				Void _ = wait(restore.logError(tr->getDatabase(), e, format("StartFullRestore: Couldn't open '%s', got error '%s'", url.toString().c_str(), errstr.c_str()), this));
				throw err;
			}

			try {
				state BackupFileSetT files;
				Void _ = wait(scanBackupContents(bc, nullptr, nullptr, &files, nullptr, false));
				// Finalize the file set for the target restore version
				Void _ = wait(finalizeBackupFileSet(bc, &files, restoreVersion));
			} catch(Error &e) {
				state Error err2 = e;
				Void _ = wait(restore.logError(tr->getDatabase(), e, "StartFullRestore: Error reading backup contents", this));
				throw err2;
			}

			state fileBackup::BackupFileSetT::iterator start = files.begin();
			state fileBackup::BackupFileSetT::iterator end = files.end();

			while(start != end) {
				try {
					tr->reset();
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);

					bool go = wait(taskBucket->keepRunning(tr, task));
					if(!go)
						return Void();

					state fileBackup::BackupFileSetT::iterator i = start;

					state int txBytes = 0;
					state int nFileBlocks = 0;
					state int nFiles = 0;
					auto restorefileMap = restore.fileMap();
					for(; i != end && txBytes < 1e6; ++i) {
						if(i == files.begin())
							Params.firstVersion().set(task, i->beginVersion);
						if(i->beginVersion > restoreVersion) {
							end = i;
							break;
						}
						txBytes += restorefileMap.set(tr, {i->beginVersion, StringRef(i->filename)}, true);
						nFileBlocks += (i->size + i->blockSize - 1) / i->blockSize;
						++nFiles;
					}

					// Increment counts
					restore.fileCount().atomicOp(tr, nFiles, MutationRef::Type::AddValue);
					restore.fileBlockCount().atomicOp(tr, nFileBlocks, MutationRef::Type::AddValue);

					Void _ = wait(tr->commit());

					TraceEvent("FileRestoreLoadedFiles")
						.detail("UID", restore.getUid())
						.detail("FileCount", nFiles)
						.detail("FileBlockCount", nFileBlocks)
						.detail("Bytes", txBytes)
						.detail("TaskInstance", (uint64_t)this);

					start = i;
				} catch(Error &e) {
					Void _ = wait(tr->onError(e));
				}
			}

			return Void();
		}

		ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, Reference<FutureBucket> futureBucket, Reference<Task> task) {
			state Version firstVersion = Params.firstVersion().get(task);
			state RestoreConfig restore(task);
			if(firstVersion == Version()) {
				Void _ = wait(restore.logError(tr->getDatabase(), restore_missing_data(), "StartFullRestore: The backup had no data.", this));
				Key tag = wait(restore.tag().getD(tr));
				ERestoreState _ = wait(abortRestore(tr, tag));
				return Void();
			}

			restore.stateEnum().set(tr, ERestoreState::RUNNING);

			// Set applyMutation versions
			restore.setApplyBeginVersion(tr, firstVersion);
			restore.setApplyEndVersion(tr, firstVersion);

			// Apply range data and log data in order
			Key _ = wait(RestoreDispatchTaskFunc::addTask(tr, taskBucket, task, firstVersion, StringRef(), 0, CLIENT_KNOBS->RESTORE_DISPATCH_BATCH_SIZE));

			Void _ = wait(taskBucket->finish(tr, task));
			return Void();
		}

		ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> taskBucket, UID uid, TaskCompletionKey completionKey, Reference<TaskFuture> waitFor = Reference<TaskFuture>())
		{
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			Key doneKey = wait(completionKey.get(tr, taskBucket));
			state Reference<Task> task(new Task(StartFullRestoreTaskFunc::name, StartFullRestoreTaskFunc::version, doneKey));

			state RestoreConfig restore(uid);
			// Bind the restore config to the new task
			Void _ = wait(restore.toTask(tr, task));

			if (!waitFor) {
				return taskBucket->addTask(tr, task);
			}

			Void _ = wait(waitFor->onSetAddTask(tr, taskBucket, task));
			return LiteralStringRef("OnSetAddTask");
		}

		StringRef getName() const { return name; };

		Future<Void> execute(Database cx, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _execute(cx, tb, fb, task); };
		Future<Void> finish(Reference<ReadYourWritesTransaction> tr, Reference<TaskBucket> tb, Reference<FutureBucket> fb, Reference<Task> task) { return _finish(tr, tb, fb, task); };
	};
	StringRef StartFullRestoreTaskFunc::name = LiteralStringRef("restore_start");
	const uint32_t StartFullRestoreTaskFunc::version = 1;
	REGISTER_TASKFUNC(StartFullRestoreTaskFunc);
}

struct LogInfo : public ReferenceCounted<LogInfo> {
	std::string fileName;
	Reference<IAsyncFile> logFile;
	Version beginVersion;
	Version endVersion;
	int64_t offset;

	LogInfo() : offset(0) {};
};

class FileBackupAgentImpl {
public:
	static const int MAX_RESTORABLE_FILE_METASECTION_BYTES = 1024 * 8;

	// This method will return the final status of the backup
	ACTOR static Future<int> waitBackup(FileBackupAgent* backupAgent, Database cx, Key tagName, bool stopWhenDone) {
		state std::string backTrace;
		state UID logUid = wait(backupAgent->getLogUid(cx, tagName));
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
				ASSERT(oldUidAndAborted.get().first == logUid);

				state BackupConfig config(oldUidAndAborted.get().first);
				state EBackupState status = wait(config.stateEnum().getD(tr, EBackupState::STATE_NEVERRAN));
				Optional<EBackupState> configState = wait(config.stateEnum().get(tr));
				ASSERT(configState.present() && configState.get() == status);

				// Break, if no longer runnable
				if (!FileBackupAgent::isRunnable((BackupAgentBase::enumState)status)) {
					return status;
				}

				// Break, if in differential mode (restorable) and stopWhenDone is not enabled
				if ((!stopWhenDone) && (BackupAgentBase::STATE_DIFFERENTIAL == status)) {
					return status;
				}

				state Future<Void> watchFuture = tr->watch( config.stateEnum().key );
				Void _ = wait( tr->commit() );
				Void _ = wait( watchFuture );
			}
			catch (Error &e) {
				Void _ = wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> submitBackup(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key outContainer, Key tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone) {
		state UID logUid = g_random->randomUniqueID();
		state Key logUidValue = BinaryWriter::toValue(logUid, Unversioned());
		state UID logUidCurrent = wait(backupAgent->getLogUid(tr, tagName));

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		// We will use the global status for now to ensure that multiple backups do not start place with different tags
		state int status = wait(backupAgent->getStateValue(tr, logUidCurrent));

		if (FileBackupAgent::isRunnable((BackupAgentBase::enumState)status)) {
			throw backup_duplicate();
		}

		// This check will ensure that current backupUid is later than the last backup Uid
		state Standalone<StringRef> backupUid = BackupAgentBase::getCurrentTime();
		state std::string backupContainer = outContainer.toString();

		// To be consistent with directory handling behavior since FDB backup was first released, if the container string
		// describes a local directory then "/backup-UID" will be added to it.
		if(backupContainer.find("file://") == 0) {
			if(backupContainer[backupContainer.size() - 1] != '/')
				backupContainer += "/";
			backupContainer += std::string("backup-") + backupUid.toString();
		}

		Reference<IBackupContainer> bc = IBackupContainer::openContainer(backupContainer);
		try {
			Void _ = wait(timeoutError(bc->create(), 30));
		} catch(Error &e) {
			fprintf(stderr, "ERROR:  Could not create backup container: %s\n", e.what());
			throw backup_error();
		}

		Optional<Value> uidOld = wait(tr->get(backupAgent->config.pack(FileBackupAgent::keyLastUid)));

		if ((uidOld.present()) && (uidOld.get() >= backupUid)) {
			fprintf(stderr, "ERROR: The last backup `%s' happened in the future.\n", printable(uidOld.get()).c_str());
			throw backup_error();
		}

		KeyRangeMap<int> backupRangeSet;
		for (auto& backupRange : backupRanges) {
			backupRangeSet.insert(backupRange, 1);
		}

		backupRangeSet.coalesce(allKeys);
		backupRanges = Standalone<VectorRef<KeyRangeRef>>();

		for (auto& backupRange : backupRangeSet.ranges()) {
			if (backupRange.value()) {
				backupRanges.push_back_deep(backupRanges.arena(), backupRange.range());
			}
		}

		state BackupConfig config = BackupConfig(logUid);

		// Clear the backup ranges for the tag
		tr->clear(backupAgent->config.get(logUid.toString()).range());
		tr->clear(backupAgent->states.get(logUid.toString()).range());
		tr->clear(backupAgent->errors.range());
		config.clear(tr);

		// Point the tag to this new uid
		makeBackupTag(tagName).set(tr, {logUid, false});
		config.tag().set(tr, tagName);

		tr->set(backupAgent->tagNames.pack(tagName), logUidValue);
		tr->set(backupAgent->config.pack(FileBackupAgent::keyLastUid), backupUid);

		// Set the backup keys
		tr->set(backupAgent->config.get(logUid.toString()).pack(FileBackupAgent::keyConfigBackupTag), tagName);
		tr->set(backupAgent->config.get(logUid.toString()).pack(FileBackupAgent::keyConfigLogUid), logUidValue);
		tr->set(backupAgent->config.get(logUid.toString()).pack(FileBackupAgent::keyConfigBackupRanges), BinaryWriter::toValue(backupRanges, IncludeVersion()));
		config.stateEnum().set(tr, EBackupState::STATE_SUBMITTED);
		config.backupContainer().set(tr, StringRef(backupContainer));
		if (stopWhenDone) {
			tr->set(backupAgent->config.get(logUid.toString()).pack(FileBackupAgent::keyConfigStopWhenDoneKey), StringRef());
		}

		Key taskKey = wait(fileBackup::StartFullBackupTaskFunc::addTask(tr, backupAgent->taskBucket,
			backupAgent->states.get(logUid.toString()).pack(FileBackupAgent::keyStateStop),
			logUid, logUidValue, tagName, BinaryWriter::toValue(backupRanges, IncludeVersion()),
			backupAgent->config.get(logUid.toString()).pack(FileBackupAgent::keyConfigStopWhenDoneKey), backupAgent->errors.pack(logUid.toString()), TaskCompletionKey::noSignal()));

		return Void();
	}

	ACTOR static Future<Void> submitRestore(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key tagName, Key backupURL, Version restoreVersion, Key addPrefix, Key removePrefix, KeyRange restoreRange, bool lockDB, UID uid) {
		ASSERT(restoreRange.contains(removePrefix) || removePrefix.size() == 0);

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		// Get old restore config for this tag
		state KeyBackedTag tag = makeRestoreTag(tagName);
		state Optional<UidAndAbortedFlagT> oldUidAndAborted = wait(tag.get(tr));
		if(oldUidAndAborted.present()) {
			if (oldUidAndAborted.get().first == uid) {
				if (oldUidAndAborted.get().second) {
					throw restore_duplicate_uid();
				}
				else {
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
		
		KeyRange restoreIntoRange = KeyRangeRef(restoreRange.begin, restoreRange.end).removePrefix(removePrefix).withPrefix(addPrefix);
		Standalone<RangeResultRef> existingRows = wait(tr->getRange(restoreIntoRange, 1));
		if (existingRows.size() > 0) {
			throw restore_destination_not_empty();
		}

		// Make new restore config
		state RestoreConfig restore(uid);

		// Point the tag to the new uid
		tag.set(tr, {uid, false});

		// Configure the new restore
		restore.tag().set(tr, tagName);
		restore.sourceURL().set(tr, backupURL);
		restore.stateEnum().set(tr, ERestoreState::QUEUED);
		restore.restoreVersion().set(tr, restoreVersion);
		restore.restoreRange().set(tr, restoreRange);
		// this also sets restore.add/removePrefix.
		restore.initApplyMutations(tr, addPrefix, removePrefix);

		Key taskKey = wait(fileBackup::StartFullRestoreTaskFunc::addTask(tr, backupAgent->taskBucket, uid, TaskCompletionKey::noSignal()));

		if (lockDB)
			Void _ = wait(lockDatabase(tr, uid));
		else
			Void _ = wait(checkDatabaseLock(tr, uid));

		return Void();
	}

	// This method will return the final status of the backup
	ACTOR static Future<ERestoreState> waitRestore(Database cx, Key tagName, bool verbose) {
		loop {
			try {
				state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state KeyBackedTag tag = makeRestoreTag(tagName);
				Optional<UidAndAbortedFlagT> current = wait(tag.get(tr));
				if(!current.present()) {
					if(verbose)
						printf("Tag: %s  State: %s\n", tagName.toString().c_str(), FileBackupAgent::restoreStateText(ERestoreState::UNITIALIZED).toString().c_str());
					return ERestoreState::UNITIALIZED;
				}

				state RestoreConfig restore(current.get().first);

				if(verbose) {
					state std::string details = wait(restore.getProgress(tr));
					printf("%s\n", details.c_str());
				}

				state ERestoreState status = wait(restore.stateEnum().getD(tr));
				state bool runnable = wait(restore.isRunnable(tr));

				// State won't change from here
				if (!runnable)
					break;

				// Wait for a change
				state Future<Void> watchFuture = tr->watch(restore.stateEnum().key);
				Void _ = wait(tr->commit());
				if(verbose)
					Void _ = wait(watchFuture || delay(1));
				else
					Void _ = wait(watchFuture);
			}
			catch (Error &e) {
				Void _ = wait(tr->onError(e));
			}
		}

		return status;
	}

	ACTOR static Future<Void> discontinueBackup(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state UID logUid = wait(backupAgent->getLogUid(tr, tagName));
		state int status = wait(backupAgent->getStateValue(tr, logUid));

		if (!FileBackupAgent::isRunnable((BackupAgentBase::enumState)status)) {
			throw backup_unneeded();
		}

		state Optional<Value> stopWhenDoneValue = wait(tr->get(backupAgent->config.get(logUid.toString()).pack(FileBackupAgent::keyConfigStopWhenDoneKey)));

		if (stopWhenDoneValue.present()) {
			throw backup_duplicate();
		}

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->set(backupAgent->config.get(logUid.toString()).pack(BackupAgentBase::keyConfigStopWhenDoneKey), StringRef());

		return Void();
	}

	ACTOR static Future<Void> abortBackup(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		state KeyBackedTag tag = makeBackupTag(tagName);
		state Optional<UidAndAbortedFlagT> current = wait(tag.get(tr));
		if (!current.present()) {
			throw backup_error();
		}

		state UID logUid = wait(backupAgent->getLogUid(tr, tagName));
		ASSERT(current.get().first == logUid);

		int status = wait(backupAgent->getStateValue(tr, logUid));

		if (!backupAgent->isRunnable((BackupAgentBase::enumState)status)) {
			throw backup_unneeded();
		}

		// Cancel backup task through tag
		Void _ = wait(tag.cancel(tr));

		Key configPath = uidPrefixKey(logRangesRange.begin, logUid);
		Key logsPath = uidPrefixKey(backupLogKeys.begin, logUid);

		tr->clear(KeyRangeRef(configPath, strinc(configPath)));
		tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));
		state BackupConfig config(logUid);
		// FIXME: We don't want to clear config space on abort, as it is useful to have more information about
		// aborted backup later. Instead, we can clean it up while starting next backup under same tag.
		//config.clear(tr);

		config.stateEnum().set(tr, EBackupState::STATE_ABORTED);

		return Void();
	}

	ACTOR static Future<std::string> getStatus(FileBackupAgent* backupAgent, Database cx, int errorLimit, Key tagName) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state std::string statusText;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state KeyBackedTag tag;
				state UID logUid;
				state BackupConfig config;
				state EBackupState backupState;

				statusText = "";
				tag = makeBackupTag(tagName);
				state Optional<UidAndAbortedFlagT> uidAndAbortedFlag = wait(tag.get(tr));
				if (uidAndAbortedFlag.present()) {
					logUid = uidAndAbortedFlag.get().first;
					config = BackupConfig(logUid);
					state EBackupState status = wait(config.stateEnum().getD(tr, EBackupState::STATE_NEVERRAN));
					backupState = status;
				}

				if (!uidAndAbortedFlag.present() || backupState == EBackupState::STATE_NEVERRAN) {
					statusText += "No previous backups found.\n";
				} else {
					state std::string backupStatus(BackupAgentBase::getStateText(backupState));
					state std::string outContainer = wait(backupAgent->getLastBackupContainer(tr, logUid));
					state std::string tagNameDisplay = tagName.toString();
					state Optional<Value> stopVersionKey = wait(tr->get(backupAgent->states.get(logUid.toString()).pack(BackupAgentBase::keyStateStop)));

					state Standalone<VectorRef<KeyRangeRef>> backupRanges;
					Optional<Key> backupKeysPacked = wait(tr->get(backupAgent->config.get(logUid.toString()).pack(BackupAgentBase::keyConfigBackupRanges)));

					if (backupKeysPacked.present()) {
						BinaryReader br(backupKeysPacked.get(), IncludeVersion());
						br >> backupRanges;
					}

					switch (backupState) {
						case BackupAgentBase::STATE_SUBMITTED:
							statusText += "The backup on tag `" + tagNameDisplay + "' is in progress (just started) to " + outContainer + ".\n";
							break;
						case BackupAgentBase::STATE_BACKUP:
							statusText += "The backup on tag `" + tagNameDisplay + "' is in progress to " + outContainer + ".\n";
							break;
						case BackupAgentBase::STATE_DIFFERENTIAL:
							statusText += "The backup on tag `" + tagNameDisplay + "' is restorable but continuing to " + outContainer + ".\n";
							break;
						case BackupAgentBase::STATE_COMPLETED:
							{
								Version stopVersion = stopVersionKey.present() ? BinaryReader::fromStringRef<Version>(stopVersionKey.get(), Unversioned()) : -1;
								statusText += "The previous backup on tag `" + tagNameDisplay + "' at " + outContainer + " completed at version " + format("%lld", stopVersion) + ".\n";
							}
							break;
						default:
							statusText += "The previous backup on tag `" + tagNameDisplay + "' at " + outContainer + " " + backupStatus + ".\n";
							break;
					}
				}

				// Append the errors, if requested
				if (errorLimit > 0) {
					Standalone<RangeResultRef> values = wait(tr->getRange(backupAgent->errors.get(BinaryWriter::toValue(logUid, Unversioned())).range(), errorLimit, false, true));

					// Display the errors, if any
					if (values.size() > 0) {
						// Inform the user that the list of errors is complete or partial
						statusText += (values.size() < errorLimit)
							? "WARNING: Some backup agents have reported issues:\n"
							: "WARNING: Some backup agents have reported issues (printing " + std::to_string(errorLimit) + "):\n";

						for (auto &s : values) {
							statusText += "   " + printable(s.value) + "\n";
						}
					}
				}
				break;
			}
			catch (Error &e) {
				Void _ = wait(tr->onError(e));
			}
		}

		return statusText;
	}

	ACTOR static Future<int> getStateValue(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, UID logUid) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		EBackupState state = wait(BackupConfig(logUid).stateEnum().getD(tr, EBackupState::STATE_NEVERRAN));
		return state;
	}

	ACTOR static Future<Version> getStateStopVersion(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, UID logUid) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Optional<Value> stopVersion = wait(tr->get(backupAgent->states.get(logUid.toString()).pack(BackupAgentBase::keyStateStop)));
		return stopVersion.present() ? BinaryReader::fromStringRef<Version>(stopVersion.get(), Unversioned()) : -1;
	}

	ACTOR static Future<UID> getLogUid(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Optional<Value> logUid = wait(tr->get(backupAgent->tagNames.pack(tagName)));

		return (logUid.present()) ? BinaryReader::fromStringRef<UID>(logUid.get(), Unversioned()) : UID();
	}

	ACTOR static Future<Version> getLastRestorable(FileBackupAgent* backupAgent, Reference<ReadYourWritesTransaction> tr, Key tagName) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Optional<Value> version = wait(tr->get(backupAgent->lastRestorable.pack(tagName)));

		return (version.present()) ? BinaryReader::fromStringRef<Version>(version.get(), Unversioned()) : 0;
	}

	static StringRef read(StringRef& data, int bytes) {
		if (bytes > data.size()) throw restore_error();
		StringRef r = data.substr(0, bytes);
		data = data.substr(bytes);
		return r;
	}

	ACTOR static Future<std::string> getBackupInfoFromRestorableFile(Reference<IBackupContainer> container, Version minRestoreVersion, Version maxRestoreVersion, std::string restorableFile) {

		state Reference<IAsyncFile> rf = wait(container->openFile(restorableFile, IBackupContainer::READONLY));

		state std::string folderInfo;
		folderInfo.resize(MAX_RESTORABLE_FILE_METASECTION_BYTES);
		int b = wait(rf->read((char *)folderInfo.data(), MAX_RESTORABLE_FILE_METASECTION_BYTES, 0));
		folderInfo.resize(b);
		size_t metaEnd = folderInfo.find("\n\n");

		if (metaEnd != std::string::npos) {
			folderInfo.erase(metaEnd);
		}

		folderInfo += format("\n%-15s %ld\n%-15s %ld\n", "MinRestoreVer:", minRestoreVersion, "MaxRestoreVer:", maxRestoreVersion);
		return folderInfo;
	}

	ACTOR static Future<std::string> getBackupInfo(Reference<IBackupContainer> container, Version* defaultVersion) {
		state std::string restorableFile;
		state Version minRestoreVersion(LLONG_MAX);
		state Version maxRestoreVersion = -1;

		Void _ = wait(fileBackup::scanBackupContents(container, &minRestoreVersion, &maxRestoreVersion, NULL, &restorableFile, true));

		if (defaultVersion) {
			*defaultVersion = maxRestoreVersion;
		}

		std::string info = wait(getBackupInfoFromRestorableFile(container, minRestoreVersion, maxRestoreVersion, restorableFile));
		return info;
	}


	ACTOR static Future<Version> restore(FileBackupAgent* backupAgent, Database cx, Key tagName, Key url, bool waitForComplete, Version targetVersion, bool verbose, KeyRange range, Key addPrefix, Key removePrefix, bool lockDB, UID randomUid) {
		// Make sure the backup appears to be valid before actually submitting the restore
		state Version minRestoreVersion(LLONG_MAX);
		state Version maxRestoreVersion = -1;
		state std::string	restorableFile;

		state Reference<IBackupContainer> c = IBackupContainer::openContainer(url.toString());
		Void _ = wait(fileBackup::scanBackupContents(c, &minRestoreVersion, &maxRestoreVersion, nullptr, &restorableFile, true));

		if (targetVersion <= 0)
			targetVersion = maxRestoreVersion;

		if (targetVersion < minRestoreVersion)  {
			TraceEvent(SevError, "FileBackupAgentRestore").detail("targetVersion", targetVersion).detail("less_than_minRestoreVersion", minRestoreVersion);
			fprintf(stderr, "ERROR: Restore version %lld is smaller than minimum version %lld\n", (long long) targetVersion, (long long) minRestoreVersion);
			throw restore_invalid_version();
		}

		if (targetVersion > maxRestoreVersion)  {
			TraceEvent(SevError, "FileBackupAgentRestore").detail("targetVersion", targetVersion).detail("greater_than_maxRestoreVersion", maxRestoreVersion);
			fprintf(stderr, "ERROR: Restore version %lld is larger than maximum version %lld\n", (long long) targetVersion, (long long) maxRestoreVersion);
			throw restore_invalid_version();
		}

		if (verbose) {
			printf("Restoring backup to version: %lld\n", (long long) targetVersion);
			std::string info = wait(getBackupInfoFromRestorableFile(c, minRestoreVersion, maxRestoreVersion, restorableFile));
			printf("%s\n", info.c_str());
		}

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		loop {
			try {
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				Void _ = wait(submitRestore(backupAgent, tr, tagName, url, targetVersion, addPrefix, removePrefix, range, lockDB, randomUid));
				Void _ = wait(tr->commit());
				break;
			} catch(Error &e) {
				if(e.code() != error_code_restore_duplicate_tag) {
					Void _ = wait(tr->onError(e));
				}
			}
		}

		if(waitForComplete) {
			ERestoreState finalState = wait(waitRestore(cx, tagName, verbose));
			if(finalState != ERestoreState::COMPLETED)
				throw restore_error();
		}

		return targetVersion;
	}

	//used for correctness only, locks the database before discontinuing the backup and that same lock is then used while doing the restore.
	//the tagname of the backup must be the same as the restore.
	ACTOR static Future<Version> atomicRestore(FileBackupAgent* backupAgent, Database cx, Key tagName, KeyRange range, Key addPrefix, Key removePrefix) {
		state Reference<ReadYourWritesTransaction> ryw_tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
		state UID logUid;
		loop {
			try {
				ryw_tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ryw_tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				UID _logUid = wait(backupAgent->getLogUid(ryw_tr, tagName));
				logUid = _logUid;
				state int status = wait(backupAgent->getStateValue(ryw_tr, logUid));

				if ( (BackupAgentBase::enumState)status != BackupAgentBase::STATE_DIFFERENTIAL ) {
					throw backup_duplicate();
				}

				break;
			} catch( Error &e ) {
				Void _ = wait( ryw_tr->onError(e) );
			}
		}
		
		//Lock src, record commit version
		state Transaction tr(cx);
		state Version commitVersion;
		state UID randomUid = g_random->randomUniqueID();
		loop {
			try {
				Void _ = wait( lockDatabase(&tr, randomUid) );
				Void _ = wait(tr.commit());
				commitVersion = tr.getCommittedVersion();
				break;
			} catch( Error &e ) {
				Void _ = wait(tr.onError(e));
			}
		}

		ryw_tr->reset();
		loop {
			try {
				Void _ = wait( discontinueBackup(backupAgent, ryw_tr, tagName) );
				Void _ = wait( ryw_tr->commit() );
				break;
			} catch( Error &e ) {
				if(e.code() == error_code_backup_unneeded || e.code() == error_code_backup_duplicate){
					break;
				}
				Void _ = wait( ryw_tr->onError(e) );
			}
		}

		int _ = wait( waitBackup(backupAgent, cx, tagName, true) );

		ryw_tr->reset();
		loop {
			try {
				ryw_tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ryw_tr->setOption(FDBTransactionOptions::LOCK_AWARE);	
				ryw_tr->addReadConflictRange(range);
				ryw_tr->clear(range);
				Void _ = wait( ryw_tr->commit() );
				break;
			} catch( Error &e ) {
				Void _ = wait( ryw_tr->onError(e) );
			}
		}

		std::string lastBackupContainer = wait(backupAgent->getLastBackupContainer(cx, logUid));
		
		Version ver = wait( restore(backupAgent, cx, tagName, KeyRef(lastBackupContainer), true, -1, true, range, addPrefix, removePrefix, true, randomUid) );
		return ver;
	}
};

const KeyRef BackupAgentBase::defaultTag = LiteralStringRef("default");
const int BackupAgentBase::logHeaderSize = 12;
const int FileBackupAgent::dataFooterSize = 20;

Future<Version> FileBackupAgent::restore(Database cx, Key tagName, Key url, bool waitForComplete, Version targetVersion, bool verbose, KeyRange range, Key addPrefix, Key removePrefix, bool lockDB) {
	return FileBackupAgentImpl::restore(this, cx, tagName, url, waitForComplete, targetVersion, verbose, range, addPrefix, removePrefix, lockDB, g_random->randomUniqueID());
}

Future<Version> FileBackupAgent::atomicRestore(Database cx, Key tagName, KeyRange range, Key addPrefix, Key removePrefix) {
	return FileBackupAgentImpl::atomicRestore(this, cx, tagName, range, addPrefix, removePrefix);
}

Future<ERestoreState> FileBackupAgent::abortRestore(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return fileBackup::abortRestore(tr, tagName);
}

Future<std::string> FileBackupAgent::restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return fileBackup::restoreStatus(tr, tagName);
}

Future<ERestoreState> FileBackupAgent::waitRestore(Database cx, Key tagName, bool verbose) {
	return FileBackupAgentImpl::waitRestore(cx, tagName, verbose);
};

Future<Void> FileBackupAgent::submitBackup(Reference<ReadYourWritesTransaction> tr, Key outContainer, Key tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone) {
	return FileBackupAgentImpl::submitBackup(this, tr, outContainer, tagName, backupRanges, stopWhenDone);
}

Future<Void> FileBackupAgent::discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName){
	return FileBackupAgentImpl::discontinueBackup(this, tr, tagName);
}

Future<Void> FileBackupAgent::abortBackup(Reference<ReadYourWritesTransaction> tr, Key tagName){
	return FileBackupAgentImpl::abortBackup(this, tr, tagName);
}

Future<std::string> FileBackupAgent::getStatus(Database cx, int errorLimit, Key tagName) {
	return FileBackupAgentImpl::getStatus(this, cx, errorLimit, tagName);
}

Future<int> FileBackupAgent::getStateValue(Reference<ReadYourWritesTransaction> tr, UID logUid) {
	return FileBackupAgentImpl::getStateValue(this, tr, logUid);
}

Future<int64_t> FileBackupAgent::getRangeBytesWritten(Reference<ReadYourWritesTransaction> tr, UID logUid) {
	return BackupConfig(logUid).rangeBytesWritten().getD(tr);
}

Future<int64_t> FileBackupAgent::getLogBytesWritten(Reference<ReadYourWritesTransaction> tr, UID logUid) {
	return BackupConfig(logUid).logBytesWritten().getD(tr);
}

Future<Version> FileBackupAgent::getStateStopVersion(Reference<ReadYourWritesTransaction> tr, UID logUid) {
	return FileBackupAgentImpl::getStateStopVersion(this, tr, logUid);
}

Future<UID> FileBackupAgent::getLogUid(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return FileBackupAgentImpl::getLogUid(this, tr, tagName);
}

Future<Version> FileBackupAgent::getLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return FileBackupAgentImpl::getLastRestorable(this, tr, tagName);
}

Future<int> FileBackupAgent::waitBackup(Database cx, Key tagName, bool stopWhenDone) {
	return FileBackupAgentImpl::waitBackup(this, cx, tagName, stopWhenDone);
}

std::string FileBackupAgent::getTempFilename() {
	return "temp." + g_random->randomUniqueID().toString() + ".part";
}

std::string FileBackupAgent::getDataFilename(Version version, int64_t size, int blocksize) {
	return format("kvrange,%lld,%s,%lld,%d", version, g_random->randomUniqueID().toString().c_str(), size, blocksize);
}

std::string FileBackupAgent::getLogFilename(Version beginVer, Version endVer, int64_t size, int blocksize) {
	return format("log,%lld,%lld,%lld,%d", beginVer, endVer, size, blocksize);
}

Future<std::string> FileBackupAgent::getBackupInfo(std::string container, Version* defaultVersion) {
	return FileBackupAgentImpl::getBackupInfo(IBackupContainer::openContainer(container), defaultVersion);
}

Future<std::string> FileBackupAgent::getLastBackupContainer(Reference<ReadYourWritesTransaction> tr, UID logUid) {
	return fileBackup::getPath(tr, BackupConfig(logUid).backupContainer().key);
}
