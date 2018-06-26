/*
 * BackupAgentBase.actor.cpp
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
#include "fdbrpc/simulator.h"
#include "flow/ActorCollection.h"

const Key BackupAgentBase::keyFolderId = LiteralStringRef("config_folderid");
const Key BackupAgentBase::keyBeginVersion = LiteralStringRef("beginVersion");
const Key BackupAgentBase::keyEndVersion = LiteralStringRef("endVersion");
const Key BackupAgentBase::keyPrevBeginVersion = LiteralStringRef("prevBeginVersion");
const Key BackupAgentBase::keyConfigBackupTag = LiteralStringRef("config_backup_tag");
const Key BackupAgentBase::keyConfigLogUid = LiteralStringRef("config_log_uid");
const Key BackupAgentBase::keyConfigBackupRanges = LiteralStringRef("config_backup_ranges");
const Key BackupAgentBase::keyConfigStopWhenDoneKey = LiteralStringRef("config_stop_when_done");
const Key BackupAgentBase::keyStateStop = LiteralStringRef("state_stop");
const Key BackupAgentBase::keyStateStatus = LiteralStringRef("state_status");
const Key BackupAgentBase::keyLastUid = LiteralStringRef("last_uid");
const Key BackupAgentBase::keyBeginKey = LiteralStringRef("beginKey");
const Key BackupAgentBase::keyEndKey = LiteralStringRef("endKey");
const Key BackupAgentBase::keyDrVersion = LiteralStringRef("drVersion");
const Key BackupAgentBase::destUid = LiteralStringRef("destUid");
const Key BackupAgentBase::backupStartVersion = LiteralStringRef("backupStartVersion");

const Key BackupAgentBase::keyTagName = LiteralStringRef("tagname");
const Key BackupAgentBase::keyStates = LiteralStringRef("state");
const Key BackupAgentBase::keyConfig = LiteralStringRef("config");
const Key BackupAgentBase::keyErrors = LiteralStringRef("errors");
const Key BackupAgentBase::keyRanges = LiteralStringRef("ranges");
const Key BackupAgentBase::keyTasks = LiteralStringRef("tasks");
const Key BackupAgentBase::keyFutures = LiteralStringRef("futures");
const Key BackupAgentBase::keySourceStates = LiteralStringRef("source_states");
const Key BackupAgentBase::keySourceTagName = LiteralStringRef("source_tagname");

bool copyParameter(Reference<Task> source, Reference<Task> dest, Key key) {
	if (source) {
		dest->params[key] = source->params[key];
		return true;
	}

	return false;
}

Version getVersionFromString(std::string const& value) {
	Version version(-1);
	int n = 0;
	if (sscanf(value.c_str(), "%lld%n", (long long*)&version, &n) != 1 || n != value.size()) {
		TraceEvent(SevWarnAlways, "getVersionFromString").detail("InvalidVersion", value);
		throw restore_invalid_version();
	}
	return version;
}

// Transaction log data is stored by the FoundationDB core in the
// \xff / bklog / keyspace in a funny order for performance reasons.
// Return the ranges of keys that contain the data for the given range
// of versions.
Standalone<VectorRef<KeyRangeRef>> getLogRanges(Version beginVersion, Version endVersion, Key destUidValue, int blockSize) {
	Standalone<VectorRef<KeyRangeRef>> ret;

	Key baLogRangePrefix = destUidValue.withPrefix(backupLogKeys.begin);

	//TraceEvent("getLogRanges").detail("destUidValue", destUidValue).detail("prefix", printable(StringRef(baLogRangePrefix)));

	for (int64_t vblock = beginVersion / blockSize; vblock < (endVersion + blockSize - 1) / blockSize; ++vblock) {
		int64_t tb = vblock * blockSize / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
		uint64_t bv = bigEndian64(std::max(beginVersion, vblock * blockSize));
		uint64_t ev = bigEndian64(std::min(endVersion, (vblock + 1) * blockSize));
		uint32_t data = tb & 0xffffffff;
		uint8_t hash = (uint8_t)hashlittle(&data, sizeof(uint32_t), 0);

		Key vblockPrefix = StringRef(&hash, sizeof(uint8_t)).withPrefix(baLogRangePrefix);

		ret.push_back_deep(ret.arena(), KeyRangeRef(StringRef((uint8_t*)&bv, sizeof(uint64_t)).withPrefix(vblockPrefix),
			StringRef((uint8_t*)&ev, sizeof(uint64_t)).withPrefix(vblockPrefix)));
	}

	return ret;
}

Standalone<VectorRef<KeyRangeRef>> getApplyRanges(Version beginVersion, Version endVersion, Key backupUid) {
	Standalone<VectorRef<KeyRangeRef>> ret;

	Key baLogRangePrefix = backupUid.withPrefix(applyLogKeys.begin);

	//TraceEvent("getLogRanges").detail("backupUid", backupUid).detail("prefix", printable(StringRef(baLogRangePrefix)));

	for (int64_t vblock = beginVersion / CLIENT_KNOBS->APPLY_BLOCK_SIZE; vblock < (endVersion + CLIENT_KNOBS->APPLY_BLOCK_SIZE - 1) / CLIENT_KNOBS->APPLY_BLOCK_SIZE; ++vblock) {
		int64_t tb = vblock * CLIENT_KNOBS->APPLY_BLOCK_SIZE / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
		uint64_t bv = bigEndian64(std::max(beginVersion, vblock * CLIENT_KNOBS->APPLY_BLOCK_SIZE));
		uint64_t ev = bigEndian64(std::min(endVersion, (vblock + 1) * CLIENT_KNOBS->APPLY_BLOCK_SIZE));
		uint32_t data = tb & 0xffffffff;
		uint8_t hash = (uint8_t)hashlittle(&data, sizeof(uint32_t), 0);

		Key vblockPrefix = StringRef(&hash, sizeof(uint8_t)).withPrefix(baLogRangePrefix);

		ret.push_back_deep(ret.arena(), KeyRangeRef(StringRef((uint8_t*)&bv, sizeof(uint64_t)).withPrefix(vblockPrefix),
			StringRef((uint8_t*)&ev, sizeof(uint64_t)).withPrefix(vblockPrefix)));
	}

	return ret;
}

Key getApplyKey( Version version, Key backupUid ) {
	int64_t vblock = (version-1) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
	uint64_t v = bigEndian64(version);
	uint32_t data = vblock & 0xffffffff;
	uint8_t hash = (uint8_t)hashlittle(&data, sizeof(uint32_t), 0);
	Key k1 = StringRef((uint8_t*)&v, sizeof(uint64_t)).withPrefix(StringRef(&hash, sizeof(uint8_t)));
	Key k2 = k1.withPrefix(backupUid);
	return k2.withPrefix(applyLogKeys.begin);
}

//Given a key from one of the ranges returned by get_log_ranges,
//returns(version, part) where version is the database version number of
//the transaction log data in the value, and part is 0 for the first such
//data for a given version, 1 for the second block of data, etc.
std::pair<uint64_t, uint32_t> decodeBKMutationLogKey(Key key) {
	return std::make_pair(bigEndian64(*(int64_t*)(key.begin() + backupLogPrefixBytes + sizeof(UID) + sizeof(uint8_t))),
		bigEndian32(*(int32_t*)(key.begin() + backupLogPrefixBytes + sizeof(UID) + sizeof(uint8_t) + sizeof(int64_t))));
}

// value is an iterable representing all of the transaction log data for
// a given version.Returns an iterable(generator) yielding a tuple for
// each mutation in the log.At present, all mutations are represented as
// (type, param1, param2) where type is an integer and param1 and param2 are byte strings
Standalone<VectorRef<MutationRef>> decodeBackupLogValue(StringRef value) {
	try {
		uint64_t offset(0);
		uint64_t protocolVersion = 0;
		memcpy(&protocolVersion, value.begin(), sizeof(uint64_t));
		offset += sizeof(uint64_t);
		if (protocolVersion <= 0x0FDB00A200090001){
			TraceEvent(SevError, "decodeBackupLogValue").detail("incompatible_protocol_version", protocolVersion)
				.detail("valueSize", value.size()).detail("value", printable(value));
			throw incompatible_protocol_version();
		}

		Standalone<VectorRef<MutationRef>> result;
		uint32_t totalBytes = 0;
		memcpy(&totalBytes, value.begin() + offset, sizeof(uint32_t));
		offset += sizeof(uint32_t);
		uint32_t consumed = 0;

		if(totalBytes + offset > value.size())
			throw restore_missing_data();

		int originalOffset = offset;

		while (consumed < totalBytes){
			uint32_t type = 0;
			memcpy(&type, value.begin() + offset, sizeof(uint32_t));
			offset += sizeof(uint32_t);
			uint32_t len1 = 0;
			memcpy(&len1, value.begin() + offset, sizeof(uint32_t));
			offset += sizeof(uint32_t);
			uint32_t len2 = 0;
			memcpy(&len2, value.begin() + offset, sizeof(uint32_t));
			offset += sizeof(uint32_t);

			MutationRef logValue;
			logValue.type = type;
			logValue.param1 = value.substr(offset, len1);
			offset += len1;
			logValue.param2 = value.substr(offset, len2);
			offset += len2;
			result.push_back_deep(result.arena(), logValue);

			consumed += BackupAgentBase::logHeaderSize + len1 + len2;
		}

		ASSERT(consumed == totalBytes);
		if (value.size() != offset) {
			TraceEvent(SevError, "BA_decodeBackupLogValue").detail("unexpected_extra_data_size", value.size()).detail("offset", offset).detail("totalBytes", totalBytes).detail("consumed", consumed).detail("originalOffset", originalOffset);
			throw restore_corrupted_data();
		}

		return result;
	}
	catch (Error& e) {
		TraceEvent(e.code() == error_code_restore_missing_data ? SevWarn : SevError, "BA_decodeBackupLogValue").error(e).GetLastError().detail("valueSize", value.size()).detail("value", printable(value));
		throw;
	}
}

void decodeBackupLogValue(Arena& arena, VectorRef<MutationRef>& result, int& mutationSize, StringRef value, StringRef addPrefix, StringRef removePrefix, Version version, Reference<KeyRangeMap<Version>> key_version) {
	try {
		uint64_t offset(0);
		uint64_t protocolVersion = 0;
		memcpy(&protocolVersion, value.begin(), sizeof(uint64_t));
		offset += sizeof(uint64_t);
		if (protocolVersion <= 0x0FDB00A200090001){
			TraceEvent(SevError, "decodeBackupLogValue").detail("incompatible_protocol_version", protocolVersion)
				.detail("valueSize", value.size()).detail("value", printable(value));
			throw incompatible_protocol_version();
		}

		uint32_t totalBytes = 0;
		memcpy(&totalBytes, value.begin() + offset, sizeof(uint32_t));
		offset += sizeof(uint32_t);
		uint32_t consumed = 0;

		if(totalBytes + offset > value.size())
			throw restore_missing_data();

		int originalOffset = offset;

		while (consumed < totalBytes){
			uint32_t type = 0;
			memcpy(&type, value.begin() + offset, sizeof(uint32_t));
			offset += sizeof(uint32_t);
			uint32_t len1 = 0;
			memcpy(&len1, value.begin() + offset, sizeof(uint32_t));
			offset += sizeof(uint32_t);
			uint32_t len2 = 0;
			memcpy(&len2, value.begin() + offset, sizeof(uint32_t));
			offset += sizeof(uint32_t);

			ASSERT(offset+len1+len2<=value.size() && isValidMutationType(type));

			MutationRef logValue;
			Arena tempArena;
			logValue.type = type;
			logValue.param1 = value.substr(offset, len1);
			offset += len1;
			logValue.param2 = value.substr(offset, len2);
			offset += len2;

			if (logValue.type == MutationRef::ClearRange) {
				KeyRangeRef range(logValue.param1, logValue.param2);
				auto ranges = key_version->intersectingRanges(range);
				for (auto r : ranges) {
					if (version > r.value() && r.value() != invalidVersion) {
						KeyRef minKey = std::min(r.range().end, range.end);
						if (minKey == (removePrefix == StringRef() ? normalKeys.end : strinc(removePrefix))) {
							logValue.param1 = std::max(r.range().begin, range.begin);
							if(removePrefix.size()) {
								logValue.param1 = logValue.param1.removePrefix(removePrefix);
							}
							if(addPrefix.size()) {
								logValue.param1 = logValue.param1.withPrefix(addPrefix, tempArena);
							}
							logValue.param2 = addPrefix == StringRef() ? normalKeys.end : strinc(addPrefix, tempArena);
							result.push_back_deep(arena, logValue);
							mutationSize += logValue.expectedSize();
						}
						else {
							logValue.param1 = std::max(r.range().begin, range.begin);
							logValue.param2 = minKey;
							if(removePrefix.size()) {
								logValue.param1 = logValue.param1.removePrefix(removePrefix);
								logValue.param2 = logValue.param2.removePrefix(removePrefix);
							}
							if(addPrefix.size()) {
								logValue.param1 = logValue.param1.withPrefix(addPrefix, tempArena);
								logValue.param2 = logValue.param2.withPrefix(addPrefix, tempArena);
							}
							result.push_back_deep(arena, logValue);
							mutationSize += logValue.expectedSize();
						}
					}
				}
			}
			else {
				Version ver = key_version->rangeContaining(logValue.param1).value();
				//TraceEvent("ApplyMutation").detail("logValue", logValue.toString()).detail("version", version).detail("ver", ver).detail("apply", version > ver && ver != invalidVersion);
				if (version > ver && ver != invalidVersion) {
					if(removePrefix.size()) {
						logValue.param1 = logValue.param1.removePrefix(removePrefix);
					}
					if(addPrefix.size()) {
						logValue.param1 = logValue.param1.withPrefix(addPrefix, tempArena);
					}
					result.push_back_deep(arena, logValue);
					mutationSize += logValue.expectedSize();
				}
			}

			consumed += BackupAgentBase::logHeaderSize + len1 + len2;
		}

		ASSERT(consumed == totalBytes);
		if (value.size() != offset) {
			TraceEvent(SevError, "BA_decodeBackupLogValue").detail("unexpected_extra_data_size", value.size()).detail("offset", offset).detail("totalBytes", totalBytes).detail("consumed", consumed).detail("originalOffset", originalOffset);
			throw restore_corrupted_data();
		}
	}
	catch (Error& e) {
		TraceEvent(e.code() == error_code_restore_missing_data ? SevWarn : SevError, "BA_decodeBackupLogValue").error(e).GetLastError().detail("valueSize", value.size()).detail("value", printable(value));
		throw;
	}
}
static double lastErrorTime = 0;
ACTOR Future<Void> logErrorWorker(Reference<ReadYourWritesTransaction> tr, Key keyErrors, std::string message) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	if(now() - lastErrorTime > CLIENT_KNOBS->BACKUP_ERROR_DELAY) {
		TraceEvent("BA_logError").detail("key", printable(keyErrors)).detail("message", message);
		lastErrorTime = now();
	}
	tr->set(keyErrors, message);
	return Void();
}


Future<Void> logError(Database cx, Key keyErrors, const std::string& message) {
	return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){return logErrorWorker(tr, keyErrors, message); });
}

Future<Void> logError(Reference<ReadYourWritesTransaction> tr, Key keyErrors, const std::string& message) {
	return logError(tr->getDatabase(), keyErrors, message);
}

ACTOR Future<Void> readCommitted(Database cx, PromiseStream<RangeResultWithVersion> results, Reference<FlowLock> lock,
	KeyRangeRef range, bool terminator, bool systemAccess, bool lockAware) {
	state KeySelector begin = firstGreaterOrEqual(range.begin);
	state KeySelector end = firstGreaterOrEqual(range.end);
	state Transaction tr(cx);
	state FlowLock::Releaser releaser;

	loop{
		try {
			state GetRangeLimits limits(CLIENT_KNOBS->ROW_LIMIT_UNLIMITED, (g_network->isSimulated() && !g_simulator.speedUpSimulation) ? CLIENT_KNOBS->BACKUP_SIMULATED_LIMIT_BYTES : CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES);

			if (systemAccess)
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (lockAware)
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			//add lock
			releaser.release();
			Void _ = wait(lock->take(TaskDefaultYield, limits.bytes + CLIENT_KNOBS->VALUE_SIZE_LIMIT + CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT));
			releaser = FlowLock::Releaser(*lock, limits.bytes + CLIENT_KNOBS->VALUE_SIZE_LIMIT + CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT);

			state Standalone<RangeResultRef> values = wait(tr.getRange(begin, end, limits));

			// When this buggify line is enabled, if there are more than 1 result then use half of the results
			if(values.size() > 1 && BUGGIFY) {
				values.resize(values.arena(), values.size() / 2);
				values.more = true;
				// Half of the time wait for this tr to expire so that the next read is at a different version
				if(g_random->random01() < 0.5)
					Void _ = wait(delay(6.0));
			}

			releaser.remaining -= values.expectedSize(); //its the responsibility of the caller to release after this point
			ASSERT(releaser.remaining >= 0);

			results.send(RangeResultWithVersion(values, tr.getReadVersion().get()));

			if (values.size() > 0)
				begin = firstGreaterThan(values.end()[-1].key);

			if (!values.more && !limits.isReached()) {
				if(terminator)
					results.sendError(end_of_stream());
				return Void();
			}
		}
		catch (Error &e) {
			if (e.code() != error_code_transaction_too_old && e.code() != error_code_future_version)
				throw;
			tr = Transaction(cx);
		}
	}
}

ACTOR Future<Void> readCommitted(Database cx, PromiseStream<RCGroup> results, Future<Void> active, Reference<FlowLock> lock,
	KeyRangeRef range, std::function< std::pair<uint64_t, uint32_t>(Key key) > groupBy,
	bool terminator, bool systemAccess, bool lockAware)
{
	state KeySelector nextKey = firstGreaterOrEqual(range.begin);
	state KeySelector end = firstGreaterOrEqual(range.end);

	state RCGroup rcGroup = RCGroup();
	state uint64_t skipGroup(ULLONG_MAX);
	state Transaction tr(cx);
	state FlowLock::Releaser releaser;

	loop{
		try {
			state GetRangeLimits limits(CLIENT_KNOBS->ROW_LIMIT_UNLIMITED, (g_network->isSimulated() && !g_simulator.speedUpSimulation) ? CLIENT_KNOBS->BACKUP_SIMULATED_LIMIT_BYTES : CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES);

			if (systemAccess)
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (lockAware)
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			state Standalone<RangeResultRef> rangevalue = wait(tr.getRange(nextKey, end, limits));

			// When this buggify line is enabled, if there are more than 1 result then use half of the results
			if(rangevalue.size() > 1 && BUGGIFY) {
				rangevalue.resize(rangevalue.arena(), rangevalue.size() / 2);
				rangevalue.more = true;
				// Half of the time wait for this tr to expire so that the next read is at a different version
				if(g_random->random01() < 0.5)
					Void _ = wait(delay(6.0));
			}

			//add lock
			Void _ = wait(active);
			releaser.release();
			Void _ = wait(lock->take(TaskDefaultYield, rangevalue.expectedSize() + rcGroup.items.expectedSize()));
			releaser = FlowLock::Releaser(*lock, rangevalue.expectedSize() + rcGroup.items.expectedSize());

			int index(0);
			for (auto & s : rangevalue){
				uint64_t groupKey = groupBy(s.key).first;
				//TraceEvent("log_readCommitted").detail("groupKey", groupKey).detail("skipGroup", skipGroup).detail("nextKey", printable(nextKey.key)).detail("end", printable(end.key)).detail("valuesize", value.size()).detail("index",index++).detail("size",s.value.size());
				if (groupKey != skipGroup){
					if (rcGroup.version == -1){
						rcGroup.version = tr.getReadVersion().get();
						rcGroup.groupKey = groupKey;
					}
					else if (rcGroup.groupKey != groupKey) {
						//TraceEvent("log_readCommitted").detail("sendGroup0", rcGroup.groupKey).detail("itemSize", rcGroup.items.size()).detail("data_length",rcGroup.items[0].value.size());
						//state uint32_t len(0);
						//for (size_t j = 0; j < rcGroup.items.size(); ++j) {
						//	len += rcGroup.items[j].value.size();
						//}
						//TraceEvent("SendGroup").detail("groupKey", rcGroup.groupKey).detail("version", rcGroup.version).detail("length", len).detail("releaser.remaining", releaser.remaining);
						releaser.remaining -= rcGroup.items.expectedSize(); //its the responsibility of the caller to release after this point
						ASSERT(releaser.remaining >= 0);
						results.send(rcGroup);
						nextKey = firstGreaterThan(rcGroup.items.end()[-1].key);
						skipGroup = rcGroup.groupKey;

						rcGroup = RCGroup();
						rcGroup.version = tr.getReadVersion().get();
						rcGroup.groupKey = groupKey;
					}
					rcGroup.items.push_back_deep(rcGroup.items.arena(), s);
				}
			}

			if (!rangevalue.more) {
				if (rcGroup.version != -1){
					releaser.remaining -= rcGroup.items.expectedSize(); //its the responsibility of the caller to release after this point
					ASSERT(releaser.remaining >= 0);
					//TraceEvent("log_readCommitted").detail("sendGroup1", rcGroup.groupKey).detail("itemSize", rcGroup.items.size()).detail("data_length", rcGroup.items[0].value.size());
					results.send(rcGroup);
				}

				if(terminator)
					results.sendError(end_of_stream());
				return Void();
			}

			nextKey = firstGreaterThan(rangevalue.end()[-1].key);
		}
		catch (Error &e) {
			if (e.code() != error_code_transaction_too_old && e.code() != error_code_future_version)
				throw;
			Void _ = wait(tr.onError(e));
		}
	}
}

Future<Void> readCommitted(Database cx, PromiseStream<RCGroup> results, Reference<FlowLock> lock, KeyRangeRef range, std::function< std::pair<uint64_t, uint32_t>(Key key) > groupBy) {
	return readCommitted(cx, results, Void(), lock, range, groupBy, true, true, true);
}

ACTOR Future<int> dumpData(Database cx, PromiseStream<RCGroup> results, Reference<FlowLock> lock, Key uid, Key addPrefix, Key removePrefix, RequestStream<CommitTransactionRequest> commit,
	NotifiedVersion* committedVersion, Optional<Version> endVersion, Key rangeBegin, PromiseStream<Future<Void>> addActor, FlowLock* commitLock, Reference<KeyRangeMap<Version>> keyVersion ) {
	state Version lastVersion = invalidVersion;
	state bool endOfStream = false;
	state int totalBytes = 0;
	loop {
		state CommitTransactionRequest req;
		state Version newBeginVersion = invalidVersion;
		state int mutationSize = 0;
		loop {
			try {
				RCGroup group = waitNext(results.getFuture());
				lock->release(group.items.expectedSize());

				BinaryWriter bw(Unversioned());
				for(int i = 0; i < group.items.size(); ++i) {
					bw.serializeBytes(group.items[i].value);
				}
				decodeBackupLogValue(req.arena, req.transaction.mutations, mutationSize, bw.toStringRef(), addPrefix, removePrefix, group.groupKey, keyVersion);
				newBeginVersion = group.groupKey + 1;
				if(mutationSize >= CLIENT_KNOBS->BACKUP_LOG_WRITE_BATCH_MAX_SIZE) {
					break;
				}
			}
			catch (Error &e) {
				if (e.code() == error_code_end_of_stream) {
					if(endVersion.present() && endVersion.get() > lastVersion && endVersion.get() > newBeginVersion) {
						newBeginVersion = endVersion.get();
					}
					if(newBeginVersion == invalidVersion)
						return totalBytes;
					endOfStream = true;
					break;
				}
				throw;
			}
		}

		Key applyBegin = uid.withPrefix(applyMutationsBeginRange.begin);
		Key versionKey = BinaryWriter::toValue(newBeginVersion, Unversioned());
		Key rangeEnd = getApplyKey(newBeginVersion, uid);

		req.transaction.mutations.push_back_deep(req.arena, MutationRef(MutationRef::SetValue, applyBegin, versionKey));
		req.transaction.write_conflict_ranges.push_back_deep(req.arena, singleKeyRange(applyBegin));
		req.transaction.mutations.push_back_deep(req.arena, MutationRef(MutationRef::ClearRange, rangeBegin, rangeEnd));
		req.transaction.write_conflict_ranges.push_back_deep(req.arena, singleKeyRange(rangeBegin));

		// The commit request contains no read conflict ranges, so regardless of what read version we
		// choose, it's impossible for us to get a transaction_too_old error back, and it's impossible
		// for our transaction to be aborted due to conflicts.
		req.transaction.read_snapshot = committedVersion->get();
		req.isLockAware = true;

		totalBytes += mutationSize;
		Void _ = wait( commitLock->take(TaskDefaultYield, mutationSize) );
		addActor.send( commitLock->releaseWhen( success(commit.getReply(req)), mutationSize ) );

		if(endOfStream) {
			return totalBytes;
		}
	}
}

ACTOR Future<Void> coalesceKeyVersionCache(Key uid, Version endVersion, Reference<KeyRangeMap<Version>> keyVersion, RequestStream<CommitTransactionRequest> commit, NotifiedVersion* committedVersion, PromiseStream<Future<Void>> addActor, FlowLock* commitLock) {
	Version lastVersion = -1000;
	int64_t removed = 0;
	state CommitTransactionRequest req;
	state int64_t mutationSize = 0;
	Key mapPrefix = uid.withPrefix(applyMutationsKeyVersionMapRange.begin);

	for(auto it : keyVersion->ranges()) {
		if( lastVersion == -1000 ) {
			lastVersion = it.value();
		} else {
			Version ver = it.value();
			if(ver < endVersion && lastVersion < endVersion && ver != invalidVersion && lastVersion != invalidVersion) {
				Key removeKey = it.range().begin.withPrefix(mapPrefix);
				Key removeEnd = keyAfter(removeKey);
				req.transaction.mutations.push_back_deep(req.arena, MutationRef(MutationRef::ClearRange, removeKey, removeEnd));
				mutationSize += removeKey.size() + removeEnd.size();
				removed--;
			} else {
				lastVersion = ver;
			}
		}
	}

	if(removed != 0) {
		Key countKey = uid.withPrefix(applyMutationsKeyVersionCountRange.begin);
		req.transaction.write_conflict_ranges.push_back_deep(req.arena, singleKeyRange(countKey));
		req.transaction.mutations.push_back_deep(req.arena, MutationRef(MutationRef::AddValue, countKey, StringRef((uint8_t*)&removed, 8)));
		req.transaction.read_snapshot = committedVersion->get();
		req.isLockAware = true;

		Void _ = wait( commitLock->take(TaskDefaultYield, mutationSize) );
		addActor.send( commitLock->releaseWhen( success(commit.getReply(req)), mutationSize ) );
	}

	return Void();
}

ACTOR Future<Void> applyMutations(Database cx, Key uid, Key addPrefix, Key removePrefix, Version beginVersion, Version* endVersion, RequestStream<CommitTransactionRequest> commit, NotifiedVersion* committedVersion, Reference<KeyRangeMap<Version>> keyVersion ) {
	state FlowLock commitLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection( addActor.getFuture() );
	state int maxBytes = CLIENT_KNOBS->APPLY_MIN_LOCK_BYTES;

	try {
		loop {
			if(beginVersion >= *endVersion) {
				Void _ = wait( commitLock.take(TaskDefaultYield, CLIENT_KNOBS->BACKUP_LOCK_BYTES) );
				commitLock.release(CLIENT_KNOBS->BACKUP_LOCK_BYTES);
				if(beginVersion >= *endVersion) {
					return Void();
				}
			}
		
			int rangeCount = std::max(1, CLIENT_KNOBS->APPLY_MAX_LOCK_BYTES / maxBytes);
			state Version newEndVersion = std::min(*endVersion, ((beginVersion / CLIENT_KNOBS->APPLY_BLOCK_SIZE) + rangeCount) * CLIENT_KNOBS->APPLY_BLOCK_SIZE);
			state Standalone<VectorRef<KeyRangeRef>> ranges = getApplyRanges(beginVersion, newEndVersion, uid);
			state size_t idx;
			state std::vector<PromiseStream<RCGroup>> results;
			state std::vector<Future<Void>> rc;
			state std::vector<Reference<FlowLock>> locks;

			for (int i = 0; i < ranges.size(); ++i) {
				results.push_back(PromiseStream<RCGroup>());
				locks.push_back(Reference<FlowLock>( new FlowLock(std::max(CLIENT_KNOBS->APPLY_MAX_LOCK_BYTES/ranges.size(), CLIENT_KNOBS->APPLY_MIN_LOCK_BYTES))));
				rc.push_back(readCommitted(cx, results[i], locks[i], ranges[i], decodeBKMutationLogKey));
			}

			maxBytes = std::max<int>(maxBytes*CLIENT_KNOBS->APPLY_MAX_DECAY_RATE, CLIENT_KNOBS->APPLY_MIN_LOCK_BYTES);
			for (idx = 0; idx < ranges.size(); ++idx) {
				int bytes = wait(dumpData(cx, results[idx], locks[idx], uid, addPrefix, removePrefix, commit, committedVersion, idx==ranges.size()-1 ? newEndVersion : Optional<Version>(), ranges[idx].begin, addActor, &commitLock, keyVersion));
				maxBytes = std::max<int>(CLIENT_KNOBS->APPLY_MAX_INCREASE_FACTOR*bytes, maxBytes);
				if(error.isError()) throw error.getError();
			}

			Void _ = wait(coalesceKeyVersionCache(uid, newEndVersion, keyVersion, commit, committedVersion, addActor, &commitLock));
			beginVersion = newEndVersion;
		}
	} catch( Error &e ) {
		TraceEvent(e.code() == error_code_restore_missing_data ? SevWarnAlways : SevError, "AM_error").error(e);
		throw;	
	}
}

ACTOR static Future<Void> _eraseLogData(Database cx, Key logUidValue, Key destUidValue, Optional<Version> endVersion, bool checkBackupUid, Version backupUid) {
	state Key backupLatestVersionsPath = destUidValue.withPrefix(backupLatestVersionsPrefix);
	state Key backupLatestVersionsKey = logUidValue.withPrefix(backupLatestVersionsPath);

	if (!destUidValue.size()) {
		return Void();
	}

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop{
		try {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			if (checkBackupUid) {
				Subspace sourceStates = Subspace(databaseBackupPrefixRange.begin).get(BackupAgentBase::keySourceStates).get(logUidValue);
				Optional<Value> v = wait( tr->get( sourceStates.pack(DatabaseBackupAgent::keyFolderId) ) );
				if(v.present() && BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) > backupUid)
					return Void();
			}

			state Standalone<RangeResultRef> backupVersions = wait(tr->getRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath)), CLIENT_KNOBS->TOO_MANY));

			// Make sure version history key does exist and lower the beginVersion if needed
			state Version currBeginVersion = invalidVersion;
			for (auto backupVersion : backupVersions) {
				Key currLogUidValue = backupVersion.key.removePrefix(backupLatestVersionsPrefix).removePrefix(destUidValue);

				if (currLogUidValue == logUidValue) {
					currBeginVersion = BinaryReader::fromStringRef<Version>(backupVersion.value, Unversioned());
					break;
				}
			}

			// Do not clear anything if version history key cannot be found
			if (currBeginVersion == invalidVersion) {
				return Void();
			}

			state Version currEndVersion = currBeginVersion + CLIENT_KNOBS->CLEAR_LOG_RANGE_COUNT * CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
			if(endVersion.present()) {
				currEndVersion = std::min(currEndVersion, endVersion.get());
			}

			state Version nextSmallestVersion = currEndVersion;
			bool clearLogRangesRequired = true;

			// More than one backup/DR with the same range
			if (backupVersions.size() > 1) {
				for (auto backupVersion : backupVersions) {
					Key currLogUidValue = backupVersion.key.removePrefix(backupLatestVersionsPrefix).removePrefix(destUidValue);
					Version currVersion = BinaryReader::fromStringRef<Version>(backupVersion.value, Unversioned());

					if (currLogUidValue == logUidValue) {
						continue;
					} else if (currVersion > currBeginVersion) {
						nextSmallestVersion = std::min(currVersion, nextSmallestVersion);
					} else {
						// If we can find a version less than or equal to beginVersion, clearing log ranges is not required
						clearLogRangesRequired = false;
						break;
					}
				}
			}

			if (!endVersion.present() && backupVersions.size() == 1) {
				// Clear version history
				tr->clear(prefixRange(backupLatestVersionsPath));

				// Clear everything under blog/[destUid]
				tr->clear(prefixRange(destUidValue.withPrefix(backupLogKeys.begin)));

				// Disable committing mutations into blog
				tr->clear(prefixRange(destUidValue.withPrefix(logRangesRange.begin)));
			} else {
				if (!endVersion.present() && currEndVersion >= nextSmallestVersion) {
					// Clear current backup version history
					tr->clear(backupLatestVersionsKey);
				} else {
					// Update current backup latest version
					tr->set(backupLatestVersionsKey, BinaryWriter::toValue<Version>(currEndVersion, Unversioned()));
				}

				// Clear log ranges if needed
				if (clearLogRangesRequired) {
					Standalone<VectorRef<KeyRangeRef>> ranges = getLogRanges(currBeginVersion, nextSmallestVersion, destUidValue);
					for (auto& range : ranges) {
						tr->clear(range);
					}
				}
			}
			Void _ = wait(tr->commit());

			if (!endVersion.present() && (backupVersions.size() == 1 || currEndVersion >= nextSmallestVersion)) {
				return Void();
			}
			if(endVersion.present() && currEndVersion == endVersion.get()) {
				return Void();
			}
			tr->reset();
		} catch (Error &e) {
			Void _ = wait(tr->onError(e));
		}
	}
}

Future<Void> eraseLogData(Database cx, Key logUidValue, Key destUidValue, Optional<Version> endVersion, bool checkBackupUid, Version backupUid) {
	return _eraseLogData(cx, logUidValue, destUidValue, endVersion, checkBackupUid, backupUid);
}
