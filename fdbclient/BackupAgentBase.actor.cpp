/*
 * BackupAgentBase.actor.cpp
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

#include <iomanip>
#include <time.h>

#include "fdbclient/BackupAgent.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/ActorCollection.h"
#include "flow/actorcompiler.h" // has to be last include

FDB_DEFINE_BOOLEAN_PARAM(LockDB);
FDB_DEFINE_BOOLEAN_PARAM(UnlockDB);
FDB_DEFINE_BOOLEAN_PARAM(StopWhenDone);
FDB_DEFINE_BOOLEAN_PARAM(Verbose);
FDB_DEFINE_BOOLEAN_PARAM(WaitForComplete);
FDB_DEFINE_BOOLEAN_PARAM(ForceAction);
FDB_DEFINE_BOOLEAN_PARAM(Terminator);
FDB_DEFINE_BOOLEAN_PARAM(UsePartitionedLog);
FDB_DEFINE_BOOLEAN_PARAM(InconsistentSnapshotOnly);
FDB_DEFINE_BOOLEAN_PARAM(ShowErrors);
FDB_DEFINE_BOOLEAN_PARAM(AbortOldBackup);
FDB_DEFINE_BOOLEAN_PARAM(DstOnly);
FDB_DEFINE_BOOLEAN_PARAM(WaitForDestUID);
FDB_DEFINE_BOOLEAN_PARAM(CheckBackupUID);
FDB_DEFINE_BOOLEAN_PARAM(DeleteData);
FDB_DEFINE_BOOLEAN_PARAM(SetValidation);
FDB_DEFINE_BOOLEAN_PARAM(PartialBackup);

std::string BackupAgentBase::formatTime(int64_t epochs) {
	time_t curTime = (time_t)epochs;
	char buffer[30];
	struct tm timeinfo;
	getLocalTime(&curTime, &timeinfo);
	strftime(buffer, 30, "%Y/%m/%d.%H:%M:%S%z", &timeinfo);
	return buffer;
}

int64_t BackupAgentBase::parseTime(std::string timestamp) {
	struct tm out;
	out.tm_isdst = -1; // This field is not set by strptime. -1 tells mktime to determine whether DST is in effect

	std::string timeOnly = timestamp.substr(0, 19);

	// TODO:  Use std::get_time implementation for all platforms once supported
	// It would be nice to read the timezone using %z, but it seems not all get_time()
	// or strptime() implementations handle it correctly in all environments so we
	// will read the date and time independent of timezone at first and then adjust it.
#ifdef _WIN32
	std::istringstream s(timeOnly);
	s.imbue(std::locale(setlocale(LC_TIME, nullptr)));
	s >> std::get_time(&out, "%Y/%m/%d.%H:%M:%S");
	if (s.fail()) {
		return -1;
	}
#else
	if (strptime(timeOnly.c_str(), "%Y/%m/%d.%H:%M:%S", &out) == nullptr) {
		return -1;
	}
#endif

	// Read timezone offset in +/-HHMM format then convert to seconds
	int tzHH;
	int tzMM;
	if (sscanf(timestamp.substr(19, 5).c_str(), "%3d%2d", &tzHH, &tzMM) != 2) {
		return -1;
	}
	if (tzHH < 0) {
		tzMM = -tzMM;
	}
	// tzOffset is the number of seconds EAST of GMT
	int tzOffset = tzHH * 60 * 60 + tzMM * 60;

	// The goal is to convert the timestamp string to epoch seconds assuming the date/time was expressed in the timezone
	// at the end of the string. However, mktime() will ONLY return epoch seconds assuming the date/time is expressed in
	// local time (based on locale / environment) mktime() will set out.tm_gmtoff when available
	int64_t ts = mktime(&out);

	// localTZOffset is the number of seconds EAST of GMT
	long localTZOffset;
#ifdef _WIN32
	// _get_timezone() returns the number of seconds WEST of GMT
	if (_get_timezone(&localTZOffset) != 0) {
		return -1;
	}
	// Negate offset to match the orientation of tzOffset
	localTZOffset = -localTZOffset;
#else
	// tm.tm_gmtoff is the number of seconds EAST of GMT
	localTZOffset = out.tm_gmtoff;
#endif

	// Add back the difference between the local timezone assumed by mktime() and the intended timezone from the input
	// string
	ts += (localTZOffset - tzOffset);
	return ts;
}

const Key BackupAgentBase::keyFolderId = "config_folderid"_sr;
const Key BackupAgentBase::keyBeginVersion = "beginVersion"_sr;
const Key BackupAgentBase::keyEndVersion = "endVersion"_sr;
const Key BackupAgentBase::keyPrevBeginVersion = "prevBeginVersion"_sr;
const Key BackupAgentBase::keyConfigBackupTag = "config_backup_tag"_sr;
const Key BackupAgentBase::keyConfigLogUid = "config_log_uid"_sr;
const Key BackupAgentBase::keyConfigBackupRanges = "config_backup_ranges"_sr;
const Key BackupAgentBase::keyConfigStopWhenDoneKey = "config_stop_when_done"_sr;
const Key BackupAgentBase::keyStateStop = "state_stop"_sr;
const Key BackupAgentBase::keyStateStatus = "state_status"_sr;
const Key BackupAgentBase::keyStateLogBeginVersion = "last_begin_version"_sr;
const Key BackupAgentBase::keyLastUid = "last_uid"_sr;
const Key BackupAgentBase::keyBeginKey = "beginKey"_sr;
const Key BackupAgentBase::keyEndKey = "endKey"_sr;
const Key BackupAgentBase::keyDrVersion = "drVersion"_sr;
const Key BackupAgentBase::destUid = "destUid"_sr;
const Key BackupAgentBase::backupStartVersion = "backupStartVersion"_sr;

const Key BackupAgentBase::keyTagName = "tagname"_sr;
const Key BackupAgentBase::keyStates = "state"_sr;
const Key BackupAgentBase::keyConfig = "config"_sr;
const Key BackupAgentBase::keyErrors = "errors"_sr;
const Key BackupAgentBase::keyRanges = "ranges"_sr;
const Key BackupAgentBase::keyTasks = "tasks"_sr;
const Key BackupAgentBase::keyFutures = "futures"_sr;
const Key BackupAgentBase::keySourceStates = "source_states"_sr;
const Key BackupAgentBase::keySourceTagName = "source_tagname"_sr;

bool copyParameter(Reference<Task> source, Reference<Task> dest, Key key) {
	if (source) {
		dest->params[key] = source->params[key];
		return true;
	}

	return false;
}

Version getVersionFromString(std::string const& value) {
	Version version = invalidVersion;
	int n = 0;
	if (sscanf(value.c_str(), "%lld%n", (long long*)&version, &n) != 1 || n != value.size()) {
		TraceEvent(SevWarnAlways, "GetVersionFromString").detail("InvalidVersion", value);
		throw restore_invalid_version();
	}
	return version;
}

// Transaction log data is stored by the FoundationDB core in the
// "backupLogKeys" (i.e., \xff\x02/blog/) keyspace in a funny order for
// performance reasons.
// Returns the ranges of keys that contain the data for the given range
// of versions.
// assert CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE % blocksize = 0. Otherwise calculation of hash will be incorrect
Standalone<VectorRef<KeyRangeRef>> getLogRanges(Version beginVersion,
                                                Version endVersion,
                                                Key destUidValue,
                                                int blockSize) {
	Standalone<VectorRef<KeyRangeRef>> ret;

	Key baLogRangePrefix = destUidValue.withPrefix(backupLogKeys.begin);

	//TraceEvent("GetLogRanges").detail("DestUidValue", destUidValue).detail("Prefix", baLogRangePrefix);

	for (int64_t vblock = beginVersion / blockSize; vblock < (endVersion + blockSize - 1) / blockSize; ++vblock) {
		int64_t tb = vblock * blockSize / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
		uint64_t bv = bigEndian64(std::max(beginVersion, vblock * blockSize));
		uint64_t ev = bigEndian64(std::min(endVersion, (vblock + 1) * blockSize));
		uint32_t data = tb & 0xffffffff;
		uint8_t hash = (uint8_t)hashlittle(&data, sizeof(uint32_t), 0);

		Key vblockPrefix = StringRef(&hash, sizeof(uint8_t)).withPrefix(baLogRangePrefix);

		ret.push_back_deep(ret.arena(),
		                   KeyRangeRef(StringRef((uint8_t*)&bv, sizeof(uint64_t)).withPrefix(vblockPrefix),
		                               StringRef((uint8_t*)&ev, sizeof(uint64_t)).withPrefix(vblockPrefix)));
	}

	return ret;
}

Standalone<VectorRef<KeyRangeRef>> getApplyRanges(Version beginVersion, Version endVersion, Key backupUid) {
	Standalone<VectorRef<KeyRangeRef>> ret;

	Key baLogRangePrefix = backupUid.withPrefix(applyLogKeys.begin);

	//TraceEvent("GetLogRanges").detail("BackupUid", backupUid).detail("Prefix", baLogRangePrefix);

	for (int64_t vblock = beginVersion / CLIENT_KNOBS->APPLY_BLOCK_SIZE;
	     vblock < (endVersion + CLIENT_KNOBS->APPLY_BLOCK_SIZE - 1) / CLIENT_KNOBS->APPLY_BLOCK_SIZE;
	     ++vblock) {
		int64_t tb = vblock * CLIENT_KNOBS->APPLY_BLOCK_SIZE / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
		uint64_t bv = bigEndian64(std::max(beginVersion, vblock * CLIENT_KNOBS->APPLY_BLOCK_SIZE));
		uint64_t ev = bigEndian64(std::min(endVersion, (vblock + 1) * CLIENT_KNOBS->APPLY_BLOCK_SIZE));
		uint32_t data = tb & 0xffffffff;
		uint8_t hash = (uint8_t)hashlittle(&data, sizeof(uint32_t), 0);

		Key vblockPrefix = StringRef(&hash, sizeof(uint8_t)).withPrefix(baLogRangePrefix);

		ret.push_back_deep(ret.arena(),
		                   KeyRangeRef(StringRef((uint8_t*)&bv, sizeof(uint64_t)).withPrefix(vblockPrefix),
		                               StringRef((uint8_t*)&ev, sizeof(uint64_t)).withPrefix(vblockPrefix)));
	}

	return ret;
}

Key getApplyKey(Version version, Key backupUid) {
	int64_t vblock = (version - 1) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
	uint64_t v = bigEndian64(version);
	uint32_t data = vblock & 0xffffffff;
	uint8_t hash = (uint8_t)hashlittle(&data, sizeof(uint32_t), 0);
	Key k1 = StringRef((uint8_t*)&v, sizeof(uint64_t)).withPrefix(StringRef(&hash, sizeof(uint8_t)));
	Key k2 = k1.withPrefix(backupUid);
	return k2.withPrefix(applyLogKeys.begin);
}

Key getLogKey(Version version, Key backupUid) {
	int64_t vblock = (version - 1) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE;
	uint64_t v = bigEndian64(version);
	uint32_t data = vblock & 0xffffffff;
	uint8_t hash = (uint8_t)hashlittle(&data, sizeof(uint32_t), 0);
	Key k1 = StringRef((uint8_t*)&v, sizeof(uint64_t)).withPrefix(StringRef(&hash, sizeof(uint8_t)));
	Key k2 = k1.withPrefix(backupUid);
	return k2.withPrefix(backupLogKeys.begin);
}

Version getLogKeyVersion(Key key) {
	return bigEndian64(*(int64_t*)(key.begin() + backupLogPrefixBytes + sizeof(UID) + sizeof(uint8_t)));
}

// Given a key from one of the ranges returned by get_log_ranges,
// returns(version, part) where version is the database version number of
// the transaction log data in the value, and part is 0 for the first such
// data for a given version, 1 for the second block of data, etc.
std::pair<Version, uint32_t> decodeBKMutationLogKey(Key key) {
	return std::make_pair(
	    getLogKeyVersion(key),
	    bigEndian32(*(int32_t*)(key.begin() + backupLogPrefixBytes + sizeof(UID) + sizeof(uint8_t) + sizeof(int64_t))));
}

void decodeBackupLogValue(Arena& arena,
                          VectorRef<MutationRef>& result,
                          int& mutationSize,
                          StringRef value,
                          StringRef addPrefix,
                          StringRef removePrefix,
                          Version version,
                          Reference<KeyRangeMap<Version>> key_version) {
	try {
		uint64_t offset(0);
		uint64_t protocolVersion = 0;
		memcpy(&protocolVersion, value.begin(), sizeof(uint64_t));
		offset += sizeof(uint64_t);
		if (protocolVersion <= 0x0FDB00A200090001) {
			TraceEvent(SevError, "DecodeBackupLogValue")
			    .detail("IncompatibleProtocolVersion", protocolVersion)
			    .detail("ValueSize", value.size())
			    .detail("Value", value);
			throw incompatible_protocol_version();
		}

		uint32_t totalBytes = 0;
		memcpy(&totalBytes, value.begin() + offset, sizeof(uint32_t));
		offset += sizeof(uint32_t);
		uint32_t consumed = 0;

		if (totalBytes + offset > value.size())
			throw restore_missing_data();

		int originalOffset = offset;

		while (consumed < totalBytes) {
			uint32_t type = 0;
			memcpy(&type, value.begin() + offset, sizeof(uint32_t));
			offset += sizeof(uint32_t);
			uint32_t len1 = 0;
			memcpy(&len1, value.begin() + offset, sizeof(uint32_t));
			offset += sizeof(uint32_t);
			uint32_t len2 = 0;
			memcpy(&len2, value.begin() + offset, sizeof(uint32_t));
			offset += sizeof(uint32_t);

			ASSERT(offset + len1 + len2 <= value.size() && isValidMutationType(type));

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
							if (removePrefix.size()) {
								logValue.param1 = logValue.param1.removePrefix(removePrefix);
							}
							if (addPrefix.size()) {
								logValue.param1 = logValue.param1.withPrefix(addPrefix, tempArena);
							}
							logValue.param2 = addPrefix == StringRef() ? normalKeys.end : strinc(addPrefix, tempArena);
							result.push_back_deep(arena, logValue);
							mutationSize += logValue.expectedSize();
						} else {
							logValue.param1 = std::max(r.range().begin, range.begin);
							logValue.param2 = minKey;
							if (removePrefix.size()) {
								logValue.param1 = logValue.param1.removePrefix(removePrefix);
								logValue.param2 = logValue.param2.removePrefix(removePrefix);
							}
							if (addPrefix.size()) {
								logValue.param1 = logValue.param1.withPrefix(addPrefix, tempArena);
								logValue.param2 = logValue.param2.withPrefix(addPrefix, tempArena);
							}
							result.push_back_deep(arena, logValue);
							mutationSize += logValue.expectedSize();
						}
					}
				}
			} else {
				Version ver = key_version->rangeContaining(logValue.param1).value();
				//TraceEvent("ApplyMutation").detail("LogValue", logValue).detail("Version", version).detail("Ver", ver).detail("Apply", version > ver && ver != invalidVersion);
				if (version > ver && ver != invalidVersion) {
					if (removePrefix.size()) {
						logValue.param1 = logValue.param1.removePrefix(removePrefix);
					}
					if (addPrefix.size()) {
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
			TraceEvent(SevError, "BA_DecodeBackupLogValue")
			    .detail("UnexpectedExtraDataSize", value.size())
			    .detail("Offset", offset)
			    .detail("TotalBytes", totalBytes)
			    .detail("Consumed", consumed)
			    .detail("OriginalOffset", originalOffset);
			throw restore_corrupted_data();
		}
	} catch (Error& e) {
		TraceEvent(e.code() == error_code_restore_missing_data ? SevWarn : SevError, "BA_DecodeBackupLogValue")
		    .error(e)
		    .GetLastError()
		    .detail("ValueSize", value.size())
		    .detail("Value", value);
		throw;
	}
}

static double lastErrorTime = 0;

void logErrorWorker(Reference<ReadYourWritesTransaction> tr, Key keyErrors, std::string message) {
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	if (now() - lastErrorTime > CLIENT_KNOBS->BACKUP_ERROR_DELAY) {
		TraceEvent("BA_LogError").detail("Key", keyErrors).detail("Message", message);
		lastErrorTime = now();
	}
	tr->set(keyErrors, message);
}

Future<Void> logError(Database cx, Key keyErrors, const std::string& message) {
	return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
		logErrorWorker(tr, keyErrors, message);
		return Future<Void>(Void());
	});
}

Future<Void> logError(Reference<ReadYourWritesTransaction> tr, Key keyErrors, const std::string& message) {
	return logError(tr->getDatabase(), keyErrors, message);
}

ACTOR Future<Void> readCommitted(Database cx,
                                 PromiseStream<RangeResultWithVersion> results,
                                 Reference<FlowLock> lock,
                                 KeyRangeRef range,
                                 Terminator terminator,
                                 AccessSystemKeys systemAccess,
                                 LockAware lockAware) {
	state KeySelector begin = firstGreaterOrEqual(range.begin);
	state KeySelector end = firstGreaterOrEqual(range.end);
	state Transaction tr(cx);
	state FlowLock::Releaser releaser;

	loop {
		try {
			state GetRangeLimits limits(GetRangeLimits::ROW_LIMIT_UNLIMITED,
			                            (g_network->isSimulated() && !g_simulator.speedUpSimulation)
			                                ? CLIENT_KNOBS->BACKUP_SIMULATED_LIMIT_BYTES
			                                : CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES);

			if (systemAccess)
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (lockAware)
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			// add lock
			releaser.release();
			wait(lock->take(TaskPriority::DefaultYield,
			                limits.bytes + CLIENT_KNOBS->VALUE_SIZE_LIMIT + CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT));
			releaser = FlowLock::Releaser(
			    *lock, limits.bytes + CLIENT_KNOBS->VALUE_SIZE_LIMIT + CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT);

			state RangeResult values = wait(tr.getRange(begin, end, limits));

			// When this buggify line is enabled, if there are more than 1 result then use half of the results
			// Copy the data instead of messing with the results directly to avoid TSS issues.
			if (values.size() > 1 && BUGGIFY) {
				RangeResult copy;
				// only copy first half of values into copy
				for (int i = 0; i < values.size() / 2; i++) {
					copy.push_back_deep(copy.arena(), values[i]);
				}
				values = copy;
				values.more = true;
				// Half of the time wait for this tr to expire so that the next read is at a different version
				if (deterministicRandom()->random01() < 0.5)
					wait(delay(6.0));
			}

			releaser.remaining -=
			    values.expectedSize(); // its the responsibility of the caller to release after this point
			ASSERT(releaser.remaining >= 0);

			results.send(RangeResultWithVersion(values, tr.getReadVersion().get()));

			if (values.size() > 0)
				begin = firstGreaterThan(values.end()[-1].key);

			if (!values.more && !limits.isReached()) {
				if (terminator)
					results.sendError(end_of_stream());
				return Void();
			}
		} catch (Error& e) {
			if (e.code() == error_code_transaction_too_old) {
				// We are using this transaction until it's too old and then resetting to a fresh one,
				// so we don't need to delay.
				tr.fullReset();
			} else {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> readCommitted(Database cx,
                                 PromiseStream<RCGroup> results,
                                 Future<Void> active,
                                 Reference<FlowLock> lock,
                                 KeyRangeRef range,
                                 std::function<std::pair<uint64_t, uint32_t>(Key key)> groupBy,
                                 Terminator terminator,
                                 AccessSystemKeys systemAccess,
                                 LockAware lockAware) {
	state KeySelector nextKey = firstGreaterOrEqual(range.begin);
	state KeySelector end = firstGreaterOrEqual(range.end);

	state RCGroup rcGroup = RCGroup();
	state uint64_t skipGroup(ULLONG_MAX);
	state Transaction tr(cx);
	state FlowLock::Releaser releaser;

	loop {
		try {
			state GetRangeLimits limits(GetRangeLimits::ROW_LIMIT_UNLIMITED,
			                            (g_network->isSimulated() && !g_simulator.speedUpSimulation)
			                                ? CLIENT_KNOBS->BACKUP_SIMULATED_LIMIT_BYTES
			                                : CLIENT_KNOBS->BACKUP_GET_RANGE_LIMIT_BYTES);

			if (systemAccess)
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			if (lockAware)
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			state RangeResult rangevalue = wait(tr.getRange(nextKey, end, limits));

			// When this buggify line is enabled, if there are more than 1 result then use half of the results.
			// Copy the data instead of messing with the results directly to avoid TSS issues.
			if (rangevalue.size() > 1 && BUGGIFY) {
				RangeResult copy;
				// only copy first half of rangevalue into copy
				for (int i = 0; i < rangevalue.size() / 2; i++) {
					copy.push_back_deep(copy.arena(), rangevalue[i]);
				}
				rangevalue = copy;
				rangevalue.more = true;
				// Half of the time wait for this tr to expire so that the next read is at a different version
				if (deterministicRandom()->random01() < 0.5)
					wait(delay(6.0));
			}

			// add lock
			wait(active);
			releaser.release();
			wait(lock->take(TaskPriority::DefaultYield, rangevalue.expectedSize() + rcGroup.items.expectedSize()));
			releaser = FlowLock::Releaser(*lock, rangevalue.expectedSize() + rcGroup.items.expectedSize());

			for (auto& s : rangevalue) {
				uint64_t groupKey = groupBy(s.key).first;
				//TraceEvent("Log_ReadCommitted").detail("GroupKey", groupKey).detail("SkipGroup", skipGroup).detail("NextKey", nextKey.key).detail("End", end.key).detail("Valuesize", value.size()).detail("Index",index++).detail("Size",s.value.size());
				if (groupKey != skipGroup) {
					if (rcGroup.version == -1) {
						rcGroup.version = tr.getReadVersion().get();
						rcGroup.groupKey = groupKey;
					} else if (rcGroup.groupKey != groupKey) {
						//TraceEvent("Log_ReadCommitted").detail("SendGroup0", rcGroup.groupKey).detail("ItemSize", rcGroup.items.size()).detail("DataLength",rcGroup.items[0].value.size());
						// state uint32_t len(0);
						// for (size_t j = 0; j < rcGroup.items.size(); ++j) {
						//	len += rcGroup.items[j].value.size();
						//}
						//TraceEvent("SendGroup").detail("GroupKey", rcGroup.groupKey).detail("Version", rcGroup.version).detail("Length", len).detail("Releaser.remaining", releaser.remaining);
						releaser.remaining -=
						    rcGroup.items
						        .expectedSize(); // its the responsibility of the caller to release after this point
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
				if (rcGroup.version != -1) {
					releaser.remaining -=
					    rcGroup.items
					        .expectedSize(); // its the responsibility of the caller to release after this point
					ASSERT(releaser.remaining >= 0);
					//TraceEvent("Log_ReadCommitted").detail("SendGroup1", rcGroup.groupKey).detail("ItemSize", rcGroup.items.size()).detail("DataLength", rcGroup.items[0].value.size());
					results.send(rcGroup);
				}

				if (terminator)
					results.sendError(end_of_stream());
				return Void();
			}

			nextKey = firstGreaterThan(rangevalue.end()[-1].key);
		} catch (Error& e) {
			if (e.code() == error_code_transaction_too_old) {
				// We are using this transaction until it's too old and then resetting to a fresh one,
				// so we don't need to delay.
				tr.fullReset();
			} else {
				wait(tr.onError(e));
			}
		}
	}
}

Future<Void> readCommitted(Database cx,
                           PromiseStream<RCGroup> results,
                           Reference<FlowLock> lock,
                           KeyRangeRef range,
                           std::function<std::pair<uint64_t, uint32_t>(Key key)> groupBy) {
	return readCommitted(
	    cx, results, Void(), lock, range, groupBy, Terminator::True, AccessSystemKeys::True, LockAware::True);
}

ACTOR Future<int> dumpData(Database cx,
                           PromiseStream<RCGroup> results,
                           Reference<FlowLock> lock,
                           Key uid,
                           Key addPrefix,
                           Key removePrefix,
                           RequestStream<CommitTransactionRequest> commit,
                           NotifiedVersion* committedVersion,
                           Optional<Version> endVersion,
                           Key rangeBegin,
                           PromiseStream<Future<Void>> addActor,
                           FlowLock* commitLock,
                           Reference<KeyRangeMap<Version>> keyVersion) {
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
				for (int i = 0; i < group.items.size(); ++i) {
					bw.serializeBytes(group.items[i].value);
				}
				decodeBackupLogValue(req.arena,
				                     req.transaction.mutations,
				                     mutationSize,
				                     bw.toValue(),
				                     addPrefix,
				                     removePrefix,
				                     group.groupKey,
				                     keyVersion);
				newBeginVersion = group.groupKey + 1;
				if (mutationSize >= CLIENT_KNOBS->BACKUP_LOG_WRITE_BATCH_MAX_SIZE) {
					break;
				}
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream) {
					if (endVersion.present() && endVersion.get() > lastVersion && endVersion.get() > newBeginVersion) {
						newBeginVersion = endVersion.get();
					}
					if (newBeginVersion == invalidVersion)
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
		req.flags = req.flags | CommitTransactionRequest::FLAG_IS_LOCK_AWARE;

		totalBytes += mutationSize;
		wait(commitLock->take(TaskPriority::DefaultYield, mutationSize));
		addActor.send(commitLock->releaseWhen(success(commit.getReply(req)), mutationSize));

		if (endOfStream) {
			return totalBytes;
		}
	}
}

ACTOR Future<Void> coalesceKeyVersionCache(Key uid,
                                           Version endVersion,
                                           Reference<KeyRangeMap<Version>> keyVersion,
                                           RequestStream<CommitTransactionRequest> commit,
                                           NotifiedVersion* committedVersion,
                                           PromiseStream<Future<Void>> addActor,
                                           FlowLock* commitLock) {
	Version lastVersion = -1000;
	int64_t removed = 0;
	state CommitTransactionRequest req;
	state int64_t mutationSize = 0;
	Key mapPrefix = uid.withPrefix(applyMutationsKeyVersionMapRange.begin);

	for (auto it : keyVersion->ranges()) {
		if (lastVersion == -1000) {
			lastVersion = it.value();
		} else {
			Version ver = it.value();
			if (ver < endVersion && lastVersion < endVersion && ver != invalidVersion &&
			    lastVersion != invalidVersion) {
				Key removeKey = it.range().begin.withPrefix(mapPrefix);
				Key removeEnd = keyAfter(removeKey);
				req.transaction.mutations.push_back_deep(req.arena,
				                                         MutationRef(MutationRef::ClearRange, removeKey, removeEnd));
				mutationSize += removeKey.size() + removeEnd.size();
				removed--;
			} else {
				lastVersion = ver;
			}
		}
	}

	if (removed != 0) {
		Key countKey = uid.withPrefix(applyMutationsKeyVersionCountRange.begin);
		req.transaction.write_conflict_ranges.push_back_deep(req.arena, singleKeyRange(countKey));
		req.transaction.mutations.push_back_deep(
		    req.arena, MutationRef(MutationRef::AddValue, countKey, StringRef((uint8_t*)&removed, 8)));
		req.transaction.read_snapshot = committedVersion->get();
		req.flags = req.flags | CommitTransactionRequest::FLAG_IS_LOCK_AWARE;

		wait(commitLock->take(TaskPriority::DefaultYield, mutationSize));
		addActor.send(commitLock->releaseWhen(success(commit.getReply(req)), mutationSize));
	}

	return Void();
}

ACTOR Future<Void> applyMutations(Database cx,
                                  Key uid,
                                  Key addPrefix,
                                  Key removePrefix,
                                  Version beginVersion,
                                  Version* endVersion,
                                  RequestStream<CommitTransactionRequest> commit,
                                  NotifiedVersion* committedVersion,
                                  Reference<KeyRangeMap<Version>> keyVersion) {
	state FlowLock commitLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection(addActor.getFuture());
	state int maxBytes = CLIENT_KNOBS->APPLY_MIN_LOCK_BYTES;

	keyVersion->insert(metadataVersionKey, 0);

	try {
		loop {
			if (beginVersion >= *endVersion) {
				wait(commitLock.take(TaskPriority::DefaultYield, CLIENT_KNOBS->BACKUP_LOCK_BYTES));
				commitLock.release(CLIENT_KNOBS->BACKUP_LOCK_BYTES);
				if (beginVersion >= *endVersion) {
					return Void();
				}
			}

			int rangeCount = std::max(1, CLIENT_KNOBS->APPLY_MAX_LOCK_BYTES / maxBytes);
			state Version newEndVersion = std::min(*endVersion,
			                                       ((beginVersion / CLIENT_KNOBS->APPLY_BLOCK_SIZE) + rangeCount) *
			                                           CLIENT_KNOBS->APPLY_BLOCK_SIZE);
			state Standalone<VectorRef<KeyRangeRef>> ranges = getApplyRanges(beginVersion, newEndVersion, uid);
			state size_t idx;
			state std::vector<PromiseStream<RCGroup>> results;
			state std::vector<Future<Void>> rc;
			state std::vector<Reference<FlowLock>> locks;

			for (int i = 0; i < ranges.size(); ++i) {
				results.push_back(PromiseStream<RCGroup>());
				locks.push_back(makeReference<FlowLock>(
				    std::max(CLIENT_KNOBS->APPLY_MAX_LOCK_BYTES / ranges.size(), CLIENT_KNOBS->APPLY_MIN_LOCK_BYTES)));
				rc.push_back(readCommitted(cx, results[i], locks[i], ranges[i], decodeBKMutationLogKey));
			}

			maxBytes = std::max<int>(maxBytes * CLIENT_KNOBS->APPLY_MAX_DECAY_RATE, CLIENT_KNOBS->APPLY_MIN_LOCK_BYTES);
			for (idx = 0; idx < ranges.size(); ++idx) {
				int bytes = wait(dumpData(cx,
				                          results[idx],
				                          locks[idx],
				                          uid,
				                          addPrefix,
				                          removePrefix,
				                          commit,
				                          committedVersion,
				                          idx == ranges.size() - 1 ? newEndVersion : Optional<Version>(),
				                          ranges[idx].begin,
				                          addActor,
				                          &commitLock,
				                          keyVersion));
				maxBytes = std::max<int>(CLIENT_KNOBS->APPLY_MAX_INCREASE_FACTOR * bytes, maxBytes);
				if (error.isError())
					throw error.getError();
			}

			wait(coalesceKeyVersionCache(
			    uid, newEndVersion, keyVersion, commit, committedVersion, addActor, &commitLock));
			beginVersion = newEndVersion;
			if (BUGGIFY) {
				wait(delay(2.0));
			}
		}
	} catch (Error& e) {
		TraceEvent(e.code() == error_code_restore_missing_data ? SevWarnAlways : SevError, "ApplyMutationsError")
		    .error(e);
		throw;
	}
}

ACTOR static Future<Void> _eraseLogData(Reference<ReadYourWritesTransaction> tr,
                                        Key logUidValue,
                                        Key destUidValue,
                                        Optional<Version> endVersion,
                                        CheckBackupUID checkBackupUid,
                                        Version backupUid) {
	state Key backupLatestVersionsPath = destUidValue.withPrefix(backupLatestVersionsPrefix);
	state Key backupLatestVersionsKey = logUidValue.withPrefix(backupLatestVersionsPath);

	if (!destUidValue.size()) {
		return Void();
	}

	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

	if (checkBackupUid) {
		Subspace sourceStates =
		    Subspace(databaseBackupPrefixRange.begin).get(BackupAgentBase::keySourceStates).get(logUidValue);
		Optional<Value> v = wait(tr->get(sourceStates.pack(DatabaseBackupAgent::keyFolderId)));
		if (v.present() && BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) > backupUid)
			return Void();
	}

	state RangeResult backupVersions = wait(
	    tr->getRange(KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath)), CLIENT_KNOBS->TOO_MANY));

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

	state Version currEndVersion = std::numeric_limits<Version>::max();
	if (endVersion.present()) {
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

	if (endVersion.present() || backupVersions.size() != 1 || BUGGIFY) {
		if (!endVersion.present()) {
			// Clear current backup version history
			tr->clear(backupLatestVersionsKey);
			if (backupVersions.size() == 1) {
				tr->clear(prefixRange(destUidValue.withPrefix(logRangesRange.begin)));
			}
		} else {
			// Update current backup latest version
			tr->set(backupLatestVersionsKey, BinaryWriter::toValue<Version>(currEndVersion, Unversioned()));
		}

		// Clear log ranges if needed
		if (clearLogRangesRequired) {
			if ((nextSmallestVersion - currBeginVersion) / CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE >=
			        std::numeric_limits<uint8_t>::max() ||
			    BUGGIFY) {
				Key baLogRangePrefix = destUidValue.withPrefix(backupLogKeys.begin);

				for (int h = 0; h <= std::numeric_limits<uint8_t>::max(); h++) {
					uint64_t bv = bigEndian64(Version(0));
					uint64_t ev = bigEndian64(nextSmallestVersion);
					uint8_t h1 = h;
					Key vblockPrefix = StringRef(&h1, sizeof(uint8_t)).withPrefix(baLogRangePrefix);
					tr->clear(KeyRangeRef(StringRef((uint8_t*)&bv, sizeof(uint64_t)).withPrefix(vblockPrefix),
					                      StringRef((uint8_t*)&ev, sizeof(uint64_t)).withPrefix(vblockPrefix)));
				}
			} else {
				Standalone<VectorRef<KeyRangeRef>> ranges =
				    getLogRanges(currBeginVersion, nextSmallestVersion, destUidValue);
				for (auto& range : ranges) {
					tr->clear(range);
				}
			}
		}
	} else {
		// Clear version history
		tr->clear(prefixRange(backupLatestVersionsPath));

		// Clear everything under blog/[destUid]
		tr->clear(prefixRange(destUidValue.withPrefix(backupLogKeys.begin)));

		// Disable committing mutations into blog
		tr->clear(prefixRange(destUidValue.withPrefix(logRangesRange.begin)));
	}

	if (!endVersion.present() && backupVersions.size() == 1) {
		RangeResult existingDestUidValues =
		    wait(tr->getRange(KeyRangeRef(destUidLookupPrefix, strinc(destUidLookupPrefix)), CLIENT_KNOBS->TOO_MANY));
		for (auto it : existingDestUidValues) {
			if (it.value == destUidValue) {
				tr->clear(it.key);
			}
		}
	}

	return Void();
}

Future<Void> eraseLogData(Reference<ReadYourWritesTransaction> tr,
                          Key logUidValue,
                          Key destUidValue,
                          Optional<Version> endVersion,
                          CheckBackupUID checkBackupUid,
                          Version backupUid) {
	return _eraseLogData(tr, logUidValue, destUidValue, endVersion, checkBackupUid, backupUid);
}

ACTOR Future<Void> cleanupLogMutations(Database cx, Value destUidValue, bool deleteData) {
	state Key backupLatestVersionsPath = destUidValue.withPrefix(backupLatestVersionsPrefix);

	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	state Optional<Key> removingLogUid;
	state std::set<Key> loggedLogUids;

	loop {
		try {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state RangeResult backupVersions = wait(tr->getRange(
			    KeyRangeRef(backupLatestVersionsPath, strinc(backupLatestVersionsPath)), CLIENT_KNOBS->TOO_MANY));
			state Version readVer = tr->getReadVersion().get();

			state Version minVersion = std::numeric_limits<Version>::max();
			state Key minVersionLogUid;

			state int backupIdx = 0;
			for (; backupIdx < backupVersions.size(); backupIdx++) {
				state Version currVersion =
				    BinaryReader::fromStringRef<Version>(backupVersions[backupIdx].value, Unversioned());
				state Key currLogUid =
				    backupVersions[backupIdx].key.removePrefix(backupLatestVersionsPrefix).removePrefix(destUidValue);
				if (currVersion < minVersion) {
					minVersionLogUid = currLogUid;
					minVersion = currVersion;
				}

				if (!loggedLogUids.count(currLogUid)) {
					state Future<Optional<Value>> foundDRKey = tr->get(Subspace(databaseBackupPrefixRange.begin)
					                                                       .get(BackupAgentBase::keySourceStates)
					                                                       .get(currLogUid)
					                                                       .pack(DatabaseBackupAgent::keyStateStatus));
					state Future<Optional<Value>> foundBackupKey =
					    tr->get(Subspace(currLogUid.withPrefix(LiteralStringRef("uid->config/"))
					                         .withPrefix(fileBackupPrefixRange.begin))
					                .pack(LiteralStringRef("stateEnum")));
					wait(success(foundDRKey) && success(foundBackupKey));

					if (foundDRKey.get().present() && foundBackupKey.get().present()) {
						printf("WARNING: Found a tag that looks like both a backup and a DR. This tag is %.4f hours "
						       "behind.\n",
						       (readVer - currVersion) / (3600.0 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
					} else if (foundDRKey.get().present() && !foundBackupKey.get().present()) {
						printf("Found a DR that is %.4f hours behind.\n",
						       (readVer - currVersion) / (3600.0 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
					} else if (!foundDRKey.get().present() && foundBackupKey.get().present()) {
						printf("Found a Backup that is %.4f hours behind.\n",
						       (readVer - currVersion) / (3600.0 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
					} else {
						printf("WARNING: Found an unknown tag that is %.4f hours behind.\n",
						       (readVer - currVersion) / (3600.0 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
					}
					loggedLogUids.insert(currLogUid);
				}
			}

			if (deleteData) {
				if (readVer - minVersion > CLIENT_KNOBS->MIN_CLEANUP_SECONDS * CLIENT_KNOBS->CORE_VERSIONSPERSECOND &&
				    (!removingLogUid.present() || minVersionLogUid == removingLogUid.get())) {
					removingLogUid = minVersionLogUid;
					wait(eraseLogData(tr, minVersionLogUid, destUidValue));
					wait(tr->commit());
					printf("\nSuccessfully removed the tag that was %.4f hours behind.\n\n",
					       (readVer - minVersion) / (3600.0 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
				} else if (removingLogUid.present() && minVersionLogUid != removingLogUid.get()) {
					printf("\nWARNING: The oldest tag was possibly removed, run again without `--delete-data' to "
					       "check.\n\n");
				} else {
					printf("\nWARNING: Did not delete data because the tag is not at least %.4f hours behind. Change "
					       "`--min-cleanup-seconds' to adjust this threshold.\n\n",
					       CLIENT_KNOBS->MIN_CLEANUP_SECONDS / 3600.0);
				}
			} else if (readVer - minVersion >
			           CLIENT_KNOBS->MIN_CLEANUP_SECONDS * CLIENT_KNOBS->CORE_VERSIONSPERSECOND) {
				printf("\nPassing `--delete-data' would delete the tag that is %.4f hours behind.\n\n",
				       (readVer - minVersion) / (3600.0 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
			} else {
				printf("\nPassing `--delete-data' would not delete the tag that is %.4f hours behind. Change "
				       "`--min-cleanup-seconds' to adjust the cleanup threshold.\n\n",
				       (readVer - minVersion) / (3600.0 * CLIENT_KNOBS->CORE_VERSIONSPERSECOND));
			}

			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> cleanupBackup(Database cx, DeleteData deleteData) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
	loop {
		try {
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state RangeResult destUids = wait(
			    tr->getRange(KeyRangeRef(destUidLookupPrefix, strinc(destUidLookupPrefix)), CLIENT_KNOBS->TOO_MANY));

			for (auto destUid : destUids) {
				wait(cleanupLogMutations(cx, destUid.value, deleteData));
			}
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Convert the status text to an enumerated value
BackupAgentBase::EnumState BackupAgentBase::getState(std::string const& stateText) {
	auto enState = EnumState::STATE_ERRORED;

	if (stateText.empty()) {
		enState = EnumState::STATE_NEVERRAN;
	}

	else if (!stateText.compare("has been submitted")) {
		enState = EnumState::STATE_SUBMITTED;
	}

	else if (!stateText.compare("has been started")) {
		enState = EnumState::STATE_RUNNING;
	}

	else if (!stateText.compare("is differential")) {
		enState = EnumState::STATE_RUNNING_DIFFERENTIAL;
	}

	else if (!stateText.compare("has been completed")) {
		enState = EnumState::STATE_COMPLETED;
	}

	else if (!stateText.compare("has been aborted")) {
		enState = EnumState::STATE_ABORTED;
	}

	else if (!stateText.compare("has been partially aborted")) {
		enState = EnumState::STATE_PARTIALLY_ABORTED;
	}

	return enState;
}

const char* BackupAgentBase::getStateText(EnumState enState) {
	const char* stateText;

	switch (enState) {
	case EnumState::STATE_ERRORED:
		stateText = "has errored";
		break;
	case EnumState::STATE_NEVERRAN:
		stateText = "has never been started";
		break;
	case EnumState::STATE_SUBMITTED:
		stateText = "has been submitted";
		break;
	case EnumState::STATE_RUNNING:
		stateText = "has been started";
		break;
	case EnumState::STATE_RUNNING_DIFFERENTIAL:
		stateText = "is differential";
		break;
	case EnumState::STATE_COMPLETED:
		stateText = "has been completed";
		break;
	case EnumState::STATE_ABORTED:
		stateText = "has been aborted";
		break;
	case EnumState::STATE_PARTIALLY_ABORTED:
		stateText = "has been partially aborted";
		break;
	default:
		stateText = "<undefined>";
		break;
	}

	return stateText;
}

const char* BackupAgentBase::getStateName(EnumState enState) {
	switch (enState) {
	case EnumState::STATE_ERRORED:
		return "Errored";
	case EnumState::STATE_NEVERRAN:
		return "NeverRan";
	case EnumState::STATE_SUBMITTED:
		return "Submitted";
		break;
	case EnumState::STATE_RUNNING:
		return "Running";
	case EnumState::STATE_RUNNING_DIFFERENTIAL:
		return "RunningDifferentially";
	case EnumState::STATE_COMPLETED:
		return "Completed";
	case EnumState::STATE_ABORTED:
		return "Aborted";
	case EnumState::STATE_PARTIALLY_ABORTED:
		return "Aborting";
	default:
		return "<undefined>";
	}
}

bool BackupAgentBase::isRunnable(EnumState enState) {
	switch (enState) {
	case EnumState::STATE_SUBMITTED:
	case EnumState::STATE_RUNNING:
	case EnumState::STATE_RUNNING_DIFFERENTIAL:
	case EnumState::STATE_PARTIALLY_ABORTED:
		return true;
	default:
		return false;
	}
}

Standalone<StringRef> BackupAgentBase::getCurrentTime() {
	double t = now();
	time_t curTime = t;
	char buffer[128];
	struct tm* timeinfo;
	timeinfo = localtime(&curTime);
	strftime(buffer, 128, "%Y-%m-%d-%H-%M-%S", timeinfo);

	std::string time(buffer);
	return StringRef(time + format(".%06d", (int)(1e6 * (t - curTime))));
}

std::string const BackupAgentBase::defaultTagName = "default";
