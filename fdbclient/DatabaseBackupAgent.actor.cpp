/*
 * DatabaseBackupAgent.actor.cpp
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

#include <iterator>
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/Status.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include <ctime>
#include <climits>
#include "fdbrpc/IAsyncFile.h"
#include "flow/genericactors.actor.h"
#include "flow/Hash3.h"
#include <numeric>
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/KeyBackedTypes.h"
#include <inttypes.h>
#include <map>

#include "flow/actorcompiler.h" // has to be last include

const Key DatabaseBackupAgent::keyAddPrefix = LiteralStringRef("add_prefix");
const Key DatabaseBackupAgent::keyRemovePrefix = LiteralStringRef("remove_prefix");
const Key DatabaseBackupAgent::keyRangeVersions = LiteralStringRef("range_versions");
const Key DatabaseBackupAgent::keyCopyStop = LiteralStringRef("copy_stop");
const Key DatabaseBackupAgent::keyDatabasesInSync = LiteralStringRef("databases_in_sync");
const int DatabaseBackupAgent::LATEST_DR_VERSION = 1;

DatabaseBackupAgent::DatabaseBackupAgent()
  : subspace(Subspace(databaseBackupPrefixRange.begin)), states(subspace.get(BackupAgentBase::keyStates)),
    config(subspace.get(BackupAgentBase::keyConfig)), errors(subspace.get(BackupAgentBase::keyErrors)),
    ranges(subspace.get(BackupAgentBase::keyRanges)), tagNames(subspace.get(BackupAgentBase::keyTagName)),
    sourceStates(subspace.get(BackupAgentBase::keySourceStates)),
    sourceTagNames(subspace.get(BackupAgentBase::keyTagName)),
    taskBucket(new TaskBucket(subspace.get(BackupAgentBase::keyTasks),
                              AccessSystemKeys::True,
                              PriorityBatch::False,
                              LockAware::True)),
    futureBucket(new FutureBucket(subspace.get(BackupAgentBase::keyFutures), AccessSystemKeys::True, LockAware::True)) {
}

DatabaseBackupAgent::DatabaseBackupAgent(Database src)
  : subspace(Subspace(databaseBackupPrefixRange.begin)), states(subspace.get(BackupAgentBase::keyStates)),
    config(subspace.get(BackupAgentBase::keyConfig)), errors(subspace.get(BackupAgentBase::keyErrors)),
    ranges(subspace.get(BackupAgentBase::keyRanges)), tagNames(subspace.get(BackupAgentBase::keyTagName)),
    sourceStates(subspace.get(BackupAgentBase::keySourceStates)),
    sourceTagNames(subspace.get(BackupAgentBase::keyTagName)),
    taskBucket(new TaskBucket(subspace.get(BackupAgentBase::keyTasks),
                              AccessSystemKeys::True,
                              PriorityBatch::False,
                              LockAware::True)),
    futureBucket(new FutureBucket(subspace.get(BackupAgentBase::keyFutures), AccessSystemKeys::True, LockAware::True)) {
	taskBucket->src = src;
}

// Any new per-DR properties should go here.
class DRConfig {
public:
	DRConfig(UID uid = UID())
	  : uid(uid),
	    configSpace(uidPrefixKey(LiteralStringRef("uid->config/").withPrefix(databaseBackupPrefixRange.begin), uid)) {}
	DRConfig(Reference<Task> task)
	  : DRConfig(BinaryReader::fromStringRef<UID>(task->params[BackupAgentBase::keyConfigLogUid], Unversioned())) {}

	KeyBackedBinaryValue<int64_t> rangeBytesWritten() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }

	KeyBackedBinaryValue<int64_t> logBytesWritten() { return configSpace.pack(LiteralStringRef(__FUNCTION__)); }

	void clear(Reference<ReadYourWritesTransaction> tr) { tr->clear(configSpace.range()); }

	UID getUid() { return uid; }

private:
	UID uid;
	Subspace configSpace;
};

namespace dbBackup {

bool copyDefaultParameters(Reference<Task> source, Reference<Task> dest) {
	if (source) {
		copyParameter(source, dest, BackupAgentBase::keyFolderId);
		copyParameter(source, dest, BackupAgentBase::keyConfigLogUid);
		copyParameter(source, dest, BackupAgentBase::destUid);

		copyParameter(source, dest, DatabaseBackupAgent::keyAddPrefix);
		copyParameter(source, dest, DatabaseBackupAgent::keyRemovePrefix);
		return true;
	}

	return false;
}

ACTOR template <class Tr>
Future<Void> checkTaskVersion(Tr tr, Reference<Task> task, StringRef name, uint32_t version) {
	uint32_t taskVersion = task->getVersion();
	if (taskVersion > version) {
		TraceEvent(SevError, "BA_BackupRangeTaskFuncExecute")
		    .detail("TaskVersion", taskVersion)
		    .detail("Name", name)
		    .detail("Version", version);
		wait(logError(tr,
		              Subspace(databaseBackupPrefixRange.begin)
		                  .get(BackupAgentBase::keyErrors)
		                  .pack(task->params[BackupAgentBase::keyConfigLogUid]),
		              format("ERROR: %s task version `%lu' is greater than supported version `%lu'",
		                     task->params[Task::reservedTaskParamKeyType].toString().c_str(),
		                     (unsigned long)taskVersion,
		                     (unsigned long)version)));

		throw task_invalid_version();
	}

	return Void();
}

struct BackupRangeTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<int64_t> bytesWritten() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	static const Key keyAddBackupRangeTasks;
	static const Key keyBackupRangeBeginKey;

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

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 Key begin,
	                                 Key end,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(BackupRangeTaskFunc::name, BackupRangeTaskFunc::version, doneKey);

		copyDefaultParameters(parentTask, task);

		task->params[BackupAgentBase::keyBeginKey] = begin;
		task->params[BackupAgentBase::keyEndKey] = end;

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
		return LiteralStringRef("OnSetAddTask");
	}

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));
		state Subspace conf = Subspace(databaseBackupPrefixRange.begin)
		                          .get(BackupAgentBase::keyConfig)
		                          .get(task->params[BackupAgentBase::keyConfigLogUid]);

		wait(checkTaskVersion(cx, task, BackupRangeTaskFunc::name, BackupRangeTaskFunc::version));
		// Find out if there is a shard boundary in(beginKey, endKey)
		Standalone<VectorRef<KeyRef>> keys =
		    wait(runRYWTransaction(taskBucket->src, [=](Reference<ReadYourWritesTransaction> tr) {
			    return getBlockOfShards(tr,
			                            task->params[DatabaseBackupAgent::keyBeginKey],
			                            task->params[DatabaseBackupAgent::keyEndKey],
			                            CLIENT_KNOBS->BACKUP_SHARD_TASK_LIMIT);
		    }));
		if (keys.size() > 0) {
			task->params[BackupRangeTaskFunc::keyAddBackupRangeTasks] = BinaryWriter::toValue(keys, IncludeVersion());
			return Void();
		}

		// Read everything from beginKey to endKey, write it to an output file, run the output file processor, and
		// then set on_done.If we are still writing after X seconds, end the output file and insert a new backup_range
		// task for the remainder.
		state double timeout = now() + CLIENT_KNOBS->BACKUP_RANGE_TIMEOUT;
		state Key addPrefix = task->params[DatabaseBackupAgent::keyAddPrefix];
		state Key removePrefix = task->params[DatabaseBackupAgent::keyRemovePrefix];

		state KeyRange range(
		    KeyRangeRef(task->params[BackupAgentBase::keyBeginKey], task->params[BackupAgentBase::keyEndKey]));

		// retrieve kvData
		state PromiseStream<RangeResultWithVersion> results;

		state Future<Void> rc = readCommitted(
		    taskBucket->src, results, lock, range, Terminator::True, AccessSystemKeys::True, LockAware::True);
		state Key rangeBegin = range.begin;
		state Key rangeEnd;
		state bool endOfStream = false;
		state RangeResultWithVersion nextValues;
		state int64_t nextValuesSize = 0;
		nextValues.second = invalidVersion;
		loop {
			if (endOfStream && nextValues.second == invalidVersion) {
				return Void();
			}
			state RangeResultWithVersion values = std::move(nextValues);
			state int64_t valuesSize = nextValuesSize;
			nextValues = RangeResultWithVersion();
			nextValues.second = invalidVersion;
			nextValuesSize = 0;

			if (!endOfStream) {
				loop {
					try {
						RangeResultWithVersion v = waitNext(results.getFuture());
						int64_t resultSize = v.first.expectedSize();
						lock->release(resultSize);

						if (values.second == invalidVersion) {
							values = v;
						} else if ((values.second != v.second) ||
						           (valuesSize > 0 && resultSize > 0 &&
						            valuesSize + resultSize > CLIENT_KNOBS->BACKUP_LOG_WRITE_BATCH_MAX_SIZE)) {
							nextValues = v;
							nextValuesSize = resultSize;
							break;
						} else {
							values.first.append_deep(values.first.arena(), v.first.begin(), v.first.size());
							values.first.more = v.first.more;
						}

						valuesSize += resultSize;
					} catch (Error& e) {
						state Error err = e;
						if (err.code() == error_code_actor_cancelled)
							throw err;

						if (err.code() == error_code_end_of_stream) {
							endOfStream = true;
							if (values.second != invalidVersion)
								break;
							return Void();
						}

						wait(logError(cx,
						              Subspace(databaseBackupPrefixRange.begin)
						                  .get(BackupAgentBase::keyErrors)
						                  .pack(task->params[BackupAgentBase::keyConfigLogUid]),
						              format("ERROR: %s", err.what())));

						throw err;
					}
				}
			}

			if (now() >= timeout) {
				task->params[BackupRangeTaskFunc::keyBackupRangeBeginKey] = rangeBegin;
				return Void();
			}

			rangeEnd = values.first.more ? keyAfter(values.first.end()[-1].key) : range.end;

			state int valueLoc = 0;
			state int committedValueLoc = 0;
			state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
			loop {
				try {
					tr->reset();
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					state Key prefix = task->params[BackupAgentBase::keyConfigLogUid].withPrefix(
					    applyMutationsKeyVersionMapRange.begin);
					state Key rangeCountKey = task->params[BackupAgentBase::keyConfigLogUid].withPrefix(
					    applyMutationsKeyVersionCountRange.begin);
					state Future<RangeResult> backupVersions =
					    krmGetRanges(tr, prefix, KeyRangeRef(rangeBegin, rangeEnd), BUGGIFY ? 2 : 2000, 1e5);
					state Future<Optional<Value>> logVersionValue =
					    tr->get(task->params[BackupAgentBase::keyConfigLogUid].withPrefix(applyMutationsEndRange.begin),
					            Snapshot::True);
					state Future<Optional<Value>> rangeCountValue = tr->get(rangeCountKey, Snapshot::True);
					state Future<RangeResult> prevRange = tr->getRange(firstGreaterOrEqual(prefix),
					                                                   lastLessOrEqual(rangeBegin.withPrefix(prefix)),
					                                                   1,
					                                                   Snapshot::True,
					                                                   Reverse::True);
					state Future<RangeResult> nextRange = tr->getRange(firstGreaterOrEqual(rangeEnd.withPrefix(prefix)),
					                                                   firstGreaterOrEqual(strinc(prefix)),
					                                                   1,
					                                                   Snapshot::True,
					                                                   Reverse::False);
					state Future<Void> verified = taskBucket->keepRunning(tr, task);

					wait(checkDatabaseLock(tr,
					                       BinaryReader::fromStringRef<UID>(
					                           task->params[BackupAgentBase::keyConfigLogUid], Unversioned())));
					wait(success(backupVersions) && success(logVersionValue) && success(rangeCountValue) &&
					     success(prevRange) && success(nextRange) && success(verified));

					int64_t rangeCount = 0;
					if (rangeCountValue.get().present()) {
						ASSERT(rangeCountValue.get().get().size() == sizeof(int64_t));
						memcpy(&rangeCount, rangeCountValue.get().get().begin(), rangeCountValue.get().get().size());
					}

					bool prevAdjacent =
					    prevRange.get().size() && prevRange.get()[0].value.size() &&
					    BinaryReader::fromStringRef<Version>(prevRange.get()[0].value, Unversioned()) != invalidVersion;
					bool nextAdjacent =
					    nextRange.get().size() && nextRange.get()[0].value.size() &&
					    BinaryReader::fromStringRef<Version>(nextRange.get()[0].value, Unversioned()) != invalidVersion;

					if ((!prevAdjacent || !nextAdjacent) &&
					    rangeCount > ((prevAdjacent || nextAdjacent) ? CLIENT_KNOBS->BACKUP_MAP_KEY_UPPER_LIMIT
					                                                 : CLIENT_KNOBS->BACKUP_MAP_KEY_LOWER_LIMIT)) {
						TEST(true); // range insert delayed because too versionMap is too large

						if (rangeCount > CLIENT_KNOBS->BACKUP_MAP_KEY_UPPER_LIMIT)
							TraceEvent(SevWarnAlways, "DBA_KeyRangeMapTooLarge").log();

						wait(delay(1));
						task->params[BackupRangeTaskFunc::keyBackupRangeBeginKey] = rangeBegin;
						return Void();
					}

					Version logVersion =
					    logVersionValue.get().present()
					        ? BinaryReader::fromStringRef<Version>(logVersionValue.get().get(), Unversioned())
					        : ::invalidVersion;
					if (logVersion >= values.second) {
						task->params[BackupRangeTaskFunc::keyBackupRangeBeginKey] = rangeBegin;
						return Void();
					}

					//TraceEvent("DBA_Range").detail("Range", KeyRangeRef(rangeBegin, rangeEnd)).detail("Version", values.second).detail("Size", values.first.size()).detail("LogUID", task->params[BackupAgentBase::keyConfigLogUid]).detail("AddPrefix", addPrefix).detail("RemovePrefix", removePrefix);

					Subspace krv(conf.get(DatabaseBackupAgent::keyRangeVersions));
					state KeyRange versionRange = singleKeyRange(krv.pack(values.second));
					tr->addReadConflictRange(versionRange);
					tr->addWriteConflictRange(versionRange);

					int versionLoc = 0;
					std::vector<Future<Void>> setRanges;
					state int64_t bytesSet = 0;

					loop {
						while (versionLoc < backupVersions.get().size() - 1 &&
						       (backupVersions.get()[versionLoc].value.size() < sizeof(Version) ||
						        BinaryReader::fromStringRef<Version>(backupVersions.get()[versionLoc].value,
						                                             Unversioned()) != invalidVersion)) {
							versionLoc++;
						}

						if (versionLoc == backupVersions.get().size() - 1)
							break;

						if (backupVersions.get()[versionLoc + 1].key ==
						    (removePrefix == StringRef() ? normalKeys.end : strinc(removePrefix))) {
							tr->clear(KeyRangeRef(
							    backupVersions.get()[versionLoc].key.removePrefix(removePrefix).withPrefix(addPrefix),
							    addPrefix == StringRef() ? normalKeys.end : strinc(addPrefix)));
						} else {
							tr->clear(KeyRangeRef(backupVersions.get()[versionLoc].key,
							                      backupVersions.get()[versionLoc + 1].key)
							              .removePrefix(removePrefix)
							              .withPrefix(addPrefix));
						}

						setRanges.push_back(krmSetRange(
						    tr,
						    prefix,
						    KeyRangeRef(backupVersions.get()[versionLoc].key, backupVersions.get()[versionLoc + 1].key),
						    BinaryWriter::toValue(values.second, Unversioned())));
						int64_t added = 1;
						tr->atomicOp(rangeCountKey, StringRef((uint8_t*)&added, 8), MutationRef::AddValue);

						for (; valueLoc < values.first.size(); ++valueLoc) {
							if (values.first[valueLoc].key >= backupVersions.get()[versionLoc + 1].key)
								break;

							if (values.first[valueLoc].key >= backupVersions.get()[versionLoc].key) {
								//TraceEvent("DBA_Set", debugID).detail("Key", values.first[valueLoc].key).detail("Value", values.first[valueLoc].value);
								tr->set(values.first[valueLoc].key.removePrefix(removePrefix).withPrefix(addPrefix),
								        values.first[valueLoc].value);
								bytesSet += values.first[valueLoc].expectedSize() - removePrefix.expectedSize() +
								            addPrefix.expectedSize();
							}
						}

						versionLoc++;
					}

					wait(waitForAll(setRanges));

					wait(tr->commit());
					Params.bytesWritten().set(task, Params.bytesWritten().getOrDefault(task) + bytesSet);
					//TraceEvent("DBA_SetComplete", debugID).detail("Ver", values.second).detail("LogVersion", logVersion).detail("ReadVersion", readVer).detail("CommitVer", tr.getCommittedVersion()).detail("Range", versionRange);

					if (backupVersions.get().more) {
						tr->reset();
						committedValueLoc = valueLoc;
						rangeBegin = backupVersions.get().end()[-1].key;
					} else {
						break;
					}
				} catch (Error& e) {
					wait(tr->onError(e));
					valueLoc = committedValueLoc;
				}
			}

			rangeBegin = rangeEnd;
		}
	}

	ACTOR static Future<Void> startBackupRangeInternal(Reference<ReadYourWritesTransaction> tr,
	                                                   Standalone<VectorRef<KeyRef>> keys,
	                                                   Reference<TaskBucket> taskBucket,
	                                                   Reference<FutureBucket> futureBucket,
	                                                   Reference<Task> task,
	                                                   Reference<TaskFuture> onDone) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Key nextKey = task->params[BackupAgentBase::keyBeginKey];

		std::vector<Future<Key>> addTaskVector;
		for (int idx = 0; idx < keys.size(); ++idx) {
			if (nextKey != keys[idx]) {
				addTaskVector.push_back(
				    addTask(tr, taskBucket, task, nextKey, keys[idx], TaskCompletionKey::joinWith(onDone)));
			}
			nextKey = keys[idx];
		}

		if (nextKey != task->params[BackupAgentBase::keyEndKey]) {
			addTaskVector.push_back(addTask(tr,
			                                taskBucket,
			                                task,
			                                nextKey,
			                                task->params[BackupAgentBase::keyEndKey],
			                                TaskCompletionKey::joinWith(onDone)));
		}

		wait(waitForAll(addTaskVector));

		return Void();
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		// Get the bytesWritten parameter from task and atomically add it to the rangeBytesWritten() property of the DR
		// config.
		DRConfig config(task);
		int64_t bytesWritten = Params.bytesWritten().getOrDefault(task);
		config.rangeBytesWritten().atomicOp(tr, bytesWritten, MutationRef::AddValue);

		if (task->params.find(BackupRangeTaskFunc::keyAddBackupRangeTasks) != task->params.end()) {
			wait(startBackupRangeInternal(
			         tr,
			         BinaryReader::fromStringRef<Standalone<VectorRef<KeyRef>>>(
			             task->params[BackupRangeTaskFunc::keyAddBackupRangeTasks], IncludeVersion()),
			         taskBucket,
			         futureBucket,
			         task,
			         taskFuture) &&
			     taskBucket->finish(tr, task));
		} else if (task->params.find(BackupRangeTaskFunc::keyBackupRangeBeginKey) != task->params.end() &&
		           task->params[BackupRangeTaskFunc::keyBackupRangeBeginKey] <
		               task->params[BackupAgentBase::keyEndKey]) {
			ASSERT(taskFuture->key.size() > 0);
			wait(success(BackupRangeTaskFunc::addTask(tr,
			                                          taskBucket,
			                                          task,
			                                          task->params[BackupRangeTaskFunc::keyBackupRangeBeginKey],
			                                          task->params[BackupAgentBase::keyEndKey],
			                                          TaskCompletionKey::signal(taskFuture->key))) &&
			     taskBucket->finish(tr, task));
		} else {
			wait(taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task));
		}

		return Void();
	}
};
StringRef BackupRangeTaskFunc::name = LiteralStringRef("dr_backup_range");
const Key BackupRangeTaskFunc::keyAddBackupRangeTasks = LiteralStringRef("addBackupRangeTasks");
const Key BackupRangeTaskFunc::keyBackupRangeBeginKey = LiteralStringRef("backupRangeBeginKey");
REGISTER_TASKFUNC(BackupRangeTaskFunc);

struct FinishFullBackupTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Subspace states = Subspace(databaseBackupPrefixRange.begin)
		                            .get(BackupAgentBase::keyStates)
		                            .get(task->params[BackupAgentBase::keyConfigLogUid]);
		wait(checkTaskVersion(tr, task, FinishFullBackupTaskFunc::name, FinishFullBackupTaskFunc::version));

		// Enable the stop key
		Transaction srcTr(taskBucket->src);
		srcTr.setOption(FDBTransactionOptions::LOCK_AWARE);
		Version readVersion = wait(srcTr.getReadVersion());
		tr->set(states.pack(DatabaseBackupAgent::keyCopyStop), BinaryWriter::toValue(readVersion, Unversioned()));
		TraceEvent("DBA_FinishFullBackup").detail("CopyStop", readVersion);
		wait(taskBucket->finish(tr, task));

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		// After the BackupRangeTask completes, set the stop key which will stop the BackupLogsTask
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(FinishFullBackupTaskFunc::name, FinishFullBackupTaskFunc::version, doneKey);

		copyDefaultParameters(parentTask, task);

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
		return LiteralStringRef("OnSetAddTask");
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
StringRef FinishFullBackupTaskFunc::name = LiteralStringRef("dr_finish_full_backup");
REGISTER_TASKFUNC(FinishFullBackupTaskFunc);

struct EraseLogRangeTaskFunc : TaskFuncBase {
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

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state FlowLock lock(CLIENT_KNOBS->BACKUP_LOCK_BYTES);

		wait(checkTaskVersion(cx, task, EraseLogRangeTaskFunc::name, EraseLogRangeTaskFunc::version));

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(taskBucket->src));
		loop {
			try {
				Version endVersion = BinaryReader::fromStringRef<Version>(
				    task->params[DatabaseBackupAgent::keyEndVersion], Unversioned());
				wait(eraseLogData(
				    tr,
				    task->params[BackupAgentBase::keyConfigLogUid],
				    task->params[BackupAgentBase::destUid],
				    Optional<Version>(endVersion),
				    CheckBackupUID::True,
				    BinaryReader::fromStringRef<Version>(task->params[BackupAgentBase::keyFolderId], Unversioned())));
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 Version endVersion,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(EraseLogRangeTaskFunc::name, EraseLogRangeTaskFunc::version, doneKey, 1);

		copyDefaultParameters(parentTask, task);

		task->params[DatabaseBackupAgent::keyBeginVersion] =
		    BinaryWriter::toValue(1, Unversioned()); // FIXME: remove in 6.X, only needed for 5.2 backward compatibility
		task->params[DatabaseBackupAgent::keyEndVersion] = BinaryWriter::toValue(endVersion, Unversioned());

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
		return LiteralStringRef("OnSetAddTask");
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {

		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		wait(taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task));
		return Void();
	}
};
StringRef EraseLogRangeTaskFunc::name = LiteralStringRef("dr_erase_log_range");
REGISTER_TASKFUNC(EraseLogRangeTaskFunc);

struct CopyLogRangeTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<int64_t> bytesWritten() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	static const Key keyNextBeginVersion;

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

	// store mutation data from results until the end of stream or the timeout. If breaks on timeout returns the first
	// uncopied version
	ACTOR static Future<Optional<Version>> dumpData(Database cx,
	                                                Reference<Task> task,
	                                                PromiseStream<RCGroup> results,
	                                                FlowLock* lock,
	                                                Reference<TaskBucket> tb,
	                                                double breakTime) {
		state bool endOfStream = false;
		state Subspace conf = Subspace(databaseBackupPrefixRange.begin)
		                          .get(BackupAgentBase::keyConfig)
		                          .get(task->params[BackupAgentBase::keyConfigLogUid]);
		state std::vector<RangeResult> nextMutations;
		state bool isTimeoutOccured = false;
		state Optional<KeyRef> lastKey;
		state Version lastVersion;
		state int64_t nextMutationSize = 0;
		loop {
			try {
				if (endOfStream && !nextMutationSize) {
					return Optional<Version>();
				}

				state std::vector<RangeResult> mutations = std::move(nextMutations);
				state int64_t mutationSize = nextMutationSize;
				nextMutations = std::vector<RangeResult>();
				nextMutationSize = 0;

				if (!endOfStream) {
					loop {
						try {
							RCGroup group = waitNext(results.getFuture());
							lock->release(group.items.expectedSize());

							int vecSize = group.items.expectedSize();
							if (mutationSize + vecSize >= CLIENT_KNOBS->BACKUP_LOG_WRITE_BATCH_MAX_SIZE) {

								nextMutations.push_back(group.items);
								nextMutationSize = vecSize;
								break;
							}

							mutations.push_back(group.items);
							mutationSize += vecSize;
						} catch (Error& e) {
							state Error error = e;
							if (e.code() == error_code_end_of_stream) {
								endOfStream = true;
								break;
							}

							throw error;
						}
					}
				}

				state Optional<Version> nextVersionAfterBreak;
				state Transaction tr(cx);

				loop {
					try {
						tr.setOption(FDBTransactionOptions::LOCK_AWARE);
						tr.trState->options.sizeLimit = 2 * CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT;
						wait(checkDatabaseLock(&tr,
						                       BinaryReader::fromStringRef<UID>(
						                           task->params[BackupAgentBase::keyConfigLogUid], Unversioned())));
						state int64_t bytesSet = 0;

						bool first = true;
						for (auto m : mutations) {
							for (auto kv : m) {
								if (isTimeoutOccured) {
									Version newVersion = getLogKeyVersion(kv.key);

									if (newVersion > lastVersion) {
										nextVersionAfterBreak = newVersion;
										break;
									}
								}
								if (first) {
									tr.addReadConflictRange(singleKeyRange(kv.key));
									first = false;
								}
								tr.set(kv.key.removePrefix(backupLogKeys.begin)
								           .removePrefix(task->params[BackupAgentBase::destUid])
								           .withPrefix(task->params[BackupAgentBase::keyConfigLogUid])
								           .withPrefix(applyLogKeys.begin),
								       kv.value);
								bytesSet += kv.expectedSize() - backupLogKeys.begin.expectedSize() +
								            applyLogKeys.begin.expectedSize();
								lastKey = kv.key;
							}
						}

						wait(tr.commit());
						Params.bytesWritten().set(task, Params.bytesWritten().getOrDefault(task) + bytesSet);
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}
				if (nextVersionAfterBreak.present()) {
					return nextVersionAfterBreak;
				}
				if (!isTimeoutOccured && timer_monotonic() >= breakTime && lastKey.present()) {
					// timeout occured
					// continue to copy mutations with the
					// same version before break because
					// the next run should start from the beginning of a version > lastVersion.
					lastVersion = getLogKeyVersion(lastKey.get());
					isTimeoutOccured = true;
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled || e.code() == error_code_backup_error)
					throw e;

				state Error err = e;
				wait(logError(cx,
				              Subspace(databaseBackupPrefixRange.begin)
				                  .get(BackupAgentBase::keyErrors)
				                  .pack(task->params[BackupAgentBase::keyConfigLogUid]),
				              format("ERROR: Failed to dump mutations because of error %s", err.what())));

				throw err;
			}
		}
	}

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		// state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

		wait(checkTaskVersion(cx, task, CopyLogRangeTaskFunc::name, CopyLogRangeTaskFunc::version));

		state Version beginVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyBeginVersion], Unversioned());
		state Version endVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyEndVersion], Unversioned());

		Version newEndVersion = std::min(endVersion,
		                                 (((beginVersion - 1) / CLIENT_KNOBS->COPY_LOG_BLOCK_SIZE) + 1 +
		                                  CLIENT_KNOBS->COPY_LOG_BLOCKS_PER_TASK +
		                                  (g_network->isSimulated() ? CLIENT_KNOBS->BACKUP_SIM_COPY_LOG_RANGES : 0)) *
		                                     CLIENT_KNOBS->COPY_LOG_BLOCK_SIZE);

		state Standalone<VectorRef<KeyRangeRef>> ranges = getLogRanges(
		    beginVersion, newEndVersion, task->params[BackupAgentBase::destUid], CLIENT_KNOBS->COPY_LOG_BLOCK_SIZE);
		state int nRanges = ranges.size();

		state std::vector<PromiseStream<RCGroup>> results;
		state std::vector<Future<Void>> rc;
		state std::vector<Reference<FlowLock>> locks;
		state Version nextVersion = beginVersion;
		state double breakTime = timer_monotonic() + CLIENT_KNOBS->COPY_LOG_TASK_DURATION_NANOS;
		state int rangeN = 0;

		loop {
			if (rangeN >= nRanges)
				break;

			// prefetch
			int prefetchTo = std::min(rangeN + CLIENT_KNOBS->COPY_LOG_PREFETCH_BLOCKS, nRanges);

			for (int j = results.size(); j < prefetchTo; j++) {
				results.push_back(PromiseStream<RCGroup>());
				locks.push_back(makeReference<FlowLock>(CLIENT_KNOBS->COPY_LOG_READ_AHEAD_BYTES));
				rc.push_back(readCommitted(taskBucket->src,
				                           results[j],
				                           Future<Void>(Void()),
				                           locks[j],
				                           ranges[j],
				                           decodeBKMutationLogKey,
				                           Terminator::True,
				                           AccessSystemKeys::True,
				                           LockAware::True));
			}

			// copy the range
			Optional<Version> nextVersionBr =
			    wait(dumpData(cx, task, results[rangeN], locks[rangeN].getPtr(), taskBucket, breakTime));

			// exit from the task if a timeout occurs
			if (nextVersionBr.present()) {
				nextVersion = nextVersionBr.get();
				// cancel prefetch
				TraceEvent(SevInfo, "CopyLogRangeTaskFuncAborted")
				    .detail("DurationNanos", CLIENT_KNOBS->COPY_LOG_TASK_DURATION_NANOS)
				    .detail("RangeN", rangeN)
				    .detail("BytesWritten", Params.bytesWritten().getOrDefault(task));
				for (int j = results.size(); --j >= rangeN;)
					rc[j].cancel();
				break;
			}
			// the whole range has been dumped
			nextVersion = getLogKeyVersion(ranges[rangeN].end);
			rangeN++;
		}

		if (nextVersion < endVersion) {
			task->params[CopyLogRangeTaskFunc::keyNextBeginVersion] = BinaryWriter::toValue(nextVersion, Unversioned());
		}
		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 Version beginVersion,
	                                 Version endVersion,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(CopyLogRangeTaskFunc::name, CopyLogRangeTaskFunc::version, doneKey, 1);

		copyDefaultParameters(parentTask, task);

		task->params[DatabaseBackupAgent::keyBeginVersion] = BinaryWriter::toValue(beginVersion, Unversioned());
		task->params[DatabaseBackupAgent::keyEndVersion] = BinaryWriter::toValue(endVersion, Unversioned());

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
		return LiteralStringRef("OnSetAddTask");
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {

		state Version endVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyEndVersion], Unversioned());
		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		// Get the bytesWritten parameter from task and atomically add it to the logBytesWritten() property of the DR
		// config.
		DRConfig config(task);
		int64_t bytesWritten = Params.bytesWritten().getOrDefault(task);
		config.logBytesWritten().atomicOp(tr, bytesWritten, MutationRef::AddValue);

		if (task->params.find(CopyLogRangeTaskFunc::keyNextBeginVersion) != task->params.end()) {
			state Version nextVersion = BinaryReader::fromStringRef<Version>(
			    task->params[CopyLogRangeTaskFunc::keyNextBeginVersion], Unversioned());
			wait(success(CopyLogRangeTaskFunc::addTask(
			         tr, taskBucket, task, nextVersion, endVersion, TaskCompletionKey::signal(taskFuture->key))) &&
			     taskBucket->finish(tr, task));
		} else {
			wait(taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task));
		}

		return Void();
	}
};
StringRef CopyLogRangeTaskFunc::name = LiteralStringRef("dr_copy_log_range");
const Key CopyLogRangeTaskFunc::keyNextBeginVersion = LiteralStringRef("nextBeginVersion");
REGISTER_TASKFUNC(CopyLogRangeTaskFunc);

struct CopyLogsTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Subspace conf = Subspace(databaseBackupPrefixRange.begin)
		                          .get(BackupAgentBase::keyConfig)
		                          .get(task->params[BackupAgentBase::keyConfigLogUid]);
		state Subspace states = Subspace(databaseBackupPrefixRange.begin)
		                            .get(BackupAgentBase::keyStates)
		                            .get(task->params[BackupAgentBase::keyConfigLogUid]);
		wait(checkTaskVersion(tr, task, CopyLogsTaskFunc::name, CopyLogsTaskFunc::version));

		state Version beginVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyBeginVersion], Unversioned());
		state Version prevBeginVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyPrevBeginVersion], Unversioned());
		state Future<Optional<Value>> fStopValue = tr->get(states.pack(DatabaseBackupAgent::keyCopyStop));
		state Future<Optional<Value>> fAppliedValue =
		    tr->get(task->params[BackupAgentBase::keyConfigLogUid].withPrefix(applyMutationsBeginRange.begin));

		Transaction srcTr(taskBucket->src);
		srcTr.setOption(FDBTransactionOptions::LOCK_AWARE);
		state Version endVersion = wait(srcTr.getReadVersion());

		state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		if (endVersion <= beginVersion) {
			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			wait(success(CopyLogsTaskFunc::addTask(
			    tr, taskBucket, task, prevBeginVersion, beginVersion, TaskCompletionKey::signal(onDone))));
			wait(taskBucket->finish(tr, task));
			return Void();
		}

		Optional<Value> appliedValue = wait(fAppliedValue);
		state Version appliedVersion =
		    appliedValue.present() ? BinaryReader::fromStringRef<Version>(appliedValue.get(), Unversioned()) : 100;

		state Version applyVersion =
		    std::max<Version>(appliedVersion, beginVersion - CLIENT_KNOBS->BACKUP_VERSION_DELAY);
		Subspace krv = conf.get(DatabaseBackupAgent::keyRangeVersions);
		KeyRange versionRange = KeyRangeRef(krv.pack(0), krv.pack(applyVersion + 1));
		tr->addReadConflictRange(versionRange);
		tr->addWriteConflictRange(versionRange);
		tr->set(task->params[BackupAgentBase::keyConfigLogUid].withPrefix(applyMutationsEndRange.begin),
		        BinaryWriter::toValue(applyVersion, Unversioned()));

		Optional<Value> stopValue = wait(fStopValue);
		state Version stopVersionData =
		    stopValue.present() ? BinaryReader::fromStringRef<Version>(stopValue.get(), Unversioned()) : -1;

		if (endVersion - beginVersion > deterministicRandom()->randomInt64(0, CLIENT_KNOBS->BACKUP_VERSION_DELAY)) {
			TraceEvent("DBA_CopyLogs")
			    .detail("BeginVersion", beginVersion)
			    .detail("ApplyVersion", applyVersion)
			    .detail("EndVersion", endVersion)
			    .detail("StopVersionData", stopVersionData)
			    .detail("LogUID", task->params[BackupAgentBase::keyConfigLogUid]);
		}

		if ((stopVersionData == -1) || (stopVersionData >= applyVersion)) {
			state Reference<TaskFuture> allPartsDone = futureBucket->future(tr);
			std::vector<Future<Key>> addTaskVector;
			addTaskVector.push_back(CopyLogsTaskFunc::addTask(
			    tr, taskBucket, task, beginVersion, endVersion, TaskCompletionKey::signal(onDone), allPartsDone));
			int blockSize = std::max<int>(
			    1, ((endVersion - beginVersion) / CLIENT_KNOBS->BACKUP_COPY_TASKS) / CLIENT_KNOBS->BACKUP_BLOCK_SIZE);
			for (int64_t vblock = beginVersion / CLIENT_KNOBS->BACKUP_BLOCK_SIZE;
			     vblock < (endVersion + CLIENT_KNOBS->BACKUP_BLOCK_SIZE - 1) / CLIENT_KNOBS->BACKUP_BLOCK_SIZE;
			     vblock += blockSize) {
				addTaskVector.push_back(CopyLogRangeTaskFunc::addTask(
				    tr,
				    taskBucket,
				    task,
				    std::max(beginVersion, vblock * CLIENT_KNOBS->BACKUP_BLOCK_SIZE),
				    std::min(endVersion, (vblock + blockSize) * CLIENT_KNOBS->BACKUP_BLOCK_SIZE),
				    TaskCompletionKey::joinWith(allPartsDone)));
			}

			// Do not erase at the first time
			if (prevBeginVersion > 0) {
				addTaskVector.push_back(EraseLogRangeTaskFunc::addTask(
				    tr, taskBucket, task, beginVersion, TaskCompletionKey::joinWith(allPartsDone)));
			}

			wait(waitForAll(addTaskVector) && taskBucket->finish(tr, task));
		} else {
			if (appliedVersion < applyVersion) {
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				wait(success(CopyLogsTaskFunc::addTask(
				    tr, taskBucket, task, prevBeginVersion, beginVersion, TaskCompletionKey::signal(onDone))));
				wait(taskBucket->finish(tr, task));
				return Void();
			}

			wait(onDone->set(tr, taskBucket) && taskBucket->finish(tr, task));
			tr->set(states.pack(DatabaseBackupAgent::keyStateStop), BinaryWriter::toValue(beginVersion, Unversioned()));
		}

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 Version prevBeginVersion,
	                                 Version beginVersion,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(CopyLogsTaskFunc::name, CopyLogsTaskFunc::version, doneKey, 1);

		copyDefaultParameters(parentTask, task);
		task->params[BackupAgentBase::keyBeginVersion] = BinaryWriter::toValue(beginVersion, Unversioned());
		task->params[DatabaseBackupAgent::keyPrevBeginVersion] = BinaryWriter::toValue(prevBeginVersion, Unversioned());

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
		return LiteralStringRef("OnSetAddTask");
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
StringRef CopyLogsTaskFunc::name = LiteralStringRef("dr_copy_logs");
REGISTER_TASKFUNC(CopyLogsTaskFunc);

struct FinishedFullBackupTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;
	static const Key keyInsertTask;

	StringRef getName() const override { return name; };

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Subspace sourceStates = Subspace(databaseBackupPrefixRange.begin)
		                                  .get(BackupAgentBase::keySourceStates)
		                                  .get(task->params[BackupAgentBase::keyConfigLogUid]);

		wait(checkTaskVersion(cx, task, FinishedFullBackupTaskFunc::name, FinishedFullBackupTaskFunc::version));

		state Transaction tr2(cx);
		loop {
			try {
				tr2.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> beginValue = wait(
				    tr2.get(task->params[BackupAgentBase::keyConfigLogUid].withPrefix(applyMutationsBeginRange.begin)));
				state Version appliedVersion =
				    beginValue.present() ? BinaryReader::fromStringRef<Version>(beginValue.get(), Unversioned()) : -1;
				Optional<Value> endValue = wait(
				    tr2.get(task->params[BackupAgentBase::keyConfigLogUid].withPrefix(applyMutationsEndRange.begin)));
				Version endVersion =
				    endValue.present() ? BinaryReader::fromStringRef<Version>(endValue.get(), Unversioned()) : -1;

				//TraceEvent("DBA_FinishedFullBackup").detail("Applied", appliedVersion).detail("EndVer", endVersion);
				if (appliedVersion < endVersion) {
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
					task->params[FinishedFullBackupTaskFunc::keyInsertTask] = StringRef();
					return Void();
				}
				break;
			} catch (Error& e) {
				wait(tr2.onError(e));
			}
		}

		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(taskBucket->src));
		state Key logUidValue = task->params[DatabaseBackupAgent::keyConfigLogUid];
		state Key destUidValue = task->params[BackupAgentBase::destUid];
		state Version backupUid =
		    BinaryReader::fromStringRef<Version>(task->params[BackupAgentBase::keyFolderId], Unversioned());

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> v = wait(tr->get(sourceStates.pack(DatabaseBackupAgent::keyFolderId)));
				if (v.present() && BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) >
				                       BinaryReader::fromStringRef<Version>(
				                           task->params[DatabaseBackupAgent::keyFolderId], Unversioned()))
					return Void();

				wait(eraseLogData(tr, logUidValue, destUidValue, Optional<Version>(), CheckBackupUID::True, backupUid));
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(FinishedFullBackupTaskFunc::name, FinishedFullBackupTaskFunc::version, doneKey);

		copyDefaultParameters(parentTask, task);

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
		return LiteralStringRef("OnSetAddTask");
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Subspace conf = Subspace(databaseBackupPrefixRange.begin)
		                          .get(BackupAgentBase::keyConfig)
		                          .get(task->params[BackupAgentBase::keyConfigLogUid]);
		state Subspace states = Subspace(databaseBackupPrefixRange.begin)
		                            .get(BackupAgentBase::keyStates)
		                            .get(task->params[BackupAgentBase::keyConfigLogUid]);

		if (task->params.find(FinishedFullBackupTaskFunc::keyInsertTask) != task->params.end()) {
			state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
			wait(success(FinishedFullBackupTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::signal(onDone))));
			wait(taskBucket->finish(tr, task));
			return Void();
		}

		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);
		UID logUid =
		    BinaryReader::fromStringRef<UID>(task->params[DatabaseBackupAgent::keyConfigLogUid], Unversioned());
		Key logsPath = uidPrefixKey(applyLogKeys.begin, logUid);
		tr->clear(KeyRangeRef(logsPath, strinc(logsPath)));

		tr->clear(conf.range());
		tr->set(states.pack(DatabaseBackupAgent::keyStateStatus),
		        StringRef(BackupAgentBase::getStateText(EBackupState::STATE_COMPLETED)));

		wait(taskBucket->finish(tr, task));
		return Void();
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
StringRef FinishedFullBackupTaskFunc::name = LiteralStringRef("dr_finished_full_backup");
const Key FinishedFullBackupTaskFunc::keyInsertTask = LiteralStringRef("insertTask");
REGISTER_TASKFUNC(FinishedFullBackupTaskFunc);

struct CopyDiffLogsTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Subspace conf = Subspace(databaseBackupPrefixRange.begin)
		                          .get(BackupAgentBase::keyConfig)
		                          .get(task->params[BackupAgentBase::keyConfigLogUid]);
		state Subspace states = Subspace(databaseBackupPrefixRange.begin)
		                            .get(BackupAgentBase::keyStates)
		                            .get(task->params[BackupAgentBase::keyConfigLogUid]);
		wait(checkTaskVersion(tr, task, CopyDiffLogsTaskFunc::name, CopyDiffLogsTaskFunc::version));

		state Version beginVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyBeginVersion], Unversioned());
		state Version prevBeginVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyPrevBeginVersion], Unversioned());
		state Future<Optional<Value>> fStopWhenDone = tr->get(conf.pack(DatabaseBackupAgent::keyConfigStopWhenDoneKey));

		Transaction srcTr(taskBucket->src);
		srcTr.setOption(FDBTransactionOptions::LOCK_AWARE);
		state Version endVersion = wait(srcTr.getReadVersion());

		state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		if (endVersion <= beginVersion) {
			wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			wait(success(CopyDiffLogsTaskFunc::addTask(
			    tr, taskBucket, task, prevBeginVersion, beginVersion, TaskCompletionKey::signal(onDone))));
			wait(taskBucket->finish(tr, task));
			return Void();
		}

		tr->set(task->params[BackupAgentBase::keyConfigLogUid].withPrefix(applyMutationsEndRange.begin),
		        BinaryWriter::toValue(beginVersion, Unversioned()));
		Optional<Value> stopWhenDone = wait(fStopWhenDone);

		if (endVersion - beginVersion > deterministicRandom()->randomInt64(0, CLIENT_KNOBS->BACKUP_VERSION_DELAY)) {
			TraceEvent("DBA_CopyDiffLogs")
			    .detail("BeginVersion", beginVersion)
			    .detail("EndVersion", endVersion)
			    .detail("LogUID", task->params[BackupAgentBase::keyConfigLogUid]);
		}

		// set the log version to the state
		tr->set(StringRef(states.pack(DatabaseBackupAgent::keyStateLogBeginVersion)),
		        BinaryWriter::toValue(beginVersion, Unversioned()));

		if (!stopWhenDone.present()) {
			state Reference<TaskFuture> allPartsDone = futureBucket->future(tr);
			std::vector<Future<Key>> addTaskVector;
			addTaskVector.push_back(CopyDiffLogsTaskFunc::addTask(
			    tr, taskBucket, task, beginVersion, endVersion, TaskCompletionKey::signal(onDone), allPartsDone));
			int blockSize = std::max<int>(
			    1, ((endVersion - beginVersion) / CLIENT_KNOBS->BACKUP_COPY_TASKS) / CLIENT_KNOBS->BACKUP_BLOCK_SIZE);
			for (int64_t vblock = beginVersion / CLIENT_KNOBS->BACKUP_BLOCK_SIZE;
			     vblock < (endVersion + CLIENT_KNOBS->BACKUP_BLOCK_SIZE - 1) / CLIENT_KNOBS->BACKUP_BLOCK_SIZE;
			     vblock += blockSize) {
				addTaskVector.push_back(CopyLogRangeTaskFunc::addTask(
				    tr,
				    taskBucket,
				    task,
				    std::max(beginVersion, vblock * CLIENT_KNOBS->BACKUP_BLOCK_SIZE),
				    std::min(endVersion, (vblock + blockSize) * CLIENT_KNOBS->BACKUP_BLOCK_SIZE),
				    TaskCompletionKey::joinWith(allPartsDone)));
			}

			if (prevBeginVersion > 0) {
				addTaskVector.push_back(EraseLogRangeTaskFunc::addTask(
				    tr, taskBucket, task, beginVersion, TaskCompletionKey::joinWith(allPartsDone)));
			}

			wait(waitForAll(addTaskVector) && taskBucket->finish(tr, task));
		} else {
			wait(onDone->set(tr, taskBucket) && taskBucket->finish(tr, task));
		}
		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 Version prevBeginVersion,
	                                 Version beginVersion,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(CopyDiffLogsTaskFunc::name, CopyDiffLogsTaskFunc::version, doneKey, 1);

		copyDefaultParameters(parentTask, task);

		task->params[DatabaseBackupAgent::keyBeginVersion] = BinaryWriter::toValue(beginVersion, Unversioned());
		task->params[DatabaseBackupAgent::keyPrevBeginVersion] = BinaryWriter::toValue(prevBeginVersion, Unversioned());

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
		return LiteralStringRef("OnSetAddTask");
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
StringRef CopyDiffLogsTaskFunc::name = LiteralStringRef("dr_copy_diff_logs");
REGISTER_TASKFUNC(CopyDiffLogsTaskFunc);

// Skip unneeded EraseLogRangeTaskFunc in 5.1
struct SkipOldEraseLogRangeTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);
		wait(taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task));
		return Void();
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
StringRef SkipOldEraseLogRangeTaskFunc::name = LiteralStringRef("dr_skip_legacy_task");
REGISTER_TASKFUNC(SkipOldEraseLogRangeTaskFunc);
REGISTER_TASKFUNC_ALIAS(SkipOldEraseLogRangeTaskFunc, db_erase_log_range);

// This is almost the same as CopyLogRangeTaskFunc in 5.1. The only purpose is to support DR upgrade
struct OldCopyLogRangeTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	static struct {
		static TaskParam<int64_t> bytesWritten() { return LiteralStringRef(__FUNCTION__); }
	} Params;

	static const Key keyNextBeginVersion;

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

	ACTOR static Future<Void> dumpData(Database cx,
	                                   Reference<Task> task,
	                                   PromiseStream<RCGroup> results,
	                                   FlowLock* lock,
	                                   Reference<TaskBucket> tb) {
		state bool endOfStream = false;
		state Subspace conf = Subspace(databaseBackupPrefixRange.begin)
		                          .get(BackupAgentBase::keyConfig)
		                          .get(task->params[BackupAgentBase::keyConfigLogUid]);

		state std::vector<RangeResult> nextMutations;
		state int64_t nextMutationSize = 0;
		loop {
			try {
				if (endOfStream && !nextMutationSize) {
					return Void();
				}

				state std::vector<RangeResult> mutations = std::move(nextMutations);
				state int64_t mutationSize = nextMutationSize;
				nextMutations = std::vector<RangeResult>();
				nextMutationSize = 0;

				if (!endOfStream) {
					loop {
						try {
							RCGroup group = waitNext(results.getFuture());
							lock->release(group.items.expectedSize());

							int vecSize = group.items.expectedSize();
							if (mutationSize + vecSize >= CLIENT_KNOBS->BACKUP_LOG_WRITE_BATCH_MAX_SIZE) {

								nextMutations.push_back(group.items);
								nextMutationSize = vecSize;
								break;
							}

							mutations.push_back(group.items);
							mutationSize += vecSize;
						} catch (Error& e) {
							state Error error = e;
							if (e.code() == error_code_end_of_stream) {
								endOfStream = true;
								break;
							}

							throw error;
						}
					}
				}

				state Transaction tr(cx);

				loop {
					try {
						tr.setOption(FDBTransactionOptions::LOCK_AWARE);
						tr.trState->options.sizeLimit = 2 * CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT;
						wait(checkDatabaseLock(&tr,
						                       BinaryReader::fromStringRef<UID>(
						                           task->params[BackupAgentBase::keyConfigLogUid], Unversioned())));
						state int64_t bytesSet = 0;

						bool first = true;
						for (auto m : mutations) {
							for (auto kv : m) {
								if (first) {
									tr.addReadConflictRange(singleKeyRange(kv.key));
									first = false;
								}
								tr.set(kv.key.removePrefix(backupLogKeys.begin).withPrefix(applyLogKeys.begin),
								       kv.value);
								bytesSet += kv.expectedSize() - backupLogKeys.begin.expectedSize() +
								            applyLogKeys.begin.expectedSize();
							}
						}

						wait(tr.commit());
						Params.bytesWritten().set(task, Params.bytesWritten().getOrDefault(task) + bytesSet);
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled || e.code() == error_code_backup_error)
					throw e;

				state Error err = e;
				wait(logError(cx,
				              Subspace(databaseBackupPrefixRange.begin)
				                  .get(BackupAgentBase::keyErrors)
				                  .pack(task->params[BackupAgentBase::keyConfigLogUid]),
				              format("ERROR: Failed to dump mutations because of error %s", err.what())));

				throw err;
			}
		}
	}

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Reference<FlowLock> lock(new FlowLock(CLIENT_KNOBS->BACKUP_LOCK_BYTES));

		wait(checkTaskVersion(cx, task, OldCopyLogRangeTaskFunc::name, OldCopyLogRangeTaskFunc::version));

		state Version beginVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyBeginVersion], Unversioned());
		state Version endVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyEndVersion], Unversioned());
		state Version newEndVersion =
		    std::min(endVersion,
		             (((beginVersion - 1) / CLIENT_KNOBS->BACKUP_BLOCK_SIZE) + 2 +
		              (g_network->isSimulated() ? CLIENT_KNOBS->BACKUP_SIM_COPY_LOG_RANGES : 0)) *
		                 CLIENT_KNOBS->BACKUP_BLOCK_SIZE);

		state Standalone<VectorRef<KeyRangeRef>> ranges = getLogRanges(beginVersion,
		                                                               newEndVersion,
		                                                               task->params[BackupAgentBase::keyConfigLogUid],
		                                                               CLIENT_KNOBS->BACKUP_BLOCK_SIZE);
		state std::vector<PromiseStream<RCGroup>> results;
		state std::vector<Future<Void>> rc;
		state std::vector<Future<Void>> dump;

		for (int i = 0; i < ranges.size(); ++i) {
			results.push_back(PromiseStream<RCGroup>());
			rc.push_back(readCommitted(taskBucket->src,
			                           results[i],
			                           Future<Void>(Void()),
			                           lock,
			                           ranges[i],
			                           decodeBKMutationLogKey,
			                           Terminator::True,
			                           AccessSystemKeys::True,
			                           LockAware::True));
			dump.push_back(dumpData(cx, task, results[i], lock.getPtr(), taskBucket));
		}

		wait(waitForAll(dump));

		if (newEndVersion < endVersion) {
			task->params[OldCopyLogRangeTaskFunc::keyNextBeginVersion] =
			    BinaryWriter::toValue(newEndVersion, Unversioned());
		}

		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 Version beginVersion,
	                                 Version endVersion,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(OldCopyLogRangeTaskFunc::name, OldCopyLogRangeTaskFunc::version, doneKey, 1);

		copyDefaultParameters(parentTask, task);

		task->params[DatabaseBackupAgent::keyBeginVersion] = BinaryWriter::toValue(beginVersion, Unversioned());
		task->params[DatabaseBackupAgent::keyEndVersion] = BinaryWriter::toValue(endVersion, Unversioned());

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
		return LiteralStringRef("OnSetAddTask");
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {

		state Version endVersion =
		    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyEndVersion], Unversioned());
		state Reference<TaskFuture> taskFuture = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		// Get the bytesWritten parameter from task and atomically add it to the logBytesWritten() property of the DR
		// config.
		DRConfig config(task);
		int64_t bytesWritten = Params.bytesWritten().getOrDefault(task);
		config.logBytesWritten().atomicOp(tr, bytesWritten, MutationRef::AddValue);

		if (task->params.find(OldCopyLogRangeTaskFunc::keyNextBeginVersion) != task->params.end()) {
			state Version nextVersion = BinaryReader::fromStringRef<Version>(
			    task->params[OldCopyLogRangeTaskFunc::keyNextBeginVersion], Unversioned());
			wait(success(OldCopyLogRangeTaskFunc::addTask(
			         tr, taskBucket, task, nextVersion, endVersion, TaskCompletionKey::signal(taskFuture->key))) &&
			     taskBucket->finish(tr, task));
		} else {
			wait(taskFuture->set(tr, taskBucket) && taskBucket->finish(tr, task));
		}

		return Void();
	}
};
StringRef OldCopyLogRangeTaskFunc::name = LiteralStringRef("db_copy_log_range");
const Key OldCopyLogRangeTaskFunc::keyNextBeginVersion = LiteralStringRef("nextBeginVersion");
REGISTER_TASKFUNC(OldCopyLogRangeTaskFunc);

struct AbortOldBackupTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state DatabaseBackupAgent srcDrAgent(taskBucket->src);
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state Key tagNameKey;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				Key tagPath = srcDrAgent.states.get(task->params[DatabaseBackupAgent::keyConfigLogUid])
				                  .pack(BackupAgentBase::keyConfigBackupTag);
				Optional<Key> tagName = wait(tr->get(tagPath));
				if (!tagName.present()) {
					return Void();
				}

				tagNameKey = tagName.get();
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		TraceEvent("DBA_AbortOldBackup").detail("TagName", tagNameKey.printable());
		wait(srcDrAgent.abortBackup(cx, tagNameKey, PartialBackup::False, AbortOldBackup::True));

		return Void();
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		wait(taskBucket->finish(tr, task));
		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(AbortOldBackupTaskFunc::name, AbortOldBackupTaskFunc::version, doneKey, 1);

		copyDefaultParameters(parentTask, task);

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
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
StringRef AbortOldBackupTaskFunc::name = LiteralStringRef("dr_abort_legacy_backup");
REGISTER_TASKFUNC(AbortOldBackupTaskFunc);
REGISTER_TASKFUNC_ALIAS(AbortOldBackupTaskFunc, db_backup_range);
REGISTER_TASKFUNC_ALIAS(AbortOldBackupTaskFunc, db_finish_full_backup);
REGISTER_TASKFUNC_ALIAS(AbortOldBackupTaskFunc, db_copy_logs);
REGISTER_TASKFUNC_ALIAS(AbortOldBackupTaskFunc, db_finished_full_backup);
REGISTER_TASKFUNC_ALIAS(AbortOldBackupTaskFunc, db_backup_restorable);
REGISTER_TASKFUNC_ALIAS(AbortOldBackupTaskFunc, db_start_full_backup);

// Upgrade DR from 5.1
struct CopyDiffLogsUpgradeTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Key logUidValue = task->params[DatabaseBackupAgent::keyConfigLogUid];
		state Subspace sourceStates =
		    Subspace(databaseBackupPrefixRange.begin).get(BackupAgentBase::keySourceStates).get(logUidValue);
		state Subspace config =
		    Subspace(databaseBackupPrefixRange.begin).get(BackupAgentBase::keyConfig).get(logUidValue);
		wait(checkTaskVersion(cx, task, CopyDiffLogsUpgradeTaskFunc::name, CopyDiffLogsUpgradeTaskFunc::version));

		// Retrieve backupRanges
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		loop {
			try {
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Future<Void> verified = taskBucket->keepRunning(tr, task);
				wait(verified);

				Optional<Key> backupKeysPacked = wait(tr->get(config.pack(BackupAgentBase::keyConfigBackupRanges)));
				if (!backupKeysPacked.present()) {
					return Void();
				}

				BinaryReader br(backupKeysPacked.get(), IncludeVersion());
				br >> backupRanges;
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		// Set destUidValue and versionKey on src side
		state Key destUidValue(logUidValue);
		state Reference<ReadYourWritesTransaction> srcTr(new ReadYourWritesTransaction(taskBucket->src));
		loop {
			try {
				srcTr->setOption(FDBTransactionOptions::LOCK_AWARE);
				srcTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				state Optional<Value> v = wait(srcTr->get(sourceStates.pack(DatabaseBackupAgent::keyFolderId)));
				if (v.present() && BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) >
				                       BinaryReader::fromStringRef<Version>(
				                           task->params[DatabaseBackupAgent::keyFolderId], Unversioned())) {
					return Void();
				}

				if (backupRanges.size() == 1) {
					RangeResult existingDestUidValues = wait(srcTr->getRange(
					    KeyRangeRef(destUidLookupPrefix, strinc(destUidLookupPrefix)), CLIENT_KNOBS->TOO_MANY));
					bool found = false;
					for (auto it : existingDestUidValues) {
						if (BinaryReader::fromStringRef<KeyRange>(it.key.removePrefix(destUidLookupPrefix),
						                                          IncludeVersion()) == backupRanges[0]) {
							if (destUidValue != it.value) {
								// existing backup/DR is running
								return Void();
							} else {
								// due to unknown commit result
								found = true;
								break;
							}
						}
					}
					if (found) {
						break;
					}

					srcTr->set(
					    BinaryWriter::toValue(backupRanges[0], IncludeVersion(ProtocolVersion::withSharedMutations()))
					        .withPrefix(destUidLookupPrefix),
					    destUidValue);
				}

				Key versionKey = logUidValue.withPrefix(destUidValue).withPrefix(backupLatestVersionsPrefix);
				srcTr->set(versionKey, task->params[DatabaseBackupAgent::keyBeginVersion]);
				wait(srcTr->commit());
				break;
			} catch (Error& e) {
				wait(srcTr->onError(e));
			}
		}

		task->params[BackupAgentBase::destUid] = destUidValue;
		ASSERT(destUidValue == logUidValue);

		return Void();
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		wait(checkTaskVersion(tr, task, CopyDiffLogsUpgradeTaskFunc::name, CopyDiffLogsUpgradeTaskFunc::version));
		state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		if (task->params[BackupAgentBase::destUid].size() == 0) {
			TraceEvent("DBA_CopyDiffLogsUpgradeTaskFuncAbortInUpgrade").log();
			wait(success(AbortOldBackupTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::signal(onDone))));
		} else {
			Version beginVersion =
			    BinaryReader::fromStringRef<Version>(task->params[DatabaseBackupAgent::keyBeginVersion], Unversioned());
			Subspace config = Subspace(databaseBackupPrefixRange.begin)
			                      .get(BackupAgentBase::keyConfig)
			                      .get(task->params[DatabaseBackupAgent::keyConfigLogUid]);
			tr->set(config.pack(BackupAgentBase::destUid), task->params[BackupAgentBase::destUid]);
			tr->set(config.pack(BackupAgentBase::keyDrVersion),
			        BinaryWriter::toValue(DatabaseBackupAgent::LATEST_DR_VERSION, Unversioned()));
			wait(success(CopyDiffLogsTaskFunc::addTask(
			    tr, taskBucket, task, 0, beginVersion, TaskCompletionKey::signal(onDone))));
		}

		wait(taskBucket->finish(tr, task));
		return Void();
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
StringRef CopyDiffLogsUpgradeTaskFunc::name = LiteralStringRef("db_copy_diff_logs");
REGISTER_TASKFUNC(CopyDiffLogsUpgradeTaskFunc);

struct BackupRestorableTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Subspace sourceStates = Subspace(databaseBackupPrefixRange.begin)
		                                  .get(BackupAgentBase::keySourceStates)
		                                  .get(task->params[BackupAgentBase::keyConfigLogUid]);
		wait(checkTaskVersion(cx, task, BackupRestorableTaskFunc::name, BackupRestorableTaskFunc::version));
		state Transaction tr(taskBucket->src);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.addReadConflictRange(singleKeyRange(sourceStates.pack(DatabaseBackupAgent::keyStateStatus)));
				tr.set(sourceStates.pack(DatabaseBackupAgent::keyStateStatus),
				       StringRef(BackupAgentBase::getStateText(EBackupState::STATE_RUNNING_DIFFERENTIAL)));

				Key versionKey = task->params[DatabaseBackupAgent::keyConfigLogUid]
				                     .withPrefix(task->params[BackupAgentBase::destUid])
				                     .withPrefix(backupLatestVersionsPrefix);
				Optional<Key> prevBeginVersion = wait(tr.get(versionKey));
				if (!prevBeginVersion.present()) {
					return Void();
				}

				task->params[DatabaseBackupAgent::keyPrevBeginVersion] = prevBeginVersion.get();

				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Subspace conf = Subspace(databaseBackupPrefixRange.begin)
		                          .get(BackupAgentBase::keyConfig)
		                          .get(task->params[BackupAgentBase::keyConfigLogUid]);
		state Subspace states = Subspace(databaseBackupPrefixRange.begin)
		                            .get(BackupAgentBase::keyStates)
		                            .get(task->params[BackupAgentBase::keyConfigLogUid]);
		wait(checkTaskVersion(tr, task, BackupRestorableTaskFunc::name, BackupRestorableTaskFunc::version));

		state Reference<TaskFuture> onDone = futureBucket->unpack(task->params[Task::reservedTaskParamKeyDone]);

		state Optional<Value> stopValue = wait(tr->get(states.pack(DatabaseBackupAgent::keyStateStop)));
		state Version restoreVersion =
		    stopValue.present() ? BinaryReader::fromStringRef<Version>(stopValue.get(), Unversioned()) : -1;

		state Optional<Value> stopWhenDone = wait(tr->get(conf.pack(DatabaseBackupAgent::keyConfigStopWhenDoneKey)));
		state Reference<TaskFuture> allPartsDone;

		TraceEvent("DBA_Complete")
		    .detail("RestoreVersion", restoreVersion)
		    .detail("Differential", stopWhenDone.present())
		    .detail("LogUID", task->params[BackupAgentBase::keyConfigLogUid]);

		// Start the complete task, if differential is not enabled
		if (stopWhenDone.present()) {
			// After the Backup completes, clear the backup subspace and update the status
			wait(success(FinishedFullBackupTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal())));
		} else { // Start the writing of logs, if differential
			tr->set(states.pack(DatabaseBackupAgent::keyStateStatus),
			        StringRef(BackupAgentBase::getStateText(EBackupState::STATE_RUNNING_DIFFERENTIAL)));

			allPartsDone = futureBucket->future(tr);

			Version prevBeginVersion = BinaryReader::fromStringRef<Version>(
			    task->params[DatabaseBackupAgent::keyPrevBeginVersion], Unversioned());
			wait(success(CopyDiffLogsTaskFunc::addTask(
			    tr, taskBucket, task, prevBeginVersion, restoreVersion, TaskCompletionKey::joinWith(allPartsDone))));

			// After the Backup completes, clear the backup subspace and update the status
			wait(success(FinishedFullBackupTaskFunc::addTask(
			    tr, taskBucket, task, TaskCompletionKey::noSignal(), allPartsDone)));
		}

		wait(taskBucket->finish(tr, task));
		return Void();
	}

	ACTOR static Future<Key> addTask(Reference<ReadYourWritesTransaction> tr,
	                                 Reference<TaskBucket> taskBucket,
	                                 Reference<Task> parentTask,
	                                 TaskCompletionKey completionKey,
	                                 Reference<TaskFuture> waitFor = Reference<TaskFuture>()) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(BackupRestorableTaskFunc::name, BackupRestorableTaskFunc::version, doneKey);

		copyDefaultParameters(parentTask, task);

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           parentTask->params[Task::reservedTaskParamValidKey],
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           parentTask->params[Task::reservedTaskParamValidKey],
		                           task->params[BackupAgentBase::keyFolderId]));
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
StringRef BackupRestorableTaskFunc::name = LiteralStringRef("dr_backup_restorable");
REGISTER_TASKFUNC(BackupRestorableTaskFunc);

struct StartFullBackupTaskFunc : TaskFuncBase {
	static StringRef name;
	static constexpr uint32_t version = 1;

	ACTOR static Future<Void> _execute(Database cx,
	                                   Reference<TaskBucket> taskBucket,
	                                   Reference<FutureBucket> futureBucket,
	                                   Reference<Task> task) {
		state Key logUidValue = task->params[DatabaseBackupAgent::keyConfigLogUid];
		state Subspace sourceStates =
		    Subspace(databaseBackupPrefixRange.begin).get(BackupAgentBase::keySourceStates).get(logUidValue);
		wait(checkTaskVersion(cx, task, StartFullBackupTaskFunc::name, StartFullBackupTaskFunc::version));
		state Key destUidValue(logUidValue);

		state Standalone<VectorRef<KeyRangeRef>> backupRanges =
		    BinaryReader::fromStringRef<Standalone<VectorRef<KeyRangeRef>>>(
		        task->params[DatabaseBackupAgent::keyConfigBackupRanges], IncludeVersion());
		state Key beginVersionKey;

		state Reference<ReadYourWritesTransaction> srcTr(new ReadYourWritesTransaction(taskBucket->src));
		loop {
			try {
				srcTr->setOption(FDBTransactionOptions::LOCK_AWARE);
				srcTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				// Initialize destUid
				if (backupRanges.size() == 1) {
					RangeResult existingDestUidValues = wait(srcTr->getRange(
					    KeyRangeRef(destUidLookupPrefix, strinc(destUidLookupPrefix)), CLIENT_KNOBS->TOO_MANY));
					bool found = false;
					for (auto it : existingDestUidValues) {
						if (BinaryReader::fromStringRef<KeyRange>(it.key.removePrefix(destUidLookupPrefix),
						                                          IncludeVersion()) == backupRanges[0]) {
							destUidValue = it.value;
							found = true;
							break;
						}
					}
					if (!found) {
						destUidValue = BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned());
						srcTr->set(BinaryWriter::toValue(backupRanges[0],
						                                 IncludeVersion(ProtocolVersion::withSharedMutations()))
						               .withPrefix(destUidLookupPrefix),
						           destUidValue);
					}
				}

				Version bVersion = wait(srcTr->getReadVersion());
				beginVersionKey = BinaryWriter::toValue(bVersion, Unversioned());

				state Key versionKey = logUidValue.withPrefix(destUidValue).withPrefix(backupLatestVersionsPrefix);
				Optional<Key> versionRecord = wait(srcTr->get(versionKey));
				if (!versionRecord.present()) {
					srcTr->set(versionKey, beginVersionKey);
				}

				task->params[BackupAgentBase::destUid] = destUidValue;

				wait(srcTr->commit());
				break;
			} catch (Error& e) {
				wait(srcTr->onError(e));
			}
		}

		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			try {
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Future<Void> verified = taskBucket->keepRunning(tr, task);
				wait(verified);

				// Set destUid at destination side
				state Subspace config =
				    Subspace(databaseBackupPrefixRange.begin).get(BackupAgentBase::keyConfig).get(logUidValue);
				tr->set(config.pack(BackupAgentBase::destUid), task->params[BackupAgentBase::destUid]);

				// Use existing beginVersion if we already have one
				Optional<Key> backupStartVersion = wait(tr->get(config.pack(BackupAgentBase::backupStartVersion)));
				if (backupStartVersion.present()) {
					beginVersionKey = backupStartVersion.get();
				} else {
					tr->set(config.pack(BackupAgentBase::backupStartVersion), beginVersionKey);
				}

				task->params[BackupAgentBase::keyBeginVersion] = beginVersionKey;

				wait(tr->commit());
				break;
			} catch (Error& e) {
				TraceEvent("SetDestUidOrBeginVersionError").errorUnsuppressed(e);
				wait(tr->onError(e));
			}
		}

		state Reference<ReadYourWritesTransaction> srcTr2(new ReadYourWritesTransaction(taskBucket->src));
		loop {
			try {
				srcTr2->setOption(FDBTransactionOptions::LOCK_AWARE);
				srcTr2->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				state Optional<Value> v = wait(srcTr2->get(sourceStates.pack(DatabaseBackupAgent::keyFolderId)));

				if (v.present() && BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) >=
				                       BinaryReader::fromStringRef<Version>(
				                           task->params[DatabaseBackupAgent::keyFolderId], Unversioned()))
					return Void();

				srcTr2->set(Subspace(databaseBackupPrefixRange.begin)
				                .get(BackupAgentBase::keySourceTagName)
				                .pack(task->params[BackupAgentBase::keyTagName]),
				            logUidValue);
				srcTr2->set(sourceStates.pack(DatabaseBackupAgent::keyFolderId),
				            task->params[DatabaseBackupAgent::keyFolderId]);
				srcTr2->set(sourceStates.pack(DatabaseBackupAgent::keyStateStatus),
				            StringRef(BackupAgentBase::getStateText(EBackupState::STATE_RUNNING)));

				state Key destPath = destUidValue.withPrefix(backupLogKeys.begin);
				// Start logging the mutations for the specified ranges of the tag
				for (auto& backupRange : backupRanges) {
					srcTr2->set(logRangesEncodeKey(backupRange.begin,
					                               BinaryReader::fromStringRef<UID>(destUidValue, Unversioned())),
					            logRangesEncodeValue(backupRange.end, destPath));
				}

				wait(srcTr2->commit());
				break;
			} catch (Error& e) {
				wait(srcTr2->onError(e));
			}
		}

		state Reference<ReadYourWritesTransaction> srcTr3(new ReadYourWritesTransaction(taskBucket->src));
		loop {
			try {
				srcTr3->setOption(FDBTransactionOptions::LOCK_AWARE);
				srcTr3->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				srcTr3->atomicOp(metadataVersionKey, metadataVersionRequiredValue, MutationRef::SetVersionstampedValue);

				wait(srcTr3->commit());
				break;
			} catch (Error& e) {
				wait(srcTr3->onError(e));
			}
		}

		return Void();
	}

	ACTOR static Future<Void> _finish(Reference<ReadYourWritesTransaction> tr,
	                                  Reference<TaskBucket> taskBucket,
	                                  Reference<FutureBucket> futureBucket,
	                                  Reference<Task> task) {
		state Key logUidValue = task->params[BackupAgentBase::keyConfigLogUid];
		state Subspace states =
		    Subspace(databaseBackupPrefixRange.begin).get(BackupAgentBase::keyStates).get(logUidValue);
		state Subspace config =
		    Subspace(databaseBackupPrefixRange.begin).get(BackupAgentBase::keyConfig).get(logUidValue);

		state Version beginVersion =
		    BinaryReader::fromStringRef<Version>(task->params[BackupAgentBase::keyBeginVersion], Unversioned());
		state Standalone<VectorRef<KeyRangeRef>> backupRanges =
		    BinaryReader::fromStringRef<Standalone<VectorRef<KeyRangeRef>>>(
		        task->params[DatabaseBackupAgent::keyConfigBackupRanges], IncludeVersion());

		tr->set(logUidValue.withPrefix(applyMutationsBeginRange.begin),
		        BinaryWriter::toValue(beginVersion, Unversioned()));
		tr->set(logUidValue.withPrefix(applyMutationsEndRange.begin),
		        BinaryWriter::toValue(beginVersion, Unversioned()));
		tr->set(states.pack(DatabaseBackupAgent::keyStateStatus),
		        StringRef(BackupAgentBase::getStateText(EBackupState::STATE_RUNNING)));

		state Reference<TaskFuture> kvBackupRangeComplete = futureBucket->future(tr);
		state Reference<TaskFuture> kvBackupComplete = futureBucket->future(tr);
		state int rangeCount = 0;

		if (task->params[DatabaseBackupAgent::keyDatabasesInSync] != std::string("t")) {
			for (; rangeCount < backupRanges.size(); ++rangeCount) {
				wait(success(BackupRangeTaskFunc::addTask(tr,
				                                          taskBucket,
				                                          task,
				                                          backupRanges[rangeCount].begin,
				                                          backupRanges[rangeCount].end,
				                                          TaskCompletionKey::joinWith(kvBackupRangeComplete))));
			}
		} else {
			kvBackupRangeComplete->set(tr, taskBucket);
		}

		// After the BackupRangeTask completes, set the stop key which will stop the BackupLogsTask
		wait(success(FinishFullBackupTaskFunc::addTask(
		    tr, taskBucket, task, TaskCompletionKey::noSignal(), kvBackupRangeComplete)));

		// Backup the logs which will create BackupLogRange tasks
		wait(success(CopyLogsTaskFunc::addTask(
		    tr, taskBucket, task, 0, beginVersion, TaskCompletionKey::joinWith(kvBackupComplete))));

		// After the Backup completes, clear the backup subspace and update the status
		wait(success(
		    BackupRestorableTaskFunc::addTask(tr, taskBucket, task, TaskCompletionKey::noSignal(), kvBackupComplete)));

		wait(taskBucket->finish(tr, task));
		return Void();
	}

	ACTOR static Future<Key> addTask(
	    Reference<ReadYourWritesTransaction> tr,
	    Reference<TaskBucket> taskBucket,
	    Key logUid,
	    Key backupUid,
	    Key keyAddPrefix,
	    Key keyRemovePrefix,
	    Key keyConfigBackupRanges,
	    Key tagName,
	    TaskCompletionKey completionKey,
	    Reference<TaskFuture> waitFor = Reference<TaskFuture>(),
	    DatabaseBackupAgent::PreBackupAction backupAction = DatabaseBackupAgent::PreBackupAction::VERIFY) {
		Key doneKey = wait(completionKey.get(tr, taskBucket));
		auto task = makeReference<Task>(StartFullBackupTaskFunc::name, StartFullBackupTaskFunc::version, doneKey);

		task->params[BackupAgentBase::keyFolderId] = backupUid;
		task->params[BackupAgentBase::keyConfigLogUid] = logUid;
		task->params[DatabaseBackupAgent::keyAddPrefix] = keyAddPrefix;
		task->params[DatabaseBackupAgent::keyRemovePrefix] = keyRemovePrefix;
		task->params[BackupAgentBase::keyConfigBackupRanges] = keyConfigBackupRanges;
		task->params[BackupAgentBase::keyTagName] = tagName;
		task->params[DatabaseBackupAgent::keyDatabasesInSync] =
		    backupAction == DatabaseBackupAgent::PreBackupAction::NONE ? LiteralStringRef("t") : LiteralStringRef("f");

		if (!waitFor) {
			return taskBucket->addTask(tr,
			                           task,
			                           Subspace(databaseBackupPrefixRange.begin)
			                               .get(BackupAgentBase::keyConfig)
			                               .get(logUid)
			                               .pack(BackupAgentBase::keyFolderId),
			                           task->params[BackupAgentBase::keyFolderId]);
		}

		wait(waitFor->onSetAddTask(tr,
		                           taskBucket,
		                           task,
		                           Subspace(databaseBackupPrefixRange.begin)
		                               .get(BackupAgentBase::keyConfig)
		                               .get(logUid)
		                               .pack(BackupAgentBase::keyFolderId),
		                           task->params[BackupAgentBase::keyFolderId]));
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
StringRef StartFullBackupTaskFunc::name = LiteralStringRef("dr_start_full_backup");
REGISTER_TASKFUNC(StartFullBackupTaskFunc);
} // namespace dbBackup

std::set<std::string> getDRAgentsIds(StatusObjectReader statusObj, const char* context) {
	std::set<std::string> drBackupAgents;
	try {
		StatusObjectReader statusObjLayers;
		statusObj.get("cluster.layers", statusObjLayers);
		StatusObjectReader instances;
		std::string path = format("%s.instances", context);
		if (statusObjLayers.tryGet(path, instances)) {
			for (auto itr : instances.obj()) {
				drBackupAgents.insert(itr.first);
			}
		}
	} catch (std::runtime_error& e) {
		TraceEvent(SevWarn, "DBA_GetDRAgentsIdsFail").detail("Error", e.what());
		throw backup_error();
	}
	return drBackupAgents;
}

std::string getDRMutationStreamId(StatusObjectReader statusObj, const char* context, Key tagName) {
	try {
		StatusObjectReader statusObjLayers;
		statusObj.get("cluster.layers", statusObjLayers);
		StatusObjectReader tags;
		std::string path = format("%s.tags", context);
		if (statusObjLayers.tryGet(path, tags)) {
			for (auto itr : tags.obj()) {
				if (itr.first == tagName.toString()) {
					JSONDoc tag(itr.second);
					return tag["mutation_stream_id"].get_str();
				}
			}
		}
		TraceEvent(SevWarn, "DBA_TagNotPresentInStatus").detail("Tag", tagName).detail("Context", context);
		throw backup_error();
	} catch (std::runtime_error& e) {
		TraceEvent(SevWarn, "DBA_GetDRMutationStreamIdFail").detail("Error", e.what());
		throw backup_error();
	}
}

bool getLockedStatus(StatusObjectReader statusObj) {
	try {
		StatusObjectReader statusObjCluster = statusObj["cluster"].get_obj();
		return statusObjCluster["database_lock_state.locked"].get_bool();
	} catch (std::runtime_error& e) {
		TraceEvent(SevWarn, "DBA_GetLockedStatusFail").detail("Error", e.what());
		throw backup_error();
	}
}

void checkAtomicSwitchOverConfig(StatusObjectReader srcStatus, StatusObjectReader destStatus, Key tagName) {

	try {
		// Check if src is unlocked and dest is locked
		if (getLockedStatus(srcStatus) != false) {
			TraceEvent(SevWarn, "DBA_AtomicSwitchOverSrcLocked").log();
			throw backup_error();
		}
		if (getLockedStatus(destStatus) != true) {
			TraceEvent(SevWarn, "DBA_AtomicSwitchOverDestUnlocked").log();
			throw backup_error();
		}
		// Check if mutation-stream-id matches
		if (getDRMutationStreamId(srcStatus, "dr_backup", tagName) !=
		    getDRMutationStreamId(destStatus, "dr_backup_dest", tagName)) {
			TraceEvent(SevWarn, "DBA_AtomicSwitchOverMutationIdMismatch")
			    .detail("SourceMutationId", getDRMutationStreamId(srcStatus, "dr_backup", tagName))
			    .detail("DestMutationId", getDRMutationStreamId(destStatus, "dr_back_dest", tagName));
			throw backup_error();
		}
		// Check if there are agents set up with src as its destination cluster and dest as its source cluster
		auto srcDRAgents = getDRAgentsIds(srcStatus, "dr_backup_dest");
		auto destDRAgents = getDRAgentsIds(destStatus, "dr_backup");
		std::set<std::string> intersectingAgents;
		std::set_intersection(srcDRAgents.begin(),
		                      srcDRAgents.end(),
		                      destDRAgents.begin(),
		                      destDRAgents.end(),
		                      std::inserter(intersectingAgents, intersectingAgents.begin()));
		if (intersectingAgents.empty()) {
			TraceEvent(SevWarn, "DBA_SwitchOverPossibleDRAgentsIncorrectSetup").log();
			throw backup_error();
		}
	} catch (std::runtime_error& e) {
		TraceEvent(SevWarn, "DBA_UnableToCheckAtomicSwitchOverConfig").detail("RunTimeError", e.what());
		throw backup_error();
	}
	return;
}

class DatabaseBackupAgentImpl {
public:
	static constexpr int MAX_RESTORABLE_FILE_METASECTION_BYTES = 1024 * 8;

	ACTOR static Future<Void> waitUpgradeToLatestDrVersion(DatabaseBackupAgent* backupAgent, Database cx, Key tagName) {
		state UID logUid = wait(backupAgent->getLogUid(cx, tagName));
		state Key drVersionKey = backupAgent->config.get(BinaryWriter::toValue(logUid, Unversioned()))
		                             .pack(DatabaseBackupAgent::keyDrVersion);

		TraceEvent("DRU_WatchLatestDrVersion")
		    .detail("DrVersionKey", drVersionKey.printable())
		    .detail("LogUid", BinaryWriter::toValue(logUid, Unversioned()).printable());

		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::LOCK_AWARE);
					Optional<Value> drVersion = wait(tr->get(drVersionKey));

					TraceEvent("DRU_VersionCheck")
					    .detail("Current",
					            drVersion.present() ? BinaryReader::fromStringRef<int>(drVersion.get(), Unversioned())
					                                : -1)
					    .detail("Expected", DatabaseBackupAgent::LATEST_DR_VERSION)
					    .detail("LogUid", BinaryWriter::toValue(logUid, Unversioned()).printable());
					if (drVersion.present() && BinaryReader::fromStringRef<int>(drVersion.get(), Unversioned()) ==
					                               DatabaseBackupAgent::LATEST_DR_VERSION) {
						return Void();
					}

					state Future<Void> watchDrVersionFuture = tr->watch(drVersionKey);
					wait(tr->commit());
					wait(watchDrVersionFuture);
					break;
				} catch (Error& e) {
					wait(tr->onError(e));
				}
			}
		}
	}

	// This method will return the final status of the backup
	ACTOR static Future<EBackupState> waitBackup(DatabaseBackupAgent* backupAgent,
	                                             Database cx,
	                                             Key tagName,
	                                             StopWhenDone stopWhenDone) {
		state std::string backTrace;
		state UID logUid = wait(backupAgent->getLogUid(cx, tagName));
		state Key statusKey = backupAgent->states.get(BinaryWriter::toValue(logUid, Unversioned()))
		                          .pack(DatabaseBackupAgent::keyStateStatus);

		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			try {
				state EBackupState status = wait(backupAgent->getStateValue(tr, logUid));

				// Break, if no longer runnable
				if (!DatabaseBackupAgent::isRunnable(status) || EBackupState::STATE_PARTIALLY_ABORTED == status) {
					return status;
				}

				// Break, if in differential mode (restorable) and stopWhenDone is not enabled
				if ((!stopWhenDone) && (EBackupState::STATE_RUNNING_DIFFERENTIAL == status)) {
					return status;
				}

				state Future<Void> watchFuture = tr->watch(statusKey);
				wait(tr->commit());
				wait(watchFuture);
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	// This method will return the final status of the backup
	ACTOR static Future<EBackupState> waitSubmitted(DatabaseBackupAgent* backupAgent, Database cx, Key tagName) {
		state UID logUid = wait(backupAgent->getLogUid(cx, tagName));
		state Key statusKey = backupAgent->states.get(BinaryWriter::toValue(logUid, Unversioned()))
		                          .pack(DatabaseBackupAgent::keyStateStatus);

		loop {
			state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			try {
				state EBackupState status = wait(backupAgent->getStateValue(tr, logUid));

				// Break, if no longer runnable
				if (EBackupState::STATE_SUBMITTED != status) {
					return status;
				}

				state Future<Void> watchFuture = tr->watch(statusKey);
				wait(tr->commit());
				wait(watchFuture);
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> submitBackup(DatabaseBackupAgent* backupAgent,
	                                       Reference<ReadYourWritesTransaction> tr,
	                                       Key tagName,
	                                       Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                                       StopWhenDone stopWhenDone,
	                                       Key addPrefix,
	                                       Key removePrefix,
	                                       LockDB lockDB,
	                                       DatabaseBackupAgent::PreBackupAction backupAction) {
		state UID logUid = deterministicRandom()->randomUniqueID();
		state Key logUidValue = BinaryWriter::toValue(logUid, Unversioned());
		state UID logUidCurrent = wait(backupAgent->getLogUid(tr, tagName));

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);

		// This commit must happen on the first proxy to ensure that the applier has flushed all mutations from previous
		// DRs
		tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

		// We will use the global status for now to ensure that multiple backups do not start place with different tags
		state EBackupState status = wait(backupAgent->getStateValue(tr, logUidCurrent));

		if (DatabaseBackupAgent::isRunnable(status)) {
			throw backup_duplicate();
		}

		if (logUidCurrent.isValid()) {
			logUid = logUidCurrent;
			logUidValue = BinaryWriter::toValue(logUid, Unversioned());
		}

		Optional<Key> v = wait(tr->get(backupAgent->states.get(logUidValue).pack(DatabaseBackupAgent::keyFolderId)));
		Version uidVersion = 0;
		if (v.present())
			uidVersion = BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) + 1;
		state Standalone<StringRef> backupUid = BinaryWriter::toValue(uidVersion, Unversioned());

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

		if (backupAction == DatabaseBackupAgent::PreBackupAction::VERIFY) {
			// Make sure all of the ranges are empty before we backup into them.
			state std::vector<Future<RangeResult>> backupIntoResults;
			for (auto& backupRange : backupRanges) {
				backupIntoResults.push_back(
				    tr->getRange(backupRange.removePrefix(removePrefix).withPrefix(addPrefix), 1));
			}
			wait(waitForAll(backupIntoResults));
			for (auto result : backupIntoResults) {
				if (result.get().size() > 0) {
					// One of the ranges we will be backing up into has pre-existing data.
					throw restore_destination_not_empty();
				}
			}
		} else if (backupAction == DatabaseBackupAgent::PreBackupAction::CLEAR) {
			// Clear out all ranges before we backup into them.
			for (auto& backupRange : backupRanges) {
				tr->clear(backupRange.removePrefix(removePrefix).withPrefix(addPrefix));
			}
		}

		// Clear the backup ranges for the tag
		tr->clear(backupAgent->config.get(logUidValue).range());
		tr->clear(backupAgent->states.get(logUidValue).range());
		tr->clear(backupAgent->errors.range());

		tr->set(backupAgent->tagNames.pack(tagName), logUidValue);

		// Clear DRConfig for this UID, which unfortunately only contains some newer vars and not the stuff below.
		DRConfig(logUid).clear(tr);
		tr->set(backupAgent->config.get(logUidValue).pack(DatabaseBackupAgent::keyDrVersion),
		        BinaryWriter::toValue(DatabaseBackupAgent::LATEST_DR_VERSION, Unversioned()));
		tr->set(backupAgent->config.get(logUidValue).pack(DatabaseBackupAgent::keyAddPrefix), addPrefix);
		tr->set(backupAgent->config.get(logUidValue).pack(DatabaseBackupAgent::keyRemovePrefix), removePrefix);
		tr->set(backupAgent->states.get(logUidValue).pack(DatabaseBackupAgent::keyConfigBackupTag), tagName);
		tr->set(backupAgent->config.get(logUidValue).pack(DatabaseBackupAgent::keyConfigLogUid), logUidValue);
		tr->set(backupAgent->config.get(logUidValue).pack(DatabaseBackupAgent::keyFolderId), backupUid);
		tr->set(backupAgent->states.get(logUidValue).pack(DatabaseBackupAgent::keyFolderId),
		        backupUid); // written to config and states because it's also used by abort
		tr->set(backupAgent->config.get(logUidValue).pack(DatabaseBackupAgent::keyConfigBackupRanges),
		        BinaryWriter::toValue(backupRanges, IncludeVersion(ProtocolVersion::withDRBackupRanges())));
		tr->set(backupAgent->states.get(logUidValue).pack(DatabaseBackupAgent::keyStateStatus),
		        StringRef(BackupAgentBase::getStateText(EBackupState::STATE_SUBMITTED)));
		if (stopWhenDone) {
			tr->set(backupAgent->config.get(logUidValue).pack(DatabaseBackupAgent::keyConfigStopWhenDoneKey),
			        StringRef());
		}

		int64_t startCount = 0;
		state Key mapPrefix = logUidValue.withPrefix(applyMutationsKeyVersionMapRange.begin);
		Key mapEnd = normalKeys.end.withPrefix(mapPrefix);
		tr->set(logUidValue.withPrefix(applyMutationsAddPrefixRange.begin), addPrefix);
		tr->set(logUidValue.withPrefix(applyMutationsRemovePrefixRange.begin), removePrefix);
		tr->set(logUidValue.withPrefix(applyMutationsKeyVersionCountRange.begin), StringRef((uint8_t*)&startCount, 8));
		tr->clear(KeyRangeRef(mapPrefix, mapEnd));

		state Version readVersion = invalidVersion;
		if (backupAction == DatabaseBackupAgent::PreBackupAction::NONE) {
			Transaction readTransaction(backupAgent->taskBucket->src);
			readTransaction.setOption(FDBTransactionOptions::LOCK_AWARE);
			Version _ = wait(readTransaction.getReadVersion());
			readVersion = _;
		}
		tr->set(mapPrefix, BinaryWriter::toValue<Version>(readVersion, Unversioned()));

		Key taskKey = wait(dbBackup::StartFullBackupTaskFunc::addTask(
		    tr,
		    backupAgent->taskBucket,
		    logUidValue,
		    backupUid,
		    addPrefix,
		    removePrefix,
		    BinaryWriter::toValue(backupRanges, IncludeVersion(ProtocolVersion::withDRBackupRanges())),
		    tagName,
		    TaskCompletionKey::noSignal(),
		    Reference<TaskFuture>(),
		    backupAction));

		if (lockDB)
			wait(lockDatabase(tr, logUid));
		else
			wait(checkDatabaseLock(tr, logUid));

		TraceEvent("DBA_Submit")
		    .detail("LogUid", logUid)
		    .detail("Lock", lockDB)
		    .detail("LogUID", logUidValue)
		    .detail("Tag", tagName)
		    .detail("Key", backupAgent->states.get(logUidValue).pack(DatabaseBackupAgent::keyFolderId))
		    .detail("MapPrefix", mapPrefix);

		return Void();
	}

	ACTOR static Future<Void> unlockBackup(DatabaseBackupAgent* backupAgent,
	                                       Reference<ReadYourWritesTransaction> tr,
	                                       Key tagName) {
		UID logUid = wait(backupAgent->getLogUid(tr, tagName));
		wait(unlockDatabase(tr, logUid));
		TraceEvent("DBA_Unlock").detail("Tag", tagName);
		return Void();
	}

	ACTOR static Future<Void> atomicSwitchover(DatabaseBackupAgent* backupAgent,
	                                           Database dest,
	                                           Key tagName,
	                                           Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                                           Key addPrefix,
	                                           Key removePrefix,
	                                           ForceAction forceAction) {
		state DatabaseBackupAgent drAgent(dest);
		state UID destlogUid = wait(backupAgent->getLogUid(dest, tagName));
		state EBackupState status = wait(backupAgent->getStateValue(dest, destlogUid));

		TraceEvent("DBA_SwitchoverStart").detail("Status", status);
		if (status != EBackupState::STATE_RUNNING_DIFFERENTIAL && status != EBackupState::STATE_COMPLETED) {
			throw backup_duplicate();
		}

		if (!g_network->isSimulated() && !forceAction) {
			state StatusObject srcStatus = wait(StatusClient::statusFetcher(backupAgent->taskBucket->src));
			StatusObject destStatus = wait(StatusClient::statusFetcher(dest));
			checkAtomicSwitchOverConfig(srcStatus, destStatus, tagName);
		}

		state UID logUid = deterministicRandom()->randomUniqueID();
		state Key logUidValue = BinaryWriter::toValue(logUid, Unversioned());
		state UID logUidCurrent = wait(drAgent.getLogUid(backupAgent->taskBucket->src, tagName));

		if (logUidCurrent.isValid()) {
			logUid = logUidCurrent;
			logUidValue = BinaryWriter::toValue(logUid, Unversioned());
		}

		// Lock src, record commit version
		state Transaction tr(backupAgent->taskBucket->src);
		state Version commitVersion;
		loop {
			try {
				wait(lockDatabase(&tr, logUid));
				tr.set(backupAgent->tagNames.pack(tagName), logUidValue);
				wait(tr.commit());
				commitVersion = tr.getCommittedVersion();
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		TraceEvent("DBA_SwitchoverLocked").detail("Version", commitVersion);

		// Wait for the destination to apply mutations up to the lock commit before switching over.
		state ReadYourWritesTransaction tr2(dest);
		loop {
			try {
				tr2.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr2.setOption(FDBTransactionOptions::LOCK_AWARE);
				state Optional<Value> backupUid =
				    wait(tr2.get(backupAgent->states.get(BinaryWriter::toValue(destlogUid, Unversioned()))
				                     .pack(DatabaseBackupAgent::keyFolderId)));
				TraceEvent("DBA_SwitchoverBackupUID")
				    .detail("Uid", backupUid)
				    .detail("Key",
				            backupAgent->states.get(BinaryWriter::toValue(destlogUid, Unversioned()))
				                .pack(DatabaseBackupAgent::keyFolderId));
				if (!backupUid.present())
					throw backup_duplicate();
				Optional<Value> v = wait(tr2.get(
				    BinaryWriter::toValue(destlogUid, Unversioned()).withPrefix(applyMutationsBeginRange.begin)));
				TraceEvent("DBA_SwitchoverVersion")
				    .detail("Version", v.present() ? BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) : 0);
				if (v.present() && BinaryReader::fromStringRef<Version>(v.get(), Unversioned()) >= commitVersion)
					break;

				state Future<Void> versionWatch = tr2.watch(
				    BinaryWriter::toValue(destlogUid, Unversioned()).withPrefix(applyMutationsBeginRange.begin));
				wait(tr2.commit());
				wait(versionWatch);
				tr2.reset();
			} catch (Error& e) {
				wait(tr2.onError(e));
			}
		}

		TraceEvent("DBA_SwitchoverReady").log();

		try {
			wait(backupAgent->discontinueBackup(dest, tagName));
		} catch (Error& e) {
			if (e.code() != error_code_backup_duplicate && e.code() != error_code_backup_unneeded)
				throw;
		}

		wait(success(backupAgent->waitBackup(dest, tagName, StopWhenDone::True)));

		TraceEvent("DBA_SwitchoverStopped").log();

		state ReadYourWritesTransaction tr3(dest);
		loop {
			try {
				tr3.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr3.setOption(FDBTransactionOptions::LOCK_AWARE);
				Version destVersion = wait(tr3.getReadVersion());
				TraceEvent("DBA_SwitchoverVersionUpgrade").detail("Src", commitVersion).detail("Dest", destVersion);
				if (destVersion <= commitVersion) {
					TEST(true); // Forcing dest backup cluster to higher version
					tr3.set(minRequiredCommitVersionKey, BinaryWriter::toValue(commitVersion + 1, Unversioned()));
					wait(tr3.commit());
				} else {
					break;
				}
			} catch (Error& e) {
				wait(tr3.onError(e));
			}
		}

		TraceEvent("DBA_SwitchoverVersionUpgraded").log();

		try {
			wait(drAgent.submitBackup(backupAgent->taskBucket->src,
			                          tagName,
			                          backupRanges,
			                          StopWhenDone::False,
			                          addPrefix,
			                          removePrefix,
			                          LockDB::True,
			                          DatabaseBackupAgent::PreBackupAction::NONE));
		} catch (Error& e) {
			if (e.code() != error_code_backup_duplicate)
				throw;
		}

		TraceEvent("DBA_SwitchoverSubmitted").log();

		wait(success(drAgent.waitSubmitted(backupAgent->taskBucket->src, tagName)));

		TraceEvent("DBA_SwitchoverStarted").log();

		wait(backupAgent->unlockBackup(dest, tagName));

		TraceEvent("DBA_SwitchoverUnlocked").log();

		return Void();
	}

	ACTOR static Future<Void> discontinueBackup(DatabaseBackupAgent* backupAgent,
	                                            Reference<ReadYourWritesTransaction> tr,
	                                            Key tagName) {
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state UID logUid = wait(backupAgent->getLogUid(tr, tagName));
		state EBackupState status = wait(backupAgent->getStateValue(tr, logUid));

		TraceEvent("DBA_Discontinue").detail("Status", status);
		if (!DatabaseBackupAgent::isRunnable(status)) {
			throw backup_unneeded();
		}

		state Optional<Value> stopWhenDoneValue =
		    wait(tr->get(backupAgent->config.get(BinaryWriter::toValue(logUid, Unversioned()))
		                     .pack(DatabaseBackupAgent::keyConfigStopWhenDoneKey)));

		if (stopWhenDoneValue.present()) {
			throw backup_duplicate();
		}

		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->set(backupAgent->config.get(BinaryWriter::toValue(logUid, Unversioned()))
		            .pack(BackupAgentBase::keyConfigStopWhenDoneKey),
		        StringRef());

		return Void();
	}

	ACTOR static Future<Void> abortBackup(DatabaseBackupAgent* backupAgent,
	                                      Database cx,
	                                      Key tagName,
	                                      PartialBackup partial,
	                                      AbortOldBackup abortOldBackup,
	                                      DstOnly dstOnly,
	                                      WaitForDestUID waitForDestUID) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		state Key logUidValue, destUidValue;
		state UID logUid, destUid;
		state Value backupUid;

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);

				UID _logUid = wait(backupAgent->getLogUid(tr, tagName));
				logUid = _logUid;
				logUidValue = BinaryWriter::toValue(logUid, Unversioned());

				state Future<EBackupState> statusFuture = backupAgent->getStateValue(tr, logUid);
				state Future<UID> destUidFuture = backupAgent->getDestUid(tr, logUid);
				wait(success(statusFuture) && success(destUidFuture));

				EBackupState status = statusFuture.get();
				if (!backupAgent->isRunnable(status)) {
					throw backup_unneeded();
				}
				UID destUid = destUidFuture.get();
				if (destUid.isValid()) {
					destUidValue = BinaryWriter::toValue(destUid, Unversioned());
				} else if (destUidValue.size() == 0 && waitForDestUID) {
					// Give DR task a chance to update destUid to avoid the problem of
					// leftover version key. If we got an commit_unknown_result before,
					// reuse the previous destUidValue.
					throw not_committed();
				}

				Optional<Value> _backupUid =
				    wait(tr->get(backupAgent->states.get(logUidValue).pack(DatabaseBackupAgent::keyFolderId)));
				backupUid = _backupUid.get();

				// Clearing the folder id will prevent future tasks from executing
				tr->clear(backupAgent->config.get(logUidValue).range());

				// Clearing the end version of apply mutation cancels ongoing apply work
				tr->clear(logUidValue.withPrefix(applyMutationsEndRange.begin));

				tr->clear(prefixRange(logUidValue.withPrefix(applyLogKeys.begin)));

				tr->set(StringRef(backupAgent->states.get(logUidValue).pack(DatabaseBackupAgent::keyStateStatus)),
				        StringRef(DatabaseBackupAgent::getStateText(EBackupState::STATE_PARTIALLY_ABORTED)));

				wait(tr->commit());
				TraceEvent("DBA_Abort").detail("CommitVersion", tr->getCommittedVersion());
				break;
			} catch (Error& e) {
				TraceEvent("DBA_AbortError").errorUnsuppressed(e);
				wait(tr->onError(e));
			}
		}

		tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			// dumpData's commits are unstoppable, and we need to make sure that no dumpData commits
			// happen after this transaction, as it would mean that the applyMutationsBeginRange read we
			// do isn't the final value, and thus a greater version of commits could have been applied.
			// Thus, we need to commit it against the same proxy that all dumpData transactions were
			// submitted to. The transaction above will stop any further dumpData calls from adding
			// transactions to the proxy's commit promise stream, so our commit will come after all
			// dumpData transactions.
			tr->setOption(FDBTransactionOptions::COMMIT_ON_FIRST_PROXY);
			try {
				// Ensure that we're at a version higher than the data that we've written.
				Optional<Value> lastApplied = wait(tr->get(logUidValue.withPrefix(applyMutationsBeginRange.begin)));
				if (lastApplied.present()) {
					Version current = tr->getReadVersion().get();
					Version applied = BinaryReader::fromStringRef<Version>(lastApplied.get(), Unversioned());
					TraceEvent("DBA_AbortVersionUpgrade").detail("Src", applied).detail("Dest", current);
					if (current <= applied) {
						TEST(true); // Upgrading version of local database.
						// The +1 is because we want to make sure that a versionstamped operation can't reuse
						// the same version as an already-applied transaction.
						tr->set(minRequiredCommitVersionKey, BinaryWriter::toValue(applied + 1, Unversioned()));
					} else {
						// We need to enforce that the read we did of the applyMutationsBeginKey is the most
						// recent and up to date value, as the proxy might have accepted a commit previously
						// queued by dumpData after our read. Transactions that don't have write conflict ranges
						// have a no-op commit(), as they become snapshot transactions to which we don't promise
						// strict serializability.  Therefore, we add an arbitrary write conflict range to
						// request the strict serializability guarantee that is required.
						tr->addWriteConflictRange(singleKeyRange(minRequiredCommitVersionKey));
					}
				}
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		if (!dstOnly) {
			state Future<Void> partialTimeout = partial ? delay(30.0) : Never();
			state Reference<ReadYourWritesTransaction> srcTr(
			    new ReadYourWritesTransaction(backupAgent->taskBucket->src));
			state Version beginVersion;
			state Version endVersion;

			loop {
				try {
					srcTr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					srcTr->setOption(FDBTransactionOptions::LOCK_AWARE);
					state Future<Optional<Value>> backupVersionF =
					    srcTr->get(backupAgent->sourceStates.get(logUidValue).pack(DatabaseBackupAgent::keyFolderId));
					wait(success(backupVersionF) || partialTimeout);
					if (partialTimeout.isReady()) {
						return Void();
					}

					if (backupVersionF.get().present() &&
					    BinaryReader::fromStringRef<Version>(backupVersionF.get().get(), Unversioned()) >
					        BinaryReader::fromStringRef<Version>(backupUid, Unversioned())) {
						break;
					}

					if (abortOldBackup) {
						srcTr->set(backupAgent->sourceStates.pack(DatabaseBackupAgent::keyStateStatus),
						           StringRef(BackupAgentBase::getStateText(EBackupState::STATE_ABORTED)));
						srcTr->set(backupAgent->sourceStates.get(logUidValue).pack(DatabaseBackupAgent::keyFolderId),
						           backupUid);
						srcTr->clear(prefixRange(logUidValue.withPrefix(backupLogKeys.begin)));
						srcTr->clear(prefixRange(logUidValue.withPrefix(logRangesRange.begin)));
						break;
					}

					Key latestVersionKey = logUidValue.withPrefix(destUidValue.withPrefix(backupLatestVersionsPrefix));

					state Future<Optional<Key>> bVersionF = srcTr->get(latestVersionKey);
					wait(success(bVersionF) || partialTimeout);
					if (partialTimeout.isReady()) {
						return Void();
					}

					if (bVersionF.get().present()) {
						beginVersion = BinaryReader::fromStringRef<Version>(bVersionF.get().get(), Unversioned());
					} else {
						break;
					}

					srcTr->set(backupAgent->sourceStates.pack(DatabaseBackupAgent::keyStateStatus),
					           StringRef(DatabaseBackupAgent::getStateText(EBackupState::STATE_PARTIALLY_ABORTED)));
					srcTr->set(backupAgent->sourceStates.get(logUidValue).pack(DatabaseBackupAgent::keyFolderId),
					           backupUid);

					wait(eraseLogData(srcTr, logUidValue, destUidValue) || partialTimeout);
					if (partialTimeout.isReady()) {
						return Void();
					}

					wait(srcTr->commit() || partialTimeout);
					if (partialTimeout.isReady()) {
						return Void();
					}

					endVersion = srcTr->getCommittedVersion() + 1;

					break;
				} catch (Error& e) {
					wait(srcTr->onError(e));
				}
			}
		}

		tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				Optional<Value> v = wait(
				    tr->get(StringRef(backupAgent->config.get(logUidValue).pack(DatabaseBackupAgent::keyFolderId))));
				if (v.present()) {
					return Void();
				}

				tr->set(StringRef(backupAgent->states.get(logUidValue).pack(DatabaseBackupAgent::keyStateStatus)),
				        StringRef(DatabaseBackupAgent::getStateText(EBackupState::STATE_ABORTED)));

				wait(tr->commit());

				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<std::string> getStatus(DatabaseBackupAgent* backupAgent,
	                                           Database cx,
	                                           int errorLimit,
	                                           Key tagName) {
		state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state std::string statusText;
		state int retries = 0;

		loop {
			try {
				wait(success(tr->getReadVersion())); // get the read version before getting a version from the source
				                                     // database to prevent the time differential from going negative

				state Transaction scrTr(backupAgent->taskBucket->src);
				scrTr.setOption(FDBTransactionOptions::LOCK_AWARE);
				state Future<Version> srcReadVersion = scrTr.getReadVersion();

				statusText = "";

				state UID logUid = wait(backupAgent->getLogUid(tr, tagName));

				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				state Future<Optional<Value>> fPaused = tr->get(backupAgent->taskBucket->getPauseKey());
				state Future<RangeResult> fErrorValues =
				    errorLimit > 0
				        ? tr->getRange(backupAgent->errors.get(BinaryWriter::toValue(logUid, Unversioned())).range(),
				                       errorLimit,
				                       Snapshot::False,
				                       Reverse::True)
				        : Future<RangeResult>();
				state Future<Optional<Value>> fBackupUid =
				    tr->get(backupAgent->states.get(BinaryWriter::toValue(logUid, Unversioned()))
				                .pack(DatabaseBackupAgent::keyFolderId));
				state Future<Optional<Value>> fBackupVerison =
				    tr->get(BinaryWriter::toValue(logUid, Unversioned()).withPrefix(applyMutationsBeginRange.begin));
				state Future<Optional<Key>> fTagName =
				    tr->get(backupAgent->states.get(BinaryWriter::toValue(logUid, Unversioned()))
				                .pack(BackupAgentBase::keyConfigBackupTag));
				state Future<Optional<Value>> fStopVersionKey =
				    tr->get(backupAgent->states.get(BinaryWriter::toValue(logUid, Unversioned()))
				                .pack(BackupAgentBase::keyStateStop));
				state Future<Optional<Key>> fBackupKeysPacked =
				    tr->get(backupAgent->config.get(BinaryWriter::toValue(logUid, Unversioned()))
				                .pack(BackupAgentBase::keyConfigBackupRanges));
				state Future<Optional<Value>> flogVersionKey =
				    tr->get(backupAgent->states.get(BinaryWriter::toValue(logUid, Unversioned()))
				                .pack(BackupAgentBase::keyStateLogBeginVersion));

				state EBackupState backupState = wait(backupAgent->getStateValue(tr, logUid));

				if (backupState == EBackupState::STATE_NEVERRAN) {
					statusText += "No previous backups found.\n";
				} else {
					state std::string tagNameDisplay;
					Optional<Key> tagName = wait(fTagName);

					// Define the display tag name
					if (tagName.present()) {
						tagNameDisplay = tagName.get().toString();
					}

					state Optional<Value> stopVersionKey = wait(fStopVersionKey);
					Optional<Value> logVersionKey = wait(flogVersionKey);
					state std::string logVersionText =
					    ". Last log version is " +
					    (logVersionKey.present()
					         ? format("%lld", BinaryReader::fromStringRef<Version>(logVersionKey.get(), Unversioned()))
					         : "unset");
					Optional<Key> backupKeysPacked = wait(fBackupKeysPacked);

					state Standalone<VectorRef<KeyRangeRef>> backupRanges;
					if (backupKeysPacked.present()) {
						BinaryReader br(backupKeysPacked.get(), IncludeVersion());
						br >> backupRanges;
					}

					switch (backupState) {
					case EBackupState::STATE_SUBMITTED:
						statusText += "The DR on tag `" + tagNameDisplay +
						              "' is NOT a complete copy of the primary database (just started).\n";
						break;
					case EBackupState::STATE_RUNNING:
						statusText +=
						    "The DR on tag `" + tagNameDisplay + "' is NOT a complete copy of the primary database.\n";
						break;
					case EBackupState::STATE_RUNNING_DIFFERENTIAL:
						statusText += "The DR on tag `" + tagNameDisplay +
						              "' is a complete copy of the primary database" + logVersionText + ".\n";
						break;
					case EBackupState::STATE_COMPLETED: {
						Version stopVersion =
						    stopVersionKey.present()
						        ? BinaryReader::fromStringRef<Version>(stopVersionKey.get(), Unversioned())
						        : -1;
						statusText += "The previous DR on tag `" + tagNameDisplay + "' completed at version " +
						              format("%lld", stopVersion) + ".\n";
					} break;
					case EBackupState::STATE_PARTIALLY_ABORTED: {
						statusText += "The previous DR on tag `" + tagNameDisplay + "' " +
						              BackupAgentBase::getStateText(backupState) + logVersionText + ".\n";
						statusText += "Abort the DR with --cleanup before starting a new DR.\n";
						break;
					}
					default:
						statusText += "The previous DR on tag `" + tagNameDisplay + "' " +
						              BackupAgentBase::getStateText(backupState) + logVersionText + ".\n";
						break;
					}
				}

				// Append the errors, if requested
				if (errorLimit > 0) {
					RangeResult values = wait(fErrorValues);

					// Display the errors, if any
					if (values.size() > 0) {
						// Inform the user that the list of errors is complete or partial
						statusText += (values.size() < errorLimit)
						                  ? "WARNING: Some DR agents have reported issues:\n"
						                  : "WARNING: Some DR agents have reported issues (printing " +
						                        std::to_string(errorLimit) + "):\n";

						for (auto& s : values) {
							statusText += "   " + printable(s.value) + "\n";
						}
					}
				}

				// calculate time differential
				Optional<Value> backupUid = wait(fBackupUid);
				if (backupUid.present()) {
					Optional<Value> v = wait(fBackupVerison);
					if (v.present()) {
						state Version destApplyBegin = BinaryReader::fromStringRef<Version>(v.get(), Unversioned());
						Version sourceVersion = wait(srcReadVersion);
						double secondsBehind =
						    ((double)(sourceVersion - destApplyBegin)) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
						statusText += format("\nThe DR is %.6f seconds behind.\n", secondsBehind);
					}
				}

				Optional<Value> paused = wait(fPaused);
				if (paused.present()) {
					statusText += format("\nAll DR agents have been paused.\n");
				}

				break;
			} catch (Error& e) {
				retries++;
				if (retries > 5) {
					statusText += format("\nWARNING: Could not fetch full DR status: %s\n", e.name());
					return statusText;
				}
				wait(tr->onError(e));
			}
		}

		return statusText;
	}

	ACTOR static Future<EBackupState> getStateValue(DatabaseBackupAgent* backupAgent,
	                                                Reference<ReadYourWritesTransaction> tr,
	                                                UID logUid,
	                                                Snapshot snapshot) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Key statusKey = backupAgent->states.get(BinaryWriter::toValue(logUid, Unversioned()))
		                          .pack(DatabaseBackupAgent::keyStateStatus);
		Optional<Value> status = wait(tr->get(statusKey, snapshot));

		return (!status.present()) ? EBackupState::STATE_NEVERRAN : BackupAgentBase::getState(status.get().toString());
	}

	ACTOR static Future<UID> getDestUid(DatabaseBackupAgent* backupAgent,
	                                    Reference<ReadYourWritesTransaction> tr,
	                                    UID logUid,
	                                    Snapshot snapshot) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Key destUidKey =
		    backupAgent->config.get(BinaryWriter::toValue(logUid, Unversioned())).pack(BackupAgentBase::destUid);
		Optional<Value> destUid = wait(tr->get(destUidKey, snapshot));

		return (destUid.present()) ? BinaryReader::fromStringRef<UID>(destUid.get(), Unversioned()) : UID();
	}

	ACTOR static Future<UID> getLogUid(DatabaseBackupAgent* backupAgent,
	                                   Reference<ReadYourWritesTransaction> tr,
	                                   Key tagName,
	                                   Snapshot snapshot) {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		state Optional<Value> logUid = wait(tr->get(backupAgent->tagNames.pack(tagName), snapshot));

		return (logUid.present()) ? BinaryReader::fromStringRef<UID>(logUid.get(), Unversioned()) : UID();
	}
};

Future<Void> DatabaseBackupAgent::unlockBackup(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return DatabaseBackupAgentImpl::unlockBackup(this, tr, tagName);
}

Future<Void> DatabaseBackupAgent::atomicSwitchover(Database dest,
                                                   Key tagName,
                                                   Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                                   Key addPrefix,
                                                   Key removePrefix,
                                                   ForceAction forceAction) {
	return DatabaseBackupAgentImpl::atomicSwitchover(
	    this, dest, tagName, backupRanges, addPrefix, removePrefix, forceAction);
}

Future<Void> DatabaseBackupAgent::submitBackup(Reference<ReadYourWritesTransaction> tr,
                                               Key tagName,
                                               Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                               StopWhenDone stopWhenDone,
                                               Key addPrefix,
                                               Key removePrefix,
                                               LockDB lockDatabase,
                                               PreBackupAction backupAction) {
	return DatabaseBackupAgentImpl::submitBackup(
	    this, tr, tagName, backupRanges, stopWhenDone, addPrefix, removePrefix, lockDatabase, backupAction);
}

Future<Void> DatabaseBackupAgent::discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName) {
	return DatabaseBackupAgentImpl::discontinueBackup(this, tr, tagName);
}

Future<Void> DatabaseBackupAgent::abortBackup(Database cx,
                                              Key tagName,
                                              PartialBackup partial,
                                              AbortOldBackup abortOldBackup,
                                              DstOnly dstOnly,
                                              WaitForDestUID waitForDestUID) {
	return DatabaseBackupAgentImpl::abortBackup(this, cx, tagName, partial, abortOldBackup, dstOnly, waitForDestUID);
}

Future<std::string> DatabaseBackupAgent::getStatus(Database cx, int errorLimit, Key tagName) {
	return DatabaseBackupAgentImpl::getStatus(this, cx, errorLimit, tagName);
}

Future<EBackupState> DatabaseBackupAgent::getStateValue(Reference<ReadYourWritesTransaction> tr,
                                                        UID logUid,
                                                        Snapshot snapshot) {
	return DatabaseBackupAgentImpl::getStateValue(this, tr, logUid, snapshot);
}

Future<UID> DatabaseBackupAgent::getDestUid(Reference<ReadYourWritesTransaction> tr, UID logUid, Snapshot snapshot) {
	return DatabaseBackupAgentImpl::getDestUid(this, tr, logUid, snapshot);
}

Future<UID> DatabaseBackupAgent::getLogUid(Reference<ReadYourWritesTransaction> tr, Key tagName, Snapshot snapshot) {
	return DatabaseBackupAgentImpl::getLogUid(this, tr, tagName, snapshot);
}

Future<Void> DatabaseBackupAgent::waitUpgradeToLatestDrVersion(Database cx, Key tagName) {
	return DatabaseBackupAgentImpl::waitUpgradeToLatestDrVersion(this, cx, tagName);
}

Future<EBackupState> DatabaseBackupAgent::waitBackup(Database cx, Key tagName, StopWhenDone stopWhenDone) {
	return DatabaseBackupAgentImpl::waitBackup(this, cx, tagName, stopWhenDone);
}

Future<EBackupState> DatabaseBackupAgent::waitSubmitted(Database cx, Key tagName) {
	return DatabaseBackupAgentImpl::waitSubmitted(this, cx, tagName);
}

Future<int64_t> DatabaseBackupAgent::getRangeBytesWritten(Reference<ReadYourWritesTransaction> tr,
                                                          UID logUid,
                                                          Snapshot snapshot) {
	return DRConfig(logUid).rangeBytesWritten().getD(tr, snapshot);
}

Future<int64_t> DatabaseBackupAgent::getLogBytesWritten(Reference<ReadYourWritesTransaction> tr,
                                                        UID logUid,
                                                        Snapshot snapshot) {
	return DRConfig(logUid).logBytesWritten().getD(tr, snapshot);
}
