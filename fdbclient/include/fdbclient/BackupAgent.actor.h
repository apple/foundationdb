/*
 * BackupAgent.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_BACKUP_AGENT_ACTOR_G_H)
#define FDBCLIENT_BACKUP_AGENT_ACTOR_G_H
#include "fdbclient/BackupAgent.actor.g.h"
#elif !defined(FDBCLIENT_BACKUP_AGENT_ACTOR_H)
#define FDBCLIENT_BACKUP_AGENT_ACTOR_H

#include "flow/flow.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/TaskBucket.h"
#include "fdbclient/Notified.h"
#include "flow/IAsyncFile.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include <ctime>
#include <climits>
#include "fdbclient/BackupContainer.h"
#include "flow/actorcompiler.h" // has to be last include

FDB_BOOLEAN_PARAM(LockDB);
FDB_BOOLEAN_PARAM(UnlockDB);
FDB_BOOLEAN_PARAM(StopWhenDone);
FDB_BOOLEAN_PARAM(Verbose);
FDB_BOOLEAN_PARAM(WaitForComplete);
FDB_BOOLEAN_PARAM(ForceAction);
FDB_BOOLEAN_PARAM(Terminator);
FDB_BOOLEAN_PARAM(IncrementalBackupOnly);
FDB_BOOLEAN_PARAM(UsePartitionedLog);
FDB_BOOLEAN_PARAM(OnlyApplyMutationLogs);
FDB_BOOLEAN_PARAM(SnapshotBackupUseTenantCache);
FDB_BOOLEAN_PARAM(InconsistentSnapshotOnly);
FDB_BOOLEAN_PARAM(ShowErrors);
FDB_BOOLEAN_PARAM(AbortOldBackup);
FDB_BOOLEAN_PARAM(DstOnly); // TODO: More descriptive name?
FDB_BOOLEAN_PARAM(WaitForDestUID);
FDB_BOOLEAN_PARAM(CheckBackupUID);
FDB_BOOLEAN_PARAM(DeleteData);
FDB_BOOLEAN_PARAM(SetValidation);
FDB_BOOLEAN_PARAM(PartialBackup);

extern Optional<std::string> fileBackupAgentProxy;

class BackupAgentBase : NonCopyable {
public:
	// Time formatter for anything backup or restore related
	static std::string formatTime(int64_t epochs);
	static int64_t parseTime(std::string timestamp);

	static std::string timeFormat() { return "YYYY/MM/DD.HH:MI:SS[+/-]HHMM"; }

	enum class EnumState {
		STATE_ERRORED = 0,
		STATE_SUBMITTED = 1,
		STATE_RUNNING = 2,
		STATE_RUNNING_DIFFERENTIAL = 3,
		STATE_COMPLETED = 4,
		STATE_NEVERRAN = 5,
		STATE_ABORTED = 6,
		STATE_PARTIALLY_ABORTED = 7
	};

	static const Key keyFolderId;
	static const Key keyBeginVersion;
	static const Key keyEndVersion;
	static const Key keyPrevBeginVersion;
	static const Key keyConfigBackupTag;
	static const Key keyConfigLogUid;
	static const Key keyConfigBackupRanges;
	static const Key keyConfigStopWhenDoneKey;
	static const Key keyStateStatus;
	static const Key keyStateStop;
	static const Key keyStateLogBeginVersion;
	static const Key keyLastUid;
	static const Key keyBeginKey;
	static const Key keyEndKey;
	static const Key keyDrVersion;
	static const Key destUid;
	static const Key backupStartVersion;

	static const Key keyTagName;
	static const Key keyStates;
	static const Key keyConfig;
	static const Key keyErrors;
	static const Key keyRanges;
	static const Key keyTasks;
	static const Key keyFutures;
	static const Key keySourceStates;
	static const Key keySourceTagName;

	static constexpr int logHeaderSize = 12;

	// Convert the status text to an enumerated value
	static EnumState getState(std::string const& stateText);

	// Convert the status enum to a text description
	static const char* getStateText(EnumState enState);

	// Convert the status enum to a name
	static const char* getStateName(EnumState enState);

	// Determine if the specified state is runnable
	static bool isRunnable(EnumState enState);

	static KeyRef getDefaultTag() { return StringRef(defaultTagName); }

	static std::string getDefaultTagName() { return defaultTagName; }

	// This is only used for automatic backup name generation
	static Standalone<StringRef> getCurrentTime();

protected:
	static const std::string defaultTagName;
};

class FileBackupAgent : public BackupAgentBase {
public:
	FileBackupAgent();

	FileBackupAgent(FileBackupAgent&& r) noexcept
	  : subspace(std::move(r.subspace)), config(std::move(r.config)), lastRestorable(std::move(r.lastRestorable)),
	    taskBucket(std::move(r.taskBucket)), futureBucket(std::move(r.futureBucket)) {}

	void operator=(FileBackupAgent&& r) noexcept {
		subspace = std::move(r.subspace);
		config = std::move(r.config);
		lastRestorable = std::move(r.lastRestorable), taskBucket = std::move(r.taskBucket);
		futureBucket = std::move(r.futureBucket);
	}

	KeyBackedProperty<Key> lastBackupTimestamp() { return config.pack(__FUNCTION__sr); }

	Future<Void> run(Database cx, double pollDelay, int maxConcurrentTasks) {
		return taskBucket->run(cx, futureBucket, std::make_shared<double const>(pollDelay), maxConcurrentTasks);
	}

	Future<Void> run(Database cx, std::shared_ptr<double const> pollDelay, int maxConcurrentTasks) {
		return taskBucket->run(cx, futureBucket, pollDelay, maxConcurrentTasks);
	}

	/** RESTORE **/

	enum ERestoreState { UNINITIALIZED = 0, QUEUED = 1, STARTING = 2, RUNNING = 3, COMPLETED = 4, ABORTED = 5 };
	static StringRef restoreStateText(ERestoreState id);
	static Key getPauseKey();

	// parallel restore
	Future<Void> parallelRestoreFinish(Database cx, UID randomUID, UnlockDB = UnlockDB::True);
	Future<Void> submitParallelRestore(Database cx,
	                                   Key backupTag,
	                                   Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                                   Key bcUrl,
	                                   Optional<std::string> proxy,
	                                   Version targetVersion,
	                                   LockDB lockDB,
	                                   UID randomUID,
	                                   Key addPrefix,
	                                   Key removePrefix);
	Future<Void> atomicParallelRestore(Database cx,
	                                   Key tagName,
	                                   Standalone<VectorRef<KeyRangeRef>> ranges,
	                                   Key addPrefix,
	                                   Key removePrefix);

	// restore() will
	//   - make sure that url is readable and appears to be a complete backup
	//   - make sure the requested TargetVersion is valid
	//   - submit a restore on the given tagName
	//   - Optionally wait for the restore's completion.  Will restore_error if restore fails or is aborted.
	// restore() will return the targetVersion which will be either the valid version passed in or the max restorable
	// version for the given url.
	Future<Version> restore(Database cx,
	                        Optional<Database> cxOrig,
	                        Key tagName,
	                        Key url,
	                        Optional<std::string> proxy,
	                        Standalone<VectorRef<KeyRangeRef>> ranges,
	                        Standalone<VectorRef<Version>> beginVersions,
	                        WaitForComplete = WaitForComplete::True,
	                        Version targetVersion = ::invalidVersion,
	                        Verbose = Verbose::True,
	                        Key addPrefix = Key(),
	                        Key removePrefix = Key(),
	                        LockDB = LockDB::True,
	                        UnlockDB = UnlockDB::True,
	                        OnlyApplyMutationLogs = OnlyApplyMutationLogs::False,
	                        InconsistentSnapshotOnly = InconsistentSnapshotOnly::False,
	                        Optional<std::string> const& encryptionKeyFileName = {},
	                        Optional<std::string> blobManifestUrl = {});

	Future<Version> restore(Database cx,
	                        Optional<Database> cxOrig,
	                        Key tagName,
	                        Key url,
	                        Optional<std::string> proxy,
	                        WaitForComplete = WaitForComplete::True,
	                        Version targetVersion = ::invalidVersion,
	                        Verbose = Verbose::True,
	                        KeyRange range = KeyRange(),
	                        Key addPrefix = Key(),
	                        Key removePrefix = Key(),
	                        LockDB = LockDB::True,
	                        OnlyApplyMutationLogs = OnlyApplyMutationLogs::False,
	                        InconsistentSnapshotOnly = InconsistentSnapshotOnly::False,
	                        Version beginVersion = ::invalidVersion,
	                        Optional<std::string> const& encryptionKeyFileName = {},
	                        Optional<std::string> blobManifestUrl = {});

	Future<Version> restore(Database cx,
	                        Optional<Database> cxOrig,
	                        Key tagName,
	                        Key url,
	                        Optional<std::string> proxy,
	                        Standalone<VectorRef<KeyRangeRef>> ranges,
	                        WaitForComplete waitForComplete = WaitForComplete::True,
	                        Version targetVersion = ::invalidVersion,
	                        Verbose verbose = Verbose::True,
	                        Key addPrefix = Key(),
	                        Key removePrefix = Key(),
	                        LockDB lockDB = LockDB::True,
	                        UnlockDB unlockDB = UnlockDB::True,
	                        OnlyApplyMutationLogs onlyApplyMutationLogs = OnlyApplyMutationLogs::False,
	                        InconsistentSnapshotOnly inconsistentSnapshotOnly = InconsistentSnapshotOnly::False,
	                        Version beginVersion = ::invalidVersion,
	                        Optional<std::string> const& encryptionKeyFileName = {},
	                        Optional<std::string> blobManifestUrl = {});

	Future<Version> atomicRestore(Database cx,
	                              Key tagName,
	                              Standalone<VectorRef<KeyRangeRef>> ranges,
	                              Key addPrefix = Key(),
	                              Key removePrefix = Key());
	Future<Version> atomicRestore(Database cx,
	                              Key tagName,
	                              KeyRange range = KeyRange(),
	                              Key addPrefix = Key(),
	                              Key removePrefix = Key());

	// Tries to abort the restore for a tag.  Returns the final (stable) state of the tag.
	Future<ERestoreState> abortRestore(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<ERestoreState> abortRestore(Database cx, Key tagName);

	// Waits for a restore tag to reach a final (stable) state.
	Future<ERestoreState> waitRestore(Database cx, Key tagName, Verbose);

	// Get a string describing the status of a tag
	Future<std::string> restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<std::string> restoreStatus(Database cx, Key tagName) {
		return runRYWTransaction(cx,
		                         [=](Reference<ReadYourWritesTransaction> tr) { return restoreStatus(tr, tagName); });
	}

	/** BACKUP METHODS **/

	Future<Void> submitBackup(Reference<ReadYourWritesTransaction> tr,
	                          Key outContainer,
	                          Optional<std::string> proxy,
	                          int initialSnapshotIntervalSeconds,
	                          int snapshotIntervalSeconds,
	                          std::string const& tagName,
	                          Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                          bool encryptionEnabled,
	                          StopWhenDone = StopWhenDone::True,
	                          UsePartitionedLog = UsePartitionedLog::False,
	                          IncrementalBackupOnly = IncrementalBackupOnly::False,
	                          Optional<std::string> const& encryptionKeyFileName = {},
	                          Optional<std::string> const& blobManifestUrl = {});
	Future<Void> submitBackup(Database cx,
	                          Key outContainer,
	                          Optional<std::string> proxy,
	                          int initialSnapshotIntervalSeconds,
	                          int snapshotIntervalSeconds,
	                          std::string const& tagName,
	                          Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                          bool encryptionEnabled,
	                          StopWhenDone stopWhenDone = StopWhenDone::True,
	                          UsePartitionedLog partitionedLog = UsePartitionedLog::False,
	                          IncrementalBackupOnly incrementalBackupOnly = IncrementalBackupOnly::False,
	                          Optional<std::string> const& encryptionKeyFileName = {},
	                          Optional<std::string> const& blobManifestUrl = {}) {
		return runRYWTransactionFailIfLocked(cx, [=](Reference<ReadYourWritesTransaction> tr) {
			return submitBackup(tr,
			                    outContainer,
			                    proxy,
			                    initialSnapshotIntervalSeconds,
			                    snapshotIntervalSeconds,
			                    tagName,
			                    backupRanges,
			                    encryptionEnabled,
			                    stopWhenDone,
			                    partitionedLog,
			                    incrementalBackupOnly,
			                    encryptionKeyFileName,
			                    blobManifestUrl);
		});
	}

	Future<Void> discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<Void> discontinueBackup(Database cx, Key tagName) {
		return runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return discontinueBackup(tr, tagName); });
	}

	// Terminate an ongoing backup, without waiting for the backup to finish.
	// Preconditions:
	//   A backup is running with the tag of `tagName`.
	//     Otherwise `backup_unneeded` will be thrown indicating that the backup never existed or already finished.
	// Postconditions:
	//   No more tasks will be spawned to backup ranges of the database.
	//   logRangesRange and backupLogKeys will be cleared for this backup.
	Future<Void> abortBackup(Reference<ReadYourWritesTransaction> tr, std::string tagName);
	Future<Void> abortBackup(Database cx, std::string tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return abortBackup(tr, tagName); });
	}

	Future<std::string> getStatus(Database cx, ShowErrors, std::string tagName);
	Future<std::string> getStatusJSON(Database cx, std::string tagName);

	Future<Optional<Version>> getLastRestorable(Reference<ReadYourWritesTransaction> tr,
	                                            Key tagName,
	                                            Snapshot = Snapshot::False);
	void setLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName, Version version);

	// stopWhenDone will return when the backup is stopped, if enabled. Otherwise, it
	// will return when the backup directory is restorable.
	Future<EnumState> waitBackup(Database cx,
	                             std::string tagName,
	                             StopWhenDone = StopWhenDone::True,
	                             Reference<IBackupContainer>* pContainer = nullptr,
	                             UID* pUID = nullptr);

	static const Key keyLastRestorable;

	Future<int64_t> getTaskCount(Reference<ReadYourWritesTransaction> tr) { return taskBucket->getTaskCount(tr); }
	Future<int64_t> getTaskCount(Database cx) { return taskBucket->getTaskCount(cx); }
	Future<Void> watchTaskCount(Reference<ReadYourWritesTransaction> tr) { return taskBucket->watchTaskCount(tr); }

	Future<bool> checkActive(Database cx) { return taskBucket->checkActive(cx); }

	// If "pause" is true, pause all backups; otherwise, resume all.
	Future<Void> changePause(Database db, bool pause);

	friend class FileBackupAgentImpl;
	static const int dataFooterSize;

	Subspace subspace;
	Subspace config;
	Subspace lastRestorable;

	Reference<TaskBucket> taskBucket;
	Reference<FutureBucket> futureBucket;
};

template <>
inline Standalone<StringRef> TupleCodec<FileBackupAgent::ERestoreState>::pack(
    FileBackupAgent::ERestoreState const& val) {
	return Tuple::makeTuple(static_cast<int>(val)).pack();
}
template <>
inline FileBackupAgent::ERestoreState TupleCodec<FileBackupAgent::ERestoreState>::unpack(
    Standalone<StringRef> const& val) {
	return (FileBackupAgent::ERestoreState)Tuple::unpack(val).getInt(0);
}

class DatabaseBackupAgent : public BackupAgentBase {
public:
	DatabaseBackupAgent();
	explicit DatabaseBackupAgent(Database src);

	DatabaseBackupAgent(DatabaseBackupAgent&& r) noexcept
	  : subspace(std::move(r.subspace)), states(std::move(r.states)), config(std::move(r.config)),
	    errors(std::move(r.errors)), ranges(std::move(r.ranges)), tagNames(std::move(r.tagNames)),
	    sourceStates(std::move(r.sourceStates)), sourceTagNames(std::move(r.sourceTagNames)),
	    taskBucket(std::move(r.taskBucket)), futureBucket(std::move(r.futureBucket)) {}

	void operator=(DatabaseBackupAgent&& r) noexcept {
		subspace = std::move(r.subspace);
		states = std::move(r.states);
		config = std::move(r.config);
		errors = std::move(r.errors);
		ranges = std::move(r.ranges);
		tagNames = std::move(r.tagNames);
		taskBucket = std::move(r.taskBucket);
		futureBucket = std::move(r.futureBucket);
		sourceStates = std::move(r.sourceStates);
		sourceTagNames = std::move(r.sourceTagNames);
	}

	Future<Void> run(Database cx, double pollDelay, int maxConcurrentTasks) {
		return taskBucket->run(cx, futureBucket, std::make_shared<double const>(pollDelay), maxConcurrentTasks);
	}

	Future<Void> run(Database cx, std::shared_ptr<double const> pollDelay, int maxConcurrentTasks) {
		return taskBucket->run(cx, futureBucket, pollDelay, maxConcurrentTasks);
	}

	Future<Void> atomicSwitchover(Database dest,
	                              Key tagName,
	                              Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                              Key addPrefix,
	                              Key removePrefix,
	                              ForceAction = ForceAction::False);

	Future<Void> unlockBackup(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<Void> unlockBackup(Database cx, Key tagName) {
		return runRYWTransaction(cx,
		                         [=](Reference<ReadYourWritesTransaction> tr) { return unlockBackup(tr, tagName); });
	}

	// Specifies the action to take on the backup's destination key range
	// before the backup begins.
	enum PreBackupAction {
		NONE = 0, // No action is taken
		VERIFY = 1, // Verify the key range being restored to is empty.
		CLEAR = 2 // Clear the key range being restored to.
	};

	Future<Void> submitBackup(Reference<ReadYourWritesTransaction> tr,
	                          Key tagName,
	                          Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                          StopWhenDone = StopWhenDone::True,
	                          Key addPrefix = StringRef(),
	                          Key removePrefix = StringRef(),
	                          LockDB lockDatabase = LockDB::False,
	                          PreBackupAction backupAction = PreBackupAction::VERIFY);
	Future<Void> submitBackup(Database cx,
	                          Key tagName,
	                          Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                          StopWhenDone stopWhenDone = StopWhenDone::True,
	                          Key addPrefix = StringRef(),
	                          Key removePrefix = StringRef(),
	                          LockDB lockDatabase = LockDB::False,
	                          PreBackupAction backupAction = PreBackupAction::VERIFY) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) {
			return submitBackup(
			    tr, tagName, backupRanges, stopWhenDone, addPrefix, removePrefix, lockDatabase, backupAction);
		});
	}

	Future<Void> discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<Void> discontinueBackup(Database cx, Key tagName) {
		return runRYWTransaction(
		    cx, [=](Reference<ReadYourWritesTransaction> tr) { return discontinueBackup(tr, tagName); });
	}

	Future<Void> abortBackup(Database cx,
	                         Key tagName,
	                         PartialBackup = PartialBackup::False,
	                         AbortOldBackup = AbortOldBackup::False,
	                         DstOnly = DstOnly::False,
	                         WaitForDestUID = WaitForDestUID::False);

	Future<std::string> getStatus(Database cx, int errorLimit, Key tagName);

	Future<EnumState> getStateValue(Reference<ReadYourWritesTransaction> tr, UID logUid, Snapshot = Snapshot::False);
	Future<EnumState> getStateValue(Database cx, UID logUid) {
		return runRYWTransaction(cx,
		                         [=](Reference<ReadYourWritesTransaction> tr) { return getStateValue(tr, logUid); });
	}

	Future<UID> getDestUid(Reference<ReadYourWritesTransaction> tr, UID logUid, Snapshot = Snapshot::False);
	Future<UID> getDestUid(Database cx, UID logUid) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return getDestUid(tr, logUid); });
	}

	Future<UID> getLogUid(Reference<ReadYourWritesTransaction> tr, Key tagName, Snapshot = Snapshot::False);
	Future<UID> getLogUid(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return getLogUid(tr, tagName); });
	}

	Future<int64_t> getRangeBytesWritten(Reference<ReadYourWritesTransaction> tr,
	                                     UID logUid,
	                                     Snapshot = Snapshot::False);
	Future<int64_t> getLogBytesWritten(Reference<ReadYourWritesTransaction> tr, UID logUid, Snapshot = Snapshot::False);
	// stopWhenDone will return when the backup is stopped, if enabled. Otherwise, it
	// will return when the backup directory is restorable.
	Future<EnumState> waitBackup(Database cx, Key tagName, StopWhenDone = StopWhenDone::True);
	Future<EnumState> waitSubmitted(Database cx, Key tagName);
	Future<Void> waitUpgradeToLatestDrVersion(Database cx, Key tagName);

	static const Key keyAddPrefix;
	static const Key keyRemovePrefix;
	static const Key keyRangeVersions;
	static const Key keyCopyStop;
	static const Key keyDatabasesInSync;
	static const int LATEST_DR_VERSION;

	Future<int64_t> getTaskCount(Reference<ReadYourWritesTransaction> tr) { return taskBucket->getTaskCount(tr); }
	Future<int64_t> getTaskCount(Database cx) { return taskBucket->getTaskCount(cx); }
	Future<Void> watchTaskCount(Reference<ReadYourWritesTransaction> tr) { return taskBucket->watchTaskCount(tr); }

	Future<bool> checkActive(Database cx) { return taskBucket->checkActive(cx); }

	friend class DatabaseBackupAgentImpl;

	Subspace subspace;
	Subspace states;
	Subspace config;
	Subspace errors;
	Subspace ranges;
	Subspace tagNames;
	Subspace sourceStates;
	Subspace sourceTagNames;

	Reference<TaskBucket> taskBucket;
	Reference<FutureBucket> futureBucket;
};

using RangeResultWithVersion = std::pair<RangeResult, Version>;

struct RCGroup {
	RangeResult items;
	Version version;
	uint64_t groupKey;

	RCGroup() : version(-1), groupKey(ULLONG_MAX){};

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, items, version, groupKey);
	}
};

bool copyParameter(Reference<Task> source, Reference<Task> dest, Key key);
Version getVersionFromString(std::string const& value);
Standalone<VectorRef<KeyRangeRef>> getLogRanges(Version beginVersion,
                                                Version endVersion,
                                                Key destUidValue,
                                                int blockSize = CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
Standalone<VectorRef<KeyRangeRef>> getApplyRanges(Version beginVersion, Version endVersion, Key backupUid);
Future<Void> eraseLogData(Reference<ReadYourWritesTransaction> tr,
                          Key logUidValue,
                          Key destUidValue,
                          Optional<Version> endVersion = Optional<Version>(),
                          CheckBackupUID = CheckBackupUID::False,
                          Version backupUid = 0);
Key getApplyKey(Version version, Key backupUid);
Key getLogKey(Version version, Key backupUid, int blockSize = CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
Version getLogKeyVersion(Key key);
std::pair<Version, uint32_t> decodeBKMutationLogKey(Key key);
Future<Void> logError(Database cx, Key keyErrors, const std::string& message);
Future<Void> logError(Reference<ReadYourWritesTransaction> tr, Key keyErrors, const std::string& message);
Future<Void> checkVersion(Reference<ReadYourWritesTransaction> const& tr);
ACTOR Future<Void> readCommitted(Database cx,
                                 PromiseStream<RangeResultWithVersion> results,
                                 Reference<FlowLock> lock,
                                 KeyRangeRef range,
                                 Terminator terminator = Terminator::True,
                                 AccessSystemKeys systemAccess = AccessSystemKeys::False,
                                 LockAware lockAware = LockAware::False);
ACTOR Future<Void> readCommitted(Database cx,
                                 PromiseStream<RCGroup> results,
                                 Future<Void> active,
                                 Reference<FlowLock> lock,
                                 KeyRangeRef range,
                                 std::function<std::pair<uint64_t, uint32_t>(Key key)> groupBy,
                                 Terminator terminator = Terminator::True,
                                 AccessSystemKeys systemAccess = AccessSystemKeys::False,
                                 LockAware lockAware = LockAware::False);
ACTOR Future<Void> applyMutations(Database cx,
                                  Key uid,
                                  Key addPrefix,
                                  Key removePrefix,
                                  Version beginVersion,
                                  Version* endVersion,
                                  PublicRequestStream<CommitTransactionRequest> commit,
                                  NotifiedVersion* committedVersion,
                                  Reference<KeyRangeMap<Version>> keyVersion,
                                  std::map<int64_t, TenantName>* tenantMap,
                                  bool provisionalProxy);
ACTOR Future<Void> cleanupBackup(Database cx, DeleteData deleteData);

using EBackupState = BackupAgentBase::EnumState;
template <>
inline Standalone<StringRef> TupleCodec<EBackupState>::pack(EBackupState const& val) {
	return Tuple::makeTuple(static_cast<int>(val)).pack();
}
template <>
inline EBackupState TupleCodec<EBackupState>::unpack(Standalone<StringRef> const& val) {
	return static_cast<EBackupState>(Tuple::unpack(val).getInt(0));
}

// Key backed tags are a single-key slice of the TagUidMap, defined below.
// The Value type of the key is a UidAndAbortedFlagT which is a pair of {UID, aborted_flag}
// All tasks on the UID will have a validation key/value that requires aborted_flag to be
// false, so changing that value, such as changing the UID or setting aborted_flag to true,
// will kill all of the active tasks on that backup/restore UID.
typedef std::pair<UID, bool> UidAndAbortedFlagT;
class KeyBackedTag : public KeyBackedProperty<UidAndAbortedFlagT> {
public:
	KeyBackedTag() : KeyBackedProperty(StringRef()) {}
	KeyBackedTag(std::string tagName, StringRef tagMapPrefix);

	Future<Void> cancel(Reference<ReadYourWritesTransaction> tr) {
		std::string tag = tagName;
		Key _tagMapPrefix = tagMapPrefix;
		return map(get(tr), [tag, _tagMapPrefix, tr](Optional<UidAndAbortedFlagT> up) -> Void {
			if (up.present()) {
				// Set aborted flag to true
				up.get().second = true;
				KeyBackedTag(tag, _tagMapPrefix).set(tr, up.get());
			}
			return Void();
		});
	}

	std::string tagName;
	Key tagMapPrefix;
};

typedef KeyBackedMap<std::string, UidAndAbortedFlagT> TagMap;
// Map of tagName to {UID, aborted_flag} located in the fileRestorePrefixRange keyspace.
class TagUidMap : public KeyBackedMap<std::string, UidAndAbortedFlagT> {
	ACTOR static Future<std::vector<KeyBackedTag>> getAll_impl(TagUidMap* tagsMap,
	                                                           Reference<ReadYourWritesTransaction> tr,
	                                                           Snapshot snapshot);

public:
	TagUidMap(const StringRef& prefix) : TagMap("tag->uid/"_sr.withPrefix(prefix)), prefix(prefix) {}

	Future<std::vector<KeyBackedTag>> getAll(Reference<ReadYourWritesTransaction> tr,
	                                         Snapshot snapshot = Snapshot::False) {
		return getAll_impl(this, tr, snapshot);
	}

	Key prefix;
};

class KeyBackedTaskConfig : public KeyBackedClass {
protected:
	UID uid;
	Subspace configSpace;

public:
	static struct {
		static TaskParam<UID> uid() { return __FUNCTION__sr; }
	} TaskParams;

	KeyBackedTaskConfig(StringRef prefix, UID uid = UID())
	  : KeyBackedClass(prefix), uid(uid), configSpace(uidPrefixKey("uid->config/"_sr.withPrefix(prefix), uid)) {}

	KeyBackedTaskConfig(StringRef prefix, Reference<Task> task)
	  : KeyBackedTaskConfig(prefix, TaskParams.uid().get(task)) {}

	KeyBackedProperty<std::string> tag() { return configSpace.pack(__FUNCTION__sr); }

	UID getUid() { return uid; }

	Key getUidAsKey() { return BinaryWriter::toValue(uid, Unversioned()); }

	template <class TrType>
	void clear(TrType tr) {
		tr->clear(configSpace.range());
	}

	// lastError is a pair of error message and timestamp expressed as an int64_t
	KeyBackedProperty<std::pair<std::string, Version>> lastError() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedMap<int64_t, std::pair<std::string, Version>> lastErrorPerType() {
		return configSpace.pack(__FUNCTION__sr);
	}

	Future<Void> toTask(Reference<ReadYourWritesTransaction> tr,
	                    Reference<Task> task,
	                    SetValidation setValidation = SetValidation::True) {
		// Set the uid task parameter
		TaskParams.uid().set(task, uid);

		if (!setValidation) {
			return Void();
		}

		// Set the validation condition for the task which is that the restore uid's tag's uid is the same as the
		// restore uid. Get this uid's tag, then get the KEY for the tag's uid but don't read it.  That becomes the
		// validation key which TaskBucket will check, and its value must be this restore config's uid.
		UID u = uid; // 'this' could be invalid in lambda
		Key p = subspace.key();
		return map(tag().get(tr), [u, p, task](Optional<std::string> const& tag) -> Void {
			if (!tag.present())
				throw restore_error();
			// Validation condition is that the uidPair key must be exactly {u, false}
			TaskBucket::setValidationCondition(
			    task, KeyBackedTag(tag.get(), p).key, TupleCodec<UidAndAbortedFlagT>::pack({ u, false }));
			return Void();
		});
	}

	// Updates the error per type map and the last error property
	Future<Void> updateErrorInfo(Database cx, Error e, std::string message) {
		// Avoid capture of this ptr
		auto& copy = *this;

		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) mutable {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return map(tr->getReadVersion(), [=](Version v) mutable {
				copy.lastError().set(tr, { message, v });
				copy.lastErrorPerType().set(tr, e.code(), { message, v });
				return Void();
			});
		});
	}
};

static inline KeyBackedTag makeRestoreTag(std::string tagName) {
	return KeyBackedTag(tagName, fileRestorePrefixRange.begin);
}

static inline KeyBackedTag makeBackupTag(std::string tagName) {
	return KeyBackedTag(tagName, fileBackupPrefixRange.begin);
}

static inline Future<std::vector<KeyBackedTag>> getAllRestoreTags(Reference<ReadYourWritesTransaction> tr,
                                                                  Snapshot snapshot = Snapshot::False) {
	return TagUidMap(fileRestorePrefixRange.begin).getAll(tr, snapshot);
}

static inline Future<std::vector<KeyBackedTag>> getAllBackupTags(Reference<ReadYourWritesTransaction> tr,
                                                                 Snapshot snapshot = Snapshot::False) {
	return TagUidMap(fileBackupPrefixRange.begin).getAll(tr, snapshot);
}

template <>
inline Standalone<StringRef> TupleCodec<Reference<IBackupContainer>>::pack(Reference<IBackupContainer> const& bc) {
	Tuple tuple = Tuple::makeTuple(bc->getURL());

	if (bc->getEncryptionKeyFileName().present()) {
		tuple.append(bc->getEncryptionKeyFileName().get());
	} else {
		tuple.append(StringRef());
	}

	if (bc->getProxy().present()) {
		tuple.append(StringRef(bc->getProxy().get()));
	} else {
		tuple.append(StringRef());
	}

	return tuple.pack();
}
template <>
inline Reference<IBackupContainer> TupleCodec<Reference<IBackupContainer>>::unpack(Standalone<StringRef> const& val) {
	Tuple t = Tuple::unpack(val);
	ASSERT(t.size() >= 1);

	auto url = t.getString(0).toString();

	Optional<std::string> encryptionKeyFileName;
	if (t.size() > 1 && !t.getString(1).empty()) {
		encryptionKeyFileName = t.getString(1).toString();
	}

	Optional<std::string> proxy;
	if (t.size() > 2 && !t.getString(2).empty()) {
		proxy = t.getString(2).toString();
	}

	return IBackupContainer::openContainer(url, proxy, encryptionKeyFileName);
}

class BackupConfig : public KeyBackedTaskConfig {
public:
	BackupConfig(UID uid = UID()) : KeyBackedTaskConfig(fileBackupPrefixRange.begin, uid) {}
	BackupConfig(Reference<Task> task) : KeyBackedTaskConfig(fileBackupPrefixRange.begin, task) {}

	// rangeFileMap maps a keyrange file's End to its Begin and Filename
	struct RangeSlice {
		Key begin;
		Version version;
		std::string fileName;
		int64_t fileSize;
		Tuple pack() const { return Tuple::makeTuple(begin, version, fileName, fileSize); }
		static RangeSlice unpack(Tuple const& t) {
			RangeSlice r;
			int i = 0;
			r.begin = t.getString(i++);
			r.version = t.getInt(i++);
			r.fileName = t.getString(i++).toString();
			r.fileSize = t.getInt(i++);
			return r;
		}
	};

	// Map of range end boundaries to info about the backup file written for that range.
	typedef KeyBackedMap<Key, RangeSlice> RangeFileMapT;
	RangeFileMapT snapshotRangeFileMap() { return configSpace.pack(__FUNCTION__sr); }

	// Number of kv range files that were both committed to persistent storage AND inserted into
	// the snapshotRangeFileMap.  Note that since insertions could replace 1 or more existing
	// map entries this is not necessarily the number of entries currently in the map.
	// This value exists to help with sizing of kv range folders for BackupContainers that
	// require it.
	KeyBackedBinaryValue<int64_t> snapshotRangeFileCount() { return configSpace.pack(__FUNCTION__sr); }

	// Coalesced set of ranges already dispatched for writing.
	typedef KeyBackedMap<Key, bool> RangeDispatchMapT;
	RangeDispatchMapT snapshotRangeDispatchMap() { return configSpace.pack(__FUNCTION__sr); }

	// Interval to use for the first (initial) snapshot.
	KeyBackedProperty<int64_t> initialSnapshotIntervalSeconds() { return configSpace.pack(__FUNCTION__sr); }

	// Interval to use for determining the target end version for new snapshots
	KeyBackedProperty<int64_t> snapshotIntervalSeconds() { return configSpace.pack(__FUNCTION__sr); }

	// When the current snapshot began
	KeyBackedProperty<Version> snapshotBeginVersion() { return configSpace.pack(__FUNCTION__sr); }

	// When the current snapshot is desired to end.
	// This can be changed at runtime to speed up or slow down a snapshot
	KeyBackedProperty<Version> snapshotTargetEndVersion() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedProperty<int64_t> snapshotBatchSize() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedProperty<Key> snapshotBatchFuture() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedProperty<Key> snapshotBatchDispatchDoneKey() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedProperty<int64_t> snapshotDispatchLastShardsBehind() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedProperty<Version> snapshotDispatchLastVersion() { return configSpace.pack(__FUNCTION__sr); }

	Future<Void> initNewSnapshot(Reference<ReadYourWritesTransaction> tr, int64_t intervalSeconds = -1) {
		BackupConfig& copy = *this; // Capture this by value instead of this ptr

		Future<Version> beginVersion = tr->getReadVersion();
		Future<int64_t> defaultInterval = 0;
		if (intervalSeconds < 0) {
			defaultInterval = copy.snapshotIntervalSeconds().getOrThrow(tr);
		}

		// Make sure read version and possibly the snapshot interval value are ready, then clear/init the snapshot
		// config members
		return map(success(beginVersion) && success(defaultInterval), [=](Void) mutable {
			copy.snapshotRangeFileMap().clear(tr);
			copy.snapshotRangeDispatchMap().clear(tr);
			copy.snapshotBatchSize().clear(tr);
			copy.snapshotBatchFuture().clear(tr);
			copy.snapshotBatchDispatchDoneKey().clear(tr);

			if (intervalSeconds < 0)
				intervalSeconds = defaultInterval.get();
			Version endVersion = beginVersion.get() + intervalSeconds * CLIENT_KNOBS->CORE_VERSIONSPERSECOND;

			copy.snapshotBeginVersion().set(tr, beginVersion.get());
			copy.snapshotTargetEndVersion().set(tr, endVersion);
			copy.snapshotRangeFileCount().set(tr, 0);
			copy.snapshotDispatchLastVersion().clear(tr);
			copy.snapshotDispatchLastShardsBehind().clear(tr);

			return Void();
		});
	}

	KeyBackedBinaryValue<int64_t> rangeBytesWritten() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedBinaryValue<int64_t> logBytesWritten() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedProperty<EBackupState> stateEnum() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedProperty<Reference<IBackupContainer>> backupContainer() { return configSpace.pack(__FUNCTION__sr); }

	// Set to true when all backup workers for saving mutation logs have been started.
	// configSpace::pack() returns a Key, how can we return a `KeyBackedProperty`
	KeyBackedProperty<Version> allWorkerStarted() { return configSpace.pack(__FUNCTION__sr); }

	// Each backup worker adds its (epoch, tag.id) to this property.
	KeyBackedProperty<std::vector<std::pair<int64_t, int64_t>>> startedBackupWorkers() {
		return configSpace.pack(__FUNCTION__sr);
	}

	// Set to true if backup worker is enabled.
	KeyBackedProperty<bool> backupWorkerEnabled() { return configSpace.pack(__FUNCTION__sr); }

	// Set to true if partitioned log is enabled (only useful if backup worker is also enabled).
	KeyBackedProperty<bool> partitionedLogEnabled() { return configSpace.pack(__FUNCTION__sr); }

	// Set to true if only requesting incremental backup without base snapshot.
	KeyBackedProperty<bool> incrementalBackupOnly() { return configSpace.pack(__FUNCTION__sr); }

	// Latest version for which all prior versions have saved by backup workers.
	KeyBackedProperty<Version> latestBackupWorkerSavedVersion() { return configSpace.pack(__FUNCTION__sr); }

	// Stop differential logging if already started or don't start after completing KV ranges
	KeyBackedProperty<bool> stopWhenDone() { return configSpace.pack(__FUNCTION__sr); }

	// Enable snapshot backup file encryption
	KeyBackedProperty<bool> enableSnapshotBackupEncryption() { return configSpace.pack(__FUNCTION__sr); }

	// Latest version for which all prior versions have had their log copy tasks completed
	KeyBackedProperty<Version> latestLogEndVersion() { return configSpace.pack(__FUNCTION__sr); }

	// The end version of the last complete snapshot
	KeyBackedProperty<Version> latestSnapshotEndVersion() { return configSpace.pack(__FUNCTION__sr); }

	// The end version of the first complete snapshot
	KeyBackedProperty<Version> firstSnapshotEndVersion() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedProperty<Key> destUidValue() { return configSpace.pack(__FUNCTION__sr); }

	Future<Optional<Version>> getLatestRestorableVersion(Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		auto lastLog = latestLogEndVersion().get(tr);
		auto firstSnapshot = firstSnapshotEndVersion().get(tr);
		auto workerEnabled = backupWorkerEnabled().get(tr);
		auto plogEnabled = partitionedLogEnabled().get(tr);
		auto workerVersion = latestBackupWorkerSavedVersion().get(tr);
		auto incrementalBackup = incrementalBackupOnly().get(tr);
		return map(success(lastLog) && success(firstSnapshot) && success(workerEnabled) && success(plogEnabled) &&
		               success(workerVersion) && success(incrementalBackup),
		           [=](Void) -> Optional<Version> {
			           // The latest log greater than the oldest snapshot is the restorable version
			           Optional<Version> logVersion = workerEnabled.get().present() && workerEnabled.get().get() &&
			                                                  plogEnabled.get().present() && plogEnabled.get().get()
			                                              ? workerVersion.get()
			                                              : lastLog.get();
			           if (logVersion.present() && firstSnapshot.get().present() &&
			               logVersion.get() > firstSnapshot.get().get()) {
				           return std::max(logVersion.get() - 1, firstSnapshot.get().get());
			           }
			           if (logVersion.present() && incrementalBackup.isReady() && incrementalBackup.get().present() &&
			               incrementalBackup.get().get()) {
				           return logVersion.get() - 1;
			           }
			           return {};
		           });
	}

	KeyBackedProperty<std::vector<KeyRange>> backupRanges() { return configSpace.pack(__FUNCTION__sr); }

	void startMutationLogs(Reference<ReadYourWritesTransaction> tr, KeyRangeRef backupRange, Key destUidValue) {
		Key mutationLogsDestKey = destUidValue.withPrefix(backupLogKeys.begin);
		tr->set(logRangesEncodeKey(backupRange.begin, BinaryReader::fromStringRef<UID>(destUidValue, Unversioned())),
		        logRangesEncodeValue(backupRange.end, mutationLogsDestKey));
	}

	Future<Void> logError(Database cx, Error e, std::string details, void* taskInstance = nullptr) {
		if (!uid.isValid()) {
			TraceEvent(SevError, "FileBackupErrorNoUID").error(e).detail("Description", details);
			return Void();
		}
		TraceEvent t(SevWarn, "FileBackupError");
		t.error(e)
		    .detail("BackupUID", uid)
		    .detail("Description", details)
		    .detail("TaskInstance", (uint64_t)taskInstance);
		// key_not_found could happen
		if (e.code() == error_code_key_not_found)
			t.backtrace();

		return updateErrorInfo(cx, e, details);
	}

	KeyBackedProperty<bool> blobBackupEnabled() { return configSpace.pack(__FUNCTION__sr); }
};

// Helper class for reading restore data from a buffer and throwing the right errors.
struct StringRefReader {
	StringRefReader(StringRef s = StringRef(), Error e = Error()) : rptr(s.begin()), end(s.end()), failure_error(e) {}

	// Return remainder of data as a StringRef
	StringRef remainder() { return StringRef(rptr, end - rptr); }

	// Return a pointer to len bytes at the current read position and advance read pos
	const uint8_t* consume(unsigned int len) {
		if (rptr == end && len != 0)
			throw end_of_stream();
		const uint8_t* p = rptr;
		rptr += len;
		if (rptr > end)
			throw failure_error;
		return p;
	}

	// Return a T from the current read position and advance read pos
	template <typename T>
	const T consume() {
		return *(const T*)consume(sizeof(T));
	}

	// Functions for consuming big endian (network byte order) integers.
	// Consumes a big endian number, swaps it to little endian, and returns it.
	int32_t consumeNetworkInt32() { return (int32_t)bigEndian32((uint32_t)consume<int32_t>()); }
	uint32_t consumeNetworkUInt32() { return bigEndian32(consume<uint32_t>()); }

	// Convert big Endian value (e.g., encoded in log file) into a littleEndian uint64_t value.
	int64_t consumeNetworkInt64() { return (int64_t)bigEndian64((uint32_t)consume<int64_t>()); }
	uint64_t consumeNetworkUInt64() { return bigEndian64(consume<uint64_t>()); }

	bool eof() { return rptr == end; }

	const uint8_t *rptr, *end;
	Error failure_error;
};

namespace fileBackup {
Standalone<VectorRef<KeyValueRef>> decodeRangeFileBlock(const Standalone<StringRef>& buf);

ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file,
                                                                      int64_t offset,
                                                                      int len,
                                                                      Database cx);

Standalone<VectorRef<KeyValueRef>> decodeMutationLogFileBlock(const Standalone<StringRef>& buf);

// Reads a mutation log block from file and parses into batch mutation blocks for further parsing.
ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeMutationLogFileBlock(Reference<IAsyncFile> file,
                                                                            int64_t offset,
                                                                            int len);

// Return a block of contiguous padding bytes "\0xff" for backup files, growing if needed.
Value makePadding(int size);
} // namespace fileBackup

// For fast restore simulation test
// For testing addPrefix feature in fast restore.
// Transform db content in restoreRanges by removePrefix and then addPrefix.
// Assume: DB is locked
ACTOR Future<Void> transformRestoredDatabase(Database cx,
                                             Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                             Key addPrefix,
                                             Key removePrefix);

void simulateBlobFailure();

// Add the set of ranges that are backed up in a default backup to the given vector. This consists of all normal keys
// and the system backup ranges.
void addDefaultBackupRanges(Standalone<VectorRef<KeyRangeRef>>& backupKeys);

// Return a vector containing the key ranges in system key-space that should be backed up in a default backup.
VectorRef<KeyRangeRef> const& getSystemBackupRanges();

// Return a key-range map that can be used to check whether a system key is a candidate backup key (i.e. whether it is
// part of any system backup ranges).
KeyRangeMap<bool> const& systemBackupMutationMask();

// Returns true if the given set of ranges exactly matches the set of ranges included in a default backup.
template <class Container>
bool isDefaultBackup(Container ranges) {
	std::unordered_set<KeyRangeRef> uniqueRanges(ranges.begin(), ranges.end());
	auto& systemBackupRanges = getSystemBackupRanges();

	if (uniqueRanges.size() != systemBackupRanges.size() + 1) {
		return false;
	}

	if (!uniqueRanges.count(normalKeys)) {
		return false;
	}
	for (auto range : getSystemBackupRanges()) {
		if (!uniqueRanges.count(range)) {
			return false;
		}
	}

	return true;
}

// Returns a key-range used to denote that a shared mutation stream belongs to the default backup set.
KeyRangeRef const& getDefaultBackupSharedRange();

#include "flow/unactorcompiler.h"
#endif
