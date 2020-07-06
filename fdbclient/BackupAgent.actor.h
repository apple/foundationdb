/*
 * BackupAgent.actor.h
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
#include "fdbrpc/IAsyncFile.h"
#include "fdbclient/KeyBackedTypes.h"
#include <ctime>
#include <climits>
#include "fdbclient/BackupContainer.h"
#include "flow/actorcompiler.h" // has to be last include

class BackupAgentBase : NonCopyable {
public:
	// Time formatter for anything backup or restore related
	static std::string formatTime(int64_t epochs);
	static int64_t parseTime(std::string timestamp);

	static std::string timeFormat() {
		return "YYYY/MM/DD.HH:MI:SS[+/-]HHMM";
	}

	// Type of program being executed
	enum enumActionResult {
		RESULT_SUCCESSFUL = 0, RESULT_ERRORED = 1, RESULT_DUPLICATE = 2, RESULT_UNNEEDED = 3
	};

	enum enumState {
		STATE_ERRORED = 0, STATE_SUBMITTED = 1, STATE_RUNNING = 2, STATE_RUNNING_DIFFERENTIAL = 3, STATE_COMPLETED = 4, STATE_NEVERRAN = 5, STATE_ABORTED = 6, STATE_PARTIALLY_ABORTED = 7
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

	static const int logHeaderSize;

	// Convert the status text to an enumerated value
	static enumState getState(std::string stateText)
	{
		enumState enState = STATE_ERRORED;

		if (stateText.empty()) {
			enState = STATE_NEVERRAN;
		}

		else if (!stateText.compare("has been submitted")) {
			enState = STATE_SUBMITTED;
		}

		else if (!stateText.compare("has been started")) {
			enState = STATE_RUNNING;
		}

		else if (!stateText.compare("is differential")) {
			enState = STATE_RUNNING_DIFFERENTIAL;
		}

		else if (!stateText.compare("has been completed")) {
			enState = STATE_COMPLETED;
		}

		else if (!stateText.compare("has been aborted")) {
			enState = STATE_ABORTED;
		}

		else if (!stateText.compare("has been partially aborted")) {
			enState = STATE_PARTIALLY_ABORTED;
		}

		return enState;
	}

	// Convert the status enum to a text description
	static const char* getStateText(enumState enState)
	{
		const char* stateText;

		switch (enState)
		{
		case STATE_ERRORED:
			stateText = "has errored";
			break;
		case STATE_NEVERRAN:
			stateText = "has never been started";
			break;
		case STATE_SUBMITTED:
			stateText = "has been submitted";
			break;
		case STATE_RUNNING:
			stateText = "has been started";
			break;
		case STATE_RUNNING_DIFFERENTIAL:
			stateText = "is differential";
			break;
		case STATE_COMPLETED:
			stateText = "has been completed";
			break;
		case STATE_ABORTED:
			stateText = "has been aborted";
			break;
		case STATE_PARTIALLY_ABORTED:
			stateText = "has been partially aborted";
			break;
		default:
			stateText = "<undefined>";
			break;
		}

		return stateText;
	}

	// Convert the status enum to a name
	static const char* getStateName(enumState enState)
	{
		const char* s;

		switch (enState)
		{
		case STATE_ERRORED:
			s = "Errored";
			break;
		case STATE_NEVERRAN:
			s = "NeverRan";
			break;
		case STATE_SUBMITTED:
			s = "Submitted";
			break;
		case STATE_RUNNING:
			s = "Running";
			break;
		case STATE_RUNNING_DIFFERENTIAL:
			s = "RunningDifferentially";
			break;
		case STATE_COMPLETED:
			s = "Completed";
			break;
		case STATE_ABORTED:
			s = "Aborted";
			break;
		case STATE_PARTIALLY_ABORTED:
			s = "Aborting";
			break;
		default:
			s = "<undefined>";
			break;
		}

		return s;
	}

	// Determine if the specified state is runnable
	static bool isRunnable(enumState enState)
	{
		bool isRunnable = false;

		switch (enState)
		{
		case STATE_SUBMITTED:
		case STATE_RUNNING:
		case STATE_RUNNING_DIFFERENTIAL:
		case STATE_PARTIALLY_ABORTED:
			isRunnable = true;
			break;
		default:
			break;
		}

		return isRunnable;
	}

	static const KeyRef getDefaultTag() {
		return StringRef(defaultTagName);
	}

	static const std::string getDefaultTagName() {
		return defaultTagName;
	}

	// This is only used for automatic backup name generation
	static Standalone<StringRef> getCurrentTime() {
		double t = now();
		time_t curTime = t;
		char buffer[128];
		struct tm* timeinfo;
		timeinfo = localtime(&curTime);
		strftime(buffer, 128, "%Y-%m-%d-%H-%M-%S", timeinfo);

		std::string time(buffer);
		return StringRef(time + format(".%06d", (int)(1e6*(t - curTime))));
	}

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
		lastRestorable = std::move(r.lastRestorable),
		taskBucket = std::move(r.taskBucket);
		futureBucket = std::move(r.futureBucket);
	}

	KeyBackedProperty<Key> lastBackupTimestamp() {
		return config.pack(LiteralStringRef(__FUNCTION__));
	}

	Future<Void> run(Database cx, double *pollDelay, int maxConcurrentTasks) {
		return taskBucket->run(cx, futureBucket, pollDelay, maxConcurrentTasks);
	}

	/** RESTORE **/

	enum ERestoreState { UNITIALIZED = 0, QUEUED = 1, STARTING = 2, RUNNING = 3, COMPLETED = 4, ABORTED = 5 };
	static StringRef restoreStateText(ERestoreState id);

	// parallel restore
	Future<Void> parallelRestoreFinish(Database cx, UID randomUID, bool unlockDB = true);
	Future<Void> submitParallelRestore(Database cx, Key backupTag, Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                                   Key bcUrl, Version targetVersion, bool lockDB, UID randomUID, Key addPrefix,
	                                   Key removePrefix);
	Future<Void> atomicParallelRestore(Database cx, Key tagName, Standalone<VectorRef<KeyRangeRef>> ranges,
	                                   Key addPrefix, Key removePrefix);

	// restore() will
	//   - make sure that url is readable and appears to be a complete backup
	//   - make sure the requested TargetVersion is valid
	//   - submit a restore on the given tagName
	//   - Optionally wait for the restore's completion.  Will restore_error if restore fails or is aborted.
	// restore() will return the targetVersion which will be either the valid version passed in or the max restorable version for the given url.
	Future<Version> restore(Database cx, Optional<Database> cxOrig, Key tagName, Key url, Standalone<VectorRef<KeyRangeRef>> ranges, bool waitForComplete = true, Version targetVersion = -1, bool verbose = true, Key addPrefix = Key(), Key removePrefix = Key(), bool lockDB = true);
	Future<Version> restore(Database cx, Optional<Database> cxOrig, Key tagName, Key url, bool waitForComplete = true, Version targetVersion = -1, bool verbose = true, KeyRange range = normalKeys, Key addPrefix = Key(), Key removePrefix = Key(), bool lockDB = true) {
		Standalone<VectorRef<KeyRangeRef>> rangeRef;
		rangeRef.push_back_deep(rangeRef.arena(), range);
		return restore(cx, cxOrig, tagName, url, rangeRef, waitForComplete, targetVersion, verbose, addPrefix, removePrefix, lockDB);
	}
	Future<Version> atomicRestore(Database cx, Key tagName, Standalone<VectorRef<KeyRangeRef>> ranges, Key addPrefix = Key(), Key removePrefix = Key());
	Future<Version> atomicRestore(Database cx, Key tagName, KeyRange range = normalKeys, Key addPrefix = Key(), Key removePrefix = Key()) {
		Standalone<VectorRef<KeyRangeRef>> rangeRef;
		rangeRef.push_back_deep(rangeRef.arena(), range);
		return atomicRestore(cx, tagName, rangeRef, addPrefix, removePrefix);
	}
	// Tries to abort the restore for a tag.  Returns the final (stable) state of the tag.
	Future<ERestoreState> abortRestore(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<ERestoreState> abortRestore(Database cx, Key tagName);

	// Waits for a restore tag to reach a final (stable) state.
	Future<ERestoreState> waitRestore(Database cx, Key tagName, bool verbose);

	// Get a string describing the status of a tag
	Future<std::string> restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<std::string> restoreStatus(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return restoreStatus(tr, tagName); });
	}

	/** BACKUP METHODS **/

	Future<Void> submitBackup(Reference<ReadYourWritesTransaction> tr, Key outContainer, int snapshotIntervalSeconds,
	                          std::string tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges,
	                          bool stopWhenDone = true, bool partitionedLog = false);
	Future<Void> submitBackup(Database cx, Key outContainer, int snapshotIntervalSeconds, std::string tagName,
	                          Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone = true,
	                          bool partitionedLog = false) {
		return runRYWTransactionFailIfLocked(cx, [=](Reference<ReadYourWritesTransaction> tr) {
			return submitBackup(tr, outContainer, snapshotIntervalSeconds, tagName, backupRanges, stopWhenDone,
			                    partitionedLog);
		});
	}

	Future<Void> discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<Void> discontinueBackup(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return discontinueBackup(tr, tagName); });
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
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return abortBackup(tr, tagName); });
	}

	Future<std::string> getStatus(Database cx, bool showErrors, std::string tagName);
	Future<std::string> getStatusJSON(Database cx, std::string tagName);

	Future<Version> getLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName, bool snapshot = false);
	void setLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName, Version version);

	// stopWhenDone will return when the backup is stopped, if enabled. Otherwise, it
	// will return when the backup directory is restorable.
	Future<int> waitBackup(Database cx, std::string tagName, bool stopWhenDone = true, Reference<IBackupContainer> *pContainer = nullptr, UID *pUID = nullptr);

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

class DatabaseBackupAgent : public BackupAgentBase {
public:
	DatabaseBackupAgent();
	explicit DatabaseBackupAgent(Database src);

	DatabaseBackupAgent(DatabaseBackupAgent&& r) noexcept
	  : subspace(std::move(r.subspace)), states(std::move(r.states)), config(std::move(r.config)),
	    errors(std::move(r.errors)), ranges(std::move(r.ranges)), tagNames(std::move(r.tagNames)),
	    taskBucket(std::move(r.taskBucket)), futureBucket(std::move(r.futureBucket)),
	    sourceStates(std::move(r.sourceStates)), sourceTagNames(std::move(r.sourceTagNames)) {}

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

	Future<Void> run(Database cx, double *pollDelay, int maxConcurrentTasks) {
		return taskBucket->run(cx, futureBucket, pollDelay, maxConcurrentTasks);
	}

	Future<Void> atomicSwitchover(Database dest, Key tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, Key addPrefix, Key removePrefix, bool forceAction=false);
	
	Future<Void> unlockBackup(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<Void> unlockBackup(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return unlockBackup(tr, tagName); });
	}

	Future<Void> submitBackup(Reference<ReadYourWritesTransaction> tr, Key tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone = true, Key addPrefix = StringRef(), Key removePrefix = StringRef(), bool lockDatabase = false, bool databasesInSync=false);
	Future<Void> submitBackup(Database cx, Key tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone = true, Key addPrefix = StringRef(), Key removePrefix = StringRef(), bool lockDatabase = false, bool databasesInSync=false) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return submitBackup(tr, tagName, backupRanges, stopWhenDone, addPrefix, removePrefix, lockDatabase, databasesInSync); });
	}

	Future<Void> discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<Void> discontinueBackup(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return discontinueBackup(tr, tagName); });
	}

	Future<Void> abortBackup(Database cx, Key tagName, bool partial = false, bool abortOldBackup = false);

	Future<std::string> getStatus(Database cx, int errorLimit, Key tagName);

	Future<int> getStateValue(Reference<ReadYourWritesTransaction> tr, UID logUid, bool snapshot = false);
	Future<int> getStateValue(Database cx, UID logUid) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getStateValue(tr, logUid); });
	}

	Future<UID> getDestUid(Reference<ReadYourWritesTransaction> tr, UID logUid, bool snapshot = false);
	Future<UID> getDestUid(Database cx, UID logUid) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getDestUid(tr, logUid); });
	}

	Future<UID> getLogUid(Reference<ReadYourWritesTransaction> tr, Key tagName, bool snapshot = false);
	Future<UID> getLogUid(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getLogUid(tr, tagName); });
	}

	Future<int64_t> getRangeBytesWritten(Reference<ReadYourWritesTransaction> tr, UID logUid, bool snapshot = false);
	Future<int64_t> getLogBytesWritten(Reference<ReadYourWritesTransaction> tr, UID logUid, bool snapshot = false);

	// stopWhenDone will return when the backup is stopped, if enabled. Otherwise, it
	// will return when the backup directory is restorable.
	Future<int> waitBackup(Database cx, Key tagName, bool stopWhenDone = true);
	Future<int> waitSubmitted(Database cx, Key tagName);
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

typedef std::pair<Standalone<RangeResultRef>, Version> RangeResultWithVersion;

struct RCGroup {
	Standalone<RangeResultRef> items;
	Version version;
	uint64_t groupKey;

	RCGroup() : version(-1), groupKey(ULLONG_MAX) {};

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, items, version, groupKey);
	}
};

bool copyParameter(Reference<Task> source, Reference<Task> dest, Key key);
Version getVersionFromString(std::string const& value);
Standalone<VectorRef<KeyRangeRef>> getLogRanges(Version beginVersion, Version endVersion, Key destUidValue, int blockSize = CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
Standalone<VectorRef<KeyRangeRef>> getApplyRanges(Version beginVersion, Version endVersion, Key backupUid);
Future<Void> eraseLogData(Reference<ReadYourWritesTransaction> tr, Key logUidValue, Key destUidValue, Optional<Version> endVersion = Optional<Version>(), bool checkBackupUid = false, Version backupUid = 0);
Key getApplyKey( Version version, Key backupUid );
Version getLogKeyVersion(Key key);
std::pair<Version, uint32_t> decodeBKMutationLogKey(Key key);
Future<Void> logError(Database cx, Key keyErrors, const std::string& message);
Future<Void> logError(Reference<ReadYourWritesTransaction> tr, Key keyErrors, const std::string& message);
Future<Void> checkVersion(Reference<ReadYourWritesTransaction> const& tr);
ACTOR Future<Void> readCommitted(Database cx, PromiseStream<RangeResultWithVersion> results, Reference<FlowLock> lock,
                                 KeyRangeRef range, bool terminator = true, bool systemAccess = false,
                                 bool lockAware = false);
ACTOR Future<Void> readCommitted(Database cx, PromiseStream<RCGroup> results, Future<Void> active,
                                 Reference<FlowLock> lock, KeyRangeRef range,
                                 std::function<std::pair<uint64_t, uint32_t>(Key key)> groupBy, bool terminator = true,
                                 bool systemAccess = false, bool lockAware = false);
ACTOR Future<Void> applyMutations(Database cx, Key uid, Key addPrefix, Key removePrefix, Version beginVersion,
                                  Version* endVersion, RequestStream<CommitTransactionRequest> commit,
                                  NotifiedVersion* committedVersion, Reference<KeyRangeMap<Version>> keyVersion);
ACTOR Future<Void> cleanupBackup(Database cx, bool deleteData);

typedef BackupAgentBase::enumState EBackupState;
template<> inline Tuple Codec<EBackupState>::pack(EBackupState const &val) { return Tuple().append(val); }
template<> inline EBackupState Codec<EBackupState>::unpack(Tuple const &val) { return (EBackupState)val.getInt(0); }

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
public:
	TagUidMap(const StringRef & prefix) : TagMap(LiteralStringRef("tag->uid/").withPrefix(prefix)), prefix(prefix) {}

	ACTOR static Future<std::vector<KeyBackedTag>> getAll_impl(TagUidMap* tagsMap, Reference<ReadYourWritesTransaction> tr, bool snapshot);

	Future<std::vector<KeyBackedTag>> getAll(Reference<ReadYourWritesTransaction> tr, bool snapshot = false) {
		return getAll_impl(this, tr, snapshot);
	}

	Key prefix;
};

static inline KeyBackedTag makeRestoreTag(std::string tagName) {
	return KeyBackedTag(tagName, fileRestorePrefixRange.begin);
}

static inline KeyBackedTag makeBackupTag(std::string tagName) {
	return KeyBackedTag(tagName, fileBackupPrefixRange.begin);
}

static inline Future<std::vector<KeyBackedTag>> getAllRestoreTags(Reference<ReadYourWritesTransaction> tr, bool snapshot = false) {
	return TagUidMap(fileRestorePrefixRange.begin).getAll(tr, snapshot);
}

static inline Future<std::vector<KeyBackedTag>> getAllBackupTags(Reference<ReadYourWritesTransaction> tr, bool snapshot = false) {
	return TagUidMap(fileBackupPrefixRange.begin).getAll(tr, snapshot);
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

	Future<Void> toTask(Reference<ReadYourWritesTransaction> tr, Reference<Task> task, bool setValidation = true) {
		// Set the uid task parameter
		TaskParams.uid().set(task, uid);

		if (!setValidation) {
			return Void();
		}

		// Set the validation condition for the task which is that the restore uid's tag's uid is the same as the restore uid.
		// Get this uid's tag, then get the KEY for the tag's uid but don't read it.  That becomes the validation key
		// which TaskBucket will check, and its value must be this restore config's uid.
		UID u = uid;  // 'this' could be invalid in lambda
		Key p = prefix;
		return map(tag().get(tr), [u,p,task](Optional<std::string> const &tag) -> Void {
			if(!tag.present())
				throw restore_error();
			// Validation contition is that the uidPair key must be exactly {u, false}
			TaskBucket::setValidationCondition(task, KeyBackedTag(tag.get(), p).key, Codec<UidAndAbortedFlagT>::pack({u, false}).pack());
			return Void();
		});
	}

	KeyBackedProperty<std::string> tag() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	UID getUid() { return uid; }

	Key getUidAsKey() { return BinaryWriter::toValue(uid, Unversioned()); }

	void clear(Reference<ReadYourWritesTransaction> tr) {
		tr->clear(configSpace.range());
	}

	// lastError is a pair of error message and timestamp expressed as an int64_t
	KeyBackedProperty<std::pair<std::string, Version>> lastError() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedMap<int64_t, std::pair<std::string, Version>> lastErrorPerType() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Updates the error per type map and the last error property
	Future<Void> updateErrorInfo(Database cx, Error e, std::string message) {
		// Avoid capture of this ptr
		auto &copy = *this;

		return runRYWTransaction(cx, [=] (Reference<ReadYourWritesTransaction> tr) mutable {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return map(tr->getReadVersion(), [=] (Version v) mutable {
				copy.lastError().set(tr, {message, v});
				copy.lastErrorPerType().set(tr, e.code(), {message, v});
				return Void();
			});
		});
	}

protected:
	UID uid;
	Key prefix;
	Subspace configSpace;
};

template<> inline Tuple Codec<Reference<IBackupContainer>>::pack(Reference<IBackupContainer> const &bc) { 
	return Tuple().append(StringRef(bc->getURL()));
}
template<> inline Reference<IBackupContainer> Codec<Reference<IBackupContainer>>::unpack(Tuple const &val) {
	return IBackupContainer::openContainer(val.getString(0).toString());
}

class BackupConfig : public KeyBackedConfig {
public:
	BackupConfig(UID uid = UID()) : KeyBackedConfig(fileBackupPrefixRange.begin, uid) {}
	BackupConfig(Reference<Task> task) : KeyBackedConfig(fileBackupPrefixRange.begin, task) {}

	// rangeFileMap maps a keyrange file's End to its Begin and Filename
	struct RangeSlice {
		Key begin;
		Version version;
		std::string fileName;
		int64_t fileSize;
		Tuple pack() const {
			return Tuple().append(begin).append(version).append(StringRef(fileName)).append(fileSize);
		}
		static RangeSlice unpack(Tuple const &t) {
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
	RangeFileMapT snapshotRangeFileMap() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Number of kv range files that were both committed to persistent storage AND inserted into
	// the snapshotRangeFileMap.  Note that since insertions could replace 1 or more existing
	// map entries this is not necessarily the number of entries currently in the map.
	// This value exists to help with sizing of kv range folders for BackupContainers that 
	// require it.
	KeyBackedBinaryValue<int64_t> snapshotRangeFileCount() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Coalesced set of ranges already dispatched for writing.
	typedef KeyBackedMap<Key, bool> RangeDispatchMapT;
	RangeDispatchMapT snapshotRangeDispatchMap() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Interval to use for determining the target end version for new snapshots
	KeyBackedProperty<int64_t> snapshotIntervalSeconds() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// When the current snapshot began
	KeyBackedProperty<Version> snapshotBeginVersion() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// When the current snapshot is desired to end.  
	// This can be changed at runtime to speed up or slow down a snapshot 
	KeyBackedProperty<Version> snapshotTargetEndVersion() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<int64_t> snapshotBatchSize() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<Key> snapshotBatchFuture() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<Key> snapshotBatchDispatchDoneKey() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<int64_t> snapshotDispatchLastShardsBehind() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<Version> snapshotDispatchLastVersion() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	Future<Void> initNewSnapshot(Reference<ReadYourWritesTransaction> tr, int64_t intervalSeconds = -1) {
		BackupConfig &copy = *this;  // Capture this by value instead of this ptr

		Future<Version> beginVersion = tr->getReadVersion();
		Future<int64_t> defaultInterval = 0;
		if(intervalSeconds < 0)
			defaultInterval = copy.snapshotIntervalSeconds().getOrThrow(tr);

		// Make sure read version and possibly the snapshot interval value are ready, then clear/init the snapshot config members
		return map(success(beginVersion) && success(defaultInterval), [=](Void) mutable {
			copy.snapshotRangeFileMap().clear(tr);
			copy.snapshotRangeDispatchMap().clear(tr);
			copy.snapshotBatchSize().clear(tr);
			copy.snapshotBatchFuture().clear(tr);
			copy.snapshotBatchDispatchDoneKey().clear(tr);

			if(intervalSeconds < 0)
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

	KeyBackedBinaryValue<int64_t> rangeBytesWritten() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedBinaryValue<int64_t> logBytesWritten() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<EBackupState> stateEnum() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<Reference<IBackupContainer>> backupContainer() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Set to true when all backup workers for saving mutation logs have been started.
	KeyBackedProperty<bool> allWorkerStarted() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Each backup worker adds its (epoch, tag.id) to this property.
	KeyBackedProperty<std::vector<std::pair<int64_t, int64_t>>> startedBackupWorkers() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Set to true if backup worker is enabled.
	KeyBackedProperty<bool> backupWorkerEnabled() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Set to true if partitioned log is enabled (only useful if backup worker is also enabled).
	KeyBackedProperty<bool> partitionedLogEnabled() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Latest version for which all prior versions have saved by backup workers.
	KeyBackedProperty<Version> latestBackupWorkerSavedVersion() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Stop differntial logging if already started or don't start after completing KV ranges
	KeyBackedProperty<bool> stopWhenDone() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// Latest version for which all prior versions have had their log copy tasks completed
	KeyBackedProperty<Version> latestLogEndVersion() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// The end version of the last complete snapshot
	KeyBackedProperty<Version> latestSnapshotEndVersion() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	// The end version of the first complete snapshot
	KeyBackedProperty<Version> firstSnapshotEndVersion() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	KeyBackedProperty<Key> destUidValue() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	Future<Optional<Version>> getLatestRestorableVersion(Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		auto lastLog = latestLogEndVersion().get(tr);
		auto firstSnapshot = firstSnapshotEndVersion().get(tr);
		auto workerEnabled = backupWorkerEnabled().get(tr);
		auto plogEnabled = partitionedLogEnabled().get(tr);
		auto workerVersion = latestBackupWorkerSavedVersion().get(tr);
		return map(success(lastLog) && success(firstSnapshot) && success(workerEnabled) && success(plogEnabled) && success(workerVersion), [=](Void) -> Optional<Version> {
			// The latest log greater than the oldest snapshot is the restorable version
			Optional<Version> logVersion = workerEnabled.get().present() && workerEnabled.get().get() &&
			                                       plogEnabled.get().present() && plogEnabled.get().get()
			                                   ? workerVersion.get()
			                                   : lastLog.get();
			if (logVersion.present() && firstSnapshot.get().present() && logVersion.get() > firstSnapshot.get().get()) {
				return std::max(logVersion.get() - 1, firstSnapshot.get().get());
			}
			return {};
		});
	}

	KeyBackedProperty<std::vector<KeyRange>> backupRanges() {
		return configSpace.pack(LiteralStringRef(__FUNCTION__));
	}

	void startMutationLogs(Reference<ReadYourWritesTransaction> tr, KeyRangeRef backupRange, Key destUidValue) {
		Key mutationLogsDestKey = destUidValue.withPrefix(backupLogKeys.begin);
		tr->set(logRangesEncodeKey(backupRange.begin, BinaryReader::fromStringRef<UID>(destUidValue, Unversioned())), logRangesEncodeValue(backupRange.end, mutationLogsDestKey));
	}

	Future<Void> logError(Database cx, Error e, std::string details, void *taskInstance = nullptr) {
		if(!uid.isValid()) {
			TraceEvent(SevError, "FileBackupErrorNoUID").error(e).detail("Description", details);
			return Void();
		}
		TraceEvent t(SevWarn, "FileBackupError");
		t.error(e).detail("BackupUID", uid).detail("Description", details).detail("TaskInstance", (uint64_t)taskInstance);
		// key_not_found could happen
		if(e.code() == error_code_key_not_found)
			t.backtrace();

		return updateErrorInfo(cx, e, details);
	}
};

// Helper class for reading restore data from a buffer and throwing the right errors.
struct StringRefReader {
	StringRefReader(StringRef s = StringRef(), Error e = Error()) : rptr(s.begin()), end(s.end()), failure_error(e) {}

	// Return remainder of data as a StringRef
	StringRef remainder() { return StringRef(rptr, end - rptr); }

	// Return a pointer to len bytes at the current read position and advance read pos
	const uint8_t* consume(unsigned int len) {
		if (rptr == end && len != 0) throw end_of_stream();
		const uint8_t* p = rptr;
		rptr += len;
		if (rptr > end) throw failure_error;
		return p;
	}

	// Return a T from the current read position and advance read pos
	template <typename T>
	const T consume() {
		return *(const T*)consume(sizeof(T));
	}

	// Functions for consuming big endian (network byte order) integers.
	// Consumes a big endian number, swaps it to little endian, and returns it.
	const int32_t consumeNetworkInt32() { return (int32_t)bigEndian32((uint32_t)consume<int32_t>()); }
	const uint32_t consumeNetworkUInt32() { return bigEndian32(consume<uint32_t>()); }

	// Convert big Endian value (e.g., encoded in log file) into a littleEndian uint64_t value.
	int64_t consumeNetworkInt64() { return (int64_t)bigEndian64((uint32_t)consume<int64_t>()); }
	uint64_t consumeNetworkUInt64() { return bigEndian64(consume<uint64_t>()); }

	bool eof() { return rptr == end; }

	const uint8_t *rptr, *end;
	Error failure_error;
};

namespace fileBackup {
ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file, int64_t offset,
                                                                      int len);

// Return a block of contiguous padding bytes "\0xff" for backup files, growing if needed.
Value makePadding(int size);
}

// For fast restore simulation test
// For testing addPrefix feature in fast restore.
// Transform db content in restoreRanges by removePrefix and then addPrefix.
// Assume: DB is locked
ACTOR Future<Void> transformRestoredDatabase(Database cx, Standalone<VectorRef<KeyRangeRef>> backupRanges,
                                             Key addPrefix, Key removePrefix);

#include "flow/unactorcompiler.h"
#endif
