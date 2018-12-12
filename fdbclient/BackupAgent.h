/*
 * BackupAgent.h
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

#ifndef FDBCLIENT_BACKUP_AGENT_H
#define FDBCLIENT_BACKUP_AGENT_H
#pragma once

#include "flow/flow.h"
#include "NativeAPI.h"
#include "TaskBucket.h"
#include "Notified.h"
#include <fdbrpc/IAsyncFile.h>
#include "KeyBackedTypes.h"
#include <ctime>
#include <climits>
#include "BackupContainer.h"

class BackupAgentBase : NonCopyable {
public:
	// Type of program being executed
	enum enumActionResult {
		RESULT_SUCCESSFUL = 0, RESULT_ERRORED = 1, RESULT_DUPLICATE = 2, RESULT_UNNEEDED = 3
	};

	enum enumState {
		STATE_ERRORED = 0, STATE_SUBMITTED = 1, STATE_BACKUP = 2, STATE_DIFFERENTIAL = 3, STATE_COMPLETED = 4, STATE_NEVERRAN = 5, STATE_ABORTED = 6, STATE_PARTIALLY_ABORTED = 7
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
			enState = STATE_BACKUP;
		}

		else if (!stateText.compare("is differential")) {
			enState = STATE_DIFFERENTIAL;
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

	// Convert the status text to an enumerated value
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
		case STATE_BACKUP:
			stateText = "has been started";
			break;
		case STATE_DIFFERENTIAL:
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

	// Determine if the specified state is runnable
	static bool isRunnable(enumState enState)
	{
		bool isRunnable = false;

		switch (enState)
		{
		case STATE_SUBMITTED:
		case STATE_BACKUP:
		case STATE_DIFFERENTIAL:
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

	FileBackupAgent( FileBackupAgent&& r ) noexcept(true) :
		subspace( std::move(r.subspace) ),
		config( std::move(r.config) ),
		lastRestorable( std::move(r.lastRestorable) ),
		taskBucket( std::move(r.taskBucket) ),
		futureBucket( std::move(r.futureBucket) ) {}

	void operator=( FileBackupAgent&& r ) noexcept(true) {
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

	// restore() will
	//   - make sure that url is readable and appears to be a complete backup
	//   - make sure the requested TargetVersion is valid
	//   - submit a restore on the given tagName
	//   - Optionally wait for the restore's completion.  Will restore_error if restore fails or is aborted.
	// restore() will return the targetVersion which will be either the valid version passed in or the max restorable version for the given url.
	Future<Version> restore(Database cx, Key tagName, Key url, bool waitForComplete = true, Version targetVersion = -1, bool verbose = true, KeyRange range = normalKeys, Key addPrefix = Key(), Key removePrefix = Key(), bool lockDB = true);
	Future<Version> atomicRestore(Database cx, Key tagName, KeyRange range = normalKeys, Key addPrefix = Key(), Key removePrefix = Key());

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

	Future<Void> submitBackup(Reference<ReadYourWritesTransaction> tr, Key outContainer, int snapshotIntervalSeconds, std::string tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone = true);
	Future<Void> submitBackup(Database cx, Key outContainer, int snapshotIntervalSeconds, std::string tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone = true) {
		return runRYWTransactionFailIfLocked(cx, [=](Reference<ReadYourWritesTransaction> tr){ return submitBackup(tr, outContainer, snapshotIntervalSeconds, tagName, backupRanges, stopWhenDone); });
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

	Future<Version> getLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName);
	void setLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName, Version version);

	// stopWhenDone will return when the backup is stopped, if enabled. Otherwise, it
	// will return when the backup directory is restorable.
	Future<int> waitBackup(Database cx, std::string tagName, bool stopWhenDone = true);

	static const Key keyLastRestorable;

	Future<int64_t> getTaskCount(Reference<ReadYourWritesTransaction> tr) { return taskBucket->getTaskCount(tr); }
	Future<int64_t> getTaskCount(Database cx) { return taskBucket->getTaskCount(cx); }
	Future<Void> watchTaskCount(Reference<ReadYourWritesTransaction> tr) { return taskBucket->watchTaskCount(tr); }

	Future<bool> checkActive(Database cx) { return taskBucket->checkActive(cx); }

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

	DatabaseBackupAgent( DatabaseBackupAgent&& r ) noexcept(true) :
		subspace( std::move(r.subspace) ),
		states( std::move(r.states) ),
		config( std::move(r.config) ),
		errors( std::move(r.errors) ),
		ranges( std::move(r.ranges) ),
		tagNames( std::move(r.tagNames) ),
		taskBucket( std::move(r.taskBucket) ),
		futureBucket( std::move(r.futureBucket) ),
		sourceStates( std::move(r.sourceStates) ),
		sourceTagNames( std::move(r.sourceTagNames) ) {}

	void operator=( DatabaseBackupAgent&& r ) noexcept(true) {
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

	Future<Void> atomicSwitchover(Database dest, Key tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, Key addPrefix, Key removePrefix);
	
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

	Future<int> getStateValue(Reference<ReadYourWritesTransaction> tr, UID logUid);
	Future<int> getStateValue(Database cx, UID logUid) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getStateValue(tr, logUid); });
	}

	Future<UID> getDestUid(Reference<ReadYourWritesTransaction> tr, UID logUid);
	Future<UID> getDestUid(Database cx, UID logUid) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getDestUid(tr, logUid); });
	}

	Future<UID> getLogUid(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<UID> getLogUid(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getLogUid(tr, tagName); });
	}

	Future<int64_t> getRangeBytesWritten(Reference<ReadYourWritesTransaction> tr, UID logUid);
	Future<int64_t> getLogBytesWritten(Reference<ReadYourWritesTransaction> tr, UID logUid);

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
		ar & items & version & groupKey;
	}
};

bool copyParameter(Reference<Task> source, Reference<Task> dest, Key key);
Version getVersionFromString(std::string const& value);
Standalone<VectorRef<KeyRangeRef>> getLogRanges(Version beginVersion, Version endVersion, Key destUidValue, int blockSize = CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
Standalone<VectorRef<KeyRangeRef>> getApplyRanges(Version beginVersion, Version endVersion, Key backupUid);
Future<Void> eraseLogData(Database cx, Key logUidValue, Key destUidValue, Optional<Version> endVersion = Optional<Version>(), bool checkBackupUid = false, Version backupUid = 0);
Key getApplyKey( Version version, Key backupUid );
std::pair<uint64_t, uint32_t> decodeBKMutationLogKey(Key key);
Standalone<VectorRef<MutationRef>> decodeBackupLogValue(StringRef value);
void decodeBackupLogValue(Arena& arena, VectorRef<MutationRef>& result, int64_t& mutationSize, StringRef value, StringRef addPrefix = StringRef(), StringRef removePrefix = StringRef());
Future<Void> logErrorWorker(Reference<ReadYourWritesTransaction> const& tr, Key const& keyErrors, std::string const& message);
Future<Void> logError(Database cx, Key keyErrors, const std::string& message);
Future<Void> logError(Reference<ReadYourWritesTransaction> tr, Key keyErrors, const std::string& message);
Future<Void> checkVersion(Reference<ReadYourWritesTransaction> const& tr);
Future<Void> readCommitted(Database const& cx, PromiseStream<RangeResultWithVersion> const& results, Reference<FlowLock> const& lock, KeyRangeRef const& range, bool const& terminator = true, bool const& systemAccess = false, bool const& lockAware = false);
Future<Void> readCommitted(Database const& cx, PromiseStream<RCGroup> const& results, Future<Void> const& active, Reference<FlowLock> const& lock, KeyRangeRef const& range, std::function< std::pair<uint64_t, uint32_t>(Key key) > const& groupBy, bool const& terminator = true, bool const& systemAccess = false, bool const& lockAware = false);
Future<Void> applyMutations(Database const& cx, Key const& uid, Key const& addPrefix, Key const& removePrefix, Version const& beginVersion, Version* const& endVersion, RequestStream<CommitTransactionRequest> const& commit, NotifiedVersion* const& committedVersion, Reference<KeyRangeMap<Version>> const& keyVersion);

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

	static Future<std::vector<KeyBackedTag>> getAll_impl(TagUidMap * const & tagsMap, Reference<ReadYourWritesTransaction> const & tr);

	Future<std::vector<KeyBackedTag>> getAll(Reference<ReadYourWritesTransaction> tr) {
		return getAll_impl(this, tr);
	}

	Key prefix;
};

static inline KeyBackedTag makeRestoreTag(std::string tagName) {
	return KeyBackedTag(tagName, fileRestorePrefixRange.begin);
}

static inline KeyBackedTag makeBackupTag(std::string tagName) {
	return KeyBackedTag(tagName, fileBackupPrefixRange.begin);
}

static inline Future<std::vector<KeyBackedTag>> getAllRestoreTags(Reference<ReadYourWritesTransaction> tr) {
	return TagUidMap(fileRestorePrefixRange.begin).getAll(tr);
}

static inline Future<std::vector<KeyBackedTag>> getAllBackupTags(Reference<ReadYourWritesTransaction> tr) {
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

	// Get the backup container URL only without creating a backup container instance.
	KeyBackedProperty<Reference<IBackupContainer>> backupContainerURL() {
		return configSpace.pack(LiteralStringRef("backupContainer"));
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
		auto &copy = *this;
		auto lastLog = latestLogEndVersion().get(tr);
		auto firstSnapshot = firstSnapshotEndVersion().get(tr);
		return map(success(lastLog) && success(firstSnapshot), [=](Void) -> Optional<Version> {
			// The latest log greater than the oldest snapshot is the restorable version
			if(lastLog.get().present() && firstSnapshot.get().present() && lastLog.get().get() >= firstSnapshot.get().get()) {
				return lastLog.get().get();
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
		// These should not happen
		if(e.code() == error_code_key_not_found)
			t.backtrace();

		return updateErrorInfo(cx, e, details);
	}
};
#endif
