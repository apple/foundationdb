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
	static const Key keyConfigBackupTag;
	static const Key keyConfigLogUid;
	static const Key keyConfigBackupRanges;
	static const Key keyConfigStopWhenDoneKey;
	static const Key keyStateStatus;
	static const Key keyStateStop;
	static const Key keyLastUid;
	static const Key keyBeginKey;
	static const Key keyEndKey;

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

	// The following function will return the textual name of the
	// start status
	static const char* getResultText(enumActionResult enResult)
	{
		switch (enResult)
		{
		case RESULT_SUCCESSFUL:
			return "action was successful";
			break;
		case RESULT_ERRORED:
			return "error received during action process";
			break;
		case RESULT_DUPLICATE:
			return "requested action has already been performed";
			break;
		case RESULT_UNNEEDED:
			return "requested action is not needed";
			break;
		}
		return "<undefined>";
	}

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

	static bool isRunnable(std::string stateText) {	
		return isRunnable(getState(stateText));	
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
		errors( std::move(r.errors) ),
		tagNames( std::move(r.tagNames) ),
		lastRestorable( std::move(r.lastRestorable) ),
		taskBucket( std::move(r.taskBucket) ),
		futureBucket( std::move(r.futureBucket) ) {}

	void operator=( FileBackupAgent&& r ) noexcept(true) {
		subspace = std::move(r.subspace);
		config = std::move(r.config);
		errors = std::move(r.errors);
		tagNames = std::move(r.tagNames);
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
	Future<ERestoreState> abortRestore(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return abortRestore(tr, tagName); });
	}

	// Waits for a restore tag to reach a final (stable) state.
	Future<ERestoreState> waitRestore(Database cx, Key tagName, bool verbose);

	// Get a string describing the status of a tag
	Future<std::string> restoreStatus(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<std::string> restoreStatus(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return restoreStatus(tr, tagName); });
	}

	/** BACKUP METHODS **/
	
	Future<Void> submitBackup(Reference<ReadYourWritesTransaction> tr, Key outContainer, std::string tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone = true);
	Future<Void> submitBackup(Database cx, Key outContainer, std::string tagName, Standalone<VectorRef<KeyRangeRef>> backupRanges, bool stopWhenDone = true) {
		return runRYWTransactionFailIfLocked(cx, [=](Reference<ReadYourWritesTransaction> tr){ return submitBackup(tr, outContainer, tagName, backupRanges, stopWhenDone); });
	}

	Future<Void> discontinueBackup(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<Void> discontinueBackup(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return discontinueBackup(tr, tagName); });
	}

	Future<Void> abortBackup(Reference<ReadYourWritesTransaction> tr, std::string tagName);
	Future<Void> abortBackup(Database cx, std::string tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return abortBackup(tr, tagName); });
	}

	Future<std::string> getStatus(Database cx, int errorLimit, std::string tagName);

	Future<int> getStateValue(Reference<ReadYourWritesTransaction> tr, UID logUid);
	Future<int> getStateValue(Database cx, UID logUid) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getStateValue(tr, logUid); });
	}

	Future<Version> getStateStopVersion(Reference<ReadYourWritesTransaction> tr, UID logUid);
	Future<Version> getStateStopVersion(Database cx, UID logUid) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getStateStopVersion(tr, logUid); });
	}

	Future<UID> getLogUid(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<UID> getLogUid(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getLogUid(tr, tagName); });
	}

	Future<Version> getLastRestorable(Reference<ReadYourWritesTransaction> tr, Key tagName);
	Future<Version> getLastRestorable(Database cx, Key tagName) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getLastRestorable(tr, tagName); });
	}

	Future<int64_t> getRangeBytesWritten(Reference<ReadYourWritesTransaction> tr, UID logUid);
	Future<int64_t> getLogBytesWritten(Reference<ReadYourWritesTransaction> tr, UID logUid);

	// stopWhenDone will return when the backup is stopped, if enabled. Otherwise, it
	// will return when the backup directory is restorable.
	Future<int> waitBackup(Database cx, std::string tagName, bool stopWhenDone = true);

	Future<std::string> getLastBackupContainer(Reference<ReadYourWritesTransaction> tr, UID logUid);
	Future<std::string> getLastBackupContainer(Database cx, UID logUid) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getLastBackupContainer(tr, logUid); });
	}
	static Future<std::string> getBackupInfo(std::string backupContainer, Version* defaultVersion = NULL);

	static std::string getTempFilename();
	// Data(key ranges) and Log files will have their file size in the name because it is not at all convenient
	// to fetch filesizes from either of the current BackupContainer implementations. LocalDirectory requires
	// querying each file separately, and Blob Store doesn't support renames so the apparent log and data files
	// are actually a kind of symbolic link so to get the size of the final file it would have to be read.
	static std::string getDataFilename(Version version, int64_t size, int blockSize);
	static std::string getLogFilename(Version beginVer, Version endVer, int64_t size, int blockSize);

	Future<int64_t> getTaskCount(Reference<ReadYourWritesTransaction> tr) { return taskBucket->getTaskCount(tr); }
	Future<int64_t> getTaskCount(Database cx) { return taskBucket->getTaskCount(cx); }
	Future<Void> watchTaskCount(Reference<ReadYourWritesTransaction> tr) { return taskBucket->watchTaskCount(tr); }

	Future<bool> checkActive(Database cx) { return taskBucket->checkActive(cx); }

	friend class FileBackupAgentImpl;
	static const int dataFooterSize;

	Subspace subspace;
	Subspace config;
	Subspace errors;
	Subspace tagNames;
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

	Future<Void> abortBackup(Database cx, Key tagName, bool partial = false);

	Future<std::string> getStatus(Database cx, int errorLimit, Key tagName);

	Future<int> getStateValue(Reference<ReadYourWritesTransaction> tr, UID logUid);
	Future<int> getStateValue(Database cx, UID logUid) {
		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr){ return getStateValue(tr, logUid); });
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

	static const Key keyAddPrefix;
	static const Key keyRemovePrefix;
	static const Key keyRangeVersions;
	static const Key keyCopyStop;
	static const Key keyDatabasesInSync;

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
Standalone<VectorRef<KeyRangeRef>> getLogRanges(Version beginVersion, Version endVersion, Key backupUid, int blockSize = CLIENT_KNOBS->LOG_RANGE_BLOCK_SIZE);
Standalone<VectorRef<KeyRangeRef>> getApplyRanges(Version beginVersion, Version endVersion, Key backupUid);
Key getApplyKey( Version version, Key backupUid );
std::pair<uint64_t, uint32_t> decodeBKMutationLogKey(Key key);
Standalone<VectorRef<MutationRef>> decodeBackupLogValue(StringRef value);
void decodeBackupLogValue(Arena& arena, VectorRef<MutationRef>& result, int64_t& mutationSize, StringRef value, StringRef addPrefix = StringRef(), StringRef removePrefix = StringRef());
Future<Void> logErrorWorker(Reference<ReadYourWritesTransaction> const& tr, Key const& keyErrors, std::string const& message);
Future<Void> logError(Database cx, Key keyErrors, const std::string& message);
Future<Void> logError(Reference<ReadYourWritesTransaction> tr, Key keyErrors, const std::string& message);
Future<Void> checkVersion(Reference<ReadYourWritesTransaction> const& tr);
Future<Void> readCommitted(Database const& cx, PromiseStream<RangeResultWithVersion> const& results, Reference<FlowLock> const& lock, KeyRangeRef const& range, bool const& terminator = false, bool const& systemAccess = false, bool const& lockAware = false);
Future<Void> readCommitted(Database const& cx, PromiseStream<RCGroup> const& results, Future<Void> const& active, Reference<FlowLock> const& lock, KeyRangeRef const& range, std::function< std::pair<uint64_t, uint32_t>(Key key) > const& groupBy, bool const& terminator = false, bool const& systemAccess = false, bool const& lockAware = false, std::function< Future<Void>(Reference<ReadYourWritesTransaction> tr) > const& withEachFunction = nullptr);
Future<Void> applyMutations(Database const& cx, Key const& uid, Key const& addPrefix, Key const& removePrefix, Version const& beginVersion, Version* const& endVersion, RequestStream<CommitTransactionRequest> const& commit, NotifiedVersion* const& committedVersion, Reference<KeyRangeMap<Version>> const& keyVersion);

template <typename T>
class TaskParam {
public:
	TaskParam(StringRef key) : key(key) {}
	T get(Reference<Task> task) const {
		return Codec<T>::unpack(Tuple::unpack(task->params[key]));
	}
	void set(Reference<Task> task, T const &val) const {
		task->params[key] = Codec<T>::pack(val).pack();
	}
	bool exists(Reference<Task> task) const {
		return task->params.find(key) != task->params.end();
	}
	T getOrDefault(Reference<Task> task, const T defaultValue = T()) const {
		if(!exists(task))
			return defaultValue;
		return get(task);
	}
	StringRef key;
};

#endif
