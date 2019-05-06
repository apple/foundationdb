/*
 * RestoreCommon.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESTORECOMMON_ACTOR_G_H)
	#define FDBSERVER_RESTORECOMMON_ACTOR_G_H
	#include "fdbserver/RestoreCommon.actor.g.h"
#elif !defined(FDBSERVER_RESTORECOMMON_ACTOR_H)
	#define FDBSERVER_RESTORECOMMON_ACTOR_H

#include "fdbclient/Tuple.h"

#include "flow/flow.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbclient/BackupAgent.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // has to be last include

// RestoreConfig copied from FileBackupAgent.actor.cpp
// We copy RestoreConfig instead of using (and potentially changing) it in place to avoid conflict with the existing code
// TODO: Merge this RestoreConfig with the original RestoreConfig in FileBackupAgent.actor.cpp
typedef FileBackupAgent::ERestoreState ERestoreState;
struct RestoreFileFR;

// We copy RestoreConfig copied from FileBackupAgent.actor.cpp instead of using (and potentially changing) it in place to avoid conflict with the existing code
// Split RestoreConfig defined in FileBackupAgent.actor.cpp to declaration in Restore.actor.h and implementation in RestoreCommon.actor.cpp,
// so that we can use in both the existing restore and the new fast restore subsystems
// We use RestoreConfig as a Reference<RestoreConfig>, which leads to some non-functional changes in RestoreConfig
class RestoreConfig : public KeyBackedConfig, public ReferenceCounted<RestoreConfig> {
public:
	RestoreConfig(UID uid = UID()) : KeyBackedConfig(fileRestorePrefixRange.begin, uid) {}
	RestoreConfig(Reference<Task> task) : KeyBackedConfig(fileRestorePrefixRange.begin, task) {}

	KeyBackedProperty<ERestoreState> stateEnum(); 

	Future<StringRef> stateText(Reference<ReadYourWritesTransaction> tr);

	KeyBackedProperty<Key> addPrefix();

	KeyBackedProperty<Key> removePrefix();

	// XXX: Remove restoreRange() once it is safe to remove. It has been changed to restoreRanges
	KeyBackedProperty<KeyRange> restoreRange();

	KeyBackedProperty<std::vector<KeyRange>> restoreRanges();

	KeyBackedProperty<Key> batchFuture();

	KeyBackedProperty<Version> restoreVersion();

	KeyBackedProperty<Reference<IBackupContainer>> sourceContainer();

	// Get the source container as a bare URL, without creating a container instance
	KeyBackedProperty<Value> sourceContainerURL();

	// Total bytes written by all log and range restore tasks.
	KeyBackedBinaryValue<int64_t> bytesWritten();

	// File blocks that have had tasks created for them by the Dispatch task
	KeyBackedBinaryValue<int64_t> filesBlocksDispatched();

	// File blocks whose tasks have finished
	KeyBackedBinaryValue<int64_t> fileBlocksFinished();

	// Total number of files in the fileMap
	KeyBackedBinaryValue<int64_t> fileCount();

	// Total number of file blocks in the fileMap
	KeyBackedBinaryValue<int64_t> fileBlockCount();

	Future<std::vector<KeyRange>> getRestoreRangesOrDefault(Reference<ReadYourWritesTransaction> tr);
	ACTOR static Future<std::vector<KeyRange>> getRestoreRangesOrDefault_impl(RestoreConfig *self, Reference<ReadYourWritesTransaction> tr);

	// Describes a file to load blocks from during restore.  Ordered by version and then fileName to enable
	// incrementally advancing through the map, saving the version and path of the next starting point.
	struct RestoreFile {
		Version version;
		std::string fileName;
		bool isRange;  // false for log file
		int64_t blockSize;
		int64_t fileSize;
		Version endVersion;  // not meaningful for range files

		Tuple pack() const {
			//fprintf(stderr, "Filename:%s\n", fileName.c_str());
			return Tuple()
				.append(version)
				.append(StringRef(fileName))
				.append(isRange)
				.append(fileSize)
				.append(blockSize)
				.append(endVersion);
		}
		static RestoreFile unpack(Tuple const &t) {
			RestoreFile r;
			int i = 0;
			r.version = t.getInt(i++);
			r.fileName = t.getString(i++).toString();
			r.isRange = t.getInt(i++) != 0;
			r.fileSize = t.getInt(i++);
			r.blockSize = t.getInt(i++);
			r.endVersion = t.getInt(i++);
			return r;
		}
	};

	//typedef KeyBackedSet<RestoreFile> FileSetT;
	KeyBackedSet<RestoreFile> fileSet();

	Future<bool> isRunnable(Reference<ReadYourWritesTransaction> tr);

	Future<Void> logError(Database cx, Error e, std::string const &details, void *taskInstance = nullptr);

	Key mutationLogPrefix();

	Key applyMutationsMapPrefix();

	ACTOR Future<int64_t> getApplyVersionLag_impl(Reference<ReadYourWritesTransaction> tr, UID uid);

	Future<int64_t> getApplyVersionLag(Reference<ReadYourWritesTransaction> tr);

	void initApplyMutations(Reference<ReadYourWritesTransaction> tr, Key addPrefix, Key removePrefix);

	void clearApplyMutationsKeys(Reference<ReadYourWritesTransaction> tr);

	void setApplyBeginVersion(Reference<ReadYourWritesTransaction> tr, Version ver);

	void setApplyEndVersion(Reference<ReadYourWritesTransaction> tr, Version ver);

	Future<Version> getApplyEndVersion(Reference<ReadYourWritesTransaction> tr);

	ACTOR static Future<std::string> getProgress_impl(Reference<RestoreConfig> restore, Reference<ReadYourWritesTransaction> tr);
	Future<std::string> getProgress(Reference<ReadYourWritesTransaction> tr);

	ACTOR static Future<std::string> getFullStatus_impl(Reference<RestoreConfig> restore, Reference<ReadYourWritesTransaction> tr);
	Future<std::string> getFullStatus(Reference<ReadYourWritesTransaction> tr);

	std::string toString(); // Added by Meng
};

typedef RestoreConfig::RestoreFile RestoreFile;


// Describes a file to load blocks from during restore.  Ordered by version and then fileName to enable
// incrementally advancing through the map, saving the version and path of the next starting point.
// NOTE: The struct RestoreFileFR can NOT be named RestoreFile, because compiler will get confused in linking which RestoreFile should be used.
// If we use RestoreFile, the compilation can succeed, but weird segmentation fault will happen.
struct RestoreFileFR {
	Version version;
	std::string fileName;
	bool isRange;  // false for log file
	int64_t blockSize;
	int64_t fileSize;
	Version endVersion;  // not meaningful for range files
	Version beginVersion;  // range file's beginVersion == endVersion; log file contains mutations in version [beginVersion, endVersion)
	int64_t cursor; //The start block location to be restored. All blocks before cursor have been scheduled to load and restore

	Tuple pack() const {
		return Tuple()
				.append(version)
				.append(StringRef(fileName))
				.append(isRange)
				.append(fileSize)
				.append(blockSize)
				.append(endVersion)
				.append(beginVersion)
				.append(cursor);
	}
	static RestoreFileFR unpack(Tuple const &t) {
		RestoreFileFR r;
		int i = 0;
		r.version = t.getInt(i++);
		r.fileName = t.getString(i++).toString();
		r.isRange = t.getInt(i++) != 0;
		r.fileSize = t.getInt(i++);
		r.blockSize = t.getInt(i++);
		r.endVersion = t.getInt(i++);
		r.beginVersion = t.getInt(i++);
		r.cursor = t.getInt(i++);
		return r;
	}

	bool operator<(const RestoreFileFR& rhs) const { return endVersion < rhs.endVersion; }

	RestoreFileFR() : version(invalidVersion), isRange(false), blockSize(0), fileSize(0), endVersion(invalidVersion), beginVersion(invalidVersion), cursor(0) {}

	RestoreFileFR(Version version, std::string fileName, bool isRange, int64_t blockSize, int64_t fileSize, Version endVersion, Version beginVersion) : version(version), fileName(fileName), isRange(isRange), blockSize(blockSize), fileSize(fileSize), endVersion(endVersion), beginVersion(beginVersion), cursor(0) {}


	std::string toString() const {
		std::stringstream ss;
		ss << "version:" << std::to_string(version) << " fileName:" << fileName  << " isRange:" << std::to_string(isRange)
				<< " blockSize:" << std::to_string(blockSize) << " fileSize:" << std::to_string(fileSize)
				<< " endVersion:" << std::to_string(endVersion) << std::to_string(beginVersion)  
				<< " cursor:" << std::to_string(cursor);
		return ss.str();
	}
};

namespace parallelFileRestore {
	ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeRangeFileBlock(Reference<IAsyncFile> file, int64_t offset, int len);
	ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeLogFileBlock(Reference<IAsyncFile> file, int64_t offset, int len);
}

#include "flow/unactorcompiler.h"
#endif //FDBCLIENT_Restore_H