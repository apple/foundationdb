/*
 * RestoreCommon.actor.h
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

// This file includes the code copied from the old restore in FDB 5.2
// The functions and structure declared in this file can be shared by
// the old restore and the new performant restore systems

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_RESTORECOMMON_ACTOR_G_H)
#define FDBSERVER_RESTORECOMMON_ACTOR_G_H
#include "fdbserver/RestoreCommon.actor.g.h"
#elif !defined(FDBSERVER_RESTORECOMMON_ACTOR_H)
#define FDBSERVER_RESTORECOMMON_ACTOR_H

#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "fdbclient/Tuple.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbserver/Knobs.h"

#include "flow/actorcompiler.h" // has to be last include

// RestoreConfig copied from FileBackupAgent.actor.cpp
// We copy RestoreConfig instead of using (and potentially changing) it in place
// to avoid conflict with the existing code.
// We also made minor changes to allow RestoreConfig to be ReferenceCounted
// TODO: Merge this RestoreConfig with the original RestoreConfig in FileBackupAgent.actor.cpp
// For convenience
typedef FileBackupAgent::ERestoreState ERestoreState;

struct RestoreFileFR;

// We copy RestoreConfig copied from FileBackupAgent.actor.cpp instead of using (and potentially changing) it in place
// to avoid conflict with the existing code Split RestoreConfig defined in FileBackupAgent.actor.cpp to declaration in
// Restore.actor.h and implementation in RestoreCommon.actor.cpp, so that we can use in both the existing restore and
// the new fast restore subsystems. We use RestoreConfig as a Reference<RestoreConfig>, which leads to some
// non-functional changes in RestoreConfig
class RestoreConfigFR : public KeyBackedConfig, public ReferenceCounted<RestoreConfigFR> {
public:
	RestoreConfigFR(UID uid = UID()) : KeyBackedConfig(fileRestorePrefixRange.begin, uid) {}
	RestoreConfigFR(Reference<Task> task) : KeyBackedConfig(fileRestorePrefixRange.begin, task) {}

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
	ACTOR static Future<std::vector<KeyRange>> getRestoreRangesOrDefault_impl(RestoreConfigFR* self,
	                                                                          Reference<ReadYourWritesTransaction> tr);

	// Describes a file to load blocks from during restore.  Ordered by version and then fileName to enable
	// incrementally advancing through the map, saving the version and path of the next starting point.
	struct RestoreFile {
		Version version;
		std::string fileName;
		bool isRange; // false for log file
		int64_t blockSize;
		int64_t fileSize;
		Version endVersion; // not meaningful for range files

		Tuple pack() const {
			// fprintf(stderr, "Filename:%s\n", fileName.c_str());
			return Tuple()
			    .append(version)
			    .append(StringRef(fileName))
			    .append(isRange)
			    .append(fileSize)
			    .append(blockSize)
			    .append(endVersion);
		}
		static RestoreFile unpack(Tuple const& t) {
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

	// typedef KeyBackedSet<RestoreFile> FileSetT;
	KeyBackedSet<RestoreFile> fileSet();

	Future<bool> isRunnable(Reference<ReadYourWritesTransaction> tr);

	Future<Void> logError(Database cx, Error e, std::string const& details, void* taskInstance = nullptr);

	Key mutationLogPrefix();

	Key applyMutationsMapPrefix();

	ACTOR Future<int64_t> getApplyVersionLag_impl(Reference<ReadYourWritesTransaction> tr, UID uid);

	Future<int64_t> getApplyVersionLag(Reference<ReadYourWritesTransaction> tr);

	void initApplyMutations(Reference<ReadYourWritesTransaction> tr, Key addPrefix, Key removePrefix);

	void clearApplyMutationsKeys(Reference<ReadYourWritesTransaction> tr);

	void setApplyBeginVersion(Reference<ReadYourWritesTransaction> tr, Version ver);

	void setApplyEndVersion(Reference<ReadYourWritesTransaction> tr, Version ver);

	Future<Version> getApplyEndVersion(Reference<ReadYourWritesTransaction> tr);

	ACTOR static Future<std::string> getProgress_impl(Reference<RestoreConfigFR> restore,
	                                                  Reference<ReadYourWritesTransaction> tr);
	Future<std::string> getProgress(Reference<ReadYourWritesTransaction> tr);

	ACTOR static Future<std::string> getFullStatus_impl(Reference<RestoreConfigFR> restore,
	                                                    Reference<ReadYourWritesTransaction> tr);
	Future<std::string> getFullStatus(Reference<ReadYourWritesTransaction> tr);

	std::string toString(); // Added by Meng
};

// typedef RestoreConfigFR::RestoreFile RestoreFile;

// Describes a file to load blocks from during restore.  Ordered by version and then fileName to enable
// incrementally advancing through the map, saving the version and path of the next starting point.
// NOTE: The struct RestoreFileFR can NOT be named RestoreFile, because compiler will get confused in linking which
// RestoreFile should be used. If we use RestoreFile, compilation succeeds, but weird segmentation fault will happen.
struct RestoreFileFR {
	Version version;
	std::string fileName;
	bool isRange; // false for log file
	int64_t blockSize;
	int64_t fileSize;
	Version endVersion; // not meaningful for range files
	Version beginVersion; // range file's beginVersion == endVersion; log file contains mutations in version
	                      // [beginVersion, endVersion)
	int64_t cursor; // The start block location to be restored. All blocks before cursor have been scheduled to load and
	                // restore
	int fileIndex; // index of backup file. Must be identical per file.
	int partitionId = -1; // Partition ID (Log Router Tag ID) for mutation files.

	Tuple pack() const {
		return Tuple()
		    .append(version)
		    .append(StringRef(fileName))
		    .append(isRange)
		    .append(fileSize)
		    .append(blockSize)
		    .append(endVersion)
		    .append(beginVersion)
		    .append(cursor)
		    .append(fileIndex)
		    .append(partitionId);
	}
	static RestoreFileFR unpack(Tuple const& t) {
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
		r.fileIndex = t.getInt(i++);
		r.partitionId = t.getInt(i++);
		return r;
	}

	bool operator<(const RestoreFileFR& rhs) const {
		return std::tie(beginVersion, endVersion, fileIndex, fileName) <
		       std::tie(rhs.beginVersion, rhs.endVersion, rhs.fileIndex, rhs.fileName);
	}

	RestoreFileFR()
	  : version(invalidVersion), isRange(false), blockSize(0), fileSize(0), endVersion(invalidVersion),
	    beginVersion(invalidVersion), cursor(0), fileIndex(0) {}

	explicit RestoreFileFR(const RangeFile& f)
	  : version(f.version), fileName(f.fileName), isRange(true), blockSize(f.blockSize), fileSize(f.fileSize),
	    endVersion(f.version), beginVersion(f.version), cursor(0), fileIndex(0) {}

	explicit RestoreFileFR(const LogFile& f)
	  : version(f.beginVersion), fileName(f.fileName), isRange(false), blockSize(f.blockSize), fileSize(f.fileSize),
	    endVersion(f.endVersion), beginVersion(f.beginVersion), cursor(0), fileIndex(0), partitionId(f.tagId) {}

	std::string toString() const {
		std::stringstream ss;
		ss << "version:" << version << " fileName:" << fileName << " isRange:" << isRange << " blockSize:" << blockSize
		   << " fileSize:" << fileSize << " endVersion:" << endVersion << " beginVersion:" << beginVersion
		   << " cursor:" << cursor << " fileIndex:" << fileIndex << " partitionId:" << partitionId;
		return ss.str();
	}
};

namespace parallelFileRestore {
ACTOR Future<Standalone<VectorRef<KeyValueRef>>> decodeLogFileBlock(Reference<IAsyncFile> file,
                                                                    int64_t offset,
                                                                    int len);
} // namespace parallelFileRestore

// Send each request in requests via channel of the request's interface.
// Save replies to replies if replies != nullptr
// The UID in a request is the UID of the interface to handle the request
ACTOR template <class Interface, class Request>
Future<Void> getBatchReplies(RequestStream<Request> Interface::*channel,
                             std::map<UID, Interface> interfaces,
                             std::vector<std::pair<UID, Request>> requests,
                             std::vector<REPLY_TYPE(Request)>* replies,
                             TaskPriority taskID = TaskPriority::Low,
                             bool trackRequestLatency = true) {
	if (requests.empty()) {
		return Void();
	}

	state double start = now();
	state int oustandingReplies = requests.size();
	loop {
		try {
			state std::vector<Future<REPLY_TYPE(Request)>> cmdReplies;
			state std::vector<std::tuple<UID, Request, double>> replyDurations; // double is end time of the request
			for (auto& request : requests) {
				RequestStream<Request> const* stream = &(interfaces[request.first].*channel);
				cmdReplies.push_back(stream->getReply(request.second, taskID));
				replyDurations.emplace_back(request.first, request.second, 0);
			}

			state std::vector<Future<REPLY_TYPE(Request)>> ongoingReplies;
			state std::vector<int> ongoingRepliesIndex;
			loop {
				ongoingReplies.clear();
				ongoingRepliesIndex.clear();
				for (int i = 0; i < cmdReplies.size(); ++i) {
					if (SERVER_KNOBS->FASTRESTORE_REQBATCH_LOG) {
						TraceEvent(SevInfo, "FastRestoreGetBatchReplies")
						    .suppressFor(1.0)
						    .detail("Requests", requests.size())
						    .detail("OutstandingReplies", oustandingReplies)
						    .detail("ReplyIndex", i)
						    .detail("ReplyIsReady", cmdReplies[i].isReady())
						    .detail("ReplyIsError", cmdReplies[i].isError())
						    .detail("RequestNode", requests[i].first)
						    .detail("Request", requests[i].second.toString());
					}
					if (!cmdReplies[i].isReady()) { // still wait for reply
						ongoingReplies.push_back(cmdReplies[i]);
						ongoingRepliesIndex.push_back(i);
					}
				}
				ASSERT(ongoingReplies.size() == oustandingReplies);
				if (ongoingReplies.empty()) {
					break;
				} else {
					wait(
					    quorum(ongoingReplies,
					           std::min((int)SERVER_KNOBS->FASTRESTORE_REQBATCH_PARALLEL, (int)ongoingReplies.size())));
				}
				// At least one reply is received; Calculate the reply duration
				for (int j = 0; j < ongoingReplies.size(); ++j) {
					if (ongoingReplies[j].isReady()) {
						std::get<2>(replyDurations[ongoingRepliesIndex[j]]) = now();
						--oustandingReplies;
					} else if (ongoingReplies[j].isError()) {
						// When this happens,
						// the above assertion ASSERT(ongoingReplies.size() == oustandingReplies) will fail
						TraceEvent(SevError, "FastRestoreGetBatchRepliesReplyError")
						    .detail("OngoingReplyIndex", j)
						    .detail("FutureError", ongoingReplies[j].getError().what());
					}
				}
			}
			ASSERT(oustandingReplies == 0);
			if (trackRequestLatency && SERVER_KNOBS->FASTRESTORE_TRACK_REQUEST_LATENCY) {
				// Calculate the latest end time for each interface
				std::map<UID, double> maxEndTime;
				UID bathcID = deterministicRandom()->randomUniqueID();
				for (int i = 0; i < replyDurations.size(); ++i) {
					double endTime = std::get<2>(replyDurations[i]);
					TraceEvent(SevInfo, "ProfileSendRequestBatchLatency", bathcID)
					    .detail("Node", std::get<0>(replyDurations[i]))
					    .detail("Request", std::get<1>(replyDurations[i]).toString())
					    .detail("Duration", endTime - start);
					auto item = maxEndTime.emplace(std::get<0>(replyDurations[i]), endTime);
					item.first->second = std::max(item.first->second, endTime);
				}
				// Check the time gap between the earliest and latest node
				double earliest = std::numeric_limits<double>::max();
				double latest = std::numeric_limits<double>::min();
				UID earliestNode, latestNode;

				for (auto& endTime : maxEndTime) {
					if (earliest > endTime.second) {
						earliest = endTime.second;
						earliestNode = endTime.first;
					}
					if (latest < endTime.second) {
						latest = endTime.second;
						latestNode = endTime.first;
					}
				}
				if (latest - earliest > SERVER_KNOBS->FASTRESTORE_STRAGGLER_THRESHOLD_SECONDS) {
					TraceEvent(SevWarn, "ProfileSendRequestBatchLatencyFoundStraggler", bathcID)
					    .detail("SlowestNode", latestNode)
					    .detail("FatestNode", earliestNode)
					    .detail("EarliestEndtime", earliest)
					    .detail("LagTime", latest - earliest);
				}
			}
			// Update replies
			if (replies != nullptr) {
				for (int i = 0; i < cmdReplies.size(); ++i) {
					replies->emplace_back(cmdReplies[i].get());
				}
			}
			break;
		} catch (Error& e) {
			if (e.code() == error_code_operation_cancelled)
				break;
			// fprintf(stdout, "sendBatchRequests Error code:%d, error message:%s\n", e.code(), e.what());
			TraceEvent(SevWarn, "FastRestoreSendBatchRequests").error(e);
			for (auto& request : requests) {
				TraceEvent(SevWarn, "FastRestoreSendBatchRequests")
				    .detail("SendBatchRequests", requests.size())
				    .detail("RequestID", request.first)
				    .detail("Request", request.second.toString());
				resetReply(request.second);
			}
		}
	}

	return Void();
}

// Similar to getBatchReplies except that the caller does not expect to process the reply info.
ACTOR template <class Interface, class Request>
Future<Void> sendBatchRequests(RequestStream<Request> Interface::*channel,
                               std::map<UID, Interface> interfaces,
                               std::vector<std::pair<UID, Request>> requests,
                               TaskPriority taskID = TaskPriority::Low,
                               bool trackRequestLatency = true) {
	wait(getBatchReplies(channel, interfaces, requests, nullptr, taskID, trackRequestLatency));

	return Void();
}

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_RESTORECOMMON_ACTOR_H
