/*
 * CDCProxy.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <deque>
#include <limits>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h"
#include "NativeCdcInternal.h"
#include "fdbserver/cdcproxy/CDCProxy.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/LogProtocolMessage.h"
#include "fdbserver/core/OTELSpanContextMessage.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/core/SpanContextMessage.h"
#include "fdbserver/core/WaitFailure.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/logsystem/LogSystemConsumer.h"
#include "fdbserver/logsystem/LogSystemFactory.h"
#include "flow/ActorCollection.h"
#include "flow/CodeProbe.h"
#include "flow/Error.h"
#include "flow/ScopeExit.h"
#include "flow/UnitTest.h"
#include "flow/genericactors.actor.h"

namespace {

// Snapshot from one durable metadata read, used while initializing or validating one stream.
struct CDCStreamReadState {
	Optional<KeyRange> keys;
	Version minVersion = invalidVersion;
	Version readVersion = invalidVersion;
	// Each Version is the inclusive lower bound for log versions routed to its paired tag; the next entry's
	// Version is the exclusive upper bound.
	std::vector<std::pair<Version, Tag>> tagAssignments;
};

// Half-open tag-routing interval retained with one buffered stream; bufferedThrough is its inclusive frontier.
struct CDCTagInterval {
	Tag tag;
	Version begin;
	Version end;
	Version bufferedThrough;

	CDCTagInterval(Tag tag, Version begin, Version end)
	  : tag(tag), begin(begin), end(end), bufferedThrough(begin - 1) {}
};

// Proxy-owned state for one assigned stream. In-flight actors may retain it after active becomes false.
struct CDCBufferedStream : ReferenceCounted<CDCBufferedStream> {
	CDCStreamId streamId;
	Optional<KeyRange> keys;
	bool active = true;
	bool initialized = false;
	bool tooOld = false;
	bool bufferLimitExceeded = false;
	Version minVersion = invalidVersion;
	Version bufferedThrough = invalidVersion;
	int64_t bufferedBytes = 0;
	int readDemand = 0;
	int activeConsumes = 0;
	std::vector<CDCTagInterval> tagIntervals;
	std::deque<Standalone<VersionedMutationsRef>> mutations;
	AsyncTrigger changed;

	explicit CDCBufferedStream(CDCStreamId streamId) : streamId(streamId) {}
};

// Per-stream scratch mutations from one tag-buffering pass, moved into CDCBufferedStream before the pass returns.
struct CDCBufferedBatch {
	int64_t bufferedBytes = 0;
	std::deque<Standalone<VersionedMutationsRef>> mutations;
};

// Proxy-owned state for one shared tag and its buffering actor; dropped after the last active stream detaches.
struct CDCBufferedTag : ReferenceCounted<CDCBufferedTag> {
	Tag tag;
	bool active = true;
	std::set<CDCStreamId> streamIds;
	AsyncTrigger refresh;
	AsyncTrigger stopped;

	explicit CDCBufferedTag(Tag tag) : tag(tag) {}
};

// One stream's frontier and estimated materialization cost while selecting work for a single tag-buffering pass.
struct CDCBufferCandidate {
	CDCStreamId streamId;
	Version nextVersion;
	int64_t estimatedBytes;
};

// The stream set selected from one pass's candidates; discarded after those streams' batches are materialized.
struct CDCBufferSelection {
	std::unordered_set<CDCStreamId> selectedStreamIds;
	std::vector<CDCStreamId> oversizedStreamIds;
	int64_t selectedBytes = 0;
};

// Memory reservations derived for one tag-buffering pass; discarded when that pass releases its raw peek arenas.
struct CDCBufferPassLimits {
	int64_t rawReplyBytes;
	int64_t preferredBufferedBytes;
	int64_t hardBufferedBytes;
	int64_t reservationBytes;
};

// A transactionally consistent durable-watermark snapshot consumed by one acknowledged-data pop pass.
struct CDCPopState {
	std::unordered_map<CDCStreamId, Version> minVersions;
	std::unordered_map<Tag, Version> safePopVersions;
	std::unordered_map<Tag, Version> retiredTagPopVersions;
	Version readVersion = invalidVersion;
};

bool hasCompleteLogSystemConfig(LogSystemConfig const& config) {
	return config.expectedLogSets > 0 && config.tLogs.size() == static_cast<size_t>(config.expectedLogSets);
}

enum class CDCBufferTagPassResult { RETRY, WAIT_FOR_COMMIT, STOP };

Optional<CDCBufferPassLimits> calculateBufferPassLimits(int64_t bufferBytes,
                                                        int64_t maximumPeekBytes,
                                                        int64_t retainedReplyCount) {
	if (bufferBytes <= 0 || maximumPeekBytes <= 0 || retainedReplyCount <= 0 ||
	    retainedReplyCount > (bufferBytes - 1) / maximumPeekBytes) {
		return Optional<CDCBufferPassLimits>();
	}

	const int64_t rawReplyBytes = retainedReplyCount * maximumPeekBytes;
	const int64_t hardBufferedBytes = bufferBytes - rawReplyBytes;
	const int64_t preferredBufferedBytes = std::min(maximumPeekBytes, hardBufferedBytes);
	return CDCBufferPassLimits{
		rawReplyBytes, preferredBufferedBytes, hardBufferedBytes, rawReplyBytes + preferredBufferedBytes
	};
}

// AsyncTrigger intentionally drops notifications when nobody is waiting. Pop work needs level-triggered semantics:
// an acknowledgement that arrives during a scan must cause one later pass without canceling the pass in flight.
class CoalescedTrigger : NonCopyable {
	bool pending = false;
	AsyncTrigger changed;

public:
	void trigger() {
		pending = true;
		changed.trigger();
	}

	Future<Void> onTrigger() const { return pending ? Future<Void>(Void()) : changed.onTrigger(); }

	bool consume() {
		const bool result = pending;
		pending = false;
		return result;
	}
};

CDCBufferSelection selectBufferCandidates(std::vector<CDCBufferCandidate> candidates,
                                          int64_t preferredLimit,
                                          int64_t hardLimit) {
	ASSERT_GT(preferredLimit, 0);
	ASSERT_GE(hardLimit, preferredLimit);
	std::sort(candidates.begin(), candidates.end(), [](const auto& lhs, const auto& rhs) {
		return lhs.nextVersion == rhs.nextVersion ? lhs.streamId < rhs.streamId : lhs.nextVersion < rhs.nextVersion;
	});

	CDCBufferSelection selection;
	for (const auto& candidate : candidates) {
		ASSERT_GE(candidate.estimatedBytes, 0);
		if (candidate.estimatedBytes > hardLimit) {
			selection.oversizedStreamIds.push_back(candidate.streamId);
			continue;
		}
		if (candidate.estimatedBytes == 0) {
			selection.selectedStreamIds.insert(candidate.streamId);
			continue;
		}
		if (selection.selectedBytes == 0 && candidate.estimatedBytes > preferredLimit) {
			selection.selectedStreamIds.insert(candidate.streamId);
			selection.selectedBytes = candidate.estimatedBytes;
			continue;
		}
		if (selection.selectedBytes <= preferredLimit - candidate.estimatedBytes) {
			selection.selectedStreamIds.insert(candidate.streamId);
			selection.selectedBytes += candidate.estimatedBytes;
		}
	}
	return selection;
}

Version committedPeekThrough(Version peekThrough, Version minKnownCommittedVersion) {
	return std::min(peekThrough, minKnownCommittedVersion);
}

Version cdcLagVersions(Version committedVersion, Version retainedVersion) {
	if (committedVersion < 0 || retainedVersion < 0 || retainedVersion == std::numeric_limits<Version>::max()) {
		return invalidVersion;
	}
	return std::max<Version>(0, committedVersion - retainedVersion);
}

bool rawPeekFailureBlocksStream(Optional<Version> nextReadVersion, Version failedBegin) {
	return nextReadVersion.present() && nextReadVersion.get() == failedBegin;
}

bool isCurrentStreamInitialization(std::unordered_map<CDCStreamId, Reference<CDCBufferedStream>> const& streams,
                                   Reference<CDCBufferedStream> stream) {
	auto current = streams.find(stream->streamId);
	return stream->active && current != streams.end() && current->second.getPtr() == stream.getPtr();
}

// New TLogs retain every recovered tag until it is popped past the recovery boundary. An unshared retired tag has no
// active consumer to advance it, so stopping at its pre-recovery removal version can keep the old generation forever.
Version retiredTagPopTarget(Version retiredVersion, Optional<Version> safePopVersion, Optional<Version> recoveryEnd) {
	if (safePopVersion.present()) {
		return std::min(retiredVersion, safePopVersion.get());
	}
	return recoveryEnd.present() ? std::max(retiredVersion, recoveryEnd.get()) : retiredVersion;
}

class CDCProxy {
	UID id;
	Database cx;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<AsyncVar<Reference<LogSystemConsumer>>> logSystem;
	std::unordered_map<CDCStreamId, Reference<CDCBufferedStream>> streams;
	std::unordered_map<Tag, Reference<CDCBufferedTag>> tags;
	CoalescedTrigger popAcknowledgedDataRequests;
	AsyncTrigger popLogSystemChanged;
	AsyncTrigger peekCapacityContended;
	FlowLock bufferLock;
	int64_t bufferedBytes = 0;
	int64_t totalBufferedMutationBytes = 0;
	int64_t peakActivePermits = 0;
	int activeConsumeRequests = 0;
	int64_t popRequests = 0;
	int64_t popAttempts = 0;
	int64_t popCompletions = 0;
	int64_t popCancellations = 0;
	bool popsPausedForTesting = false;
	bool popsPausedAfterSnapshotForTesting = false;
	int64_t popSnapshotsPausedForTesting = 0;
	bool popsPausedAfterIncompleteLogSystemForTesting = false;
	int64_t popIncompleteLogSystemPausesForTesting = 0;
	AsyncTrigger popPauseChangedForTesting;
	bool logSystemInitialized = false;
	bool lastLogSystemHasTLogs = false;
	uint64_t lastLogSystemRecoveryCount = 0;
	LogSystemConfig lastLogSystemConfig;
	Version latestCommittedVersion = invalidVersion;
	std::unordered_map<Tag, Version> lastSafePopVersions;
	ActorCollection actors;

	void recordBufferUsage();
	void requestAcknowledgedDataPop();
	void refreshLogSystem();
	bool isCurrentCompletePopLogSystem(Reference<LogSystemConsumer> const& currentLogSystem,
	                                   LogSystemConfig const& config,
	                                   uint64_t recoveryCount) const;
	void clearBufferedMutations(Reference<CDCBufferedStream> stream);
	void addBufferedBatch(Reference<CDCBufferedStream> stream, CDCBufferedBatch batch);
	void reconcileStreamMinVersion(Reference<CDCBufferedStream> stream, Version minVersion);
	void markTagStreamsBufferLimitExceeded(Reference<CDCBufferedTag> tag, Version begin);
	void markTagStreamsRawReplyBudgetExceeded(Reference<CDCBufferedTag> tag, Version begin, int64_t retainedReplyCount);
	void detachStreamFromTags(Reference<CDCBufferedStream> stream);
	void deactivateStream(Reference<CDCBufferedStream> stream);
	void refreshStreamTags(Reference<CDCBufferedStream> stream);
	Optional<Version> nextTagReadVersionForStream(Reference<CDCBufferedTag> tag, Reference<CDCBufferedStream> stream);
	Optional<Version> nextTagReadVersion(Reference<CDCBufferedTag> tag);
	void advanceTagBufferedThrough(Reference<CDCBufferedTag> tag,
	                               Version bufferedThrough,
	                               std::unordered_set<CDCStreamId> const& selectedStreamIds);
	void markPoppedTagStreamsTooOld(Reference<CDCBufferedTag> tag, Version popped);
	template <class Visitor>
	void visitBufferedMutations(Reference<CDCBufferedTag> tag,
	                            Reference<IReplayPeekCursor> cursor,
	                            Version throughVersion,
	                            std::unordered_set<CDCStreamId> const* selectedStreamIds,
	                            Visitor&& visitor);
	std::unordered_map<CDCStreamId, int64_t> estimateBufferedBytes(Reference<CDCBufferedTag> tag,
	                                                               Reference<IReplayPeekCursor> cursor,
	                                                               Version throughVersion);
	std::unordered_map<CDCStreamId, CDCBufferedBatch> bufferMessages(
	    Reference<CDCBufferedTag> tag,
	    Reference<IReplayPeekCursor> cursor,
	    Version throughVersion,
	    std::unordered_set<CDCStreamId> const& selectedStreamIds);
	std::vector<CDCBufferCandidate> getBufferCandidates(Reference<CDCBufferedTag> tag,
	                                                    Version throughVersion,
	                                                    std::unordered_map<CDCStreamId, int64_t> const& estimatedBytes);
	Future<Void> rotateContendedPeek();
	Future<CDCBufferTagPassResult> bufferTagPass(Reference<CDCBufferedTag> tag, Version begin);
	Future<Void> bufferTag(Reference<CDCBufferedTag> tag);
	Future<Void> initializeStream(Reference<CDCBufferedStream> stream);
	Future<Void> waitForBufferedVersion(Reference<CDCBufferedStream> stream, Version version);
	Future<Void> popAcknowledgedData();
	Future<Void> monitorAcknowledgedDataPops();
	void reconcileStreams();
	Future<Void> consume(CDCConsumeRequest request);
	Future<Void> acknowledge(CDCAckRequest request);
	Future<Void> registerStream(CDCRegisterStreamRequest request);
	Future<Void> removeStream(CDCRemoveStreamRequest request);
	Future<Void> serveConsumeRequests(FutureStream<CDCConsumeRequest> requests);
	Future<Void> serveAcknowledgeRequests(FutureStream<CDCAckRequest> requests);
	Future<Void> serveRegisterStreamRequests(FutureStream<CDCRegisterStreamRequest> requests);
	Future<Void> serveRemoveStreamRequests(FutureStream<CDCRemoveStreamRequest> requests);
	Future<Void> serveHaltForTestingRequests(FutureStream<HaltCDCProxyRequest> requests);
	Future<Void> serveBufferStatusForTestingRequests(FutureStream<GetCDCProxyBufferStatusRequest> requests);
	Future<Void> serveSetPopsPausedForTestingRequests(FutureStream<SetCDCProxyPopsPausedRequest> requests);
	Future<Void> traceMetrics();
	Future<Void> monitorDBInfo(CDCProxyInterface proxy, uint64_t recoveryCount);

public:
	CDCProxy(CDCProxyInterface const& proxy, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
	  : id(proxy.id()), cx(openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True)), dbInfo(dbInfo),
	    logSystem(makeReference<AsyncVar<Reference<LogSystemConsumer>>>()),
	    bufferLock(SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES), actors(false) {}

	Future<Void> run(CDCProxyInterface proxy, uint64_t recoveryCount);
};

Optional<MutationRef> clipCDCMutation(MutationRef const& mutation, KeyRangeRef const& keys) {
	if (isSingleKeyMutation((MutationRef::Type)mutation.type)) {
		if (keys.contains(mutation.param1)) {
			return mutation;
		}
	} else if (mutation.type == MutationRef::ClearRange) {
		KeyRangeRef intersection = keys & KeyRangeRef(mutation.param1, mutation.param2);
		if (!intersection.empty()) {
			return MutationRef(MutationRef::ClearRange, intersection.begin, intersection.end);
		}
	} else {
		ASSERT(false);
	}
	return Optional<MutationRef>();
}

Future<CDCStreamReadState> readCDCStreamState(Database cx,
                                              CDCStreamId streamId,
                                              UID expectedProxyId,
                                              bool requireKeys) {
	if (streamId == 0) {
		throw client_invalid_operation();
	}

	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			CDCStreamReadState result;
			Optional<Value> keysValue = co_await tr.get(cdcStreamKeyFor(streamId));
			if (keysValue.present()) {
				result.keys = decodeCDCStreamKeysValue(keysValue.get());
			} else if (requireKeys) {
				throw client_invalid_operation();
			}

			Optional<Value> minVersionValue = co_await tr.get(cdcMinVersionKeyFor(streamId));
			if (!minVersionValue.present()) {
				throw client_invalid_operation();
			}
			result.minVersion = decodeCDCMinVersionValue(minVersionValue.get());

			RangeResult assignedProxies = co_await tr.getRange(cdcProxyRangeFor(streamId), 2);
			if (assignedProxies.size() != 1 || decodeCDCProxyKey(assignedProxies[0].key).second != expectedProxyId) {
				CODE_PROBE(true, "CDC proxy rejects request for stream owned elsewhere");
				throw wrong_shard_server();
			}

			std::vector<std::pair<Version, Tag>> tagAssignments;
			KeyRange tagHistoryRange = cdcTagHistoryRangeFor(streamId);
			Key begin = tagHistoryRange.begin;
			while (begin < tagHistoryRange.end) {
				RangeResult history =
				    co_await tr.getRange(KeyRangeRef(begin, tagHistoryRange.end), CLIENT_KNOBS->TOO_MANY);
				for (KeyValueRef const& kv : history) {
					const CDCTagHistoryEntry historyEntry = decodeCDCTagHistoryKey(kv.key);
					ASSERT_WE_THINK(historyEntry.streamId == streamId);
					ASSERT_WE_THINK(historyEntry.tag.locality == tagLocalityCDC);
					tagAssignments.emplace_back(historyEntry.version, historyEntry.tag);
				}
				if (!history.more) {
					break;
				}
				begin = keyAfter(history.back().key);
			}
			if (tagAssignments.empty()) {
				throw client_invalid_operation();
			}

			result.readVersion = co_await tr.getReadVersion();
			result.tagAssignments = std::move(tagAssignments);
			co_return result;
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server) {
				throw;
			}
			err = e;
		}
		co_await tr.onError(err);
	}
}

void CDCProxy::recordBufferUsage() {
	peakActivePermits = std::max(peakActivePermits, bufferLock.activePermits());
	ASSERT_LE(bufferLock.activePermits(), SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES);
}

void CDCProxy::requestAcknowledgedDataPop() {
	++popRequests;
	popAcknowledgedDataRequests.trigger();
}

void CDCProxy::refreshLogSystem() {
	const ServerDBInfo& info = dbInfo->get();
	const bool hasLogSystem = !info.logSystemConfig.tLogs.empty();
	const bool logSystemChanged =
	    logSystemInitialized &&
	    (hasLogSystem != lastLogSystemHasTLogs ||
	     (hasLogSystem && (!logSystem->get() || info.recoveryCount != lastLogSystemRecoveryCount ||
	                       !info.logSystemConfig.isEqual(lastLogSystemConfig))));
	if (!logSystemInitialized || logSystemChanged) {
		logSystem->set(hasLogSystem ? makeLogSystemConsumerFromServerDBInfo(id, info) : Reference<LogSystemConsumer>());
	}
	logSystemInitialized = true;
	lastLogSystemHasTLogs = hasLogSystem;
	lastLogSystemRecoveryCount = info.recoveryCount;
	if (hasLogSystem) {
		lastLogSystemConfig = info.logSystemConfig;
	}
	if (logSystemChanged) {
		popLogSystemChanged.trigger();
	}
}

bool CDCProxy::isCurrentCompletePopLogSystem(Reference<LogSystemConsumer> const& currentLogSystem,
                                             LogSystemConfig const& config,
                                             uint64_t recoveryCount) const {
	return currentLogSystem.getPtr() != nullptr && currentLogSystem.getPtr() == logSystem->get().getPtr() &&
	       hasCompleteLogSystemConfig(config) && lastLogSystemRecoveryCount == recoveryCount &&
	       lastLogSystemConfig.isEqual(config) && dbInfo->get().recoveryCount == recoveryCount &&
	       dbInfo->get().logSystemConfig.isEqual(config);
}

void CDCProxy::clearBufferedMutations(Reference<CDCBufferedStream> stream) {
	if (stream->bufferedBytes > 0) {
		ASSERT_GE(bufferedBytes, stream->bufferedBytes);
		bufferedBytes -= stream->bufferedBytes;
		bufferLock.release(stream->bufferedBytes);
		stream->bufferedBytes = 0;
	}
	stream->mutations.clear();
}

void addMutationToBatch(Reference<CDCBufferedStream> stream,
                        CDCBufferedBatch* batch,
                        Version version,
                        MutationRef const& mutation) {
	auto batchVersion = std::find_if(batch->mutations.begin(), batch->mutations.end(), [version](const auto& buffered) {
		return buffered.version == version;
	});
	if (batchVersion == batch->mutations.end()) {
		batch->mutations.emplace_back();
		batchVersion = std::prev(batch->mutations.end());
		batchVersion->version = version;
		const bool alreadyBuffered =
		    std::any_of(stream->mutations.begin(), stream->mutations.end(), [version](const auto& buffered) {
			    return buffered.version == version;
		    });
		if (!alreadyBuffered) {
			batch->bufferedBytes += sizeof(VersionedMutationsRef);
		}
	}
	batchVersion->mutations.push_back_deep(batchVersion->arena(), mutation);
	batch->bufferedBytes += mutation.expectedSize() + sizeof(MutationRef);
}

void CDCProxy::addBufferedBatch(Reference<CDCBufferedStream> stream, CDCBufferedBatch batch) {
	while (!batch.mutations.empty()) {
		Standalone<VersionedMutationsRef> versioned = std::move(batch.mutations.front());
		batch.mutations.pop_front();
		auto location =
		    std::lower_bound(stream->mutations.begin(),
		                     stream->mutations.end(),
		                     versioned.version,
		                     [](const auto& buffered, Version version) { return buffered.version < version; });
		if (location != stream->mutations.end() && location->version == versioned.version) {
			for (const auto& mutation : versioned.mutations) {
				location->mutations.push_back_deep(location->arena(), mutation);
			}
		} else {
			stream->mutations.insert(location, std::move(versioned));
		}
	}
	stream->bufferedBytes += batch.bufferedBytes;
	bufferedBytes += batch.bufferedBytes;
	totalBufferedMutationBytes += batch.bufferedBytes;
}

void CDCProxy::markTagStreamsBufferLimitExceeded(Reference<CDCBufferedTag> tag, Version begin) {
	for (const CDCStreamId streamId : tag->streamIds) {
		auto stream = streams.find(streamId);
		if (stream == streams.end() || !stream->second->active) {
			continue;
		}
		const Optional<Version> nextReadVersion = nextTagReadVersionForStream(tag, stream->second);
		if (!rawPeekFailureBlocksStream(nextReadVersion, begin)) {
			continue;
		}
		CODE_PROBE(true, "CDC proxy rejects a raw peek larger than its reservation", probe::decoration::rare);
		TraceEvent(SevWarn, "CDCProxyRawPeekExceedsBufferLimit", id)
		    .detail("Tag", tag->tag)
		    .detail("StreamId", streamId)
		    .detail("BeginVersion", begin)
		    .detail("RawPeekLimit", SERVER_KNOBS->MAXIMUM_PEEK_BYTES);
		stream->second->bufferLimitExceeded = true;
		stream->second->changed.trigger();
	}
}

void CDCProxy::markTagStreamsRawReplyBudgetExceeded(Reference<CDCBufferedTag> tag,
                                                    Version begin,
                                                    int64_t retainedReplyCount) {
	CODE_PROBE(true, "CDC proxy rejects raw reply fanout larger than its buffer", probe::decoration::rare);
	for (const CDCStreamId streamId : tag->streamIds) {
		auto stream = streams.find(streamId);
		if (stream == streams.end() || !stream->second->active) {
			continue;
		}
		TraceEvent(SevWarn, "CDCProxyRawReplyBudgetExceedsBufferLimit", id)
		    .detail("Tag", tag->tag)
		    .detail("StreamId", streamId)
		    .detail("BeginVersion", begin)
		    .detail("RetainedReplyCount", retainedReplyCount)
		    .detail("RawPeekLimitPerReply", SERVER_KNOBS->MAXIMUM_PEEK_BYTES)
		    .detail("BufferLimit", SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES);
		stream->second->bufferLimitExceeded = true;
		stream->second->changed.trigger();
	}
}

void updateStreamBufferedThrough(Reference<CDCBufferedStream> stream) {
	Version bufferedThrough = stream->minVersion - 1;
	for (const auto& interval : stream->tagIntervals) {
		if (interval.begin > bufferedThrough + 1) {
			break;
		}
		if (interval.bufferedThrough < interval.begin) {
			break;
		}
		bufferedThrough = std::max(bufferedThrough, interval.bufferedThrough);
		if (interval.bufferedThrough < interval.end - 1) {
			break;
		}
	}
	if (bufferedThrough > stream->bufferedThrough) {
		stream->bufferedThrough = bufferedThrough;
		stream->changed.trigger();
	}
}

void advanceStreamMinVersion(Reference<CDCBufferedStream> stream, Version minVersion) {
	stream->minVersion = std::max(stream->minVersion, minVersion);
	for (auto& interval : stream->tagIntervals) {
		if (stream->minVersion > interval.begin) {
			interval.bufferedThrough =
			    std::max(interval.bufferedThrough, std::min(stream->minVersion - 1, interval.end - 1));
		}
	}
	updateStreamBufferedThrough(stream);
}

void CDCProxy::reconcileStreamMinVersion(Reference<CDCBufferedStream> stream, Version minVersion) {
	advanceStreamMinVersion(stream, minVersion);
	while (!stream->mutations.empty() && stream->mutations.front().version < minVersion) {
		const int64_t releasedBytes =
		    sizeof(VersionedMutationsRef) + stream->mutations.front().mutations.expectedSize();
		stream->bufferedBytes -= releasedBytes;
		ASSERT_GE(bufferedBytes, releasedBytes);
		bufferedBytes -= releasedBytes;
		bufferLock.release(releasedBytes);
		stream->mutations.pop_front();
	}
	ASSERT_GE(stream->bufferedBytes, 0);
}

void CDCProxy::detachStreamFromTags(Reference<CDCBufferedStream> stream) {
	for (const auto& interval : stream->tagIntervals) {
		auto tag = tags.find(interval.tag);
		if (tag == tags.end()) {
			continue;
		}
		Reference<CDCBufferedTag> bufferedTag = tag->second;
		bufferedTag->streamIds.erase(stream->streamId);
		if (bufferedTag->streamIds.empty()) {
			bufferedTag->active = false;
			tags.erase(tag);
			bufferedTag->stopped.trigger();
		} else {
			bufferedTag->refresh.trigger();
		}
	}
}

void CDCProxy::deactivateStream(Reference<CDCBufferedStream> stream) {
	CODE_PROBE(stream->readDemand > 0, "CDC proxy wakes pending consume when stream is unassigned");
	CODE_PROBE(true, "CDC proxy drops removed or reassigned stream state");
	stream->active = false;
	stream->changed.trigger();
	detachStreamFromTags(stream);
	clearBufferedMutations(stream);
}

void CDCProxy::refreshStreamTags(Reference<CDCBufferedStream> stream) {
	for (const auto& interval : stream->tagIntervals) {
		auto tag = tags.find(interval.tag);
		if (tag != tags.end()) {
			tag->second->refresh.trigger();
		}
	}
}

Optional<Version> CDCProxy::nextTagReadVersionForStream(Reference<CDCBufferedTag> tag,
                                                        Reference<CDCBufferedStream> stream) {
	if (!stream->active || !stream->initialized || stream->bufferLimitExceeded || stream->readDemand == 0) {
		return Optional<Version>();
	}
	Optional<Version> begin;
	for (const auto& interval : stream->tagIntervals) {
		if (interval.tag != tag->tag) {
			continue;
		}
		const Version next = std::max(interval.begin, interval.bufferedThrough + 1);
		if (next < interval.end && (!begin.present() || next < begin.get())) {
			begin = next;
		}
	}
	return begin;
}

Optional<Version> CDCProxy::nextTagReadVersion(Reference<CDCBufferedTag> tag) {
	Optional<Version> begin;
	for (const CDCStreamId streamId : tag->streamIds) {
		auto stream = streams.find(streamId);
		if (stream == streams.end()) {
			continue;
		}
		Optional<Version> streamBegin = nextTagReadVersionForStream(tag, stream->second);
		if (streamBegin.present() && (!begin.present() || streamBegin.get() < begin.get())) {
			begin = streamBegin;
		}
	}
	return begin;
}

void CDCProxy::advanceTagBufferedThrough(Reference<CDCBufferedTag> tag,
                                         Version bufferedThrough,
                                         std::unordered_set<CDCStreamId> const& selectedStreamIds) {
	const std::vector<CDCStreamId> streamIds(tag->streamIds.begin(), tag->streamIds.end());
	for (const CDCStreamId streamId : streamIds) {
		if (!selectedStreamIds.contains(streamId)) {
			continue;
		}
		auto stream = streams.find(streamId);
		if (stream == streams.end() || !stream->second->active || stream->second->readDemand == 0) {
			continue;
		}
		for (auto& interval : stream->second->tagIntervals) {
			if (interval.tag == tag->tag && bufferedThrough >= interval.begin) {
				interval.bufferedThrough =
				    std::max(interval.bufferedThrough, std::min(bufferedThrough, interval.end - 1));
			}
		}
		updateStreamBufferedThrough(stream->second);
	}
}

void CDCProxy::markPoppedTagStreamsTooOld(Reference<CDCBufferedTag> tag, Version popped) {
	std::vector<Reference<CDCBufferedStream>> tooOldStreams;
	for (const CDCStreamId streamId : tag->streamIds) {
		auto stream = streams.find(streamId);
		if (stream == streams.end() || !stream->second->active) {
			continue;
		}
		for (const auto& interval : stream->second->tagIntervals) {
			const Version next = std::max(interval.begin, interval.bufferedThrough + 1);
			if (interval.tag == tag->tag && next < interval.end && next < popped) {
				tooOldStreams.push_back(stream->second);
				break;
			}
		}
	}
	for (const auto& stream : tooOldStreams) {
		CODE_PROBE(true, "CDC proxy detects unread mutations already popped from TLogs", probe::decoration::rare);
		TraceEvent("CDCBufferStreamTooOld", id)
		    .detail("StreamId", stream->streamId)
		    .detail("MinVersion", stream->minVersion)
		    .detail("BufferedThrough", stream->bufferedThrough)
		    .detail("Popped", popped)
		    .detail("Tag", tag->tag);
		clearBufferedMutations(stream);
		stream->tooOld = true;
		stream->active = false;
		stream->changed.trigger();
		detachStreamFromTags(stream);
	}
}

template <class Visitor>
void CDCProxy::visitBufferedMutations(Reference<CDCBufferedTag> tag,
                                      Reference<IReplayPeekCursor> cursor,
                                      Version throughVersion,
                                      std::unordered_set<CDCStreamId> const* selectedStreamIds,
                                      Visitor&& visitor) {
	while (cursor->hasMessage() && cursor->version().version <= throughVersion) {
		const Version messageVersion = cursor->version().version;
		ArenaReader& reader = *cursor->reader();
		if (LogProtocolMessage::isNextIn(reader)) {
			LogProtocolMessage protocolMessage;
			reader >> protocolMessage;
			cursor->setProtocolVersion(reader.protocolVersion());
		} else if (reader.protocolVersion().hasSpanContext() && SpanContextMessage::isNextIn(reader)) {
			SpanContextMessage contextMessage;
			reader >> contextMessage;
		} else if (reader.protocolVersion().hasOTELSpanContext() && OTELSpanContextMessage::isNextIn(reader)) {
			OTELSpanContextMessage contextMessage;
			reader >> contextMessage;
		} else {
			MutationRef mutation;
			reader >> mutation;
			for (const CDCStreamId streamId : tag->streamIds) {
				if (selectedStreamIds != nullptr && !selectedStreamIds->contains(streamId)) {
					continue;
				}
				auto stream = streams.find(streamId);
				if (stream == streams.end() || !stream->second->active || stream->second->readDemand == 0 ||
				    !stream->second->keys.present()) {
					continue;
				}
				const bool coversVersion =
				    std::any_of(stream->second->tagIntervals.begin(),
				                stream->second->tagIntervals.end(),
				                [tag, messageVersion](const auto& interval) {
					                return interval.tag == tag->tag && interval.begin <= messageVersion &&
					                       messageVersion < interval.end && messageVersion > interval.bufferedThrough;
				                });
				if (!coversVersion) {
					continue;
				}
				Optional<MutationRef> clipped = clipCDCMutation(mutation, stream->second->keys.get());
				if (clipped.present()) {
					visitor(stream->second, messageVersion, clipped.get());
				}
			}
		}
		cursor->nextMessage();
	}
}

std::unordered_map<CDCStreamId, int64_t> CDCProxy::estimateBufferedBytes(Reference<CDCBufferedTag> tag,
                                                                         Reference<IReplayPeekCursor> cursor,
                                                                         Version throughVersion) {
	std::unordered_map<CDCStreamId, Version> lastMutationVersion;
	std::unordered_map<CDCStreamId, int64_t> estimatedBytes;
	visitBufferedMutations(
	    tag,
	    cursor,
	    throughVersion,
	    nullptr,
	    [&lastMutationVersion,
	     &estimatedBytes](Reference<CDCBufferedStream> stream, Version version, MutationRef const& mutation) {
		    auto [lastVersion, inserted] = lastMutationVersion.emplace(stream->streamId, version);
		    if (inserted || lastVersion->second != version) {
			    lastVersion->second = version;
			    const bool alreadyBuffered =
			        std::any_of(stream->mutations.begin(), stream->mutations.end(), [version](const auto& buffered) {
				        return buffered.version == version;
			        });
			    if (!alreadyBuffered) {
				    estimatedBytes[stream->streamId] += sizeof(VersionedMutationsRef);
			    }
		    }
		    estimatedBytes[stream->streamId] += mutation.expectedSize() + sizeof(MutationRef);
	    });
	return estimatedBytes;
}

std::unordered_map<CDCStreamId, CDCBufferedBatch> CDCProxy::bufferMessages(
    Reference<CDCBufferedTag> tag,
    Reference<IReplayPeekCursor> cursor,
    Version throughVersion,
    std::unordered_set<CDCStreamId> const& selectedStreamIds) {
	std::unordered_map<CDCStreamId, CDCBufferedBatch> batches;
	visitBufferedMutations(
	    tag,
	    cursor,
	    throughVersion,
	    &selectedStreamIds,
	    [&batches](Reference<CDCBufferedStream> stream, Version version, MutationRef const& mutation) {
		    addMutationToBatch(stream, &batches[stream->streamId], version, mutation);
	    });
	return batches;
}

std::vector<CDCBufferCandidate> CDCProxy::getBufferCandidates(
    Reference<CDCBufferedTag> tag,
    Version throughVersion,
    std::unordered_map<CDCStreamId, int64_t> const& estimatedBytes) {
	std::vector<CDCBufferCandidate> candidates;
	for (const CDCStreamId streamId : tag->streamIds) {
		auto stream = streams.find(streamId);
		if (stream == streams.end()) {
			continue;
		}
		Optional<Version> nextVersion = nextTagReadVersionForStream(tag, stream->second);
		if (!nextVersion.present() || nextVersion.get() > throughVersion) {
			continue;
		}
		auto estimate = estimatedBytes.find(streamId);
		candidates.push_back(
		    CDCBufferCandidate{ streamId, nextVersion.get(), estimate == estimatedBytes.end() ? 0 : estimate->second });
	}
	return candidates;
}

Future<Void> CDCProxy::rotateContendedPeek() {
	if (bufferLock.waiters() == 0) {
		co_await peekCapacityContended.onTrigger();
	}
	co_await delay(SERVER_KNOBS->BLOCKING_PEEK_TIMEOUT);
}

Future<CDCBufferTagPassResult> CDCProxy::bufferTagPass(Reference<CDCBufferedTag> tag, Version begin) {
	const int64_t bufferLimit = SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES;
	Reference<LogSystemConsumer> consumer = logSystem->get();
	Future<Void> logSystemChanged = logSystem->onChange();
	// CDC ReplayMultiCursor instances disable constructor prefetch, so constructing this cursor cannot issue a peek
	// before the proxy has reserved memory for every reply arena that its replicated read may retain.
	Reference<IReplayPeekCursor> cursor = consumer->peekSingle(id, begin, tag->tag, {});
	cursor->setReplyByteLimit(SERVER_KNOBS->MAXIMUM_PEEK_BYTES);
	const int64_t retainedReplyCount = cursor->getMaxRetainedReplyCount();
	Optional<CDCBufferPassLimits> limits =
	    calculateBufferPassLimits(bufferLimit, SERVER_KNOBS->MAXIMUM_PEEK_BYTES, retainedReplyCount);
	if (!limits.present()) {
		markTagStreamsRawReplyBudgetExceeded(tag, begin, retainedReplyCount);
		co_return CDCBufferTagPassResult::RETRY;
	}
	const int64_t rawPeekReservation = limits.get().rawReplyBytes;
	const int64_t hardBufferedBatchLimit = limits.get().hardBufferedBytes;
	const int64_t preferredBufferedBatch = limits.get().preferredBufferedBytes;
	const int64_t passReservation = limits.get().reservationBytes;
	if (bufferLock.available() < passReservation) {
		CODE_PROBE(true, "CDC proxy applies shared buffer backpressure", probe::decoration::rare);
		peekCapacityContended.trigger();
	}
	auto capacity = co_await race(bufferLock.take(TaskPriority::TLogPeekReply, passReservation),
	                              logSystemChanged,
	                              tag->stopped.onTrigger(),
	                              tag->refresh.onTrigger());
	if (capacity.index() == 1 || capacity.index() == 3) {
		co_return CDCBufferTagPassResult::RETRY;
	}
	if (capacity.index() == 2) {
		co_return CDCBufferTagPassResult::STOP;
	}
	FlowLock::Releaser reservation(bufferLock, passReservation);
	recordBufferUsage();
	// If capacity and a generation change became ready together, discard the cursor built from the old topology.
	if (logSystemChanged.isReady()) {
		co_return CDCBufferTagPassResult::RETRY;
	}
	if (!cursor->hasMessage()) {
		// Blocking peeks hold a response reservation. Once another tag queues for capacity, rotate this
		// reader after one blocking-peek interval so an idle tag cannot monopolize the shared budget.
		try {
			auto result = co_await race(cursor->getMore(TaskPriority::TLogPeekReply),
			                            logSystemChanged,
			                            tag->stopped.onTrigger(),
			                            tag->refresh.onTrigger(),
			                            rotateContendedPeek());
			if (result.index() == 1 || result.index() == 3 || result.index() == 4) {
				co_return CDCBufferTagPassResult::RETRY;
			}
			if (result.index() == 2) {
				co_return CDCBufferTagPassResult::STOP;
			}
		} catch (Error& e) {
			if (e.code() != error_code_cdc_tlog_peek_reply_too_large) {
				throw;
			}
			markTagStreamsBufferLimitExceeded(tag, begin);
			co_return CDCBufferTagPassResult::RETRY;
		}
	}
	// A newly constructed replay cursor can already contain messages, especially after log-generation
	// changes. Initialize its reader even when getMore() was unnecessary.
	cursor->setProtocolVersion(g_network->protocolVersion());
	latestCommittedVersion = std::max(latestCommittedVersion, cursor->getMinKnownCommittedVersion());
	if (cursor->popped() > begin) {
		markPoppedTagStreamsTooOld(tag, cursor->popped());
		co_return CDCBufferTagPassResult::RETRY;
	}

	const Version peekThroughVersion = cursor->hasMessage() ? cursor->version().version : cursor->version().version - 1;
	const Version throughVersion = committedPeekThrough(peekThroughVersion, cursor->getMinKnownCommittedVersion());
	CODE_PROBE(throughVersion < peekThroughVersion,
	           "CDC proxy waits for peeked mutations to become committed",
	           probe::decoration::rare);
	if (throughVersion < begin) {
		co_return CDCBufferTagPassResult::WAIT_FOR_COMMIT;
	}
	Reference<IReplayPeekCursor> estimateCursor = cursor->cloneNoMore();
	estimateCursor->setProtocolVersion(g_network->protocolVersion());
	const std::unordered_map<CDCStreamId, int64_t> estimatedBytes =
	    estimateBufferedBytes(tag, estimateCursor, throughVersion);
	const std::vector<CDCBufferCandidate> candidates = getBufferCandidates(tag, throughVersion, estimatedBytes);
	CDCBufferSelection selection = selectBufferCandidates(candidates, preferredBufferedBatch, hardBufferedBatchLimit);
	for (const CDCStreamId streamId : selection.oversizedStreamIds) {
		auto stream = streams.find(streamId);
		if (stream == streams.end() || !stream->second->active) {
			continue;
		}
		CODE_PROBE(true, "CDC proxy rejects a version larger than its complete buffer budget");
		TraceEvent(SevWarn, "CDCProxyVersionExceedsBufferLimit", id)
		    .detail("Tag", tag->tag)
		    .detail("StreamId", streamId)
		    .detail("EstimatedBytes", estimatedBytes.at(streamId))
		    .detail("BufferedBatchLimit", hardBufferedBatchLimit)
		    .detail("Version", throughVersion);
		stream->second->bufferLimitExceeded = true;
		stream->second->changed.trigger();
	}
	if (selection.selectedStreamIds.empty()) {
		co_return CDCBufferTagPassResult::RETRY;
	}

	const int64_t materializationReservation = reservation.remaining - rawPeekReservation;
	ASSERT_GE(materializationReservation, 0);
	if (selection.selectedBytes <= materializationReservation) {
		reservation.release(materializationReservation - selection.selectedBytes);
	} else {
		CODE_PROBE(
		    true, "CDC proxy materializes one stream batch larger than its peek reservation", probe::decoration::rare);
		const int64_t additionalBytes = selection.selectedBytes - materializationReservation;
		auto exactCapacity = co_await race(bufferLock.take(TaskPriority::TLogPeekReply, additionalBytes),
		                                   logSystem->onChange(),
		                                   tag->stopped.onTrigger(),
		                                   tag->refresh.onTrigger());
		if (exactCapacity.index() == 1 || exactCapacity.index() == 3) {
			co_return CDCBufferTagPassResult::RETRY;
		}
		if (exactCapacity.index() == 2) {
			co_return CDCBufferTagPassResult::STOP;
		}
		reservation.remaining += additionalBytes;
		recordBufferUsage();
	}
	if (!tag->active) {
		co_return CDCBufferTagPassResult::STOP;
	}

	std::unordered_map<CDCStreamId, CDCBufferedBatch> batches =
	    bufferMessages(tag, cursor, throughVersion, selection.selectedStreamIds);
	int64_t materializedBytes = 0;
	for (const auto& [streamId, batch] : batches) {
		materializedBytes += batch.bufferedBytes;
	}
	// An acknowledgement or removal can advance a selected stream while this pass waits for exact capacity. The
	// materialization pass rechecks current stream frontiers, so it may legitimately produce less than its estimate.
	ASSERT_LE(materializedBytes, selection.selectedBytes);
	CODE_PROBE(materializedBytes < selection.selectedBytes,
	           "CDC proxy drops acknowledged mutations while waiting for buffer capacity",
	           probe::decoration::rare);
	ASSERT_GE(reservation.remaining, rawPeekReservation + materializedBytes);

	int64_t acceptedBytes = 0;
	for (auto& [streamId, batch] : batches) {
		auto stream = streams.find(streamId);
		if (stream != streams.end() && stream->second->active && tag->streamIds.contains(streamId)) {
			acceptedBytes += batch.bufferedBytes;
			addBufferedBatch(stream->second, std::move(batch));
		}
	}
	ASSERT_LE(acceptedBytes, materializedBytes);
	ASSERT_LE(acceptedBytes, reservation.remaining);
	reservation.release(reservation.remaining - acceptedBytes);
	// Buffered mutations own these permits until acknowledgement or stream removal.
	reservation.remaining = 0;
	ASSERT_LE(bufferedBytes, bufferLimit);
	ASSERT_LE(bufferLock.activePermits(), bufferLimit);
	advanceTagBufferedThrough(tag, throughVersion, selection.selectedStreamIds);
	// Every raw cursor arena is covered by rawPeekReservation only for this pass. Reopen from the shared minimum
	// after releasing it so no cursor response remains live outside the proxy memory budget.
	co_return CDCBufferTagPassResult::RETRY;
}

Future<Void> CDCProxy::bufferTag(Reference<CDCBufferedTag> tag) {
	while (tag->active) {
		Optional<Version> begin = nextTagReadVersion(tag);
		if (!begin.present()) {
			auto waitForDemand = co_await race(tag->stopped.onTrigger(), tag->refresh.onTrigger());
			if (waitForDemand.index() == 0) {
				co_return;
			}
			continue;
		}
		if (!logSystem->get()) {
			auto waitForLogSystem =
			    co_await race(logSystem->onChange(), tag->stopped.onTrigger(), tag->refresh.onTrigger());
			if (waitForLogSystem.index() == 1) {
				co_return;
			}
			continue;
		}

		const CDCBufferTagPassResult result = co_await bufferTagPass(tag, begin.get());
		if (result == CDCBufferTagPassResult::STOP) {
			co_return;
		}
		if (result == CDCBufferTagPassResult::WAIT_FOR_COMMIT) {
			// The cursor may already hold a speculative message, so getMore() would complete immediately without
			// refreshing its committed frontier. Drop that arena and reopen after one blocking-peek interval.
			auto waitForCommit = co_await race(delay(SERVER_KNOBS->BLOCKING_PEEK_TIMEOUT),
			                                   logSystem->onChange(),
			                                   tag->stopped.onTrigger(),
			                                   tag->refresh.onTrigger());
			if (waitForCommit.index() == 2) {
				co_return;
			}
		}
	}
}

Future<Void> CDCProxy::initializeStream(Reference<CDCBufferedStream> stream) {
	try {
		const CDCStreamReadState metadata = co_await readCDCStreamState(cx, stream->streamId, id, true);
		if (!isCurrentStreamInitialization(streams, stream)) {
			CODE_PROBE(true, "CDC proxy discards stale stream initialization", probe::decoration::rare);
			co_return;
		}
		stream->keys = metadata.keys;
		stream->minVersion = metadata.minVersion;
		stream->bufferedThrough = metadata.minVersion - 1;
		for (size_t i = 0; i < metadata.tagAssignments.size(); ++i) {
			const Version begin = std::max(metadata.minVersion, metadata.tagAssignments[i].first);
			const Version end = i + 1 < metadata.tagAssignments.size() ? metadata.tagAssignments[i + 1].first
			                                                           : std::numeric_limits<Version>::max();
			if (begin < end) {
				stream->tagIntervals.emplace_back(metadata.tagAssignments[i].second, begin, end);
			}
		}
		stream->initialized = true;
		stream->changed.trigger();
		for (const auto& interval : stream->tagIntervals) {
			auto tag = tags.find(interval.tag);
			if (tag == tags.end()) {
				Reference<CDCBufferedTag> newTag = makeReference<CDCBufferedTag>(interval.tag);
				tag = tags.emplace(interval.tag, newTag).first;
				tag->second->streamIds.insert(stream->streamId);
				actors.add(bufferTag(newTag));
			} else {
				CODE_PROBE(true, "CDC proxy shares a tag reader across streams");
				tag->second->streamIds.insert(stream->streamId);
				tag->second->refresh.trigger();
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_client_invalid_operation || e.code() == error_code_wrong_shard_server) {
			clearBufferedMutations(stream);
			stream->active = false;
			stream->changed.trigger();
			co_return;
		}
		throw;
	}
}

// TODO: Persist per-tag safe-pop state or coordinate pops centrally instead of rebuilding minima from all stream
// history on every acknowledgement scan.
Future<CDCPopState> readPopState(Database cx) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			CDCPopState result;
			result.readVersion = co_await tr.getReadVersion();
			Key begin = cdcMinVersionKeys.begin;
			while (begin < cdcMinVersionKeys.end) {
				RangeResult minima =
				    co_await tr.getRange(KeyRangeRef(begin, cdcMinVersionKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : minima) {
					result.minVersions[decodeCDCMinVersionKey(kv.key)] = decodeCDCMinVersionValue(kv.value);
				}
				if (!minima.more) {
					break;
				}
				begin = keyAfter(minima.back().key);
			}

			begin = cdcTagHistoryKeys.begin;
			while (begin < cdcTagHistoryKeys.end) {
				RangeResult histories =
				    co_await tr.getRange(KeyRangeRef(begin, cdcTagHistoryKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : histories) {
					const CDCTagHistoryEntry history = decodeCDCTagHistoryKey(kv.key);
					auto minimum = result.minVersions.find(history.streamId);
					if (minimum == result.minVersions.end()) {
						continue;
					}
					auto safePop = result.safePopVersions.find(history.tag);
					if (safePop == result.safePopVersions.end()) {
						result.safePopVersions[history.tag] = minimum->second;
					} else {
						safePop->second = std::min(safePop->second, minimum->second);
					}
				}
				if (!histories.more) {
					break;
				}
				begin = keyAfter(histories.back().key);
			}

			// A retired watermark is only safe when it is bounded by live shared streams from the same snapshot.
			// Keep this scan in the transaction above instead of combining a newer watermark with stale safe-pop state.
			begin = cdcRetiredTagPopVersionKeys.begin;
			while (begin < cdcRetiredTagPopVersionKeys.end) {
				RangeResult retired =
				    co_await tr.getRange(KeyRangeRef(begin, cdcRetiredTagPopVersionKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : retired) {
					result.retiredTagPopVersions[decodeCDCRetiredTagPopVersionKey(kv.key)] =
					    decodeCDCMinVersionValue(kv.value);
				}
				if (!retired.more) {
					break;
				}
				begin = keyAfter(retired.back().key);
			}
			co_return result;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Void> clearCompletedRetiredTagPops(Database cx, std::unordered_map<Tag, Version> completedPopVersions) {
	if (completedPopVersions.empty()) {
		co_return;
	}

	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			for (const auto& [tag, completedVersion] : completedPopVersions) {
				Optional<Value> retiredVersionValue = co_await tr.get(cdcRetiredTagPopVersionKeyFor(tag));
				if (!retiredVersionValue.present() ||
				    decodeCDCMinVersionValue(retiredVersionValue.get()) > completedVersion) {
					continue;
				}
				CODE_PROBE(true, "CDC proxy clears completed retired tag pop metadata");
				tr.clear(cdcRetiredTagPopKeyFor(tag));
				tr.clear(cdcRetiredTagPopVersionKeyFor(tag));
			}

			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Void> CDCProxy::popAcknowledgedData() {
	if (popsPausedForTesting) {
		co_return;
	}
	while (!logSystem->get()) {
		CODE_PROBE(true, "CDC proxy defers pops until a log system is available", probe::decoration::rare);
		co_await logSystem->onChange();
	}
	Reference<LogSystemConsumer> currentLogSystem = logSystem->get();
	const LogSystemConfig popLogSystemConfig = lastLogSystemConfig;
	const uint64_t popLogSystemRecoveryCount = lastLogSystemRecoveryCount;
	if (popsPausedAfterIncompleteLogSystemForTesting && hasCompleteLogSystemConfig(popLogSystemConfig)) {
		co_return;
	}
	if (!hasCompleteLogSystemConfig(popLogSystemConfig)) {
		if (popsPausedAfterIncompleteLogSystemForTesting) {
			++popIncompleteLogSystemPausesForTesting;
			CODE_PROBE(true, "CDC proxy pauses after observing an incomplete log system");
			while (popsPausedAfterIncompleteLogSystemForTesting) {
				co_await popPauseChangedForTesting.onTrigger();
			}
		}
		CODE_PROBE(true, "CDC proxy defers pops until every expected log set is published", probe::decoration::rare);
		co_return;
	}
	if (!isCurrentCompletePopLogSystem(currentLogSystem, popLogSystemConfig, popLogSystemRecoveryCount)) {
		requestAcknowledgedDataPop();
		co_return;
	}

	const CDCPopState popState = co_await readPopState(cx);
	if (popsPausedAfterSnapshotForTesting) {
		++popSnapshotsPausedForTesting;
		CODE_PROBE(true, "CDC proxy pauses after reading a consistent pop snapshot");
		while (popsPausedAfterSnapshotForTesting) {
			co_await popPauseChangedForTesting.onTrigger();
		}
	}
	if (popsPausedForTesting) {
		co_return;
	}
	if (!isCurrentCompletePopLogSystem(currentLogSystem, popLogSystemConfig, popLogSystemRecoveryCount)) {
		CODE_PROBE(true, "CDC proxy retries pops after log system topology changes", probe::decoration::rare);
		requestAcknowledgedDataPop();
		co_return;
	}
	latestCommittedVersion = std::max(latestCommittedVersion, popState.readVersion);
	for (const auto& [streamId, minVersion] : popState.minVersions) {
		auto stream = streams.find(streamId);
		if (stream == streams.end() || !stream->second->active || !stream->second->initialized ||
		    stream->second->minVersion >= minVersion) {
			continue;
		}
		CODE_PROBE(true, "CDC proxy scan reconciles a durable stream acknowledgement", probe::decoration::rare);
		reconcileStreamMinVersion(stream->second, minVersion);
	}
	const std::unordered_map<Tag, Version>& safePopVersions = popState.safePopVersions;
	lastSafePopVersions = safePopVersions;
	for (const auto& [tag, version] : safePopVersions) {
		if (popsPausedForTesting) {
			co_return;
		}
		currentLogSystem->pop(version, tag);
	}
	std::unordered_map<Tag, Version> completedPopVersions;
	const Optional<Version> recoveredAt = popLogSystemConfig.recoveredAt;
	const Optional<Version> recoveryEnd =
	    recoveredAt.present() ? Optional<Version>(recoveredAt.get() + 1) : Optional<Version>();
	for (const auto& [tag, retiredVersion] : popState.retiredTagPopVersions) {
		if (popsPausedForTesting) {
			co_return;
		}
		if (!isCurrentCompletePopLogSystem(currentLogSystem, popLogSystemConfig, popLogSystemRecoveryCount)) {
			CODE_PROBE(true, "CDC proxy retries retired pops after log system topology changes");
			requestAcknowledgedDataPop();
			co_return;
		}
		const auto safePop = safePopVersions.find(tag);
		const Optional<Version> safePopVersion =
		    safePop == safePopVersions.end() ? Optional<Version>() : Optional<Version>(safePop->second);
		const Version version = retiredTagPopTarget(retiredVersion, safePopVersion, recoveryEnd);
		CODE_PROBE(!safePopVersion.present() && recoveryEnd.present() && version > retiredVersion,
		           "CDC proxy advances retired tag pop past recovery boundary");
		CODE_PROBE(safePop != safePopVersions.end() && version < retiredVersion,
		           "CDC proxy defers retired tag pop behind a live shared stream");
		currentLogSystem->pop(version, tag);
		if (version >= retiredVersion) {
			co_await currentLogSystem->waitForPopped(version, tag);
			if (!isCurrentCompletePopLogSystem(currentLogSystem, popLogSystemConfig, popLogSystemRecoveryCount)) {
				CODE_PROBE(true, "CDC proxy retries retired pop completion after log system topology changes");
				requestAcknowledgedDataPop();
				co_return;
			}
			completedPopVersions[tag] = retiredVersion;
		}
	}
	if (!isCurrentCompletePopLogSystem(currentLogSystem, popLogSystemConfig, popLogSystemRecoveryCount)) {
		CODE_PROBE(true, "CDC proxy preserves retired pop metadata after log system topology changes");
		requestAcknowledgedDataPop();
		co_return;
	}
	co_await clearCompletedRetiredTagPops(cx, std::move(completedPopVersions));
}

Future<Void> CDCProxy::monitorAcknowledgedDataPops() {
	while (true) {
		auto wake =
		    co_await race(popAcknowledgedDataRequests.onTrigger(), delay(SERVER_KNOBS->CDC_PROXY_POP_SCAN_INTERVAL));
		const bool periodicScan = wake.index() == 1;
		if (!periodicScan) {
			co_await delay(SERVER_KNOBS->CDC_PROXY_POP_MIN_INTERVAL);
		}
		popAcknowledgedDataRequests.consume();
		CODE_PROBE(periodicScan, "CDC proxy periodically scans durable acknowledgement state", probe::decoration::rare);
		++popAttempts;
		// Acknowledgements only make a completed snapshot more conservative, so they are coalesced for a later pass
		// instead of canceling this one. A log-system config change can add targeted log sets, so it invalidates
		// waitForPopped work in flight.
		auto result = co_await race(popAcknowledgedData(), popLogSystemChanged.onTrigger());
		if (result.index() == 0) {
			if (!popsPausedForTesting) {
				++popCompletions;
			}
		} else {
			++popCancellations;
			popAcknowledgedDataRequests.trigger();
		}
	}
}

void CDCProxy::reconcileStreams() {
	std::set<CDCStreamId> assignedStreams;
	for (const auto& [streamId, proxyId] : dbInfo->get().client.streamToCDCProxyId) {
		if (proxyId == id) {
			assignedStreams.insert(streamId);
			if (!streams.contains(streamId)) {
				Reference<CDCBufferedStream> stream = makeReference<CDCBufferedStream>(streamId);
				streams.emplace(streamId, stream);
				actors.add(initializeStream(stream));
			}
		}
	}

	for (auto it = streams.begin(); it != streams.end();) {
		if (!assignedStreams.contains(it->first)) {
			deactivateStream(it->second);
			it = streams.erase(it);
		} else {
			++it;
		}
	}
}

Future<Void> CDCProxy::waitForBufferedVersion(Reference<CDCBufferedStream> stream, Version version) {
	if (stream->bufferedThrough >= version) {
		co_return;
	}

	++stream->readDemand;
	refreshStreamTags(stream);
	ScopeExit releaseReadDemand([this, stream]() {
		ASSERT_GT(stream->readDemand, 0);
		--stream->readDemand;
		refreshStreamTags(stream);
	});
	while (stream->active && !stream->bufferLimitExceeded && stream->bufferedThrough < version) {
		co_await stream->changed.onTrigger();
	}
}

Future<Void> CDCProxy::consume(CDCConsumeRequest request) {
	++activeConsumeRequests;
	ScopeExit releaseConsumeRequest([this]() {
		ASSERT_GT(activeConsumeRequests, 0);
		--activeConsumeRequests;
	});
	try {
		if (request.cursor.lastConsumedVersion < invalidVersion ||
		    request.cursor.lastConsumedVersion == std::numeric_limits<Version>::max()) {
			throw client_invalid_operation();
		}

		auto found = streams.find(request.cursor.streamId);
		if (found == streams.end()) {
			throw wrong_shard_server();
		}
		Reference<CDCBufferedStream> stream = found->second;
		while (stream->active && !stream->initialized) {
			co_await stream->changed.onTrigger();
		}
		if (stream->tooOld) {
			throw transaction_too_old();
		}
		if (!stream->active) {
			throw wrong_shard_server();
		}
		if (stream->activeConsumes > 0) {
			// A stream has one durable acknowledgement frontier, so concurrent logical consumers cannot be
			// isolated. Reject overlapping server requests rather than duplicating an entire reply arena.
			CODE_PROBE(true, "CDC proxy rejects concurrent consumers for one stream", probe::decoration::rare);
			throw client_invalid_operation();
		}
		++stream->activeConsumes;
		ScopeExit releaseStreamConsume([stream]() {
			ASSERT_GT(stream->activeConsumes, 0);
			--stream->activeConsumes;
		});
		const CDCStreamReadState metadata = co_await readCDCStreamState(cx, request.cursor.streamId, id, true);
		CODE_PROBE(stream->minVersion < metadata.minVersion,
		           "Native CDC consume reconciles a durable acknowledgement",
		           probe::decoration::rare);
		reconcileStreamMinVersion(stream, metadata.minVersion);
		if (request.cursor.lastConsumedVersion > stream->bufferedThrough) {
			// A cursor is trusted only when this owner has delivered through it or when it is covered by the durable
			// acknowledgement watermark used to initialize bufferedThrough. This prevents a fabricated cursor from
			// making the proxy retain every intervening tagged mutation while trying to reach an unproven position.
			if (request.cursor.lastConsumedVersion > metadata.readVersion) {
				CODE_PROBE(true, "CDC proxy rejects a consume cursor beyond its transaction read version");
			} else {
				CODE_PROBE(true, "CDC proxy rejects an unproven consume cursor");
			}
			throw client_invalid_operation();
		}

		Version begin = request.cursor.lastConsumedVersion == invalidVersion ? stream->minVersion
		                                                                     : request.cursor.lastConsumedVersion + 1;
		if (begin < stream->minVersion) {
			throw transaction_too_old();
		}

		auto buffered =
		    co_await race(waitForBufferedVersion(stream, begin), delay(SERVER_KNOBS->CDC_PROXY_CONSUME_POLL_TIMEOUT));
		if (buffered.index() == 1) {
			CODE_PROBE(true, "CDC proxy expires an idle consume lease", probe::decoration::rare);
			CDCConsumeReply reply;
			reply.lastConsumedVersion = request.cursor.lastConsumedVersion;
			request.reply.send(reply);
			co_return;
		}
		if (stream->tooOld) {
			throw transaction_too_old();
		}
		if (stream->bufferLimitExceeded) {
			throw server_overloaded();
		}
		if (!stream->active) {
			throw wrong_shard_server();
		}

		CDCConsumeReply reply;
		for (const auto& versioned : stream->mutations) {
			if (versioned.version >= begin && versioned.version <= stream->bufferedThrough) {
				// Retain the already-accounted stream arena instead of copying mutation payloads for every reply.
				reply.arena.dependsOn(versioned.arena());
				reply.mutations.push_back(reply.arena, VersionedMutationsRef(versioned.version, versioned.mutations));
			}
		}
		reply.lastConsumedVersion = stream->bufferedThrough;
		request.reply.send(reply);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
}

Future<Void> CDCProxy::acknowledge(CDCAckRequest request) {
	try {
		if (request.version < 0 || request.version >= std::numeric_limits<Version>::max() - 1) {
			throw client_invalid_operation();
		}
		const CDCStreamReadState metadata = co_await readCDCStreamState(cx, request.streamId, id, false);
		if (metadata.minVersion <= request.version) {
			throw client_invalid_operation();
		}
		auto found = streams.find(request.streamId);
		if (found == streams.end()) {
			throw wrong_shard_server();
		}
		Reference<CDCBufferedStream> stream = found->second;
		while (stream->active && !stream->initialized) {
			co_await stream->changed.onTrigger();
		}
		if (!stream->active) {
			throw wrong_shard_server();
		}

		// The durable acknowledgement can commit before a replacement owner observes the RPC.
		// Reconcile the new owner's in-memory frontier to that already verified watermark.
		const Version minVersion = metadata.minVersion;
		CODE_PROBE(stream->minVersion < minVersion, "CDC proxy reconciles a durable stream acknowledgement");
		reconcileStreamMinVersion(stream, minVersion);
		requestAcknowledgedDataPop();
		request.reply.send(Void());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
}

Future<Void> CDCProxy::registerStream(CDCRegisterStreamRequest request) {
	try {
		const CDCStreamId streamId = co_await registerNativeCdcStream(cx, request.name, request.keys, id);
		request.reply.send(CDCRegisterStreamReply(streamId));
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
}

Future<Void> CDCProxy::removeStream(CDCRemoveStreamRequest request) {
	try {
		const bool removed = co_await removeNativeCdcStream(cx, request.name, request.streamId, id);
		if (removed) {
			auto stream = streams.find(request.streamId);
			if (stream != streams.end()) {
				deactivateStream(stream->second);
				streams.erase(stream);
			}
			requestAcknowledgedDataPop();
		}
		request.reply.send(Void());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
}
Future<Void> CDCProxy::serveConsumeRequests(FutureStream<CDCConsumeRequest> requests) {
	while (true) {
		CDCConsumeRequest request = co_await requests;
		actors.add(consume(std::move(request)));
	}
}

Future<Void> CDCProxy::serveAcknowledgeRequests(FutureStream<CDCAckRequest> requests) {
	while (true) {
		CDCAckRequest request = co_await requests;
		actors.add(acknowledge(std::move(request)));
	}
}

Future<Void> CDCProxy::serveRegisterStreamRequests(FutureStream<CDCRegisterStreamRequest> requests) {
	while (true) {
		CDCRegisterStreamRequest request = co_await requests;
		actors.add(registerStream(std::move(request)));
	}
}

Future<Void> CDCProxy::serveRemoveStreamRequests(FutureStream<CDCRemoveStreamRequest> requests) {
	while (true) {
		CDCRemoveStreamRequest request = co_await requests;
		actors.add(removeStream(std::move(request)));
	}
}
Future<Void> CDCProxy::serveHaltForTestingRequests(FutureStream<HaltCDCProxyRequest> requests) {
	while (true) {
		HaltCDCProxyRequest request = co_await requests;
		if (!g_network->isSimulated()) {
			request.reply.sendError(client_invalid_operation());
			continue;
		}
		request.reply.send(Void());
		throw worker_removed();
	}
}

Future<Void> CDCProxy::serveBufferStatusForTestingRequests(FutureStream<GetCDCProxyBufferStatusRequest> requests) {
	while (true) {
		GetCDCProxyBufferStatusRequest request = co_await requests;
		if (!g_network->isSimulated()) {
			request.reply.sendError(client_invalid_operation());
			continue;
		}
		CDCProxyBufferStatus status;
		status.bufferedBytes = bufferedBytes;
		status.activePermits = bufferLock.activePermits();
		status.peakActivePermits = peakActivePermits;
		status.bufferLimit = SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES;
		status.waiters = bufferLock.waiters();
		status.activeConsumeRequests = activeConsumeRequests;
		for (const auto& [streamId, stream] : streams) {
			status.readDemand += stream->readDemand;
		}
		status.popRequests = popRequests;
		status.popAttempts = popAttempts;
		status.popCompletions = popCompletions;
		status.popCancellations = popCancellations;
		status.popsPaused = popsPausedForTesting;
		status.popSnapshotsPaused = popSnapshotsPausedForTesting;
		status.popsPausedAfterSnapshot = popsPausedAfterSnapshotForTesting;
		status.popIncompleteLogSystemPauses = popIncompleteLogSystemPausesForTesting;
		status.popsPausedAfterIncompleteLogSystem = popsPausedAfterIncompleteLogSystemForTesting;
		request.reply.send(status);
	}
}

Future<Void> CDCProxy::serveSetPopsPausedForTestingRequests(FutureStream<SetCDCProxyPopsPausedRequest> requests) {
	while (true) {
		SetCDCProxyPopsPausedRequest request = co_await requests;
		if (!g_network->isSimulated()) {
			request.reply.sendError(client_invalid_operation());
			continue;
		}
		popsPausedForTesting = request.paused && !request.afterSnapshot && !request.afterIncompleteLogSystem;
		popsPausedAfterSnapshotForTesting = request.paused && request.afterSnapshot;
		popsPausedAfterIncompleteLogSystemForTesting = request.paused && request.afterIncompleteLogSystem;
		popPauseChangedForTesting.trigger();
		if (!request.paused || request.afterSnapshot || request.afterIncompleteLogSystem) {
			requestAcknowledgedDataPop();
		}
		request.reply.send(Void());
	}
}

Future<Void> CDCProxy::traceMetrics() {
	while (true) {
		co_await delay(SERVER_KNOBS->WORKER_LOGGING_INTERVAL);
		int totalReadDemand = 0;
		Version oldestRequiredVersion = std::numeric_limits<Version>::max();
		CDCStreamId oldestStreamId = 0;
		for (const auto& [streamId, stream] : streams) {
			totalReadDemand += stream->readDemand;
			if (stream->active && stream->minVersion >= 0 && stream->minVersion < oldestRequiredVersion) {
				oldestRequiredVersion = stream->minVersion;
				oldestStreamId = streamId;
			}
		}
		Version minimumSafePopVersion = std::numeric_limits<Version>::max();
		for (const auto& [tag, version] : lastSafePopVersions) {
			minimumSafePopVersion = std::min(minimumSafePopVersion, version);
		}
		TraceEvent("CDCProxyMetrics", id)
		    .detail("Streams", streams.size())
		    .detail("Tags", tags.size())
		    .detail("BufferedBytes", bufferedBytes)
		    .detail("TotalBufferedMutationBytes", totalBufferedMutationBytes)
		    .detail("ActivePermits", bufferLock.activePermits())
		    .detail("PeakActivePermits", peakActivePermits)
		    .detail("BufferLimit", SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES)
		    .detail("BufferWaiters", bufferLock.waiters())
		    .detail("ActiveConsumeRequests", activeConsumeRequests)
		    .detail("ReadDemand", totalReadDemand)
		    .detail("OldestStreamId", oldestStreamId)
		    .detail("OldestRequiredVersion",
		            oldestRequiredVersion == std::numeric_limits<Version>::max() ? invalidVersion
		                                                                         : oldestRequiredVersion)
		    .detail("CommittedVersion", latestCommittedVersion)
		    .detail("AcknowledgementLagVersions", cdcLagVersions(latestCommittedVersion, oldestRequiredVersion))
		    .detail("MinimumSafePopVersion",
		            minimumSafePopVersion == std::numeric_limits<Version>::max() ? invalidVersion
		                                                                         : minimumSafePopVersion)
		    .detail("SafePopDistanceVersions", cdcLagVersions(latestCommittedVersion, minimumSafePopVersion))
		    .detail("PopRequests", popRequests)
		    .detail("PopAttempts", popAttempts)
		    .detail("PopCompletions", popCompletions)
		    .detail("PopCancellations", popCancellations);
	}
}

Future<Void> CDCProxy::monitorDBInfo(CDCProxyInterface proxy, uint64_t recoveryCount) {
	bool hasBeenPublished =
	    std::find(dbInfo->get().client.cdcProxies.begin(), dbInfo->get().client.cdcProxies.end(), proxy) !=
	    dbInfo->get().client.cdcProxies.end();
	while (true) {
		co_await dbInfo->onChange();
		if (dbInfo->get().recoveryCount < recoveryCount) {
			CODE_PROBE(true, "CDC proxy ignores ServerDBInfo from an older recovery", probe::decoration::rare);
			continue;
		}
		const bool isPublished =
		    std::find(dbInfo->get().client.cdcProxies.begin(), dbInfo->get().client.cdcProxies.end(), proxy) !=
		    dbInfo->get().client.cdcProxies.end();
		if (hasBeenPublished && dbInfo->get().recoveryCount >= recoveryCount && !isPublished) {
			throw worker_removed();
		}
		hasBeenPublished = hasBeenPublished || isPublished;
		if (!dbInfo->get().logSystemConfig.tLogs.empty()) {
			CODE_PROBE(dbInfo->get().recoveryCount > recoveryCount,
			           "CDC proxy refreshes its log consumer after recovery");
			refreshLogSystem();
		}
		reconcileStreams();
		requestAcknowledgedDataPop();
	}
}

Future<Void> CDCProxy::run(CDCProxyInterface proxy, uint64_t recoveryCount) {
	// ENABLE_NATIVE_CDC gates new stream admission, not serving: recovery keeps CDC proxies alive while durable
	// streams or retired-tag pops drain after the feature is disabled.
	if (SERVER_KNOBS->MAXIMUM_PEEK_BYTES <= 0 ||
	    SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES <= SERVER_KNOBS->MAXIMUM_PEEK_BYTES ||
	    SERVER_KNOBS->CDC_PROXY_CONSUME_POLL_TIMEOUT <= 0 || SERVER_KNOBS->CDC_PROXY_POP_MIN_INTERVAL < 0 ||
	    SERVER_KNOBS->CDC_PROXY_POP_SCAN_INTERVAL <= 0) {
		TraceEvent(SevError, "InvalidCDCProxyMemoryConfiguration", id)
		    .detail("BufferBytes", SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES)
		    .detail("MaximumPeekBytes", SERVER_KNOBS->MAXIMUM_PEEK_BYTES)
		    .detail("ConsumePollTimeout", SERVER_KNOBS->CDC_PROXY_CONSUME_POLL_TIMEOUT)
		    .detail("PopMinInterval", SERVER_KNOBS->CDC_PROXY_POP_MIN_INTERVAL)
		    .detail("PopScanInterval", SERVER_KNOBS->CDC_PROXY_POP_SCAN_INTERVAL);
		throw invalid_option_value();
	}
	actors.add(waitFailureServer(proxy.waitFailure.getFuture()));
	actors.add(traceRole(Role::CDC_PROXY, proxy.id()));
	refreshLogSystem();
	reconcileStreams();
	actors.add(monitorAcknowledgedDataPops());
	requestAcknowledgedDataPop();
	actors.add(serveConsumeRequests(proxy.consume.getFuture()));
	actors.add(serveAcknowledgeRequests(proxy.ack.getFuture()));
	actors.add(serveRegisterStreamRequests(proxy.registerStream.getFuture()));
	actors.add(serveRemoveStreamRequests(proxy.removeStream.getFuture()));
	actors.add(serveHaltForTestingRequests(proxy.haltForTesting.getFuture()));
	actors.add(serveBufferStatusForTestingRequests(proxy.getBufferStatusForTesting.getFuture()));
	actors.add(serveSetPopsPausedForTestingRequests(proxy.setPopsPausedForTesting.getFuture()));
	actors.add(traceMetrics());
	actors.add(monitorDBInfo(proxy, recoveryCount));
	co_await actors.getResult();
}

} // namespace

Future<Void> cdcProxyServer(CDCProxyInterface proxy,
                            uint64_t recoveryCount,
                            Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	try {
		CDCProxy cdcProxy(proxy, dbInfo);
		co_await cdcProxy.run(proxy, recoveryCount);
	} catch (Error& e) {
		TraceEvent("CDCProxyTerminated", proxy.id()).errorUnsuppressed(e);
		if (e.code() != error_code_worker_removed) {
			throw;
		}
	}
}

TEST_CASE("/NativeCDC/ProxyMutationFiltering") {
	const KeyRangeRef keys("c"_sr, "m"_sr);

	Optional<MutationRef> inRange = clipCDCMutation(MutationRef(MutationRef::SetValue, "d"_sr, "value"_sr), keys);
	ASSERT(inRange.present());
	ASSERT_EQ(inRange.get().param1, "d"_sr);

	Optional<MutationRef> outOfRange = clipCDCMutation(MutationRef(MutationRef::SetValue, "z"_sr, "value"_sr), keys);
	ASSERT(!outOfRange.present());

	Optional<MutationRef> clippedClear = clipCDCMutation(MutationRef(MutationRef::ClearRange, "a"_sr, "f"_sr), keys);
	ASSERT(clippedClear.present());
	ASSERT_EQ(clippedClear.get().param1, "c"_sr);
	ASSERT_EQ(clippedClear.get().param2, "f"_sr);

	Optional<MutationRef> excludedClear = clipCDCMutation(MutationRef(MutationRef::ClearRange, "n"_sr, "z"_sr), keys);
	ASSERT(!excludedClear.present());

	return Void();
}

TEST_CASE("/NativeCDC/CoalescedPopTrigger") {
	CoalescedTrigger trigger;
	trigger.trigger();
	ASSERT(trigger.onTrigger().isReady());
	ASSERT(trigger.consume());
	ASSERT(!trigger.consume());

	Future<Void> waiting = trigger.onTrigger();
	ASSERT(!waiting.isReady());
	trigger.trigger();
	ASSERT(waiting.isReady());
	trigger.trigger();
	ASSERT(trigger.consume());
	ASSERT(!trigger.consume());
	return Void();
}

TEST_CASE("/NativeCDC/ProxyBufferCandidateSelection") {
	std::vector<CDCBufferCandidate> fanout;
	for (CDCStreamId streamId = 1; streamId <= 20; ++streamId) {
		fanout.push_back(CDCBufferCandidate{ streamId, 100, 25 });
	}
	CDCBufferSelection bounded = selectBufferCandidates(fanout, 100, 1000);
	ASSERT_EQ(bounded.selectedBytes, 100);
	ASSERT_EQ(bounded.selectedStreamIds.size(), 4);
	ASSERT(bounded.oversizedStreamIds.empty());

	CDCBufferSelection oneLarge =
	    selectBufferCandidates({ CDCBufferCandidate{ 1, 100, 150 }, CDCBufferCandidate{ 2, 100, 25 } }, 100, 200);
	ASSERT_EQ(oneLarge.selectedBytes, 150);
	ASSERT_EQ(oneLarge.selectedStreamIds.size(), 1);
	ASSERT(oneLarge.selectedStreamIds.contains(1));

	CDCBufferSelection rejectsOverLimit =
	    selectBufferCandidates({ CDCBufferCandidate{ 1, 100, 250 }, CDCBufferCandidate{ 2, 101, 0 } }, 100, 200);
	ASSERT_EQ(rejectsOverLimit.selectedBytes, 0);
	ASSERT_EQ(rejectsOverLimit.oversizedStreamIds.size(), 1);
	ASSERT_EQ(rejectsOverLimit.oversizedStreamIds.front(), 1);
	ASSERT(rejectsOverLimit.selectedStreamIds.contains(2));

	return Void();
}

TEST_CASE("/NativeCDC/ProxyBufferPassLimits") {
	Optional<CDCBufferPassLimits> replicated = calculateBufferPassLimits(1000, 100, 3);
	ASSERT(replicated.present());
	ASSERT_EQ(replicated.get().rawReplyBytes, 300);
	ASSERT_EQ(replicated.get().preferredBufferedBytes, 100);
	ASSERT_EQ(replicated.get().hardBufferedBytes, 700);
	ASSERT_EQ(replicated.get().reservationBytes, 400);

	ASSERT(!calculateBufferPassLimits(300, 100, 3).present());
	ASSERT(!calculateBufferPassLimits(std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max(), 2)
	            .present());
	return Void();
}

TEST_CASE("/NativeCDC/CommittedDeliveryFrontier") {
	ASSERT_EQ(committedPeekThrough(200, 150), 150);
	ASSERT_EQ(committedPeekThrough(150, 200), 150);
	ASSERT_EQ(committedPeekThrough(150, 150), 150);
	return Void();
}

TEST_CASE("/NativeCDC/RawPeekFailureFrontier") {
	ASSERT(rawPeekFailureBlocksStream(Optional<Version>(100), 100));
	ASSERT(!rawPeekFailureBlocksStream(Optional<Version>(101), 100));
	ASSERT(!rawPeekFailureBlocksStream(Optional<Version>(), 100));
	return Void();
}

TEST_CASE("/NativeCDC/LagMetrics") {
	ASSERT_EQ(cdcLagVersions(200, 150), 50);
	ASSERT_EQ(cdcLagVersions(150, 200), 0);
	ASSERT_EQ(cdcLagVersions(invalidVersion, 150), invalidVersion);
	ASSERT_EQ(cdcLagVersions(200, invalidVersion), invalidVersion);
	ASSERT_EQ(cdcLagVersions(200, std::numeric_limits<Version>::max()), invalidVersion);
	return Void();
}

TEST_CASE("/NativeCDC/StreamInitializationLifecycle") {
	std::unordered_map<CDCStreamId, Reference<CDCBufferedStream>> streams;
	Reference<CDCBufferedStream> stream = makeReference<CDCBufferedStream>(1);
	Reference<CDCBufferedStream> replacement = makeReference<CDCBufferedStream>(1);

	ASSERT(!isCurrentStreamInitialization(streams, stream));
	streams.emplace(stream->streamId, stream);
	ASSERT(isCurrentStreamInitialization(streams, stream));
	stream->active = false;
	ASSERT(!isCurrentStreamInitialization(streams, stream));
	stream->active = true;
	streams.at(stream->streamId) = replacement;
	ASSERT(!isCurrentStreamInitialization(streams, stream));
	ASSERT(isCurrentStreamInitialization(streams, replacement));
	return Void();
}

TEST_CASE("/NativeCDC/RetiredTagPopTarget") {
	const Version retiredVersion = 100;

	ASSERT_EQ(retiredTagPopTarget(retiredVersion, Optional<Version>(), Optional<Version>()), retiredVersion);
	ASSERT_EQ(retiredTagPopTarget(retiredVersion, Optional<Version>(), Optional<Version>(50)), retiredVersion);
	ASSERT_EQ(retiredTagPopTarget(retiredVersion, Optional<Version>(), Optional<Version>(150)), 150);
	ASSERT_EQ(retiredTagPopTarget(retiredVersion, Optional<Version>(75), Optional<Version>(150)), 75);
	ASSERT_EQ(retiredTagPopTarget(retiredVersion, Optional<Version>(125), Optional<Version>(150)), retiredVersion);

	return Void();
}

TEST_CASE("/NativeCDC/CompleteLogSystemConfig") {
	LogSystemConfig config;
	config.expectedLogSets = 2;
	ASSERT(!hasCompleteLogSystemConfig(config));

	config.tLogs.emplace_back();
	ASSERT(!hasCompleteLogSystemConfig(config));

	config.tLogs.emplace_back();
	ASSERT(hasCompleteLogSystemConfig(config));

	config.expectedLogSets = 0;
	ASSERT(!hasCompleteLogSystemConfig(config));
	return Void();
}
