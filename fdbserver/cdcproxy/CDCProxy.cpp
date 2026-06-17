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

struct CDCStreamReadState {
	Optional<KeyRange> keys;
	Version minVersion = invalidVersion;
	Version readVersion = invalidVersion;
	std::vector<std::pair<Version, Tag>> tagAssignments;
};

struct CDCTagInterval {
	Tag tag;
	Version begin;
	Version end;
	Version bufferedThrough;

	CDCTagInterval(Tag tag, Version begin, Version end)
	  : tag(tag), begin(begin), end(end), bufferedThrough(begin - 1) {}
};

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
	std::vector<CDCTagInterval> tagIntervals;
	std::deque<Standalone<VersionedMutationsRef>> mutations;
	AsyncTrigger changed;

	explicit CDCBufferedStream(CDCStreamId streamId) : streamId(streamId) {}
};

struct CDCBufferedBatch {
	int64_t bufferedBytes = 0;
	std::deque<Standalone<VersionedMutationsRef>> mutations;
};

struct CDCBufferedTag : ReferenceCounted<CDCBufferedTag> {
	Tag tag;
	bool active = true;
	std::set<CDCStreamId> streamIds;
	AsyncTrigger refresh;
	AsyncTrigger stopped;

	explicit CDCBufferedTag(Tag tag) : tag(tag) {}
};

struct CDCBufferCandidate {
	CDCStreamId streamId;
	Version nextVersion;
	int64_t estimatedBytes;
};

struct CDCBufferSelection {
	std::unordered_set<CDCStreamId> selectedStreamIds;
	std::vector<CDCStreamId> oversizedStreamIds;
	int64_t selectedBytes = 0;
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

class CDCProxy {
	UID id;
	Database cx;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<AsyncVar<Reference<LogSystemConsumer>>> logSystem;
	std::unordered_map<CDCStreamId, Reference<CDCBufferedStream>> streams;
	std::unordered_map<Tag, Reference<CDCBufferedTag>> tags;
	AsyncTrigger popAcknowledgedDataTrigger;
	AsyncTrigger peekCapacityContended;
	FlowLock bufferLock;
	int64_t bufferedBytes = 0;
	ActorCollection actors;

	void clearBufferedMutations(Reference<CDCBufferedStream> stream);
	void addBufferedBatch(Reference<CDCBufferedStream> stream, CDCBufferedBatch batch);
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

		Reference<IReplayPeekCursor> cursor = logSystem->get()->peekSingle(id, begin.get(), tag->tag, {});
		while (tag->active) {
			const int64_t bufferLimit = SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES;
			const int64_t peekReservation = std::min<int64_t>(bufferLimit, SERVER_KNOBS->MAXIMUM_PEEK_BYTES);
			ASSERT_GT(peekReservation, 0);
			if (bufferLock.available() < peekReservation) {
				CODE_PROBE(true, "CDC proxy applies shared buffer backpressure", probe::decoration::rare);
				peekCapacityContended.trigger();
			}
			auto capacity = co_await race(bufferLock.take(TaskPriority::TLogPeekReply, peekReservation),
			                              logSystem->onChange(),
			                              tag->stopped.onTrigger(),
			                              tag->refresh.onTrigger());
			if (capacity.index() == 1 || capacity.index() == 3) {
				break;
			}
			if (capacity.index() == 2) {
				co_return;
			}
			FlowLock::Releaser reservation(bufferLock, peekReservation);
			if (!cursor->hasMessage()) {
				// Blocking peeks hold a response reservation. Once another tag queues for capacity, rotate this
				// reader after one blocking-peek interval so an idle tag cannot monopolize the shared budget.
				auto result = co_await race(cursor->getMore(TaskPriority::TLogPeekReply),
				                            logSystem->onChange(),
				                            tag->stopped.onTrigger(),
				                            tag->refresh.onTrigger(),
				                            rotateContendedPeek());
				if (result.index() == 1 || result.index() == 3 || result.index() == 4) {
					break;
				}
				if (result.index() == 2) {
					co_return;
				}
			}
			// A newly constructed replay cursor can already contain messages, especially after log-generation
			// changes. Initialize its reader even when getMore() was unnecessary.
			cursor->setProtocolVersion(g_network->protocolVersion());
			if (cursor->popped() > begin.get()) {
				markPoppedTagStreamsTooOld(tag, cursor->popped());
				break;
			}

			const Version throughVersion =
			    cursor->hasMessage() ? cursor->version().version : cursor->version().version - 1;
			if (throughVersion < begin.get()) {
				break;
			}
			Reference<IReplayPeekCursor> estimateCursor = cursor->cloneNoMore();
			estimateCursor->setProtocolVersion(g_network->protocolVersion());
			const std::unordered_map<CDCStreamId, int64_t> estimatedBytes =
			    estimateBufferedBytes(tag, estimateCursor, throughVersion);
			const std::vector<CDCBufferCandidate> candidates = getBufferCandidates(tag, throughVersion, estimatedBytes);
			CDCBufferSelection selection = selectBufferCandidates(candidates, peekReservation, bufferLimit);
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
				    .detail("BufferLimit", bufferLimit)
				    .detail("Version", throughVersion);
				stream->second->bufferLimitExceeded = true;
				stream->second->changed.trigger();
			}
			if (selection.selectedStreamIds.empty()) {
				break;
			}

			if (selection.selectedBytes <= reservation.remaining) {
				reservation.release(reservation.remaining - selection.selectedBytes);
			} else {
				CODE_PROBE(true,
				           "CDC proxy materializes one stream batch larger than its peek reservation",
				           probe::decoration::rare);
				reservation.release();
				auto exactCapacity =
				    co_await race(bufferLock.take(TaskPriority::TLogPeekReply, selection.selectedBytes),
				                  logSystem->onChange(),
				                  tag->stopped.onTrigger(),
				                  tag->refresh.onTrigger());
				if (exactCapacity.index() == 1 || exactCapacity.index() == 3) {
					break;
				}
				if (exactCapacity.index() == 2) {
					co_return;
				}
				reservation = FlowLock::Releaser(bufferLock, selection.selectedBytes);
			}
			if (!tag->active) {
				co_return;
			}

			std::unordered_map<CDCStreamId, CDCBufferedBatch> batches =
			    bufferMessages(tag, cursor, throughVersion, selection.selectedStreamIds);
			int64_t materializedBytes = 0;
			for (const auto& [streamId, batch] : batches) {
				materializedBytes += batch.bufferedBytes;
			}
			ASSERT_EQ(materializedBytes, selection.selectedBytes);
			ASSERT_EQ(materializedBytes, reservation.remaining);

			int64_t acceptedBytes = 0;
			for (auto& [streamId, batch] : batches) {
				auto stream = streams.find(streamId);
				if (stream != streams.end() && stream->second->active && tag->streamIds.contains(streamId)) {
					acceptedBytes += batch.bufferedBytes;
					addBufferedBatch(stream->second, std::move(batch));
				}
			}
			reservation.release(materializedBytes - acceptedBytes);
			// Buffered mutations own these permits until acknowledgement or stream removal.
			reservation.remaining = 0;
			ASSERT_LE(bufferedBytes, bufferLimit);
			ASSERT_LE(bufferLock.activePermits(), bufferLimit);
			advanceTagBufferedThrough(tag, throughVersion, selection.selectedStreamIds);
			if (selection.selectedStreamIds.size() != candidates.size()) {
				// At least one stream still needs this version. Reopen at the shared minimum instead of advancing
				// the tag cursor past data that was intentionally not materialized in this bounded pass.
				break;
			}
			if (!nextTagReadVersion(tag).present()) {
				break;
			}
			if (!cursor->hasMessage() && cursor->isExhausted()) {
				break;
			}
		}
	}
}

Future<Void> CDCProxy::initializeStream(Reference<CDCBufferedStream> stream) {
	try {
		const CDCStreamReadState metadata = co_await readCDCStreamState(cx, stream->streamId, id, true);
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
// history on every acknowledgement.
Future<std::unordered_map<Tag, Version>> readSafePopVersions(Database cx) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			std::unordered_map<CDCStreamId, Version> minVersions;
			Key begin = cdcMinVersionKeys.begin;
			while (begin < cdcMinVersionKeys.end) {
				RangeResult minima =
				    co_await tr.getRange(KeyRangeRef(begin, cdcMinVersionKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : minima) {
					minVersions[decodeCDCMinVersionKey(kv.key)] = decodeCDCMinVersionValue(kv.value);
				}
				if (!minima.more) {
					break;
				}
				begin = keyAfter(minima.back().key);
			}

			std::unordered_map<Tag, Version> safePopVersions;
			begin = cdcTagHistoryKeys.begin;
			while (begin < cdcTagHistoryKeys.end) {
				RangeResult histories =
				    co_await tr.getRange(KeyRangeRef(begin, cdcTagHistoryKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : histories) {
					const CDCTagHistoryEntry history = decodeCDCTagHistoryKey(kv.key);
					auto minimum = minVersions.find(history.streamId);
					if (minimum == minVersions.end()) {
						continue;
					}
					auto safePop = safePopVersions.find(history.tag);
					if (safePop == safePopVersions.end()) {
						safePopVersions[history.tag] = minimum->second;
					} else {
						safePop->second = std::min(safePop->second, minimum->second);
					}
				}
				if (!histories.more) {
					break;
				}
				begin = keyAfter(histories.back().key);
			}
			co_return safePopVersions;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<std::unordered_map<Tag, Version>> readRetiredTagPopVersions(Database cx) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			std::unordered_map<Tag, Version> retiredTagPopVersions;
			Key begin = cdcRetiredTagPopVersionKeys.begin;
			while (begin < cdcRetiredTagPopVersionKeys.end) {
				RangeResult retired =
				    co_await tr.getRange(KeyRangeRef(begin, cdcRetiredTagPopVersionKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : retired) {
					retiredTagPopVersions[decodeCDCRetiredTagPopVersionKey(kv.key)] =
					    decodeCDCMinVersionValue(kv.value);
				}
				if (!retired.more) {
					break;
				}
				begin = keyAfter(retired.back().key);
			}
			co_return retiredTagPopVersions;
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
	while (!logSystem->get()) {
		CODE_PROBE(true, "CDC proxy defers pops until a log system is available", probe::decoration::rare);
		co_await logSystem->onChange();
	}
	const std::unordered_map<Tag, Version> safePopVersions = co_await readSafePopVersions(cx);
	Reference<LogSystemConsumer> currentLogSystem = logSystem->get();
	for (const auto& [tag, version] : safePopVersions) {
		currentLogSystem->pop(version, tag);
	}
	const std::unordered_map<Tag, Version> retiredTagPopVersions = co_await readRetiredTagPopVersions(cx);
	std::unordered_map<Tag, Version> completedPopVersions;
	for (const auto& [tag, retiredVersion] : retiredTagPopVersions) {
		const auto safePop = safePopVersions.find(tag);
		const Version version =
		    safePop == safePopVersions.end() ? retiredVersion : std::min(retiredVersion, safePop->second);
		CODE_PROBE(safePop != safePopVersions.end() && version < retiredVersion,
		           "CDC proxy defers retired tag pop behind a live shared stream");
		currentLogSystem->pop(version, tag);
		if (version >= retiredVersion) {
			co_await currentLogSystem->waitForPopped(retiredVersion, tag);
			completedPopVersions[tag] = retiredVersion;
		}
	}
	co_await clearCompletedRetiredTagPops(cx, std::move(completedPopVersions));
}

Future<Void> CDCProxy::monitorAcknowledgedDataPops() {
	co_await popAcknowledgedDataTrigger.onTrigger();
	while (true) {
		// Pop completion may wait on an unavailable log generation. A new acknowledgement or log-system
		// configuration supersedes that attempt and retries the durable work against current state.
		Future<Void> retriggered = popAcknowledgedDataTrigger.onTrigger();
		auto result = co_await race(popAcknowledgedData(), retriggered);
		if (result.index() == 0) {
			co_await popAcknowledgedDataTrigger.onTrigger();
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
	try {
		if (request.cursor.lastConsumedVersion < invalidVersion ||
		    request.cursor.lastConsumedVersion == std::numeric_limits<Version>::max()) {
			throw client_invalid_operation();
		}

		const CDCStreamReadState metadata = co_await readCDCStreamState(cx, request.cursor.streamId, id, true);
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
		advanceStreamMinVersion(stream, metadata.minVersion);
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

		co_await waitForBufferedVersion(stream, begin);
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
				reply.mutations.push_back(reply.arena, VersionedMutationsRef(versioned.version, {}));
				for (const auto& mutation : versioned.mutations) {
					reply.mutations.back().mutations.push_back_deep(reply.arena, mutation);
				}
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
		popAcknowledgedDataTrigger.trigger();
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
			popAcknowledgedDataTrigger.trigger();
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
		status.bufferLimit = SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES;
		status.waiters = bufferLock.waiters();
		request.reply.send(status);
	}
}

Future<Void> CDCProxy::monitorDBInfo(CDCProxyInterface proxy, uint64_t recoveryCount) {
	bool hasBeenPublished =
	    std::find(dbInfo->get().client.cdcProxies.begin(), dbInfo->get().client.cdcProxies.end(), proxy) !=
	    dbInfo->get().client.cdcProxies.end();
	while (true) {
		co_await dbInfo->onChange();
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
			logSystem->set(makeLogSystemConsumerFromServerDBInfo(id, dbInfo->get()));
		}
		reconcileStreams();
		popAcknowledgedDataTrigger.trigger();
	}
}

Future<Void> CDCProxy::run(CDCProxyInterface proxy, uint64_t recoveryCount) {
	actors.add(waitFailureServer(proxy.waitFailure.getFuture()));
	actors.add(traceRole(Role::CDC_PROXY, proxy.id()));
	logSystem->set(makeLogSystemConsumerFromServerDBInfo(id, dbInfo->get()));
	reconcileStreams();
	actors.add(monitorAcknowledgedDataPops());
	popAcknowledgedDataTrigger.trigger();
	actors.add(serveConsumeRequests(proxy.consume.getFuture()));
	actors.add(serveAcknowledgeRequests(proxy.ack.getFuture()));
	actors.add(serveRegisterStreamRequests(proxy.registerStream.getFuture()));
	actors.add(serveRemoveStreamRequests(proxy.removeStream.getFuture()));
	actors.add(serveHaltForTestingRequests(proxy.haltForTesting.getFuture()));
	actors.add(serveBufferStatusForTestingRequests(proxy.getBufferStatusForTesting.getFuture()));
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
