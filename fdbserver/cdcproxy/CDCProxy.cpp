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
#include <map>
#include <set>
#include <utility>
#include <vector>

#include "fdbclient/Knobs.h"
#include "fdbclient/NativeCdcInternal.h"
#include "fdbclient/SystemData.h"
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
#include "flow/UnitTest.h"
#include "flow/genericactors.actor.h"

namespace {

struct CDCStreamReadState {
	Optional<KeyRange> keys;
	Version minVersion = invalidVersion;
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

struct CDCProxyData {
	UID id;
	Database cx;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<AsyncVar<Reference<LogSystemConsumer>>> logSystem;
	std::map<CDCStreamId, Reference<CDCBufferedStream>> streams;
	std::map<Tag, Reference<CDCBufferedTag>> tags;
	AsyncTrigger popAcknowledgedDataTrigger;
	AsyncTrigger peekCapacityContended;
	FlowLock bufferLock;
	int64_t bufferedBytes = 0;

	CDCProxyData(CDCProxyInterface const& proxy, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
	  : id(proxy.id()), cx(openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True)), dbInfo(dbInfo),
	    logSystem(makeReference<AsyncVar<Reference<LogSystemConsumer>>>()),
	    bufferLock(SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES) {}
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

void clearBufferedMutations(CDCProxyData* self, Reference<CDCBufferedStream> stream) {
	if (stream->bufferedBytes > 0) {
		ASSERT(self->bufferedBytes >= stream->bufferedBytes);
		self->bufferedBytes -= stream->bufferedBytes;
		self->bufferLock.release(stream->bufferedBytes);
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

void addBufferedBatch(CDCProxyData* self, Reference<CDCBufferedStream> stream, CDCBufferedBatch batch) {
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
	self->bufferedBytes += batch.bufferedBytes;
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

void detachStreamFromTags(CDCProxyData* self, Reference<CDCBufferedStream> stream) {
	for (const auto& interval : stream->tagIntervals) {
		auto tag = self->tags.find(interval.tag);
		if (tag == self->tags.end()) {
			continue;
		}
		Reference<CDCBufferedTag> bufferedTag = tag->second;
		bufferedTag->streamIds.erase(stream->streamId);
		if (bufferedTag->streamIds.empty()) {
			bufferedTag->active = false;
			self->tags.erase(tag);
			bufferedTag->stopped.trigger();
		} else {
			bufferedTag->refresh.trigger();
		}
	}
}

void refreshStreamTags(CDCProxyData* self, Reference<CDCBufferedStream> stream) {
	for (const auto& interval : stream->tagIntervals) {
		auto tag = self->tags.find(interval.tag);
		if (tag != self->tags.end()) {
			tag->second->refresh.trigger();
		}
	}
}

Optional<Version> nextTagReadVersion(CDCProxyData* self, Reference<CDCBufferedTag> tag) {
	Optional<Version> begin;
	for (const CDCStreamId streamId : tag->streamIds) {
		auto stream = self->streams.find(streamId);
		if (stream == self->streams.end() || !stream->second->active || !stream->second->initialized ||
		    stream->second->readDemand == 0) {
			continue;
		}
		for (const auto& interval : stream->second->tagIntervals) {
			if (interval.tag != tag->tag) {
				continue;
			}
			const Version next = std::max(interval.begin, interval.bufferedThrough + 1);
			if (next < interval.end && (!begin.present() || next < begin.get())) {
				begin = next;
			}
		}
	}
	return begin;
}

void advanceTagBufferedThrough(CDCProxyData* self, Reference<CDCBufferedTag> tag, Version bufferedThrough) {
	const std::vector<CDCStreamId> streamIds(tag->streamIds.begin(), tag->streamIds.end());
	for (const CDCStreamId streamId : streamIds) {
		auto stream = self->streams.find(streamId);
		if (stream == self->streams.end() || !stream->second->active || stream->second->readDemand == 0) {
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

void markPoppedTagStreamsTooOld(CDCProxyData* self, Reference<CDCBufferedTag> tag, Version popped) {
	std::vector<Reference<CDCBufferedStream>> tooOldStreams;
	for (const CDCStreamId streamId : tag->streamIds) {
		auto stream = self->streams.find(streamId);
		if (stream == self->streams.end() || !stream->second->active) {
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
		TraceEvent("CDCBufferStreamTooOld", self->id)
		    .detail("StreamId", stream->streamId)
		    .detail("MinVersion", stream->minVersion)
		    .detail("BufferedThrough", stream->bufferedThrough)
		    .detail("Popped", popped)
		    .detail("Tag", tag->tag);
		clearBufferedMutations(self, stream);
		stream->tooOld = true;
		stream->active = false;
		stream->changed.trigger();
		detachStreamFromTags(self, stream);
	}
}

std::map<CDCStreamId, CDCBufferedBatch> bufferMessages(CDCProxyData* self,
                                                       Reference<CDCBufferedTag> tag,
                                                       Reference<IReplayPeekCursor> cursor) {
	std::map<CDCStreamId, CDCBufferedBatch> batches;
	while (cursor->hasMessage()) {
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
				auto stream = self->streams.find(streamId);
				if (stream == self->streams.end() || !stream->second->active || stream->second->readDemand == 0 ||
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
					addMutationToBatch(stream->second, &batches[streamId], messageVersion, clipped.get());
				}
			}
		}
		cursor->nextMessage();
	}
	return batches;
}

Future<Void> rotateContendedPeek(CDCProxyData* self) {
	if (self->bufferLock.waiters() == 0) {
		co_await self->peekCapacityContended.onTrigger();
	}
	co_await delay(SERVER_KNOBS->BLOCKING_PEEK_TIMEOUT);
}

Future<Void> bufferTag(CDCProxyData* self, Reference<CDCBufferedTag> tag) {
	while (tag->active) {
		Optional<Version> begin = nextTagReadVersion(self, tag);
		if (!begin.present()) {
			auto waitForDemand = co_await race(tag->stopped.onTrigger(), tag->refresh.onTrigger());
			if (waitForDemand.index() == 0) {
				co_return;
			}
			continue;
		}
		if (!self->logSystem->get()) {
			auto waitForLogSystem =
			    co_await race(self->logSystem->onChange(), tag->stopped.onTrigger(), tag->refresh.onTrigger());
			if (waitForLogSystem.index() == 1) {
				co_return;
			}
			continue;
		}

		Reference<IReplayPeekCursor> cursor = self->logSystem->get()->peekSingle(self->id, begin.get(), tag->tag, {});
		while (tag->active) {
			const int64_t peekReservation =
			    std::min<int64_t>(SERVER_KNOBS->CDC_PROXY_BUFFER_BYTES, SERVER_KNOBS->MAXIMUM_PEEK_BYTES);
			ASSERT(peekReservation > 0);
			if (self->bufferLock.available() < peekReservation) {
				CODE_PROBE(true, "CDC proxy applies shared buffer backpressure", probe::decoration::rare);
				self->peekCapacityContended.trigger();
			}
			auto capacity = co_await race(self->bufferLock.take(TaskPriority::TLogPeekReply, peekReservation),
			                              self->logSystem->onChange(),
			                              tag->stopped.onTrigger(),
			                              tag->refresh.onTrigger());
			if (capacity.index() == 1 || capacity.index() == 3) {
				break;
			}
			if (capacity.index() == 2) {
				co_return;
			}
			FlowLock::Releaser reservation(self->bufferLock, peekReservation);
			// Blocking peeks hold a response reservation. Once another tag queues for capacity, rotate this
			// reader after one blocking-peek interval so an idle tag cannot monopolize the shared budget.
			auto result = co_await race(cursor->getMore(TaskPriority::TLogPeekReply),
			                            self->logSystem->onChange(),
			                            tag->stopped.onTrigger(),
			                            tag->refresh.onTrigger(),
			                            rotateContendedPeek(self));
			if (result.index() == 1 || result.index() == 3 || result.index() == 4) {
				break;
			}
			if (result.index() == 2) {
				co_return;
			}

			cursor->setProtocolVersion(g_network->protocolVersion());
			if (cursor->popped() > begin.get()) {
				markPoppedTagStreamsTooOld(self, tag, cursor->popped());
				break;
			}

			std::map<CDCStreamId, CDCBufferedBatch> batches = bufferMessages(self, tag, cursor);
			int64_t bufferedBytes = 0;
			for (const auto& [streamId, batch] : batches) {
				bufferedBytes += batch.bufferedBytes;
			}
			if (bufferedBytes > peekReservation) {
				CODE_PROBE(true, "CDC proxy reserves capacity for oversized peek batch", probe::decoration::rare);
				TraceEvent(SevWarn, "CDCProxyOversizedPeekBatch", self->id)
				    .detail("Tag", tag->tag)
				    .detail("BufferedBytes", bufferedBytes)
				    .detail("ReservedBytes", peekReservation);
				reservation.release();
				auto oversizedCapacity =
				    co_await race(self->bufferLock.take(TaskPriority::TLogPeekReply, bufferedBytes),
				                  self->logSystem->onChange(),
				                  tag->stopped.onTrigger(),
				                  tag->refresh.onTrigger());
				if (oversizedCapacity.index() == 1 || oversizedCapacity.index() == 3) {
					break;
				}
				if (oversizedCapacity.index() == 2) {
					co_return;
				}
				reservation = FlowLock::Releaser(self->bufferLock, bufferedBytes);
			} else {
				reservation.release(peekReservation - bufferedBytes);
			}
			if (!tag->active) {
				co_return;
			}

			int64_t acceptedBytes = 0;
			for (auto& [streamId, batch] : batches) {
				auto stream = self->streams.find(streamId);
				if (stream != self->streams.end() && stream->second->active && tag->streamIds.contains(streamId)) {
					acceptedBytes += batch.bufferedBytes;
					addBufferedBatch(self, stream->second, std::move(batch));
				}
			}
			reservation.release(bufferedBytes - acceptedBytes);
			// Buffered mutations own these permits until acknowledgement or stream removal.
			reservation.remaining = 0;
			advanceTagBufferedThrough(self, tag, cursor->version().version - 1);
			if (!nextTagReadVersion(self, tag).present()) {
				break;
			}
			if (cursor->isExhausted()) {
				break;
			}
		}
	}
}

Future<Void> initializeStream(CDCProxyData* self, Reference<CDCBufferedStream> stream, ActorCollection* actors) {
	try {
		const CDCStreamReadState metadata = co_await readCDCStreamState(self->cx, stream->streamId, self->id, true);
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
			auto tag = self->tags.find(interval.tag);
			if (tag == self->tags.end()) {
				Reference<CDCBufferedTag> newTag = makeReference<CDCBufferedTag>(interval.tag);
				tag = self->tags.emplace(interval.tag, newTag).first;
				tag->second->streamIds.insert(stream->streamId);
				actors->add(bufferTag(self, newTag));
			} else {
				CODE_PROBE(true, "CDC proxy shares a tag reader across streams");
				tag->second->streamIds.insert(stream->streamId);
				tag->second->refresh.trigger();
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_client_invalid_operation || e.code() == error_code_wrong_shard_server) {
			clearBufferedMutations(self, stream);
			stream->active = false;
			stream->changed.trigger();
			co_return;
		}
		throw;
	}
}

// TODO: Persist per-tag safe-pop state or coordinate pops centrally instead of rebuilding minima from all stream
// history on every acknowledgement.
Future<std::map<Tag, Version>> readSafePopVersions(Database cx) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			std::map<CDCStreamId, Version> minVersions;
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

			std::map<Tag, Version> safePopVersions;
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

Future<std::map<Tag, Version>> readRetiredTagPopVersions(Database cx) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			std::map<Tag, Version> retiredTagPopVersions;
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

Future<Void> clearCompletedRetiredTagPops(Database cx, std::map<Tag, Version> completedPopVersions) {
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

Future<Void> popAcknowledgedData(CDCProxyData* self) {
	const std::map<Tag, Version> safePopVersions = co_await readSafePopVersions(self->cx);
	Reference<LogSystemConsumer> logSystem = self->logSystem->get();
	for (const auto& [tag, version] : safePopVersions) {
		logSystem->pop(version, tag);
	}
	const std::map<Tag, Version> retiredTagPopVersions = co_await readRetiredTagPopVersions(self->cx);
	std::map<Tag, Version> completedPopVersions;
	for (const auto& [tag, retiredVersion] : retiredTagPopVersions) {
		const auto safePop = safePopVersions.find(tag);
		const Version version =
		    safePop == safePopVersions.end() ? retiredVersion : std::min(retiredVersion, safePop->second);
		CODE_PROBE(safePop != safePopVersions.end() && version < retiredVersion,
		           "CDC proxy defers retired tag pop behind a live shared stream");
		logSystem->pop(version, tag);
		if (version >= retiredVersion) {
			co_await logSystem->waitForPopped(retiredVersion, tag);
			completedPopVersions[tag] = retiredVersion;
		}
	}
	co_await clearCompletedRetiredTagPops(self->cx, std::move(completedPopVersions));
}

Future<Void> monitorAcknowledgedDataPops(CDCProxyData* self) {
	co_await self->popAcknowledgedDataTrigger.onTrigger();
	while (true) {
		// Pop completion may wait on an unavailable log generation. A new acknowledgement or log-system
		// configuration supersedes that attempt and retries the durable work against current state.
		Future<Void> retriggered = self->popAcknowledgedDataTrigger.onTrigger();
		auto result = co_await race(popAcknowledgedData(self), retriggered);
		if (result.index() == 0) {
			co_await self->popAcknowledgedDataTrigger.onTrigger();
		}
	}
}

void reconcileStreams(CDCProxyData* self, ActorCollection* actors) {
	std::set<CDCStreamId> assignedStreams;
	for (const auto& [streamId, proxyId] : self->dbInfo->get().client.streamToCDCProxyId) {
		if (proxyId == self->id) {
			assignedStreams.insert(streamId);
			if (!self->streams.contains(streamId)) {
				Reference<CDCBufferedStream> stream = makeReference<CDCBufferedStream>(streamId);
				self->streams.emplace(streamId, stream);
				actors->add(initializeStream(self, stream, actors));
			}
		}
	}

	for (auto it = self->streams.begin(); it != self->streams.end();) {
		if (!assignedStreams.contains(it->first)) {
			CODE_PROBE(it->second->readDemand > 0, "CDC proxy wakes pending consume when stream is unassigned");
			CODE_PROBE(true, "CDC proxy drops removed or reassigned stream state");
			it->second->active = false;
			it->second->changed.trigger();
			detachStreamFromTags(self, it->second);
			clearBufferedMutations(self, it->second);
			it = self->streams.erase(it);
		} else {
			++it;
		}
	}
}

Future<Void> consume(CDCProxyData* self, CDCConsumeRequest request) {
	try {
		if (request.cursor.lastConsumedVersion < invalidVersion ||
		    request.cursor.lastConsumedVersion == std::numeric_limits<Version>::max()) {
			throw client_invalid_operation();
		}

		co_await readCDCStreamState(self->cx, request.cursor.streamId, self->id, true);
		auto found = self->streams.find(request.cursor.streamId);
		if (found == self->streams.end()) {
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

		Version begin = request.cursor.lastConsumedVersion == invalidVersion ? stream->minVersion
		                                                                     : request.cursor.lastConsumedVersion + 1;
		if (begin < stream->minVersion) {
			throw transaction_too_old();
		}

		bool issuedReadDemand = false;
		if (stream->bufferedThrough < begin) {
			++stream->readDemand;
			refreshStreamTags(self, stream);
			issuedReadDemand = true;
		}
		while (stream->active && stream->bufferedThrough < begin) {
			co_await stream->changed.onTrigger();
		}
		if (issuedReadDemand) {
			ASSERT(stream->readDemand > 0);
			--stream->readDemand;
			refreshStreamTags(self, stream);
		}
		if (stream->tooOld) {
			throw transaction_too_old();
		}
		if (!stream->active) {
			throw wrong_shard_server();
		}

		CDCConsumeReply reply;
		reply.lastConsumedVersion = request.cursor.lastConsumedVersion;
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

Future<Void> acknowledge(CDCProxyData* self, CDCAckRequest request) {
	try {
		if (request.version < 0 || request.version >= std::numeric_limits<Version>::max() - 1) {
			throw client_invalid_operation();
		}
		const CDCStreamReadState metadata = co_await readCDCStreamState(self->cx, request.streamId, self->id, false);
		if (metadata.minVersion <= request.version) {
			throw client_invalid_operation();
		}
		auto found = self->streams.find(request.streamId);
		if (found == self->streams.end()) {
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
			ASSERT(self->bufferedBytes >= releasedBytes);
			self->bufferedBytes -= releasedBytes;
			self->bufferLock.release(releasedBytes);
			stream->mutations.pop_front();
		}
		ASSERT(stream->bufferedBytes >= 0);
		self->popAcknowledgedDataTrigger.trigger();
		request.reply.send(Void());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
}

Future<Void> registerStream(CDCProxyData* self, CDCRegisterStreamRequest request) {
	try {
		const CDCStreamId streamId = co_await registerNativeCdcStream(self->cx, request.name, request.keys, self->id);
		request.reply.send(CDCRegisterStreamReply(streamId));
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
}

Future<Void> removeStream(CDCProxyData* self, CDCRemoveStreamRequest request) {
	try {
		Optional<NativeCdcRemovedStreamInfo> removed = co_await removeNativeCdcStream(self->cx, request.name, self->id);
		if (removed.present()) {
			self->popAcknowledgedDataTrigger.trigger();
		}
		request.reply.send(Void());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
}

Future<Void> listStreams(CDCProxyData* self, CDCListStreamsRequest request) {
	try {
		std::vector<NativeCdcStreamInfo> streams = co_await listNativeCdcStreams(self->cx);
		CDCListStreamsReply reply;
		for (NativeCdcStreamInfo const& stream : streams) {
			reply.streams.push_back(reply.arena,
			                        CDCStreamInfoRef(StringRef(reply.arena, stream.name),
			                                         stream.streamId,
			                                         KeyRangeRef(reply.arena, stream.keys),
			                                         stream.minVersion));
		}
		request.reply.send(reply);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
}

} // namespace

Future<Void> cdcProxyServer(CDCProxyInterface proxy,
                            uint64_t recoveryCount,
                            Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	try {
		CDCProxyData self(proxy, dbInfo);
		ActorCollection actors(false);

		actors.add(waitFailureServer(proxy.waitFailure.getFuture()));
		actors.add(traceRole(Role::CDC_PROXY, proxy.id()));
		self.logSystem->set(makeLogSystemConsumerFromServerDBInfo(self.id, dbInfo->get()));
		reconcileStreams(&self, &actors);
		actors.add(monitorAcknowledgedDataPops(&self));
		self.popAcknowledgedDataTrigger.trigger();
		Future<Void> dbInfoChange = dbInfo->onChange();
		bool hasBeenPublished =
		    std::find(dbInfo->get().client.cdcProxies.begin(), dbInfo->get().client.cdcProxies.end(), proxy) !=
		    dbInfo->get().client.cdcProxies.end();

		while (true) {
			auto result = co_await race(proxy.consume.getFuture(),
			                            proxy.ack.getFuture(),
			                            proxy.registerStream.getFuture(),
			                            proxy.removeStream.getFuture(),
			                            proxy.listStreams.getFuture(),
			                            proxy.haltForTesting.getFuture(),
			                            dbInfoChange,
			                            actors.getResult());
			switch (result.index()) {
			case 0:
				actors.add(consume(&self, std::get<0>(std::move(result))));
				break;
			case 1:
				actors.add(acknowledge(&self, std::get<1>(std::move(result))));
				break;
			case 2:
				actors.add(registerStream(&self, std::get<2>(std::move(result))));
				break;
			case 3:
				actors.add(removeStream(&self, std::get<3>(std::move(result))));
				break;
			case 4:
				actors.add(listStreams(&self, std::get<4>(std::move(result))));
				break;
			case 5:
				if (!g_network->isSimulated()) {
					std::get<5>(std::move(result)).reply.sendError(client_invalid_operation());
					break;
				}
				std::get<5>(std::move(result)).reply.send(Void());
				throw worker_removed();
			case 6: {
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
					self.logSystem->set(makeLogSystemConsumerFromServerDBInfo(self.id, dbInfo->get()));
				}
				reconcileStreams(&self, &actors);
				self.popAcknowledgedDataTrigger.trigger();
				dbInfoChange = dbInfo->onChange();
				break;
			}
			case 7:
				co_await actors.getResult();
				break;
			default:
				ASSERT(false);
			}
		}
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
	ASSERT(inRange.get().param1 == "d"_sr);

	Optional<MutationRef> outOfRange = clipCDCMutation(MutationRef(MutationRef::SetValue, "z"_sr, "value"_sr), keys);
	ASSERT(!outOfRange.present());

	Optional<MutationRef> clippedClear = clipCDCMutation(MutationRef(MutationRef::ClearRange, "a"_sr, "f"_sr), keys);
	ASSERT(clippedClear.present());
	ASSERT(clippedClear.get().param1 == "c"_sr);
	ASSERT(clippedClear.get().param2 == "f"_sr);

	Optional<MutationRef> excludedClear = clipCDCMutation(MutationRef(MutationRef::ClearRange, "n"_sr, "z"_sr), keys);
	ASSERT(!excludedClear.present());

	return Void();
}
