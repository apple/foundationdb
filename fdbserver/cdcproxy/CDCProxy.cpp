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
#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/cdcproxy/CDCProxy.h"
#include "fdbserver/core/LogProtocolMessage.h"
#include "fdbserver/core/OTELSpanContextMessage.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/core/SpanContextMessage.h"
#include "fdbserver/core/WaitFailure.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/logsystem/LogSystemConsumer.h"
#include "fdbserver/logsystem/LogSystemFactory.h"
#include "flow/ActorCollection.h"
#include "flow/Error.h"
#include "flow/UnitTest.h"

namespace {

struct CDCStreamReadState {
	Optional<KeyRange> keys;
	Version minVersion = invalidVersion;
	Tag currentTag = invalidTag;
	std::vector<std::pair<Version, Tag>> tagHistory;
};

struct CDCBufferedStream : ReferenceCounted<CDCBufferedStream> {
	CDCStreamId streamId;
	bool active = true;
	bool initialized = false;
	Version minVersion = invalidVersion;
	Version bufferedThrough = invalidVersion;
	std::deque<Standalone<VersionedMutationsRef>> mutations;
	AsyncTrigger changed;
	AsyncTrigger refresh;
	AsyncTrigger stopped;

	explicit CDCBufferedStream(CDCStreamId streamId) : streamId(streamId) {}
};

struct CDCProxyData {
	UID id;
	Database cx;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<AsyncVar<Reference<LogSystemConsumer>>> logSystem;
	std::map<CDCStreamId, Reference<CDCBufferedStream>> streams;

	CDCProxyData(CDCProxyInterface const& proxy, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
	  : id(proxy.id()), cx(openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True)), dbInfo(dbInfo),
	    logSystem(makeReference<AsyncVar<Reference<LogSystemConsumer>>>()) {}
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
				throw wrong_shard_server();
			}

			std::vector<std::pair<Version, Tag>> tagAssignments;
			KeyRange tagHistoryRange = cdcTagHistoryRangeFor(streamId);
			Key begin = tagHistoryRange.begin;
			while (begin < tagHistoryRange.end) {
				RangeResult history =
				    co_await tr.getRange(KeyRangeRef(begin, tagHistoryRange.end), CLIENT_KNOBS->TOO_MANY);
				for (KeyValueRef const& kv : history) {
					const auto [historyStreamId, version, tag] = decodeCDCTagHistoryKey(kv.key);
					ASSERT_WE_THINK(historyStreamId == streamId);
					ASSERT_WE_THINK(tag.locality == tagLocalityCDC);
					tagAssignments.emplace_back(version, tag);
				}
				if (!history.more) {
					break;
				}
				begin = keyAfter(history.back().key);
			}
			if (tagAssignments.empty()) {
				throw client_invalid_operation();
			}

			result.currentTag = tagAssignments.back().second;
			for (int i = tagAssignments.size() - 1; i > 0; --i) {
				result.tagHistory.emplace_back(tagAssignments[i].first, tagAssignments[i - 1].second);
			}
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

void bufferMessages(Reference<CDCBufferedStream> stream,
                    CDCStreamReadState const& metadata,
                    Reference<IReplayPeekCursor> cursor) {
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
			Optional<MutationRef> clipped = clipCDCMutation(mutation, metadata.keys.get());
			if (clipped.present()) {
				if (stream->mutations.empty() || stream->mutations.back().version != messageVersion) {
					stream->mutations.emplace_back();
					stream->mutations.back().version = messageVersion;
				}
				stream->mutations.back().mutations.push_back_deep(stream->mutations.back().arena(), clipped.get());
			}
		}
		stream->bufferedThrough = std::max(stream->bufferedThrough, messageVersion);
		cursor->nextMessage();
	}
}

Future<Void> bufferStream(CDCProxyData* self, Reference<CDCBufferedStream> stream) {
	try {
		CDCStreamReadState metadata = co_await readCDCStreamState(self->cx, stream->streamId, self->id, true);
		stream->minVersion = metadata.minVersion;
		stream->bufferedThrough = metadata.minVersion - 1;
		stream->initialized = true;
		stream->changed.trigger();

		while (stream->active) {
			if (!self->logSystem->get()) {
				co_await self->logSystem->onChange();
				continue;
			}

			metadata = co_await readCDCStreamState(self->cx, stream->streamId, self->id, true);
			const Version begin = stream->bufferedThrough + 1;
			Reference<IReplayPeekCursor> cursor =
			    self->logSystem->get()->peekSingle(self->id, begin, metadata.currentTag, metadata.tagHistory);
			while (stream->active) {
				auto result = co_await race(cursor->getMore(TaskPriority::TLogPeekReply),
				                            self->logSystem->onChange(),
				                            stream->stopped.onTrigger(),
				                            stream->refresh.onTrigger());
				if (result.index() == 1) {
					break;
				}
				if (result.index() == 2) {
					co_return;
				}
				if (result.index() == 3) {
					break;
				}

				cursor->setProtocolVersion(g_network->protocolVersion());
				if (cursor->popped() > begin) {
					throw transaction_too_old();
				}

				const Version previousBufferedThrough = stream->bufferedThrough;
				bufferMessages(stream, metadata, cursor);
				if (stream->bufferedThrough > previousBufferedThrough) {
					stream->changed.trigger();
				}
				if (cursor->isExhausted()) {
					Optional<Version> nextTagBoundary;
					for (const auto& historyEntry : metadata.tagHistory) {
						const Version boundary = historyEntry.first;
						if (boundary > begin && (!nextTagBoundary.present() || boundary < nextTagBoundary.get())) {
							nextTagBoundary = boundary;
						}
					}
					if (nextTagBoundary.present()) {
						const Version previousBufferedThrough = stream->bufferedThrough;
						stream->bufferedThrough = std::max(stream->bufferedThrough, nextTagBoundary.get() - 1);
						if (stream->bufferedThrough > previousBufferedThrough) {
							stream->changed.trigger();
						}
					} else {
						co_await delay(0.1);
					}
					break;
				}
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_client_invalid_operation || e.code() == error_code_wrong_shard_server) {
			stream->active = false;
			stream->changed.trigger();
			co_return;
		}
		throw;
	}
}

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
					const auto [streamId, version, tag] = decodeCDCTagHistoryKey(kv.key);
					auto minimum = minVersions.find(streamId);
					if (minimum == minVersions.end()) {
						continue;
					}
					auto safePop = safePopVersions.find(tag);
					if (safePop == safePopVersions.end()) {
						safePopVersions[tag] = minimum->second;
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

Future<Void> popAcknowledgedData(CDCProxyData* self) {
	const std::map<Tag, Version> safePopVersions = co_await readSafePopVersions(self->cx);
	for (const auto& [tag, version] : safePopVersions) {
		self->logSystem->get()->pop(version, tag);
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
				actors->add(bufferStream(self, stream));
			}
		}
	}

	for (auto it = self->streams.begin(); it != self->streams.end();) {
		if (!assignedStreams.contains(it->first)) {
			it->second->active = false;
			it->second->stopped.trigger();
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
		while (!stream->initialized) {
			co_await stream->changed.onTrigger();
		}

		Version begin = request.cursor.lastConsumedVersion == invalidVersion ? stream->minVersion
		                                                                     : request.cursor.lastConsumedVersion + 1;
		if (begin < stream->minVersion) {
			throw transaction_too_old();
		}

		if (stream->bufferedThrough < begin) {
			stream->refresh.trigger();
		}
		while (stream->active && stream->bufferedThrough < begin) {
			co_await stream->changed.onTrigger();
		}
		if (!stream->active) {
			throw wrong_shard_server();
		}

		CDCConsumeReply reply;
		reply.lastConsumedVersion = request.cursor.lastConsumedVersion;
		for (const auto& versioned : stream->mutations) {
			if (versioned.version >= begin) {
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
	co_return;
}

Future<Void> acknowledge(CDCProxyData* self, CDCAckRequest request) {
	try {
		co_await readCDCStreamState(self->cx, request.streamId, self->id, false);
		const Version minVersion = co_await acknowledgeNativeCdcStream(self->cx, request.streamId, request.version);
		auto found = self->streams.find(request.streamId);
		if (found != self->streams.end()) {
			found->second->minVersion = std::max(found->second->minVersion, minVersion);
			while (!found->second->mutations.empty() && found->second->mutations.front().version < minVersion) {
				found->second->mutations.pop_front();
			}
		}
		co_await popAcknowledgedData(self);
		request.reply.send(Void());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
	co_return;
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
	co_return;
}

Future<Void> removeStream(CDCProxyData* self, CDCRemoveStreamRequest request) {
	try {
		co_await removeNativeCdcStream(self->cx, request.name, self->id);
		request.reply.send(Void());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		request.reply.sendError(e);
	}
	co_return;
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
	co_return;
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
					self.logSystem->set(makeLogSystemConsumerFromServerDBInfo(self.id, dbInfo->get()));
				}
				reconcileStreams(&self, &actors);
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

TEST_CASE("noSim/NativeCDC/ProxyMutationFiltering") {
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
