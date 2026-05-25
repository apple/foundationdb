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
#include <limits>
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

struct CDCProxyData {
	UID id;
	Database cx;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	Reference<LogSystemConsumer> logSystem;

	CDCProxyData(CDCProxyInterface const& proxy, Reference<AsyncVar<ServerDBInfo> const> dbInfo)
	  : id(proxy.id()), cx(openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True)), dbInfo(dbInfo) {}
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

Future<Void> consume(CDCProxyData* self, CDCConsumeRequest request) {
	try {
		if (request.cursor.lastConsumedVersion < invalidVersion ||
		    request.cursor.lastConsumedVersion == std::numeric_limits<Version>::max()) {
			throw client_invalid_operation();
		}

		CDCStreamReadState state = co_await readCDCStreamState(self->cx, request.cursor.streamId, self->id, true);
		Version begin = request.cursor.lastConsumedVersion == invalidVersion ? state.minVersion
		                                                                     : request.cursor.lastConsumedVersion + 1;
		if (begin < state.minVersion) {
			throw transaction_too_old();
		}

		Reference<IReplayPeekCursor> cursor =
		    self->logSystem->peekSingle(self->id, begin, state.currentTag, state.tagHistory);
		while (!cursor->hasMessage()) {
			co_await cursor->getMore(TaskPriority::TLogPeekReply);
			cursor->setProtocolVersion(g_network->protocolVersion());
			if (cursor->popped() > begin) {
				throw transaction_too_old();
			}
		}

		CDCConsumeReply reply;
		reply.lastConsumedVersion = request.cursor.lastConsumedVersion;
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
				Optional<MutationRef> clipped = clipCDCMutation(mutation, state.keys.get());
				if (clipped.present()) {
					if (reply.mutations.empty() || reply.mutations.back().version != messageVersion) {
						reply.mutations.push_back(reply.arena, VersionedMutationsRef(messageVersion, {}));
					}
					reply.mutations.back().mutations.push_back_deep(reply.arena, clipped.get());
				}
			}
			reply.lastConsumedVersion = std::max(reply.lastConsumedVersion, messageVersion);
			cursor->nextMessage();
		}
		request.reply.send(reply);
	} catch (Error& e) {
		request.reply.sendError(e);
	}
	co_return;
}

Future<Void> acknowledge(CDCProxyData* self, CDCAckRequest request) {
	try {
		CDCStreamReadState state = co_await readCDCStreamState(self->cx, request.streamId, self->id, false);
		const Version minVersion = co_await acknowledgeNativeCdcStream(self->cx, request.streamId, request.version);
		std::set<Tag> tags{ state.currentTag };
		for (const auto& history : state.tagHistory) {
			tags.insert(history.second);
		}
		for (Tag tag : tags) {
			self->logSystem->pop(minVersion, tag);
		}
		request.reply.send(Void());
	} catch (Error& e) {
		request.reply.sendError(e);
	}
	co_return;
}

Future<Void> registerStream(CDCProxyData* self, CDCRegisterStreamRequest request) {
	try {
		const CDCStreamId streamId = co_await registerNativeCdcStream(self->cx, request.name, request.keys, self->id);
		request.reply.send(CDCRegisterStreamReply(streamId));
	} catch (Error& e) {
		request.reply.sendError(e);
	}
	co_return;
}

Future<Void> removeStream(CDCProxyData* self, CDCRemoveStreamRequest request) {
	try {
		co_await removeNativeCdcStream(self->cx, request.name, self->id);
		request.reply.send(Void());
	} catch (Error& e) {
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
		self.logSystem = makeLogSystemConsumerFromServerDBInfo(self.id, dbInfo->get());
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
					self.logSystem = makeLogSystemConsumerFromServerDBInfo(self.id, dbInfo->get());
				}
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
