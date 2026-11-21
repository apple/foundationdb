/*
 * WorkerInterfaceTests.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbserver/core/WorkerInterface.h"

#include "flow/ObjectSerializer.h"
#include "flow/UnitTest.h"

namespace {

struct LegacyCDCProxyInterface {
	constexpr static FileIdentifier file_identifier = CDCProxyInterface::file_identifier;
	Optional<Key> processId;
	PublicRequestStream<CDCConsumeRequest> consume;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, processId, consume);
	}
};

struct OrderedCDCProxyInterface {
	constexpr static FileIdentifier file_identifier = CDCProxyInterface::file_identifier;
	Optional<Key> processId;
	PublicRequestStream<CDCConsumeRequest> consume;
	bool supportsOrderedStreams = false;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, processId, consume, supportsOrderedStreams);
	}
};

} // namespace

TEST_CASE("/NativeCDC/InternalInterfaceFlatBufferRoundTrip") {
	const NetworkAddress workerAddress(IPAddress(0x01020304), 4500);
	const NetworkAddress cdcAddress(IPAddress(0x05060708), 4501);

	WorkerInterface worker;
	worker.tLog = RequestStream<InitializeTLogRequest>(Endpoint({ workerAddress }, UID(1, 2)));
	worker.cdcProxy = RequestStream<InitializeCDCProxyRequest>(Endpoint({ cdcAddress }, UID(3, 4)));

	const Standalone<StringRef> serializedWorker = ObjectWriter::toValue(worker, Unversioned());
	const auto decodedWorker = ObjectReader::fromStringRef<WorkerInterface>(serializedWorker, Unversioned());
	ASSERT_EQ(decodedWorker.tLog.getEndpoint().token, worker.tLog.getEndpoint().token);
	ASSERT_EQ(decodedWorker.cdcProxy.getEndpoint().token, worker.cdcProxy.getEndpoint().token);
	ASSERT_EQ(decodedWorker.cdcProxy.getEndpoint().getPrimaryAddress(), cdcAddress);

	CDCProxyInterface cdcProxy;
	cdcProxy.consume = PublicRequestStream<CDCConsumeRequest>(Endpoint({ cdcAddress }, UID(5, 6)));
	cdcProxy.supportsOrderedStreams = true;
	cdcProxy.supportsGuardedRemoval = true;

	RegisterMasterRequest request;
	request.id = UID(7, 8);
	request.cdcProxies.push_back(cdcProxy);
	request.recoveryCount = 1;
	request.registrationCount = 2;
	request.recoveryState = RecoveryState::UNINITIALIZED;
	request.recoveryStalled = false;

	const Standalone<StringRef> serializedRequest = ObjectWriter::toValue(request, Unversioned());
	const auto decodedRequest = ObjectReader::fromStringRef<RegisterMasterRequest>(serializedRequest, Unversioned());
	ASSERT_EQ(decodedRequest.id, request.id);
	ASSERT_EQ(decodedRequest.cdcProxies.size(), 1);
	ASSERT_EQ(decodedRequest.cdcProxies.front().id(), cdcProxy.id());
	ASSERT(decodedRequest.cdcProxies.front().supportsOrderedStreams);
	ASSERT_EQ(decodedRequest.cdcProxies.front().registerOrderedStream.getEndpoint().token,
	          cdcProxy.consume.getEndpoint().getAdjustedEndpoint(9).token);
	ASSERT(decodedRequest.cdcProxies.front().supportsGuardedRemoval);

	CachedSerialization<ClientDBInfo> clientInfo;
	clientInfo.mutate().cdcProxies = decodedRequest.cdcProxies;
	const auto published = ObjectReader::fromStringRef<ErrorOr<EnsureTable<ClientDBInfo>>>(
	    clientInfo.getCache(), AssumeVersion(g_network->protocolVersion()));
	const auto& proxies = published.get().asUnderlyingType().cdcProxies;
	ASSERT_EQ(proxies.size(), 1);
	ASSERT(proxies.front().supportsGuardedRemoval);
	ASSERT(proxies.front().removeStreamGuarded.getEndpoint() == cdcProxy.consume.getEndpoint().getAdjustedEndpoint(10));

	return Void();
}

TEST_CASE("/NativeCDC/OrderedInterfaceCompatibility") {
	const NetworkAddress address(IPAddress(0x05060708), 4501);
	LegacyCDCProxyInterface legacy;
	legacy.processId = Key("cdc-proxy"_sr);
	legacy.consume = PublicRequestStream<CDCConsumeRequest>(Endpoint({ address }, UID(5, 6)));
	const auto oldBytes = ObjectWriter::toValue(legacy, Unversioned());
	const auto decoded = ObjectReader::fromStringRef<CDCProxyInterface>(oldBytes, Unversioned());
	ASSERT(!decoded.supportsOrderedStreams);
	ASSERT(!decoded.supportsGuardedRemoval);
	ASSERT_EQ(decoded.processId, legacy.processId);
	ASSERT_EQ(decoded.consume.getEndpoint().token, legacy.consume.getEndpoint().token);
	const std::vector<Endpoint> endpoints{ decoded.registerStream.getEndpoint(),
		                                   decoded.removeStream.getEndpoint(),
		                                   decoded.ack.getEndpoint(),
		                                   decoded.waitFailure.getEndpoint(),
		                                   decoded.haltForTesting.getEndpoint(),
		                                   decoded.getBufferStatusForTesting.getEndpoint(),
		                                   decoded.setPopsPausedForTesting.getEndpoint(),
		                                   decoded.getStatus.getEndpoint() };
	for (int i = 0; i < endpoints.size(); ++i) {
		ASSERT_EQ(endpoints[i].token, legacy.consume.getEndpoint().getAdjustedEndpoint(i + 1).token);
		ASSERT_EQ(endpoints[i].getPrimaryAddress(), address);
	}

	const OrderedCDCProxyInterface ordered{ legacy.processId, legacy.consume, true };
	const auto orderedBytes = ObjectWriter::toValue(ordered, Unversioned());
	const auto decodedOrdered = ObjectReader::fromStringRef<CDCProxyInterface>(orderedBytes, Unversioned());
	ASSERT(decodedOrdered.supportsOrderedStreams);
	ASSERT(!decodedOrdered.supportsGuardedRemoval);
	ASSERT(decodedOrdered.registerOrderedStream.getEndpoint() == legacy.consume.getEndpoint().getAdjustedEndpoint(9));

	CDCProxyInterface current = decodedOrdered;
	current.supportsGuardedRemoval = true;
	const auto newBytes = ObjectWriter::toValue(current, Unversioned());
	const auto decodedLegacy = ObjectReader::fromStringRef<LegacyCDCProxyInterface>(newBytes, Unversioned());
	ASSERT_EQ(decodedLegacy.processId, legacy.processId);
	ASSERT_EQ(decodedLegacy.consume.getEndpoint().token, legacy.consume.getEndpoint().token);
	const auto previous = ObjectReader::fromStringRef<OrderedCDCProxyInterface>(newBytes, Unversioned());
	ASSERT(previous.supportsOrderedStreams);
	ASSERT_EQ(previous.processId, ordered.processId);
	ASSERT(previous.consume.getEndpoint() == ordered.consume.getEndpoint());
	const auto decodedCurrent = ObjectReader::fromStringRef<CDCProxyInterface>(newBytes, Unversioned());
	ASSERT(decodedCurrent.supportsOrderedStreams);
	ASSERT(decodedCurrent.supportsGuardedRemoval);
	ASSERT_EQ(decodedCurrent.registerOrderedStream.getEndpoint().token,
	          legacy.consume.getEndpoint().getAdjustedEndpoint(9).token);
	ASSERT_EQ(decodedCurrent.registerOrderedStream.getEndpoint().getPrimaryAddress(), address);
	static_assert(
	    std::is_same_v<decltype(CDCProxyInterface::removeStreamGuarded), PrivateRequestStream<CDCRemoveStreamRequest>>);
	ASSERT(decodedCurrent.removeStreamGuarded.getEndpoint() == legacy.consume.getEndpoint().getAdjustedEndpoint(10));
	return Void();
}

TEST_CASE("/NativeCDC/PositionalInterfaceCompatibility") {
	const NetworkAddress address(IPAddress(0x05060708), 4501);
	const std::vector<OrderedCDCProxyInterface> previous{
		{ Key("first"_sr), PublicRequestStream<CDCConsumeRequest>(Endpoint({ address }, UID(1, 2))), true },
		{ Key("second"_sr), PublicRequestStream<CDCConsumeRequest>(Endpoint({ address }, UID(3, 4))), false }
	};
	const auto oldBytes = BinaryWriter::toValue(previous, IncludeVersion());
	auto current = BinaryReader::fromStringRef<std::vector<CDCProxyInterface>>(oldBytes, IncludeVersion());
	ASSERT_EQ(current.size(), previous.size());
	for (size_t i = 0; i < current.size(); ++i) {
		ASSERT_EQ(current[i].processId, previous[i].processId);
		ASSERT(current[i].consume.getEndpoint() == previous[i].consume.getEndpoint());
		ASSERT_EQ(current[i].supportsOrderedStreams, previous[i].supportsOrderedStreams);
		ASSERT(!current[i].supportsGuardedRemoval);
		current[i].supportsGuardedRemoval = true;
	}
	// Positional readers have no field lengths: an extra byte would shift the next proxy.
	ASSERT(BinaryWriter::toValue(current, IncludeVersion()) == oldBytes);
	const auto first = BinaryWriter::toValue(previous.front(), IncludeVersion());
	BinaryReader reader(first, IncludeVersion());
	reader >> current.front();
	reader.assertEnd();
	ASSERT(!current.front().supportsGuardedRemoval);
	return Void();
}

TEST_CASE("/NativeCDC/TLogPeekRequestFlatBufferRoundTrip") {
	TLogPeekRequest request(100,
	                        Tag(tagLocalityCDC, 0),
	                        false,
	                        false,
	                        Optional<std::pair<UID, int>>(),
	                        Optional<Version>(200),
	                        Optional<bool>(true),
	                        4096);

	const Standalone<StringRef> serializedRequest = ObjectWriter::toValue(request, Unversioned());
	const auto decodedRequest = ObjectReader::fromStringRef<TLogPeekRequest>(serializedRequest, Unversioned());
	ASSERT_EQ(decodedRequest.begin, request.begin);
	ASSERT_EQ(decodedRequest.tag, request.tag);
	ASSERT_EQ(decodedRequest.end, request.end);
	ASSERT_EQ(decodedRequest.returnEmptyIfStopped, request.returnEmptyIfStopped);
	ASSERT_EQ(decodedRequest.replyByteLimit, request.replyByteLimit);

	return Void();
}
