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

	CDCProxyInterface current = decoded;
	current.supportsOrderedStreams = true;
	const auto newBytes = ObjectWriter::toValue(current, Unversioned());
	const auto decodedLegacy = ObjectReader::fromStringRef<LegacyCDCProxyInterface>(newBytes, Unversioned());
	ASSERT_EQ(decodedLegacy.processId, legacy.processId);
	ASSERT_EQ(decodedLegacy.consume.getEndpoint().token, legacy.consume.getEndpoint().token);
	const auto decodedCurrent = ObjectReader::fromStringRef<CDCProxyInterface>(newBytes, Unversioned());
	ASSERT(decodedCurrent.supportsOrderedStreams);
	ASSERT_EQ(decodedCurrent.registerOrderedStream.getEndpoint().token,
	          legacy.consume.getEndpoint().getAdjustedEndpoint(9).token);
	ASSERT_EQ(decodedCurrent.registerOrderedStream.getEndpoint().getPrimaryAddress(), address);
	return Void();
}

TEST_CASE("/NativeCDC/OrderedRegistrationRequestFlatBufferRoundTrip") {
	for (const std::vector<Key>& splitPoints : { std::vector<Key>{}, std::vector<Key>{ Key("m"_sr), Key("t"_sr) } }) {
		CDCRegisterOrderedStreamRequest request(
		    "ordered"_sr, { KeyRangeRef("a"_sr, "p"_sr), KeyRangeRef("s"_sr, "z"_sr) }, splitPoints);
		const auto bytes = ObjectWriter::toValue(request, Unversioned());
		const auto decoded = ObjectReader::fromStringRef<CDCRegisterOrderedStreamRequest>(bytes, Unversioned());
		ASSERT_EQ(decoded.name, request.name);
		ASSERT_EQ(decoded.ranges, request.ranges);
		ASSERT_EQ(decoded.splitPoints, request.splitPoints);
	}
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
